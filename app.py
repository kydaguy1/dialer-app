#!/usr/bin/env python3
"""
Power Dialer — triple-line outbound calling with FUB logging.
Start via: bash ~/Desktop/dialer/start.sh
"""

import eventlet
eventlet.monkey_patch()

import base64
import csv
import json as _json
import os
import re
import subprocess
import sys
import tempfile
import threading
import time
import uuid
from pathlib import Path

import requests as http
from dotenv import load_dotenv
from flask import Flask, Response, jsonify, redirect, render_template, request, session
from flask_socketio import SocketIO
from twilio.twiml.voice_response import Dial, VoiceResponse

try:
    import browser_cookie3 as _bc3
    _HAS_BC3 = True
except ImportError:
    _HAS_BC3 = False

load_dotenv(Path(__file__).parent / ".env")

app = Flask(__name__)
app.secret_key = os.environ.get("SECRET_KEY", os.urandom(32))
socketio = SocketIO(app, async_mode="eventlet", cors_allowed_origins="*")

DIALER_PASSWORD = os.environ.get("DIALER_PASSWORD", "")
FUB_KEY         = os.environ["FUB_API_KEY"]
PUBLIC_URL      = os.environ.get("PUBLIC_URL", "").rstrip("/")
USE_SIGNALWIRE = os.environ.get("USE_SIGNALWIRE", "").lower() in ("1", "true", "yes")

if USE_SIGNALWIRE:
    from signalwire.rest import Client
    ACCOUNT_SID = os.environ["SIGNALWIRE_PROJECT_ID"]
    AUTH_TOKEN  = os.environ["SIGNALWIRE_API_TOKEN"]
    FROM_NUMBER       = os.environ.get("SIGNALWIRE_FROM_NUMBER", "")
    AGENT_FROM_NUMBER = os.environ.get("SIGNALWIRE_AGENT_FROM", FROM_NUMBER)
    twilio            = Client(ACCOUNT_SID, AUTH_TOKEN,
                               signalwire_space_url=os.environ["SIGNALWIRE_SPACE_URL"])
else:
    from twilio.rest import Client
    ACCOUNT_SID = os.environ["TWILIO_ACCOUNT_SID"]
    AUTH_TOKEN  = os.environ["TWILIO_AUTH_TOKEN"]
    FROM_NUMBER       = os.environ["TWILIO_FROM_NUMBER"]
    AGENT_FROM_NUMBER = FROM_NUMBER
    twilio            = Client(ACCOUNT_SID, AUTH_TOKEN)
FUB_URL = "https://api.followupboss.com/v1"

# ── Auth ───────────────────────────────────────────────────────────────────────
_PUBLIC_PATHS = ("/webhook/", "/twiml/", "/login", "/socket.io")

@app.before_request
def _check_auth():
    if not DIALER_PASSWORD:
        return
    if any(request.path.startswith(p) for p in _PUBLIC_PATHS):
        return
    if session.get("authed"):
        return
    if request.is_json or request.path.startswith("/api/"):
        return jsonify({"error": "Unauthorized"}), 401
    return redirect("/login")

@app.get("/login")
def login_page():
    return render_template("login.html")

@app.post("/login")
def login_post():
    if request.form.get("password") == DIALER_PASSWORD:
        session["authed"] = True
        return redirect("/")
    return render_template("login.html", error="Wrong password")

@app.post("/logout")
def logout():
    session.clear()
    return redirect("/login")

# ── Mac helper state ──────────────────────────────────────────────────────────
_mac_sid: str | None = None          # SocketIO session ID of connected Mac helper
_text_requests: dict = {}            # req_id → {"event": threading.Event, "result": bool}
_text_lock = threading.Lock()

@socketio.on("connect")
def _on_mac_connect(auth):
    global _mac_sid
    if DIALER_PASSWORD and (not auth or auth.get("token") != DIALER_PASSWORD):
        return False
    _mac_sid = request.sid
    _log("Mac helper connected")

@socketio.on("disconnect")
def _on_mac_disconnect():
    global _mac_sid
    if _mac_sid == request.sid:
        _mac_sid = None
        _log("Mac helper disconnected — texts will fall back to SMS")

@socketio.on("text_result")
def _on_text_result(data):
    req_id = data.get("id")
    with _text_lock:
        entry = _text_requests.get(req_id)
    if entry:
        entry["result"] = bool(data.get("result", False))
        entry["event"].set()


def _setup_inbound_webhook() -> None:
    """Point the owned SignalWire number's inbound voice URL at this app so the agent
    can call in rather than being called out."""
    if not USE_SIGNALWIRE or not PUBLIC_URL:
        return
    number = os.environ.get("SIGNALWIRE_AGENT_FROM", "")
    if not number:
        return
    try:
        matches = twilio.incoming_phone_numbers.list(phone_number=number)
        if not matches:
            print(f"[dialer] WARNING: {number} not found in SignalWire account")
            return
        matches[0].update(
            voice_url=f"{PUBLIC_URL}/twiml/inbound-agent",
            voice_method="POST",
            status_callback=f"{PUBLIC_URL}/webhook/call",
            status_callback_method="POST",
        )
        print(f"[dialer] Inbound webhook set → {PUBLIC_URL}/twiml/inbound-agent")
    except Exception as e:
        print(f"[dialer] WARNING: could not update inbound webhook: {e}")

_fub_team_base: str | None = None

def _get_fub_team_base() -> str | None:
    """Return the FUB team base URL. Checks FUB_TEAM_URL env var first, then Chrome cookies."""
    global _fub_team_base
    env_url = os.environ.get("FUB_TEAM_URL", "").rstrip("/")
    if env_url:
        _fub_team_base = env_url
        return _fub_team_base
    if _fub_team_base:
        return _fub_team_base
    if not _HAS_BC3:
        return None
    try:
        jar  = _bc3.chrome(domain_name="followupboss.com")
        skip = {"app", "api", "login", "www", ""}
        candidates = sorted({
            c.domain.lstrip(".")
            for c in jar
            if c.domain.lstrip(".").endswith("followupboss.com")
            and len(c.domain.lstrip(".").split(".")) == 3
            and c.domain.lstrip(".").split(".")[0] not in skip
        })
        if not candidates:
            return None

        # Pick first candidate alphabetically (bringashometeam < kyrolosramzy)
        _fub_team_base = f"https://{candidates[0]}"
    except Exception as e:
        _log(f"FUB team base error: {e}")
    return _fub_team_base


# ── Session state ──────────────────────────────────────────────────────────────
_lock = threading.Lock()
_s: dict = {}

def _reset():
    global _s
    _s = {
        "id":              str(uuid.uuid4())[:8],
        "state":           "idle",    # idle|ready|dialing|connected|done
        "lines":           4,
        "leads":           [],
        "idx":             0,
        "source_name":     "",        # human-readable name of the loaded list
        "conf_name":       None,
        "conf_sid":        None,
        "paused":          False,
        "send_text":       False,      # auto-text no-answer/voicemail leads via FUB
        "text_type":       "buyer",    # "buyer" or "seller"
        "script_type":     "circle",   # "circle" or "fub"
        "agent_call_sid":  None,
        "active_calls":    {},        # sid -> lead dict
        "batch_dialed_at": None,      # timestamp of last _dial_batch call — for stale-call detection
        "connected_sid":   None,
        "current_lead":    None,
        "call_start":      None,
        "log":             [],
        "stats":           {
            "called": 0, "answered": 0,
            "no_answer": 0, "busy": 0, "voicemail": 0, "failed": 0,
            "texts_sent": 0,
        },
    }

_reset()

# ── Session checkpoint (resume after pause/close) ──────────────────────────────
_CHECKPOINT_PATH = Path(tempfile.gettempdir()) / "dialer_checkpoint.json"

def _save_checkpoint():
    """Save current progress so the session can be resumed later."""
    try:
        with _lock:
            idx    = _s["idx"]
            leads  = _s["leads"]
            source = _s.get("source_name", "")
        if idx == 0 or idx >= len(leads):
            return  # nothing meaningful to save
        data = {
            "timestamp": int(time.time()),
            "source":    source,
            "called":    idx,
            "total":     len(leads),
            "leads":     leads[idx:],
        }
        _CHECKPOINT_PATH.write_text(_json.dumps(data))
    except Exception as e:
        print(f"[dialer] Checkpoint save error: {e}")

def _clear_checkpoint():
    try:
        _CHECKPOINT_PATH.unlink(missing_ok=True)
    except Exception:
        pass

@app.get("/api/checkpoint")
def api_checkpoint():
    try:
        if not _CHECKPOINT_PATH.exists():
            return jsonify({"exists": False})
        data = _json.loads(_CHECKPOINT_PATH.read_text())
        age  = int(time.time()) - data.get("timestamp", 0)
        if age > 86400:
            _clear_checkpoint()
            return jsonify({"exists": False})
        return jsonify({
            "exists":    True,
            "source":    data.get("source") or "Previous session",
            "called":    data.get("called", 0),
            "total":     data.get("total", 0),
            "remaining": len(data.get("leads", [])),
            "age_min":   age // 60,
        })
    except Exception:
        return jsonify({"exists": False})

@app.post("/api/resume")
def api_resume():
    try:
        if not _CHECKPOINT_PATH.exists():
            return jsonify({"error": "No saved session"}), 404
        data  = _json.loads(_CHECKPOINT_PATH.read_text())
        leads = data.get("leads", [])
        if not leads:
            return jsonify({"error": "Checkpoint is empty"}), 400
        source = data.get("source", "")
        with _lock:
            _reset()
            _s["leads"]       = leads
            _s["conf_name"]   = f"pd-{_s['id']}"
            _s["source_name"] = source
        _log(f"Resumed — {len(leads)} leads left (skipped {data.get('called', 0)} already dialed)")
        return jsonify({"count": len(leads), "source": source})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


def _session_watchdog():
    """Recover from stuck sessions: no calls after 45s, or calls that never webhook back after 60s."""
    while True:
        time.sleep(45)
        now = time.time()
        with _lock:
            state       = _s["state"]
            active      = len(_s["active_calls"])
            has_leads   = _s["idx"] < len(_s["leads"])
            dialed_at   = _s.get("batch_dialed_at")
            stale_sids  = []
            stale_leads = []
            if state == "dialing" and active > 0 and dialed_at and (now - dialed_at) > 60:
                stale_sids = list(_s["active_calls"].keys())
                stale_leads = [_s["active_calls"].pop(sid, None) for sid in stale_sids]

        if stale_sids:
            _log(f"Watchdog: {len(stale_sids)} call(s) stuck >60s — logging No Answer & redialing")
            for sid in stale_sids:
                threading.Thread(target=_hang, args=(sid,), daemon=True).start()
            for lead in stale_leads:
                if lead:
                    with _lock:
                        _s["stats"]["no_answer"] += 1
                    threading.Thread(target=_fub_log, args=(lead, "No Answer", 0), daemon=True).start()
            threading.Thread(target=_dial_batch, daemon=True).start()
        elif state == "dialing" and active == 0 and has_leads:
            _log("Watchdog: stuck in dialing with no active calls — restarting batch")
            threading.Thread(target=_dial_batch, daemon=True).start()

# Detect team URL in background so it's ready by first page load
threading.Thread(target=_get_fub_team_base, daemon=True).start()
threading.Thread(target=_session_watchdog, daemon=True).start()


def _log(msg: str):
    ts = time.strftime("%H:%M:%S")
    line = f"{ts}  {msg}"
    print(line)
    with _lock:
        _s["log"] = ([line] + _s["log"])[:15]


# ── FUB helpers ────────────────────────────────────────────────────────────────
def _e164(raw: str) -> str:
    d = re.sub(r"\D", "", raw or "")
    if len(d) == 11 and d.startswith("1"):
        d = d[1:]
    return f"+1{d}" if len(d) == 10 else d


def _team_session() -> http.Session:
    """requests.Session authenticated against the team subdomain with the API key."""
    s = http.Session()
    s.auth = (FUB_KEY, "")
    s.headers["Accept"] = "application/json"
    return s


def _fub_smart_lists() -> list[dict]:
    base = _get_fub_team_base()
    if not base:
        return []
    try:
        r = _team_session().get(f"{base}/api/v1/smartLists",
                                params={"limit": 200, "fub2": 1, "offset": 0}, timeout=8)
        if r.status_code == 200:
            data  = r.json()
            items = data.get("smartlists") or data.get("smartLists") or []
            return [{"type": "smartlist", "id": x["id"], "name": x["name"]}
                    for x in items if x.get("name")]
    except Exception as e:
        _log(f"Smart lists error: {e}")
    return []


def _fub_load_smart_list(smart_list_id: int | str) -> list[dict]:
    base = _get_fub_team_base()
    if not base:
        return []
    s = _team_session()
    leads, offset, limit = [], 0, 100
    try:
        while True:
            r = s.get(f"{base}/api/v1/people", params={
                "smartListId": smart_list_id,
                "limit": limit, "offset": offset, "fields": "allFields",
            }, timeout=15)
            if r.status_code != 200:
                break
            people = r.json().get("people", [])
            if not people:
                break
            for p in people:
                phones = p.get("phones", [])
                if not phones:
                    continue
                primary = next((ph for ph in phones if ph.get("isPrimary")), phones[0])
                phone   = primary.get("value", "").strip()
                if not phone:
                    continue
                name = f"{p.get('firstName','').strip()} {p.get('lastName','').strip()}".strip()
                leads.append({"phone": phone, "name": name,
                              "address": p.get("address", "").strip(),
                              "id": p.get("id")})
            offset += limit
            if len(people) < limit:
                break
    except Exception as e:
        _log(f"Smart list load error: {e}")
    return leads


def _fub_attach_url(lead: dict):
    """Look up this lead's FUB person ID and attach a profile URL to the lead dict."""
    try:
        pid = lead.get("id")
        if not pid:
            phone  = _e164(lead.get("phone", ""))
            r      = http.get(f"{FUB_URL}/people", auth=(FUB_KEY, ""), params={"phone": phone})
            people = r.json().get("people", [])
            if not people:
                return
            pid = people[0]["id"]
            lead["id"] = pid
        base = _get_fub_team_base() or "https://app.followupboss.com"
        url  = f"{base}/2/people/view/{pid}"
        with _lock:
            if _s.get("current_lead") is lead:
                lead["fub_url"] = url
    except Exception:
        pass


_CONVO_THRESHOLD = 110  # seconds — FUB classifies answered calls ≥1:50 as Conversations

def _fub_log(lead: dict, outcome: str, duration: int):
    try:
        phone = _e164(lead.get("phone", ""))
        s = http.Session()
        s.auth = (FUB_KEY, "")
        s.headers.update({"Content-Type": "application/json"})

        pid = lead.get("id")
        if not pid:
            r = s.get(f"{FUB_URL}/people", params={"phone": phone})
            people = r.json().get("people", [])
            if people:
                pid = people[0]["id"]
                lead["id"] = pid  # cache for future calls
            else:
                parts = (lead.get("name") or "Unknown").strip().split(" ", 1)
                new_person = {
                    "firstName": parts[0],
                    "lastName":  parts[1] if len(parts) > 1 else "",
                    "phones": [{"value": phone, "type": "mobile", "isPrimary": True}],
                }
                if _fub_stage:
                    new_person["stage"] = _fub_stage
                r = s.post(f"{FUB_URL}/people", json=new_person)
                pid = r.json().get("person", r.json()).get("id")
                if pid:
                    lead["id"] = pid

        # Map dialer outcomes to FUB's recognized call outcomes.
        # "Interested" / "Not Interested" are real pickups; log as Conversation
        # when duration >= 1:50, otherwise as Answered. Save the disposition as a note.
        note = ""
        if outcome in ("Interested", "Not Interested"):
            fub_outcome = "Conversation" if int(duration or 0) >= _CONVO_THRESHOLD else "Answered"
            note = outcome
        elif outcome == "Left Message":
            fub_outcome = "Left Message"
        elif outcome == "No Answer":
            fub_outcome = "No Answer"
        elif outcome == "Busy":
            fub_outcome = "Busy"
        elif outcome == "Bad Number":
            fub_outcome = "Bad Number"
        elif outcome == "Hung Up":
            fub_outcome = "Answered"
        else:
            fub_outcome = outcome

        payload = {
            "personId":   pid,
            "phone":      phone,
            "isIncoming": 0,
            "fromNumber": re.sub(r"\D", "", FROM_NUMBER)[-10:],
            "duration":   int(duration or 0),
            "outcome":    fub_outcome,
        }
        if note:
            payload["note"] = note

        call_r = s.post(f"{FUB_URL}/calls", json=payload)
        label = f"{fub_outcome} ({note})" if note else fub_outcome
        if call_r.ok:
            _log(f"FUB ✓  {lead.get('name','?')} → {label}")
        else:
            _log(f"FUB ✗  {lead.get('name','?')} → {call_r.status_code}: {call_r.text[:120]}")

    except Exception as e:
        _log(f"FUB error: {e}")

    # Auto-text runs outside the try-except so a FUB API outage doesn't block it
    with _lock:
        send_text = _s.get("send_text", False)
    if send_text and outcome in ("No Answer", "Left Message") and lead.get("id"):
        _fub_text(lead, lead["id"])


_AUTO_TEXT_SELLER = (
    "Hey {first}! Ky w/ Bringas — just tried you. "
    "Sellers in your neighborhood are getting top dollar right now. "
    "Don't leave money on the table — text me back: (951) 447-8728 🏡"
)

_AUTO_TEXT_BUYER = (
    "Hey {first}! Ky w/ Bringas Home Team — just tried you. "
    "Got a couple homes in your area that just hit the market and they're moving fast. "
    "Don't miss out — text me back: (951) 447-8728 🏠"
)

# Mutable at runtime — persists across sessions until app restarts
_text_templates: dict = {
    "buyer":  _AUTO_TEXT_BUYER,
    "seller": _AUTO_TEXT_SELLER,
}
_fub_stage: str = ""   # FUB stage to set when creating new contacts ("" = FUB default = Lead)

def _sw_sms(phone: str, body: str, name: str) -> bool:
    """Send SMS via SignalWire using the purchased number (762-1736) which has SMS capability."""
    sms_from = AGENT_FROM_NUMBER or FROM_NUMBER
    try:
        twilio.messages.create(to=phone, from_=sms_from, body=body)
        _log(f"SMS ✓  {name} → sent via SignalWire ({sms_from})")
        return True
    except Exception as e:
        _log(f"SMS ✗  {name} → SignalWire error: {e}")
        return False


def _fub_note(pid: int, phone: str, body: str, name: str) -> bool:
    """Try to send via FUB's texting API. Returns True if FUB actually sent the text.
    Falls back to logging a FUB note and returns False so the caller can try SMS."""
    auth = (FUB_KEY, "")
    from_num = os.environ.get("SIGNALWIRE_AGENT_FROM") or os.environ.get("SIGNALWIRE_FROM_NUMBER") or os.environ.get("TWILIO_FROM_NUMBER", "")

    # 1. FUB internal API
    base = _get_fub_team_base()
    if base:
        try:
            r = http.post(
                f"{base}/api/v1/textMessages",
                auth=auth,
                headers={"Content-Type": "application/json", "x-system": "fub-spa"},
                json={"personId": pid, "toNumber": phone, "fromNumber": from_num, "message": body},
                timeout=10,
            )
            if r.status_code < 300:
                _log(f"FUB text ✓  {name} → sent via FUB")
                return True
            _log(f"FUB internal text {r.status_code} — {r.text[:80]}")
        except Exception as e:
            _log(f"FUB internal text error: {e}")

    # 2. FUB public REST API
    try:
        r = http.post(f"{FUB_URL}/textMessages", auth=auth, json={
            "personId":   pid,
            "toNumber":   phone,
            "fromNumber": from_num,
            "message":    body,
            "isIncoming": 0,
        }, timeout=10)
        if r.status_code < 300:
            _log(f"FUB text ✓  {name} → sent via FUB API")
            return True
        _log(f"FUB public text {r.status_code} — {r.text[:120]}")
    except Exception as e:
        _log(f"FUB public text error: {e}")

    # FUB texting unavailable — log a note so the activity still appears in FUB
    try:
        http.post(f"{FUB_URL}/notes", auth=(FUB_KEY, ""), json={
            "personId": pid,
            "subject":  "Auto-text sent",
            "body":     f"📱 {body}",
        }, timeout=10)
        _log(f"FUB note ✓  {name} → text logged as note")
    except Exception as e:
        _log(f"FUB note error: {e}")
    return False


def _twilio_sms(phone: str, body: str, name: str) -> bool:
    """Send SMS via native Twilio REST API (separate from the SignalWire client)."""
    tw_sid  = os.environ.get("TWILIO_ACCOUNT_SID", "")
    tw_tok  = os.environ.get("TWILIO_AUTH_TOKEN", "")
    tw_from = os.environ.get("TWILIO_FROM_NUMBER", "")
    if not (tw_sid and tw_tok and tw_from):
        return False
    try:
        from twilio.rest import Client as _TwilioClient
        _TwilioClient(tw_sid, tw_tok).messages.create(to=phone, from_=tw_from, body=body)
        _log(f"SMS ✓  {name} → sent via Twilio")
        return True
    except Exception as e:
        _log(f"SMS ✗  {name} → Twilio error: {e}")
        return False


def _request_mac_text(pid: int, phone: str, body: str, name: str) -> bool:
    """Ask the connected Mac helper to do Chrome→FUB injection. Returns True on success."""
    with _text_lock:
        mac = _mac_sid
    if not mac:
        return False
    req_id = str(uuid.uuid4())[:8]
    ev = threading.Event()
    with _text_lock:
        _text_requests[req_id] = {"event": ev, "result": False}
    socketio.emit("send_text", {
        "id": req_id, "pid": pid, "phone": phone, "name": name, "body": body,
    }, room=mac)
    ev.wait(timeout=15)
    with _text_lock:
        data = _text_requests.pop(req_id, {})
    return data.get("result", False)


def _fub_text_via_chrome(lead: dict, pid: int) -> bool:
    """POST to FUB's internal /api/v1/textMessages endpoint from the live FUB
    Chrome tab so session cookies are sent automatically.  No API key or auth
    token needed — the browser session already has texting permission."""
    try:
        first     = (lead.get("name") or "there").strip().split()[0]
        with _lock:
            ttype = _s.get("text_type", "buyer")
        template  = _text_templates.get(ttype, _text_templates["buyer"])
        body      = template.format(first=first)
        phone     = _e164(lead.get("phone", ""))

        # Payloads to try in order — internal FUB API uses different field
        # names than the public REST API ('to'/'from' are rejected).
        # Each attempt is tried in turn; stop on first non-400 or on a 400
        # that isn't "Invalid fields" (which means we've reached the right shape).
        from_num = os.environ.get("SIGNALWIRE_AGENT_FROM") or os.environ.get("SIGNALWIRE_FROM_NUMBER") or os.environ.get("TWILIO_FROM_NUMBER", "")
        payloads_b64 = base64.b64encode(_json.dumps([
            {"personId": pid, "toNumber": phone, "fromNumber": from_num, "message": body},
            {"personId": pid, "toNumber": phone, "message": body},
            {"personId": pid, "message": body},
            {"personId": pid, "phoneNumber": phone, "message": body},
        ]).encode()).decode()

        # Use a pid-keyed variable so simultaneous texts don't clobber each other's result
        var = f"_fubSms_{pid}"

        js_send = (
            "(function(){"
            f"window.{var}=null;"
            "var hdrs={'Content-Type':'application/json',"
            "'X-Requested-With':'XMLHttpRequest','x-system':'fub-spa'};"
            "var payloads=JSON.parse(atob('" + payloads_b64 + "'));"
            "function tryNext(i){"
            f"if(i>=payloads.length){{window.{var}='all-tried';return;}}"
            "fetch('/api/v1/textMessages',{method:'POST',headers:hdrs,body:JSON.stringify(payloads[i])})"
            ".then(function(r){"
            "var st=r.status;"
            "r.text().then(function(t){"
            f"if(st===400&&t.indexOf('Invalid')>-1){{tryNext(i+1);return;}}"
            f"window.{var}=String(st)+':'+t.substring(0,100);"
            f"}}).catch(function(){{window.{var}=String(st);}});"
            "})"
            f".catch(function(e){{window.{var}='err:'+String(e);}});}};"
            "tryNext(0);"
            "return 'started';"
            "})()"
        )
        js_poll = f"String(window.{var}===null?'__pending__':window.{var})"

        applescript = (
            'tell application "Google Chrome"\n'
            '    set fubTab to missing value\n'
            '    repeat with w in windows\n'
            '        set tc to count of tabs of w\n'
            '        repeat with i from 1 to tc\n'
            '            try\n'
            '                set t to tab i of w\n'
            '                if URL of t contains "followupboss.com" then\n'
            '                    set fubTab to t\n'
            '                    exit repeat\n'
            '                end if\n'
            '            end try\n'
            '        end repeat\n'
            '        if fubTab is not missing value then exit repeat\n'
            '    end repeat\n'
            '    if fubTab is missing value then return "no-fub-tab"\n'
            f'    execute fubTab javascript "{js_send}"\n'
            '    repeat 10 times\n'
            '        delay 1\n'
            f'        set res to execute fubTab javascript "{js_poll}"\n'
            '        if res is missing value then set res to "mv"\n'
            '        if res is not "__pending__" then return res\n'
            '    end repeat\n'
            '    return "timeout"\n'
            'end tell'
        )
        proc = subprocess.run(
            ["osascript", "-e", applescript],
            capture_output=True, text=True, timeout=20,
        )
        out = (proc.stdout or proc.stderr or "").strip()
        _log(f"SMS    [{out[:120]}]")

        if "no-fub-tab" in out:
            _log(f"SMS \u2717  {lead.get('name','?')} \u2192 FUB not open in Chrome")
        elif out.startswith("2"):
            _log(f"SMS \u2713  {lead.get('name','?')} \u2192 text sent via FUB")
            with _lock:
                _s["stats"]["texts_sent"] = _s["stats"].get("texts_sent", 0) + 1
            return True
        elif out == "all-tried":
            _log(f"SMS \u2717  {lead.get('name','?')} \u2192 all payload shapes rejected (400)")
        elif out == "timeout":
            _log(f"SMS \u2717  {lead.get('name','?')} \u2192 no response in 10s")
        else:
            _log(f"SMS \u2717  {lead.get('name','?')} \u2192 {out[:120]}")
        return False
    except Exception as e:
        _log(f"SMS error (Chrome): {e}")
        return False

def _fub_text(lead: dict, pid: int):
    """Send auto-text. Priority: Mac helper (Chrome→FUB) → local Chrome → SignalWire SMS + FUB note."""
    try:
        first    = (lead.get("name") or "there").strip().split()[0]
        with _lock:
            ttype = _s.get("text_type", "buyer")
        template = _text_templates.get(ttype, _text_templates["buyer"])
        body     = template.format(first=first)
        phone    = _e164(lead.get("phone", ""))
        name     = lead.get("name", "?")

        # 1. Mac helper connected via WebSocket → Chrome injection on the Mac
        if _mac_sid and _request_mac_text(pid, phone, body, name):
            with _lock:
                _s["stats"]["texts_sent"] += 1
            return

        # 2. Running locally on Mac → inject directly into Chrome
        if sys.platform == "darwin" and _fub_text_via_chrome(lead, pid):
            return  # stats incremented inside _fub_text_via_chrome

        # 3. Cloud fallback: try FUB texting API first; if unavailable, send via SMS
        sent = _fub_note(pid, phone, body, name)
        if not sent:
            # FUB texting not enabled — try SignalWire then Twilio to actually deliver the SMS
            sent = bool(_sw_sms(phone, body, name) or _twilio_sms(phone, body, name))
        if sent:
            with _lock:
                _s["stats"]["texts_sent"] += 1

    except Exception as e:
        _log(f"Text error: {e}")


# ── CSV loading ────────────────────────────────────────────────────────────────
def _best_col(headers: list[str], keywords: list[str]) -> str | None:
    for h in headers:
        if any(k in h.lower() for k in keywords):
            return h
    return None


def _load_csv(path: str | Path) -> list[dict]:
    leads = []
    with open(path, newline="", encoding="utf-8-sig") as f:
        rows = list(csv.DictReader(f))
    if not rows:
        return leads
    headers = list(rows[0].keys())
    pcol = _best_col(headers, ["phone", "mobile", "number", "cell"]) or headers[0]
    ncol = _best_col(headers, ["name", "contact", "first"])
    acol = _best_col(headers, ["address", "street", "addr", "property"])
    ecol = _best_col(headers, ["equity"])
    ycol = _best_col(headers, ["years", "tenure"])
    wcol = _best_col(headers, ["why", "reason", "note"])
    for row in rows:
        phone = row.get(pcol, "").strip()
        if not phone:
            continue
        lead: dict = {
            "phone":   phone,
            "name":    row.get(ncol, "").strip() if ncol else "",
            "address": row.get(acol, "").strip() if acol else "",
        }
        for key, col in [("equity", ecol), ("years_owned", ycol), ("why_call", wcol)]:
            if col:
                v = row.get(col, "").strip()
                if v:
                    lead[key] = v
        leads.append(lead)
    return leads


# ── Dialing logic ──────────────────────────────────────────────────────────────
def _hang(sid: str):
    try:
        twilio.calls(sid).update(status="completed")
    except Exception:
        pass


def _dial_batch():
    with _lock:
        if _s["state"] not in ("ready", "dialing"):
            return
        if _s["paused"]:
            _s["state"] = "ready"
            return
        leads = _s["leads"]
        idx   = _s["idx"]
        if idx >= len(leads):
            _s["state"] = "done"
            _log("All leads dialed — session complete")
            return
        lines              = _s["lines"]
        batch              = leads[idx : idx + lines]
        _s["idx"]         += len(batch)
        _s["state"]        = "dialing"
        _s["batch_dialed_at"] = time.time()
        conf               = _s["conf_name"]

    _log(f"Dialing {len(batch)} leads (#{idx+1}–{idx+len(batch)} of {len(leads)})")
    threading.Thread(target=_save_checkpoint, daemon=True).start()

    for lead in batch:
        try:
            call = twilio.calls.create(
                to=_e164(lead["phone"]),
                from_=FROM_NUMBER,
                url=f"{PUBLIC_URL}/twiml/lead?conf={conf}",
                status_callback=f"{PUBLIC_URL}/webhook/call",
                status_callback_event=["initiated", "ringing", "answered", "completed"],
                status_callback_method="POST",
                timeout=30,                          # hang up after 30s of ringing (~5 ring cycles)
                machine_detection="DetectMessageEnd",
                machine_detection_timeout=30,
                machine_detection_speech_threshold=2400,
                machine_detection_speech_end_threshold=1200,
                machine_detection_silence_timeout=5000,
            )
            with _lock:
                _s["active_calls"][call.sid] = lead
                _s["stats"]["called"] += 1
            _log(f"Ringing  {lead['name'] or lead['phone']}")
        except Exception as e:
            _log(f"Dial error {lead['phone']}: {e}")


# ── Routes ─────────────────────────────────────────────────────────────────────
@app.get("/")
def index():
    return render_template("index.html")




@app.post("/api/load")
def api_load():
    if "file" in request.files:
        f   = request.files["file"]
        tmp = tempfile.NamedTemporaryFile(suffix=".csv", delete=False)
        tmp.close()
        try:
            f.save(tmp.name)
            leads = _load_csv(tmp.name)
        finally:
            try:
                os.unlink(tmp.name)
            except Exception:
                pass
        if not leads:
            return jsonify({"error": "No phone numbers found in CSV"}), 400
    else:
        desktop = Path.home() / "Desktop"
        csvs = sorted(desktop.glob("*.csv"), key=lambda p: p.stat().st_mtime, reverse=True)
        if not csvs:
            return jsonify({"error": "No CSV found on Desktop"}), 400
        leads = _load_csv(csvs[0])
        if not leads:
            return jsonify({"error": "CSV has no phone numbers"}), 400

    with _lock:
        _reset()
        _s["leads"]       = leads
        _s["conf_name"]   = f"pd-{_s['id']}"
        _s["source_name"] = "CSV upload"
    _clear_checkpoint()
    _log(f"Loaded {len(leads)} leads")
    return jsonify({"count": len(leads)})



@app.get("/api/fub/lists")
def api_fub_lists():
    """Return FUB ponds, stages, and smart lists for the dropdown."""
    try:
        s = http.Session()
        s.auth = (FUB_KEY, "")

        ponds_r  = s.get(f"{FUB_URL}/ponds",  params={"limit": 100})
        stages_r = s.get(f"{FUB_URL}/stages", params={"limit": 100})

        ponds  = [{"type": "pond",  "id": p["id"],  "name": p["name"]}
                  for p in ponds_r.json().get("ponds", [])]
        stages = [{"type": "stage", "id": st["id"], "name": st["name"]}
                  for st in stages_r.json().get("stages", [])]
        smart  = _fub_smart_lists()

        return jsonify({"ponds": ponds, "stages": stages, "smartlists": smart})
    except Exception as e:
        return jsonify({"error": str(e)}), 500


@app.post("/api/fub/load")
def api_fub_load():
    """Load phone numbers from a FUB pond, stage, or smart list."""
    data      = request.json or {}
    list_type = data.get("type", "stage")
    name      = data.get("name", "")
    list_id   = data.get("id")
    if not name:
        return jsonify({"error": "Nothing selected"}), 400

    try:
        if list_type == "smartlist":
            leads = _fub_load_smart_list(list_id or name)
        else:
            s = http.Session()
            s.auth = (FUB_KEY, "")
            leads, offset, limit = [], 0, 100
            param_key = "pond" if list_type == "pond" else "stage"

            while True:
                r = s.get(f"{FUB_URL}/people", params={
                    param_key: name, "limit": limit, "offset": offset,
                })
                people = r.json().get("people", [])
                if not people:
                    break
                for p in people:
                    phones = p.get("phones", [])
                    if not phones:
                        continue
                    primary = next((ph for ph in phones if ph.get("isPrimary")), phones[0])
                    phone   = primary.get("value", "").strip()
                    if not phone:
                        continue
                    name_str = f"{p.get('firstName','').strip()} {p.get('lastName','').strip()}".strip()
                    leads.append({
                        "phone":   phone,
                        "name":    name_str,
                        "address": p.get("address", "").strip(),
                        "id":      p.get("id"),
                    })
                offset += limit
                if len(people) < limit:
                    break
    except Exception as e:
        return jsonify({"error": str(e)}), 500

    if not leads:
        return jsonify({"error": "No contacts with phone numbers found"}), 400

    with _lock:
        _reset()
        _s["leads"]       = leads
        _s["conf_name"]   = f"pd-{_s['id']}"
        _s["source_name"] = f"{list_type.title()}: {name}"
    _clear_checkpoint()
    _log(f"Loaded {len(leads)} leads from {list_type}: {name}")
    return jsonify({"count": len(leads)})



@app.post("/api/start")
def api_start():
    with _lock:
        if not _s["leads"]:
            return jsonify({"error": "Load leads first"}), 400
        _s["state"] = "calling-agent"
    call_in = os.environ.get("SIGNALWIRE_AGENT_FROM", "")
    _log(f"Session ready — call {call_in} from your phone to begin")
    return jsonify({"ok": True, "call_in": call_in})


@app.post("/api/lines")
def api_lines():
    n = int((request.get_json() or {}).get("lines", 3))
    n = max(1, min(5, n))
    with _lock:
        _s["lines"] = n
    return jsonify({"lines": n})


@app.post("/api/next")
def api_next():
    """Agent submits outcome for current call, moves to next batch."""
    data     = request.get_json() or {}
    outcome  = data.get("outcome", "No Answer")
    duration = int(data.get("duration") or 0)

    with _lock:
        lead = _s.get("current_lead")
        if not lead:
            # Lead already hung up; webhook_call already handled cleanup and started the
            # next batch. Don't touch active_calls or state — that would kill new calls.
            return jsonify({"ok": True})
        sids_to_kill  = list(_s["active_calls"].keys())
        connected_sid = _s.get("connected_sid")
        _s["active_calls"]   = {}
        _s["connected_sid"]  = None
        _s["current_lead"]   = None
        _s["call_start"]     = None
        _s["state"]          = "ready"
        stat = {
            "Interested": "answered", "Not Interested": "answered",
            "Left Message": "voicemail", "Busy": "busy",
            "No Answer": "no_answer", "Bad Number": "failed",
        }.get(outcome, "answered")
        _s["stats"][stat] = _s["stats"].get(stat, 0) + 1

    for sid in sids_to_kill + ([connected_sid] if connected_sid else []):
        threading.Thread(target=_hang, args=(sid,), daemon=True).start()

    threading.Thread(target=_fub_log, args=(lead, outcome, duration), daemon=True).start()
    threading.Thread(target=_dial_batch, daemon=True).start()
    return jsonify({"ok": True})


@app.get("/api/status")
def api_status():
    with _lock:
        return jsonify({
            "state":              _s["state"],
            "paused":             _s["paused"],
            "lines":              _s["lines"],
            "send_text":          _s.get("send_text", False),
            "text_type":          _s.get("text_type", "buyer"),
            "script_type":        _s.get("script_type", "circle"),
            "lead":               _s.get("current_lead"),
            "remaining":          max(0, len(_s["leads"]) - _s["idx"]),
            "total":              len(_s["leads"]),
            "active":             len(_s["active_calls"]),
            "stats":              dict(_s["stats"]),
            "log":                _s["log"][:6],
            "mac_helper":         _mac_sid is not None,
            "fub_stage":          _fub_stage,
        })


@app.post("/api/send-text")
def api_send_text():
    enabled = bool((request.get_json() or {}).get("enabled", False))
    with _lock:
        _s["send_text"] = enabled
    return jsonify({"send_text": enabled})


@app.post("/api/text-type")
def api_text_type():
    ttype = (request.get_json() or {}).get("type", "buyer")
    if ttype not in ("buyer", "seller"):
        ttype = "buyer"
    with _lock:
        _s["text_type"] = ttype
    return jsonify({"text_type": ttype})


@app.post("/api/fub-stage")
def api_fub_stage():
    global _fub_stage
    _fub_stage = (request.get_json() or {}).get("stage", "")
    return jsonify({"fub_stage": _fub_stage})


@app.post("/api/script-type")
def api_script_type():
    stype = (request.get_json() or {}).get("type", "circle")
    if stype not in ("circle", "fub", "expired"):
        stype = "circle"
    with _lock:
        _s["script_type"] = stype
    return jsonify({"script_type": stype})


@app.route("/api/text-templates", methods=["GET", "POST"])
def api_text_templates():
    global _text_templates
    if request.method == "POST":
        data = request.get_json() or {}
        for key in ("buyer", "seller"):
            val = (data.get(key) or "").strip()
            if val:
                _text_templates[key] = val
        return jsonify({"ok": True, "templates": _text_templates})
    return jsonify({"templates": _text_templates})


@app.post("/api/pause")
def api_pause():
    with _lock:
        _s["paused"] = not _s["paused"]
        paused = _s["paused"]
        state  = _s["state"]
    if not paused and state == "ready":
        threading.Thread(target=_dial_batch, daemon=True).start()
    return jsonify({"paused": paused})


@app.post("/api/end")
def api_end():
    with _lock:
        sids          = list(_s["active_calls"].keys())
        agent_sid     = _s.get("agent_call_sid")
        connected_sid = _s.get("connected_sid")
    for sid in sids + ([connected_sid] if connected_sid else []) + ([agent_sid] if agent_sid else []):
        threading.Thread(target=_hang, args=(sid,), daemon=True).start()
    _save_checkpoint()
    with _lock:
        _reset()
    return jsonify({"ok": True})


# ── TwiML ──────────────────────────────────────────────────────────────────────
@app.route("/twiml/silence", methods=["GET", "POST"])
def twiml_silence():
    """Plays silence — used as wait_url so Twilio doesn't play default hold music."""
    r = VoiceResponse()
    r.pause(length=300)
    return Response(str(r), mimetype="text/xml")


@app.route("/twiml/agent-phone", methods=["GET", "POST"])
def twiml_agent_phone():
    """Legacy outbound agent call (kept as fallback)."""
    conf = request.values.get("conf", "")
    r = VoiceResponse()
    d = Dial()
    d.conference(
        conf,
        start_conference_on_enter=False,
        end_conference_on_exit=True,
        beep=False,
        wait_url=f"{PUBLIC_URL}/twiml/silence",
        status_callback=f"{PUBLIC_URL}/webhook/conference",
        status_callback_event="join leave end",
        status_callback_method="POST",
        muted=False,
    )
    r.append(d)
    return Response(str(r), mimetype="text/xml")


@app.route("/twiml/inbound-agent", methods=["GET", "POST"])
def twiml_inbound_agent():
    """Agent calls the dialer number to join the conference session."""
    sid = request.values.get("CallSid", "")
    r   = VoiceResponse()

    with _lock:
        conf  = _s.get("conf_name")
        state = _s["state"]

    if not conf or state == "idle":
        r.say("No active session. Open the dialer first, then call back.",
              voice="Polly.Matthew", language="en-US")
        r.hangup()
        return Response(str(r), mimetype="text/xml")

    with _lock:
        _s["agent_call_sid"] = sid
        _s["state"]          = "ready"

    _log("Agent joined via call-in — starting dialing session…")
    threading.Thread(target=_dial_batch, daemon=True).start()

    d = Dial()
    d.conference(
        conf,
        start_conference_on_enter=False,
        end_conference_on_exit=True,
        beep=False,
        wait_url=f"{PUBLIC_URL}/twiml/silence",
        status_callback=f"{PUBLIC_URL}/webhook/conference",
        status_callback_event="join leave end",
        status_callback_method="POST",
        muted=False,
    )
    r.append(d)
    return Response(str(r), mimetype="text/xml")


@app.route("/twiml/lead", methods=["GET", "POST"])
def twiml_lead():
    """Called by SignalWire after AMD completes (DetectMessageEnd mode).
    For machines: greeting already finished + beeped — drop voicemail now.
    For humans: connect to conference immediately."""
    conf        = request.values.get("conf") or _s.get("conf_name", "")
    sid         = request.values.get("CallSid", "")
    answered_by = request.values.get("AnsweredBy", "")

    with _lock:
        lead = _s["active_calls"].get(sid)

    name_label = lead.get("name") or lead.get("phone") or sid if lead else sid
    _log(f"AMD result: '{answered_by}' — {name_label}")

    r = VoiceResponse()

    # ── machine or fax → voicemail drop ─────────────────────────────────────────
    if answered_by.startswith("machine") or answered_by == "fax":
        with _lock:
            _s["active_calls"].pop(sid, None)
            _s["stats"]["voicemail"] += 1
            remaining = len(_s["active_calls"])
            connected = _s["connected_sid"]

        _log(f"Voicemail ({answered_by}) — {lead.get('name') or sid if lead else sid}")

        if lead:
            threading.Thread(target=_fub_log, args=(lead, "Left Message", 0), daemon=True).start()
        if remaining == 0 and not connected:
            threading.Thread(target=_dial_batch, daemon=True).start()

        if answered_by != "fax":
            r.pause(length=1)
            r.say(
                "Hi, this is Ky Ramzy with the Bringas Home Team — just tried to reach you. "
                "Feel free to call or text me back at "
                "9 5 1, 4 4 7, 8 7 2 8. That's 9 5 1, 4 4 7, 8 7 2 8. Have a great day!",
                voice="Polly.Matthew",
                language="en-US",
            )
        r.hangup()
        return Response(str(r), mimetype="text/xml")

    # ── human or unknown → connect to conference ─────────────────────────────────
    with _lock:
        already_taken = _s["connected_sid"] is not None
        if not already_taken:
            _s["connected_sid"] = sid
            _s["current_lead"]  = lead
            _s["call_start"]    = time.time()
            _s["state"]         = "connected"
            _s["active_calls"].pop(sid, None)
            other_sids  = list(_s["active_calls"].keys())
            other_leads = list(_s["active_calls"].values())
            for s in other_sids:
                _s["active_calls"].pop(s, None)
        else:
            other_sids, other_leads = [], []

    if already_taken:
        _log(f"Late human ({lead.get('name') or sid if lead else sid}) — already connected, dropping")
        with _lock:
            _s["active_calls"].pop(sid, None)
        if lead:
            threading.Thread(target=_fub_log, args=(lead, "No Answer", 0), daemon=True).start()
        r.hangup()
        return Response(str(r), mimetype="text/xml")

    _log(f"Human → {lead.get('name') or sid if lead else sid}")
    for s in other_sids:
        threading.Thread(target=_hang, args=(s,), daemon=True).start()
    for ol in other_leads:
        if ol:
            threading.Thread(target=_fub_log, args=(ol, "No Answer", 0), daemon=True).start()
    if lead:
        threading.Thread(target=_fub_attach_url, args=(lead,), daemon=True).start()

    d = Dial()
    d.conference(
        conf,
        start_conference_on_enter=True,
        end_conference_on_exit=False,
        beep=True,
    )
    r.append(d)
    return Response(str(r), mimetype="text/xml")



@app.route("/twiml/voicemail-drop", methods=["GET", "POST"])
def twiml_voicemail_drop():
    pause = max(0, min(5, int(request.values.get("pause", 1))))
    r = VoiceResponse()
    if pause:
        r.pause(length=pause)
    r.say(
        "Hi, this is Ky Ramzy with the Bringas Home Team — just tried to reach you. "
        "Feel free to call or text me back at "
        "9 5 1, 4 4 7, 8 7 2 8. That's 9 5 1, 4 4 7, 8 7 2 8. Have a great day!",
        voice="Polly.Matthew",
        language="en-US",
    )
    r.hangup()
    return Response(str(r), mimetype="text/xml")


# ── Webhooks ───────────────────────────────────────────────────────────────────
@app.post("/webhook/call")
def webhook_call():
    sid    = request.form.get("CallSid", "")
    status = request.form.get("CallStatus", "")

    with _lock:
        is_agent = sid == _s.get("agent_call_sid")
        # Also check connected_sid — lead may have been removed from active_calls by AMD
        # but we still need to handle their "completed" event when they hang up.
        is_lead  = sid in _s["active_calls"] or sid == _s.get("connected_sid")
        lead     = _s["active_calls"].get(sid) or (
            _s.get("current_lead") if sid == _s.get("connected_sid") else None
        )

    if is_agent:
        if status == "in-progress":
            with _lock:
                # twiml_inbound_agent already set state to "ready" and started _dial_batch.
                # Only start here if state is still "calling-agent" (TwiML handler hasn't run yet).
                should_dial = _s["state"] == "calling-agent"
                if should_dial:
                    _s["state"] = "ready"
            if should_dial:
                _log("Your phone answered — dialing leads…")
                threading.Thread(target=_dial_batch, daemon=True).start()
        elif status in ("completed", "failed", "busy", "no-answer"):
            _log("Your phone call ended — session stopped")
            with _lock:
                sids      = list(_s["active_calls"].keys())
                c_sid     = _s.get("connected_sid")
                c_lead    = _s.get("current_lead")
                elapsed   = int(time.time() - (_s["call_start"] or time.time())) if c_sid else 0
            for s in sids:
                threading.Thread(target=_hang, args=(s,), daemon=True).start()
            if c_sid:
                threading.Thread(target=_hang, args=(c_sid,), daemon=True).start()
            if c_lead:
                _log(f"Agent hung up — logging call ({elapsed}s) for {c_lead.get('name') or c_lead.get('phone')}")
                threading.Thread(target=_fub_log, args=(c_lead, "Interested", elapsed), daemon=True).start()
            with _lock:
                _reset()
        return "", 204

    if not is_lead:
        return "", 204

    if status in ("answered", "in-progress"):
        return "", 204  # AMD webhook handles all connection decisions

    elif status in ("no-answer", "busy", "failed", "canceled"):
        outcome_map = {
            "no-answer": "No Answer", "busy": "Busy",
            "failed": "Bad Number", "canceled": None,
        }
        outcome = outcome_map.get(status)
        stat_map = {"no-answer": "no_answer", "busy": "busy", "failed": "failed"}

        with _lock:
            _s["active_calls"].pop(sid, None)
            remaining = len(_s["active_calls"])
            connected = _s["connected_sid"]
            if status in stat_map:
                _s["stats"][stat_map[status]] += 1

        if outcome:
            _log(f"{status.upper()}: {lead.get('name') or lead['phone']}")
            threading.Thread(target=_fub_log, args=(lead, outcome, 0), daemon=True).start()

        # All outbound calls done with nobody connected → dial next batch
        if remaining == 0 and not connected:
            threading.Thread(target=_dial_batch, daemon=True).start()

    elif status == "completed":
        with _lock:
            _s["active_calls"].pop(sid, None)
            was_connected = _s["connected_sid"] == sid
            elapsed   = int(time.time() - (_s["call_start"] or time.time())) if was_connected else 0
            remaining = len(_s["active_calls"])
            connected = _s["connected_sid"]
            if was_connected:
                _s["connected_sid"] = None
                _s["current_lead"]  = None
                _s["call_start"]    = None
                _s["state"]         = "ready"
                _s["stats"]["answered"] += 1

        if was_connected:
            _log(f"Lead hung up — auto-logging Answered  ({elapsed}s)")
            threading.Thread(target=_fub_log, args=(lead, "Interested", elapsed), daemon=True).start()
            threading.Thread(target=_dial_batch, daemon=True).start()
        elif remaining == 0 and not connected:
            # Last non-connected call finished (vm-drop, cancellation, etc.) — advance
            threading.Thread(target=_dial_batch, daemon=True).start()

    return "", 204



@app.post("/webhook/conference")
def webhook_conference():
    event    = request.form.get("StatusCallbackEvent", "")
    conf_sid = request.form.get("ConferenceSid", "")
    call_sid = request.form.get("CallSid", "")

    if event == "conference-start":
        with _lock:
            _s["conf_sid"] = conf_sid

    elif event == "participant-join":
        with _lock:
            is_agent = call_sid == _s.get("agent_call_sid")
        if not is_agent:
            with _lock:
                current_connected = _s.get("connected_sid")
                if current_connected == call_sid:
                    # This is exactly the lead AMD already confirmed — they joined correctly.
                    # Just remove from active_calls (AMD may not have popped it yet).
                    _s["active_calls"].pop(call_sid, None)
                    action = "ok"
                    lead = _s.get("current_lead")
                    other_sids, other_leads = [], []
                elif current_connected is None:
                    # AMD fallback: no one connected yet — claim this lead
                    lead = _s["active_calls"].get(call_sid) or _s.get("current_lead")
                    _s["connected_sid"] = call_sid
                    _s["current_lead"]  = lead
                    _s["call_start"]    = time.time()
                    _s["state"]         = "connected"
                    _s["active_calls"].pop(call_sid, None)
                    other_sids  = list(_s["active_calls"].keys())
                    other_leads = list(_s["active_calls"].values())
                    for s in other_sids:
                        _s["active_calls"].pop(s, None)
                    action = "connect"
                else:
                    # A different lead joined while already connected — drop them
                    action = "hangup"
                    lead, other_sids, other_leads = None, [], []

            if action == "connect":
                _log(f"Connected (AMD fallback): {lead.get('name') if lead else call_sid}")
                for s in other_sids:
                    threading.Thread(target=_hang, args=(s,), daemon=True).start()
                for ol in other_leads:
                    if ol:
                        threading.Thread(target=_fub_log, args=(ol, "No Answer", 0), daemon=True).start()
                if lead:
                    threading.Thread(target=_fub_attach_url, args=(lead,), daemon=True).start()
            elif action == "hangup":
                _log(f"Extra caller in conference — dropping {call_sid}")
                threading.Thread(target=_hang, args=(call_sid,), daemon=True).start()

    elif event == "participant-leave":
        with _lock:
            is_agent = call_sid == _s.get("agent_call_sid")
        if is_agent:
            _log("Agent disconnected — ending session")
            with _lock:
                sids    = list(_s["active_calls"].keys())
                c_sid   = _s.get("connected_sid")
                c_lead  = _s.get("current_lead")
                elapsed = int(time.time() - (_s["call_start"] or time.time())) if c_sid else 0
            for s in sids:
                threading.Thread(target=_hang, args=(s,), daemon=True).start()
            if c_sid:
                threading.Thread(target=_hang, args=(c_sid,), daemon=True).start()
            if c_lead:
                _log(f"Agent disconnected — logging call ({elapsed}s) for {c_lead.get('name') or c_lead.get('phone')}")
                threading.Thread(target=_fub_log, args=(c_lead, "Interested", elapsed), daemon=True).start()
            with _lock:
                _reset()

    return "", 204


@app.post("/api/callback")
def api_callback():
    from datetime import datetime, timedelta, timezone
    data      = request.get_json() or {}
    phone     = data.get("phone", "")
    days      = max(0, min(30, int(data.get("days", 1))))
    note_text = data.get("note") or "Callback requested"
    if not phone:
        return jsonify({"error": "No phone"}), 400
    try:
        e164    = _e164(phone)
        r       = http.get(f"{FUB_URL}/people", auth=(FUB_KEY, ""), params={"phone": e164})
        people  = r.json().get("people", [])
        if not people:
            return jsonify({"error": "Lead not found in FUB"}), 404
        pid   = people[0]["id"]
        fname = people[0].get("firstName", "")
        due   = (datetime.now(timezone.utc) + timedelta(days=days)).strftime("%Y-%m-%d")
        task_r = http.post(f"{FUB_URL}/tasks", auth=(FUB_KEY, ""), json={
            "personId": pid, "dueDate": due,
            "description": note_text, "type": "to-do", "isCompleted": False,
        })
        if task_r.ok:
            _log(f"Task ✓  {fname} → callback {due}")
            return jsonify({"ok": True, "via": "task"})
        note_r = http.post(f"{FUB_URL}/notes", auth=(FUB_KEY, ""), json={
            "personId": pid, "subject": "Callback Scheduled",
            "body": f"{note_text} ({due})",
        })
        if note_r.ok:
            _log(f"Note ✓  {fname} → {note_text} ({due})")
            return jsonify({"ok": True, "via": "note"})
        _log(f"Callback ✗  {fname} → {note_r.status_code}: {note_r.text[:80]}")
        return jsonify({"error": f"FUB {note_r.status_code}"}), 500
    except Exception as e:
        return jsonify({"error": str(e)}), 500


# ── Leaderboard ────────────────────────────────────────────────────────────────
_lb_cache: dict = {}  # {"data": {...}, "ts": float}

@app.get("/api/leaderboard")
def api_leaderboard():
    from datetime import datetime, timezone
    global _lb_cache
    if _lb_cache and time.time() - _lb_cache.get("ts", 0) < 60:
        return jsonify(_lb_cache["data"])
    try:
        users_r = http.get(f"{FUB_URL}/users", auth=(FUB_KEY, ""), timeout=8)
        if not users_r.ok:
            return jsonify({"error": f"FUB users {users_r.status_code}"}), 502
        users = {str(u["id"]): u for u in users_r.json().get("users", [])}

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        def _fetch_all(path, key):
            items, offset = [], 0
            while offset < 2000:  # max 20 pages — guard against runaway pagination
                r = http.get(f"{FUB_URL}/{path}", auth=(FUB_KEY, ""),
                             params={"limit": 100, "offset": offset, "since": today}, timeout=10)
                if not r.ok:
                    break
                batch = r.json().get(key, [])
                items.extend(batch)
                if len(batch) < 100:
                    break
                offset += 100
            return items

        calls = _fetch_all("calls", "calls")
        texts = _fetch_all("textMessages", "textMessages")

        stats = {uid: {"calls": 0, "convos": 0, "talk_s": 0, "texts": 0} for uid in users}
        for call in calls:
            uid = str(call.get("userId") or "")
            if uid not in stats:
                continue
            stats[uid]["calls"] += 1
            dur = int(call.get("duration") or 0)
            stats[uid]["talk_s"] += dur
            if dur >= _CONVO_THRESHOLD:
                stats[uid]["convos"] += 1
        for txt in texts:
            uid = str(txt.get("userId") or txt.get("sentById") or "")
            if uid in stats:
                stats[uid]["texts"] += 1

        rows = sorted([
            {
                "id":     uid,
                "name":   users[uid].get("name", "?"),
                "calls":  stats[uid]["calls"],
                "convos": stats[uid]["convos"],
                "talk_s": stats[uid]["talk_s"],
                "texts":  stats[uid]["texts"],
            }
            for uid in users
        ], key=lambda x: (x["calls"], x["convos"]), reverse=True)

        data = {"ok": True, "rows": rows, "date": today, "updated": int(time.time())}
        _lb_cache = {"data": data, "ts": time.time()}
        return jsonify(data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500


if __name__ == "__main__":
    PORT = int(os.environ.get("PORT", 5001))
    print(f"Power Dialer  →  http://localhost:{PORT}")
    print(f"Public URL    →  {PUBLIC_URL or '(not set)'}")
    _setup_inbound_webhook()
    socketio.run(app, host="0.0.0.0", port=PORT, debug=False)
