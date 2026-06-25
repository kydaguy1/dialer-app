#!/usr/bin/env python3
"""
Mac Helper — runs on your Mac, stays connected to the cloud dialer server.
When a text needs to go out, the cloud server asks this script to inject it
into your open FUB Chrome tab so the text shows up in FUB's activity feed.

Setup:
  pip3 install python-socketio websocket-client python-dotenv
  Set DIALER_URL and DIALER_PASSWORD in your .env (same file as app.py)
  then run:  python3 mac_helper.py
"""

import base64
import json
import os
import subprocess
import sys
import time
from pathlib import Path

import socketio as sio_module
from dotenv import load_dotenv

load_dotenv(Path(__file__).parent / ".env")

SERVER_URL = os.environ.get("DIALER_URL", "").rstrip("/")
PASSWORD   = os.environ.get("DIALER_PASSWORD", "")

if not SERVER_URL:
    print("ERROR: Set DIALER_URL=https://your-app.railway.app in your .env file")
    sys.exit(1)

sio = sio_module.Client(
    reconnection=True,
    reconnection_attempts=0,   # retry forever
    reconnection_delay=5,
    reconnection_delay_max=30,
    logger=False,
    engineio_logger=False,
)


@sio.event
def connect():
    print(f"[helper] Connected to {SERVER_URL}")


@sio.event
def connect_error(data):
    print(f"[helper] Connection error: {data}")


@sio.event
def disconnect():
    print("[helper] Disconnected — will reconnect…")


@sio.on("send_text")
def handle_send_text(data):
    req_id = data.get("id")
    pid    = data.get("pid")
    phone  = data.get("phone", "")
    name   = data.get("name", "?")
    body   = data.get("body", "")
    print(f"[helper] SMS  {name} ({phone}) — injecting via Chrome…")
    result = _chrome_fub_text(pid, phone, body, name)
    sio.emit("text_result", {"id": req_id, "result": result})


def _chrome_fub_text(pid: int, phone: str, body: str, name: str) -> bool:
    """Inject a text message into the open FUB Chrome tab using AppleScript."""
    try:
        from_num = os.environ.get("SIGNALWIRE_FROM_NUMBER") or os.environ.get("TWILIO_FROM_NUMBER", "")
        payloads_b64 = base64.b64encode(json.dumps([
            {"personId": pid, "toNumber": phone, "fromNumber": from_num, "message": body},
            {"personId": pid, "toNumber": phone, "message": body},
            {"personId": pid, "message": body},
            {"personId": pid, "phoneNumber": phone, "message": body},
        ]).encode()).decode()

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

        if out.startswith("2"):
            print(f"[helper] SMS ✓  {name} → sent via FUB")
            return True
        elif "no-fub-tab" in out:
            print(f"[helper] SMS ✗  {name} → FUB not open in Chrome")
        elif out == "all-tried":
            print(f"[helper] SMS ✗  {name} → all payload shapes rejected")
        elif out == "timeout":
            print(f"[helper] SMS ✗  {name} → no response in 10s")
        else:
            print(f"[helper] SMS ✗  {name} → {out[:80]}")
        return False

    except Exception as e:
        print(f"[helper] SMS error: {e}")
        return False


if __name__ == "__main__":
    print(f"[helper] Connecting to {SERVER_URL}…")
    print(f"[helper] Keep this running while you dial. Ctrl+C to stop.\n")
    while True:
        try:
            sio.connect(SERVER_URL, auth={"token": PASSWORD}, transports=["websocket"])
            sio.wait()
        except sio_module.exceptions.ConnectionError as e:
            print(f"[helper] Can't connect: {e} — retrying in 10s")
            time.sleep(10)
        except KeyboardInterrupt:
            print("\n[helper] Stopped")
            break
        except Exception as e:
            print(f"[helper] Error: {e} — retrying in 10s")
            time.sleep(10)
