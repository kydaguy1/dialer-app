#!/usr/bin/env python3
"""
setup.py — One-time setup for Power Dialer.
Run once: python3 ~/Desktop/dialer/setup.py

Creates a Twilio API Key and TwiML App, then saves
everything to ~/Desktop/dialer/.env
"""

import re
import sys
from pathlib import Path

try:
    from twilio.rest import Client
except ImportError:
    sys.exit("Run first:  pip3 install --break-system-packages twilio flask python-dotenv requests")

print("=" * 55)
print("  Power Dialer — One-Time Setup")
print("=" * 55)
print()
print("Get your credentials from https://console.twilio.com")
print()

account_sid = input("Twilio Account SID  (starts with AC): ").strip()
auth_token  = input("Twilio Auth Token:                     ").strip()
fub_key     = input("FUB API Key:                           ").strip()

client = Client(account_sid, auth_token)

# ── API Key ────────────────────────────────────────────────────────────────────
print("\nCreating API Key...")
key = client.new_keys.create(friendly_name="power-dialer")
api_key_sid    = key.sid
api_key_secret = key.secret
print(f"  {api_key_sid}")

# ── Phone number ───────────────────────────────────────────────────────────────
print("\nYour existing Twilio number: +19514331008  (951) 433-1008")
from_number = "+19514331008"

# ── TwiML App ─────────────────────────────────────────────────────────────────
print("\nCreating TwiML App...")
twiml_app = client.applications.create(
    friendly_name="power-dialer",
    voice_url="https://placeholder.trycloudflare.com/twiml/agent",
    voice_method="POST",
)
print(f"  {twiml_app.sid}")

# ── Save .env ──────────────────────────────────────────────────────────────────
env_path = Path(__file__).parent / ".env"
env_path.write_text(
    f"TWILIO_ACCOUNT_SID={account_sid}\n"
    f"TWILIO_AUTH_TOKEN={auth_token}\n"
    f"TWILIO_API_KEY_SID={api_key_sid}\n"
    f"TWILIO_API_SECRET={api_key_secret}\n"
    f"TWILIO_TWIML_APP_SID={twiml_app.sid}\n"
    f"TWILIO_FROM_NUMBER={from_number}\n"
    f"FUB_API_KEY={fub_key}\n"
    f"PUBLIC_URL=\n"
)

print(f"\n{'='*55}")
print(f"  Setup complete!  Saved to {env_path}")
print(f"{'='*55}")
print()
print("Next step — install cloudflared (if you haven't):")
print("  brew install cloudflared")
print()
print("Then start the dialer any time with:")
print("  bash ~/Desktop/dialer/start.sh")
print()
print("And open Chrome to:  http://localhost:5000")
