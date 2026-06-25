#!/usr/bin/env python3
"""
One-time FUB login for the Power Dialer.
Run this once, log into FUB in the browser window that opens, then close it.
The session is saved and the dialer will use it automatically.
"""
from pathlib import Path
from playwright.sync_api import sync_playwright

SESSION = Path.home() / ".fub_dialer_session.json"

print("=" * 50)
print("  Power Dialer — FUB Login")
print("=" * 50)
print()
print("A browser window will open.")
print("Log into FUB (bringashometeam.followupboss.com),")
print("wait for the dashboard to fully load, then CLOSE the window.")
print()

with sync_playwright() as pw:
    browser = pw.chromium.launch(
        headless=False,
        args=["--window-size=1280,800"],
    )
    ctx = browser.new_context(
        user_agent="Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) "
                   "AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
        viewport={"width": 1280, "height": 800},
    )
    page = ctx.new_page()
    page.goto("https://app.followupboss.com/2/people", wait_until="domcontentloaded")

    print("Waiting for you to log in and close the window...")
    try:
        page.wait_for_event("close", timeout=300_000)
    except Exception:
        pass

    ctx.storage_state(path=str(SESSION))
    browser.close()

print()
print(f"Session saved → {SESSION}")
print("Start the dialer:  bash ~/Desktop/dialer/start.sh")
