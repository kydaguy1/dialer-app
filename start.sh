#!/bin/bash
# Start the Power Dialer: cloudflare tunnel + Flask server
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

echo "=== Power Dialer ==="
echo ""

# Install Python deps quietly
pip3 install --break-system-packages --quiet flask twilio signalwire python-dotenv requests browser-cookie3 2>/dev/null

# Check cloudflared
if ! command -v cloudflared &>/dev/null; then
    echo "ERROR: cloudflared not installed."
    echo "Run:  brew install cloudflared"
    exit 1
fi

# Start cloudflared tunnel, capture log
CF_LOG=$(mktemp -t cloudflared).log
cloudflared tunnel --url http://localhost:5001 --no-autoupdate > "$CF_LOG" 2>&1 &
CF_PID=$!
trap "kill $CF_PID 2>/dev/null; rm -f $CF_LOG" EXIT

# Wait for tunnel URL (up to 30s)
echo -n "Starting tunnel"
TUNNEL_URL=""
for i in $(seq 1 30); do
    sleep 1
    TUNNEL_URL=$(grep -oE 'https://[a-z0-9-]+\.trycloudflare\.com' "$CF_LOG" 2>/dev/null | head -1 || true)
    [ -n "$TUNNEL_URL" ] && break
    echo -n "."
done
echo ""

if [ -z "$TUNNEL_URL" ]; then
    echo "ERROR: Could not get tunnel URL from cloudflared."
    cat "$CF_LOG"
    exit 1
fi

echo "Tunnel: $TUNNEL_URL"

# Start Flask
export PUBLIC_URL="$TUNNEL_URL"
echo ""
echo "Open in Chrome → http://localhost:5001"
echo "(Press Ctrl+C to stop)"
echo ""
python3 "$DIR/app.py"
