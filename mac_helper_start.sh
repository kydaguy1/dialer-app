#!/bin/bash
# Start the Mac Helper — keeps Chrome→FUB texting active while you dial from phone/cloud.
set -e
DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$DIR"

pip3 install --break-system-packages --quiet \
  python-socketio websocket-client python-dotenv 2>/dev/null

echo "=== Mac Helper ==="
echo "Connecting to cloud dialer…"
echo "(Keep this running while dialing. Ctrl+C to stop.)"
echo ""
python3 "$DIR/mac_helper.py"
