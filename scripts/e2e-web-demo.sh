#!/bin/bash
# E2E Web demo: starts relay + Vite dev server, opens Chrome.
# Usage: ./scripts/e2e-web-demo.sh
set -e
cd "$(dirname "$0")/.."

CERT_DIR=certs
CERT="$CERT_DIR/localhost+2.pem"
KEY="$CERT_DIR/localhost+2-key.pem"

# Generate certs if missing
if [ ! -f "$CERT" ] || [ ! -f "$KEY" ]; then
  echo "Generating certificates..."
  mkdir -p "$CERT_DIR"
  (cd "$CERT_DIR" && mkcert localhost 127.0.0.1 ::1)
fi

# Clean up from previous runs
lsof -i :4433 -t 2>/dev/null | xargs kill 2>/dev/null || true
pkill -f "vite.*5173" 2>/dev/null || true
sleep 1

cleanup() {
  kill $RELAY_PID $VITE_PID 2>/dev/null || true
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "=== Starting Relay ==="
RUST_LOG=info cargo run --bin relay -- --cert "$CERT" --key "$KEY" &
RELAY_PID=$!
sleep 2

echo "=== Starting Vite ==="
(cd apps-web && npx vite --port 5173) &
VITE_PID=$!
sleep 2

echo ""
echo "=== Ready ==="
echo "Relay PID: $RELAY_PID (port 4433)"
echo "Vite PID:  $VITE_PID (port 5173)"
echo ""
echo "Open: http://localhost:5173/"
echo ""
echo "Press Ctrl+C to stop all"

open "http://localhost:5173/"

wait
