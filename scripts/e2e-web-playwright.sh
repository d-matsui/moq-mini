#!/bin/bash
# E2E Web test (MSF): ffmpeg → msf-publisher(CLI) → relay → web msf-subscriber(Playwright)
# Records browser video as WebM artifact.
#
# Usage: ./scripts/e2e-web-msf.sh
set -e
cd "$(dirname "$0")/.."

CERT_DIR=certs
CERT="$CERT_DIR/localhost+2.pem"
KEY="$CERT_DIR/localhost+2-key.pem"

# Generate certs if missing
if [ ! -f "$CERT" ] || [ ! -f "$KEY" ]; then
  echo "Generating certificates with mkcert..."
  mkdir -p "$CERT_DIR"
  (cd "$CERT_DIR" && mkcert localhost 127.0.0.1 ::1)
fi

echo "=== Building Rust binaries... ==="
cargo build 2>&1 | tail -1

# Clean up from previous runs
pkill -f "target/debug/relay" 2>/dev/null || true
pkill -f "target/debug/msf-publisher" 2>/dev/null || true
lsof -i :5173 -t 2>/dev/null | xargs kill 2>/dev/null || true
sleep 0.5

cleanup() {
  echo "=== Cleaning up ==="
  kill $RELAY_PID $PUB_PID $VITE_PID 2>/dev/null || true
  wait 2>/dev/null
}
trap cleanup EXIT INT TERM

echo "=== Starting Relay ==="
RUST_LOG=info ./target/debug/relay --cert "$CERT" --key "$KEY" 2>&1 &
RELAY_PID=$!
sleep 1

if ! kill -0 $RELAY_PID 2>/dev/null; then
  echo "ERROR: Relay failed to start"
  exit 1
fi

echo "=== Starting MSF Publisher (ffmpeg VP8 testsrc 10s) ==="
ffmpeg -re -f lavfi -i testsrc=duration=10:size=320x240:rate=30 \
  -c:v libvpx -g 30 -f ivf pipe:1 2>/dev/null \
  | RUST_LOG=info ./target/debug/msf-publisher 127.0.0.1:4433 &
PUB_PID=$!
sleep 2

if ! kill -0 $PUB_PID 2>/dev/null; then
  echo "ERROR: Publisher exited early"
  exit 1
fi

echo "=== Starting Vite dev server ==="
(cd apps-web && npx vite --port 5173) &
VITE_PID=$!
sleep 2

if ! kill -0 $VITE_PID 2>/dev/null; then
  echo "ERROR: Vite failed to start"
  exit 1
fi

echo "=== Running Playwright E2E test ==="
mkdir -p apps-web/e2e-results
(cd apps-web && npx playwright test)
TEST_EXIT=$?

echo ""
if [ $TEST_EXIT -eq 0 ]; then
  echo "=== E2E test PASSED ==="
else
  echo "=== E2E test FAILED ==="
fi

# Show artifact locations
VIDEO=$(find apps-web/e2e-results -name "video.webm" -print -quit 2>/dev/null)
SCREENSHOT=$(find apps-web/e2e-results -name "*.png" -print -quit 2>/dev/null)
[ -n "$VIDEO" ] && echo "Video:      $VIDEO"
[ -n "$SCREENSHOT" ] && echo "Screenshot: $SCREENSHOT"

exit $TEST_EXIT
