#!/bin/bash
# E2E MSF test: ffmpeg testsrc (VP8/IVF) → msf-publisher → relay → msf-subscriber → ffplay
# Tests catalog-based pub/sub with MSF streaming format.
set -e

cd "$(dirname "$0")/.."

echo "=== Building... ==="
cargo build 2>&1 | tail -1

# Clean up from previous runs
pkill -f "target/debug/relay" 2>/dev/null || true
pkill -f "target/debug/msf-publisher" 2>/dev/null || true
pkill -f "target/debug/msf-subscriber" 2>/dev/null || true
sleep 0.5

echo "=== Starting Relay ==="
RUST_LOG=info ./target/debug/relay 2>&1 &
RELAY_PID=$!
sleep 1

if ! kill -0 $RELAY_PID 2>/dev/null; then
    echo "ERROR: Relay failed to start"
    exit 1
fi

echo "=== Starting Publisher (ffmpeg VP8 testsrc 10s) ==="
ffmpeg -re -f lavfi -i testsrc=duration=10:size=320x240:rate=30 \
    -c:v libvpx -g 30 -f ivf pipe:1 2>/dev/null \
    | RUST_LOG=info ./target/debug/msf-publisher 127.0.0.1:4433  &
PUB_PID=$!
sleep 2

if ! kill -0 $PUB_PID 2>/dev/null; then
    echo "ERROR: Publisher exited early"
    kill $RELAY_PID 2>/dev/null || true
    exit 1
fi

echo "=== Starting Subscriber (piping to ffplay) ==="
RUST_LOG=info ./target/debug/msf-subscriber 127.0.0.1:4433  \
    | ffplay -f ivf -autoexit - 2>/dev/null

echo "=== Done ==="
kill $RELAY_PID $PUB_PID 2>/dev/null || true
wait 2>/dev/null
