# moq-minimal

A minimal [MOQT (Media over QUIC Transport)](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/) implementation in Rust, based on **draft-ietf-moq-transport-17**.

Live video/audio streaming through a simple pipeline: **Publisher → Relay → Subscriber**.

## Architecture

```
                        ┌───────────┐
  ffmpeg ──stdin──▶ Publisher        │
                        │  (QUIC)   │
                        └─────┬─────┘
                              │
                        ┌─────▼─────┐
                        │   Relay   │  :4433
                        └─────┬─────┘
                       ╱             ╲
               ┌──────▼──┐     ┌────▼───────┐
               │Subscriber│     │  Browser   │
               │  (QUIC)  │     │(WebTransport)│
               └──────────┘     └────────────┘
```

The relay supports both **raw QUIC** (ALPN: `moqt-17`) and **WebTransport** (ALPN: `h3`), unified under a single `Session` type via [web-transport-quinn](https://crates.io/crates/web-transport-quinn).

## Project Structure

```
moqt-core/     # Shared library — wire protocol, stream I/O, session logic
moqt-relay/    # Relay server binary
moqt-pub/      # Publisher CLI (test/demo)
moqt-sub/      # Subscriber CLI (test/demo)
web/           # Browser client (TypeScript + WebTransport)
scripts/       # Development and testing scripts
docs/design/   # Architecture and design documents
```

## Quick Start

### Prerequisites

- [Rust](https://rustup.rs/) (edition 2024)
- [Node.js](https://nodejs.org/) (for the web client)
- [mkcert](https://github.com/FiloSottile/mkcert) (for local TLS certificates)
- [ffmpeg](https://ffmpeg.org/) (for video E2E testing)

### Build

```bash
cargo build
```

### Run the Dev Environment

Start the relay, Vite dev server, and open the browser client:

```bash
./scripts/dev.sh
```

This will:
1. Generate self-signed certificates with mkcert (if needed)
2. Start the relay on port 4433
3. Start the Vite dev server on port 5173
4. Open `http://localhost:5173/` in Chrome

### Run the E2E Video Test

Stream a test video pattern through the full pipeline:

```bash
./scripts/e2e-video-test.sh
```

This runs: ffmpeg (VP8 test source) → moqt-pub → relay → moqt-sub → ffplay

## Usage

### Relay

```bash
cargo run --bin moqt-relay -- --cert certs/localhost+2.pem --key certs/localhost+2-key.pem
```

### Publisher

```bash
# Demo mode — sends dummy data
cargo run --bin moqt-pub

# Pipe mode — publish VP8/IVF video from stdin
ffmpeg -f avfoundation -i "0" -c:v libvpx -f ivf - | cargo run --bin moqt-pub -- --pipe
```

### Subscriber

```bash
# Default — prints received objects to stderr
cargo run --bin moqt-sub

# Pipe mode — outputs IVF to stdout for playback
cargo run --bin moqt-sub -- --pipe | ffplay -f ivf -
```

### Web Client

```bash
cd web
npm install
npm run dev     # Start dev server at http://localhost:5173
```

## Testing

```bash
cargo test                                       # All Rust tests
cargo test -p moqt-core                          # Core library only
cargo test -p moqt-relay --test integration      # Integration tests
cd web && npm test                               # TypeScript tests
./scripts/e2e-video-test.sh                      # E2E video pipeline
```

## Design

Key design decisions:

- **1 Group = 1 Subgroup = 1 QUIC stream** — simple mapping
- **Object-level streaming** — the relay forwards objects as they arrive, no buffering
- **Subscription aggregation** — multiple subscribes to the same track are consolidated
- **Track Alias translation** — the relay maintains separate alias tables per side
- **NextGroupStart filter only** — subscribers always start from the next keyframe

See [`docs/design/`](docs/design/) for detailed architecture documents (Japanese).

## Spec Reference

- [draft-ietf-moq-transport-17](https://datatracker.ietf.org/doc/draft-ietf-moq-transport/17/) — implemented
- [draft-ietf-moq-loc-01](https://datatracker.ietf.org/doc/draft-ietf-moq-loc/) — planned

Implemented messages: `SETUP`, `PUBLISH_NAMESPACE`, `REQUEST_OK`, `SUBSCRIBE`, `SUBSCRIBE_OK`, `REQUEST_ERROR`, `PUBLISH_DONE`
