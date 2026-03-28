<!-- Generated: 2026-03-26 | Files scanned: 9 | Token estimate: ~400 -->

# Dependencies

## Rust (Cargo workspace)

### Runtime

| Crate | Version | Purpose |
|-------|---------|---------|
| `quinn` | 0.11 | QUIC transport (client + server endpoints) |
| `rustls` | 0.23 | TLS 1.3 (ring crypto backend) |
| `rustls-pki-types` | 1 | Certificate/key types |
| `web-transport-quinn` | 0.11 | Unified Session over raw QUIC / WebTransport |
| `tokio` | 1 (full) | Async runtime |
| `anyhow` | 1 | Error propagation |
| `thiserror` | 2 | Typed errors |
| `tracing` | 0.1 | Structured logging |
| `tracing-subscriber` | 0.3 | Log output (env-filter) |
| `url` | 2 | URL parsing (WebTransport session) |
| `serde` | 1 | JSON serialization (MSF catalog) |
| `serde_json` | 1 | JSON encode/decode (MSF catalog) |

### Build / Dev

| Crate | Version | Purpose |
|-------|---------|---------|
| `rcgen` | 0.13 | Self-signed cert generation (tests) |
| `rustls-pemfile` | (relay) | PEM cert/key file parsing |

## TypeScript (apps-web/)

| Package | Purpose |
|---------|---------|
| `vite` | Dev server + bundler |
| `vitest` | Test runner |
| `typescript` | Type checking |

## External Tools

| Tool | Usage |
|------|-------|
| `ffmpeg` | E2E test: camera capture → IVF/VP8 encoding |
| `ffplay` | E2E test: IVF/VP8 playback from subscriber pipe |
| `mkcert` | TLS certificate generation for local dev |
| `playwright` | Browser E2E testing |
