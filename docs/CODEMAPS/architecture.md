<!-- Generated: 2026-03-28 | Files scanned: 87 | Token estimate: ~950 -->

# Architecture

## System Overview

Minimal MOQT (draft-ietf-moq-transport-17) implementation with MSF/LOC media container support.
Publisher -> Relay -> Subscriber live media streaming with cache-through and Joining Fetch.

```
Publisher (CLI/Browser)       Relay (moqt-relay)          Subscriber (CLI/Browser)
  |                              |                              |
  +- QUIC/WebTransport -------->|<---- QUIC/WebTransport ------+
  +- SETUP exchange              |               SETUP exchange-+
  +- PUBLISH_NAMESPACE -------->| register ns                  |
  |                              |<- SUBSCRIBE (LargestObject) -+
  |<-- SUBSCRIBE (forwarded) ---+                              |
  +--- SUBSCRIBE_OK ----------->+-- SUBSCRIBE_OK (+ LARGEST) ->|
  +--- Data uni-stream -------->+-- TrackCache (write-through)  |
  |                              |   +-- subscriber relay task ->|
  |                              |<- FETCH (Relative Joining) --+
  |                              +-- FETCH_OK ----------------->|
  |                              +-- FETCH_HEADER uni-stream -->|
  +--- PUBLISH_DONE ----------->+-- PUBLISH_DONE (forwarded) ->|
```

## Project Type

Cargo workspace (Rust 2024 edition) + TypeScript browser client.

## Crate/Package Map

| Crate/Package | Type | Lines | Purpose |
|---------------|------|-------|---------|
| `moqt` | lib | ~5300 | Wire format, stream framing, session API |
| `relay` | bin+lib | ~1600 | Relay server (cache-through, FETCH handler) |
| `msf` | lib | ~580 | MSF catalog (JSON) + LOC header extensions |
| `apps-cli/lib` | lib | ~300 | Shared CLI helpers (client connect, IVF codec) |
| `apps-cli/ivf-*` | bin | ~140 | IVF/VP8 CLI publisher/subscriber |
| `apps-cli/msf-*` | bin | ~440 | MSF CLI publisher/subscriber |
| `apps-web/` | TS lib+app | ~3200 | Browser MOQT client (WebTransport + FETCH) |

## Transport

- Raw QUIC: ALPN `moqt-17` (native CLI clients)
- WebTransport: ALPN `h3` (browser clients)
- Unified via `web-transport-quinn::Session`

## Stream Types

- **Control** (uni): SETUP messages, session lifetime
- **Request** (bidi): SUBSCRIBE, FETCH, PUBLISH_NAMESPACE + responses
- **Data** (uni): SubgroupHeader + Object payloads (1 group = 1 stream)
- **Fetch Data** (uni): FETCH_HEADER + Objects with serialization flags

## Data Flow: Cache-Through Model

```
Publisher stream --> Relay data handler --> TrackCache.push_object()
                                               |
                                               +--> Notify waiters
                                               |
                               subscriber_relay task <-- reads from cache
                                               |
                                               +--> Subscriber session (Data uni-stream)
```

All objects flow through TrackCache. Both live and late-join subscribers
use the same cache-read path.

## Late Join Flow

```
1. Subscriber sends SUBSCRIBE(LargestObject)
2. Relay returns SUBSCRIBE_OK with LARGEST_OBJECT=(G,O) from cache
3. Subscriber sends FETCH(Relative Joining, joiningStart=N)
4. Relay computes range: Start=(G-N, 0), End=(G, O+1)
5. Relay opens FETCH_HEADER uni-stream, sends cached objects
6. Relay sends FETCH_OK on bidi stream
7. Subscriber receives cached objects, then switches to live stream
```
