<!-- Generated: 2026-03-28 | Files scanned: 28 | Token estimate: ~600 -->

# Frontend (apps-web/ -- TypeScript Browser Client)

## Overview

Browser MOQT client using WebTransport API. Mirrors Rust wire/ layer.
Supports raw VP8, MSF/LOC media, and Joining Fetch for late join.

## Source Structure (~3200 lines total)

### lib/wire/ -- Wire Format (mirrors Rust moqt/wire)

| File | Lines | Contents |
|------|-------|---------|
| `varint.ts` | 124 | Variable-length int encode/decode |
| `parameter.ts` | 130 | MessageParameter, SubscriptionFilter (NextGroupStart, LargestObject) |
| `subgroup-header.ts` | 79 | SubgroupHeader |
| `key-value-pair.ts` | 68 | SETUP key-value pairs |
| `message.ts` | 60 | Message framing, type IDs (MSG_FETCH=0x16, MSG_FETCH_OK=0x18) |
| `subscribe.ts` | 47 | SubscribeMessage |
| `track-namespace.ts` | 45 | TrackNamespace |
| `setup.ts` | 43 | SetupMessage encode/decode |
| `publish-namespace.ts` | 40 | PublishNamespaceMessage |
| `subscribe-ok.ts` | 38 | SubscribeOkMessage |
| `publish-done.ts` | 36 | PublishDoneMessage |
| `fetch.ts` | 30 | FetchMessage encode (Relative Joining only) |
| `fetch-ok.ts` | 35 | FetchOkMessage decode |
| `object.ts` | 34 | ObjectHeader |
| `request-error.ts` | 25 | RequestErrorMessage |
| `request-ok.ts` | 24 | RequestOkMessage |
| `namespace.ts` | 17 | Namespace constants |

### lib/ -- Session, Stream & LOC

| File | Lines | Contents |
|------|-------|---------|
| `session.ts` | 550 | MoqtSession (subscribe, fetch, acceptFetchStream, FetchStreamReader) |
| `stream/stream-reader.ts` | 129 | WebTransport stream reader (readExact, readVarint) |
| `loc.ts` | 97 | LOC header extensions (CaptureTimestamp, VideoConfig) |

### app/ -- Application Entry Points

| File | Lines | Contents |
|------|-------|---------|
| `msf-publisher.ts` | 370 | MSF publisher (camera + catalog + LOC timestamps) |
| `msf-subscriber.ts` | 281 | MSF subscriber (video decode + display) |
| `publisher.ts` | 163 | VP8 publisher (camera capture + MOQT publish) |
| `fetch-subscriber.ts` | 244 | Subscriber with Next Group Start / Joining Fetch comparison |

### Tests

| File | Lines | Contents |
|------|-------|---------|
| `wire/wire.test.ts` | 176 | Full wire format round-trip tests |
| `wire/varint.test.ts` | 120 | Varint encode/decode tests |
| `loc.test.ts` | 77 | LOC header extension tests |
| `wire/message.test.ts` | 49 | Message framing tests |

## HTML Entry Points

| File | Purpose |
|------|---------|
| `index.html` | Landing page |
| `publisher.html` | VP8 publisher UI |
| `fetch-subscriber.html` | Subscriber UI (Next Group Start / Joining Fetch) |
| `msf-publisher.html` | MSF publisher UI |
| `msf-subscriber.html` | MSF subscriber UI |

## Tooling

- **Vite** dev server + build
- **vitest** for unit tests
- **TypeScript** strict mode
