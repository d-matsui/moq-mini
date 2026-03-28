<!-- Generated: 2026-03-28 | Files scanned: 42 | Token estimate: ~950 -->

# Backend (Rust Crates)

## moqt -- Core Library (~5300 lines)

### wire/ -- Wire Format (encode/decode)

| File | Lines | Contents |
|------|-------|---------|
| `parameter.rs` | 430 | MessageParameter, SubscriptionFilter (NextGroupStart, LargestObject) |
| `varint.rs` | 367 | Variable-length int encode/decode |
| `subgroup_header.rs` | 313 | SubgroupHeader (track alias, group ID, subgroup ID) |
| `key_value_pair.rs` | 270 | SETUP key-value pairs |
| `setup.rs` | 223 | SetupMessage + SetupOption (Path, Authority) |
| `object.rs` | 206 | ObjectHeader (ID delta, payload length), resolve_object_id() |
| `track_namespace.rs` | 201 | TrackNamespace (Vec<Vec<u8>> fields) |
| `subscribe.rs` | 196 | SubscribeMessage |
| `subscribe_ok.rs` | 179 | SubscribeOkMessage |
| `publish_namespace.rs` | 177 | PublishNamespaceMessage |
| `fetch.rs` | 170 | FetchMessage (Relative Joining Fetch only) |
| `fetch_ok.rs` | 140 | FetchOkMessage (end_of_track, end location) |
| `fetch_header.rs` | 130 | FETCH_HEADER encode, FetchObjectFields, serialization flags |
| `publish_done.rs` | 143 | PublishDoneMessage |
| `request_error.rs` | 150 | RequestErrorMessage, error codes (INVALID_RANGE, etc.) |
| `reason_phrase.rs` | 120 | Length-prefixed UTF-8 string |
| `request_ok.rs` | 108 | RequestOkMessage |
| `mod.rs` | 100 | Message type IDs (MSG_FETCH=0x16, MSG_FETCH_OK=0x18, etc.) |

Message framing: `Type(varint) + Length(u16 BE) + Payload`

### stream/ -- Stream I/O

| File | Lines | Role |
|------|-------|------|
| `mod.rs` | 130 | read_varint(), read_message_frame() |
| `request.rs` | 300 | RequestStreamReader/Writer (bidi: SUBSCRIBE, FETCH + responses) |
| `data.rs` | 171 | DataStreamReader/Writer (uni: SubgroupHeader + Objects) |
| `fetch_data.rs` | 70 | FetchDataStreamWriter (uni: FETCH_HEADER + Objects) |
| `control.rs` | 62 | ControlStreamReader/Writer (uni: SETUP) |

### session/ -- Protocol Logic

| File | Lines | Role |
|------|-------|------|
| `mod.rs` | 420 | MoqtSession (connect/accept, subscribe, fetch, next_event, open_uni_stream) |
| `subgroup.rs` | 138 | SubgroupReader/Writer (high-level object read/write) |
| `subscribe_request.rs` | 95 | Incoming SUBSCRIBE handler (accept/reject, subscription_filter()) |
| `fetch_request.rs` | 45 | Incoming FETCH handler (accept with FETCH_OK / reject) |
| `subscription.rs` | 55 | Established subscription (track_alias, recv_publish_done) |
| `publish_namespace_request.rs` | 35 | Incoming PUBLISH_NAMESPACE handler |

## relay -- Relay Server (~1600 lines)

| File | Lines | Role |
|------|-------|------|
| `main.rs` | 68 | Entry point (TLS cert loading, listen on :4433) |
| `relay.rs` | 140 | Relay struct, connection handler, event dispatch |
| `state.rs` | 200 | RelayState, Subscription, SubscriberEntry (request_id, joining_location) |
| `control.rs` | 270 | handle_subscribe (aggregation, LargestObject, spawn relay task) |
| `data.rs` | 80 | handle_data_stream (cache write-through) |
| `cache.rs` | 200 | TrackCache (per-track, group eviction, Notify-based waiting) |
| `subscriber_relay.rs` | 200 | Per-subscriber cache reader task (partial-group support) |
| `fetch_handler.rs` | 190 | handle_fetch (range computation, serve from cache) |

### Relay Data Flow

```
handle_data_stream(publisher_stream)
  -> find_track_cache(session, alias)
  -> cache.begin_group() -> cache.push_object() (loop) -> cache.complete_group()

relay_cache_to_subscriber(cache, session, start_group, start_object)
  -> wait for group -> open_data_stream -> write objects from cache -> finish

handle_fetch(session, request)
  -> find_subscription_by_subscriber_request(session, joining_request_id)
  -> compute range -> send FETCH_OK -> send_cached_objects via FETCH_HEADER
```

## Tests

- Unit tests: `#[cfg(test)]` in each source file
- Integration: `relay/tests/integration.rs` (~1500 lines, 22 tests)
- E2E: `scripts/e2e-cli-ivf.sh`, `scripts/e2e-cli-msf.sh`
- E2E (web): `scripts/e2e-web-playwright.sh` (Playwright)
