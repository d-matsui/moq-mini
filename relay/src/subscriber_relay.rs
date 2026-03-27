//! # subscriber_relay: Per-subscriber cache reader task
//!
//! Each subscriber gets a dedicated task that reads from the per-track
//! cache and writes to the subscriber's QUIC session. This ensures
//! both live and late-joining subscribers use the same code path.

use std::sync::Arc;

use anyhow::Result;

use tracing::warn;

use moqt::session::MoqtSession;
use moqt::wire::object::ObjectHeader;

use crate::cache::TrackCache;

/// Relay objects from the cache to a subscriber session.
///
/// Reads from the cache starting at `(start_group, start_object)` and
/// opens new data streams for each group. Waits for new data via Notify.
///
/// ## Arguments
/// - `cache`: The per-track cache shared with the data handler and other subscribers
/// - `session`: The subscriber's MOQT session
/// - `start_group`: First group to send
/// - `start_object`: First object within start_group (0 for full groups)
pub(crate) async fn relay_cache_to_subscriber(
    cache: Arc<TrackCache>,
    session: Arc<MoqtSession>,
    start_group: u64,
    start_object: u64,
) {
    if let Err(e) = relay_loop(&cache, &session, start_group, start_object).await {
        warn!(error = %e, "subscriber relay task ended with error");
    }
}

async fn relay_loop(
    cache: &TrackCache,
    session: &MoqtSession,
    start_group: u64,
    start_object: u64,
) -> Result<()> {
    let mut current_group = start_group;
    let mut first_object_id = start_object;

    loop {
        // Wait until the group exists in cache or cache is closed
        loop {
            if cache.has_group(current_group).await {
                break;
            }
            if cache.is_closed().await {
                return Ok(());
            }
            cache.wait_for_update().await;
        }

        // Get the SubgroupHeader for this group
        let header = match cache.get_group_header(current_group).await {
            Some(h) => h,
            None => return Ok(()), // Group was evicted
        };

        // If starting mid-group, check whether there are any objects to send.
        // If all cached objects are below first_object_id and the group is
        // complete, skip this group entirely without opening a stream.
        if first_object_id > 0 {
            let should_skip = loop {
                let (objects, complete) = cache.read_objects(current_group, 0).await;
                let has_sendable = objects.iter().any(|(oid, _, _)| *oid >= first_object_id);
                if has_sendable {
                    break false;
                }
                if complete {
                    break true;
                }
                if cache.is_closed().await {
                    return Ok(());
                }
                cache.wait_for_update().await;
            };
            if should_skip {
                first_object_id = 0;
                current_group += 1;
                continue;
            }
        }

        // Open a data stream to the subscriber
        let mut writer = session.open_data_stream(&header).await?;

        // Send objects from cache
        let mut cursor: usize = 0;
        let mut is_first_on_stream = true;

        loop {
            let (objects, complete) = cache.read_objects(current_group, cursor).await;

            for (object_id, header_bytes, payload) in &objects {
                // Skip objects before start_object (only for the first group)
                if *object_id < first_object_id {
                    cursor += 1;
                    continue;
                }

                if is_first_on_stream && first_object_id > 0 {
                    // Re-encode header for the first object on a partial-group stream.
                    // The raw header_bytes have a delta relative to the previous object
                    // on the publisher's stream, but this is the first object on the
                    // subscriber's stream, so delta must equal the absolute object_id.
                    let reencoded = reencode_first_object_header(*object_id, header_bytes);
                    writer.write_raw(&reencoded).await?;
                } else {
                    writer.write_raw(header_bytes).await?;
                }
                writer.write_raw(payload).await?;
                is_first_on_stream = false;
                cursor += 1;
            }

            if complete {
                writer.finish()?;
                break;
            }
            if cache.is_closed().await {
                // Publisher is done but this group wasn't marked complete.
                // Finish the stream to avoid hanging.
                writer.finish()?;
                return Ok(());
            }
            cache.wait_for_update().await;
        }

        // Move to next group; subsequent groups always start from object 0
        first_object_id = 0;
        current_group += 1;
    }
}

/// Re-encode an object header with a new object_id_delta for the first
/// object on a subscriber's stream.
///
/// The raw header format is: Object ID Delta (vi64), Payload Length (vi64), [Properties...].
/// We replace only the delta, keeping payload_length and properties intact.
fn reencode_first_object_header(object_id: u64, original_header_bytes: &[u8]) -> Vec<u8> {
    // Decode the original to find where the delta ends
    let mut remaining = original_header_bytes;
    let original =
        ObjectHeader::decode(&mut remaining, false).expect("cached header bytes should be valid");

    // Build new header: new delta + original payload_length
    let mut buf = Vec::new();
    let new_header = ObjectHeader {
        object_id_delta: object_id,
        payload_length: original.payload_length,
    };
    new_header.encode(&mut buf);

    // Append any remaining bytes (properties if present)
    // The raw header_bytes from the data stream reader include properties,
    // but ObjectHeader::decode with has_properties=false doesn't consume them.
    // We need to preserve everything after the basic header (delta + length).
    //
    // Calculate how many bytes the original delta + length consumed:
    let basic_header_len = original_header_bytes.len() - remaining.len();
    let properties_bytes = &original_header_bytes[basic_header_len..];
    buf.extend_from_slice(properties_bytes);

    buf
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reencode_first_object_delta_zero_to_nonzero() {
        // Original: delta=0, payload_length=100
        let mut original = Vec::new();
        ObjectHeader {
            object_id_delta: 0,
            payload_length: 100,
        }
        .encode(&mut original);

        let reencoded = reencode_first_object_header(5, &original);

        // Decode reencoded header
        let mut slice = reencoded.as_slice();
        let header = ObjectHeader::decode(&mut slice, false).unwrap();
        assert_eq!(header.object_id_delta, 5);
        assert_eq!(header.payload_length, 100);
        assert!(slice.is_empty());
    }

    #[test]
    fn reencode_preserves_properties_bytes() {
        // Simulate header with trailing properties bytes
        let mut original = Vec::new();
        ObjectHeader {
            object_id_delta: 0,
            payload_length: 50,
        }
        .encode(&mut original);
        // Append fake properties bytes
        original.extend_from_slice(&[0x03, 0xAA, 0xBB, 0xCC]);

        let reencoded = reencode_first_object_header(10, &original);

        let mut slice = reencoded.as_slice();
        let header = ObjectHeader::decode(&mut slice, false).unwrap();
        assert_eq!(header.object_id_delta, 10);
        assert_eq!(header.payload_length, 50);
        // Properties bytes preserved
        assert_eq!(slice, &[0x03, 0xAA, 0xBB, 0xCC]);
    }
}
