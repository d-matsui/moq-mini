//! # data: Data plane stream handler
//!
//! Receives unidirectional data streams from publishers and writes objects
//! to the per-track cache (write-through model). Subscriber relay tasks
//! read from the cache independently.

use std::sync::Arc;

use anyhow::Result;

use tokio::sync::Mutex;

use moqt::session::subgroup::SubgroupReader;
use moqt::wire::object::resolve_object_id;

use crate::cache::CachedObject;
use crate::state::{RelayState, SessionId};

/// Process a unidirectional data stream from a publisher.
///
/// Writes all objects to the per-track cache. Subscriber relay tasks
/// (spawned by the control handler) read from the cache and forward
/// to subscriber sessions.
///
/// ## Flow
/// 1. Look up the TrackCache by (publisher_session, track_alias)
/// 2. Begin a new group in the cache
/// 3. Read objects one by one, resolve absolute Object IDs, write to cache
/// 4. Mark group as complete when stream ends (FIN)
pub(crate) async fn handle_data_stream(
    sender_session: SessionId,
    mut subgroup_reader: SubgroupReader,
    state: Arc<Mutex<RelayState>>,
) -> Result<()> {
    let track_alias = subgroup_reader.track_alias();
    let group_id = subgroup_reader.group_id();
    let header = subgroup_reader.header().clone();

    // Look up the TrackCache for this publisher's track
    let cache = state
        .lock()
        .await
        .find_track_cache(sender_session, track_alias);

    let Some(cache) = cache else {
        // No subscription for this track — drain the stream
        while let Ok(Some(_)) = subgroup_reader.read_object_raw().await {}
        return Ok(());
    };

    // Begin group in cache (writes SubgroupHeader)
    cache.begin_group(group_id, header).await;

    // Read objects and write to cache
    let mut prev_object_id: Option<u64> = None;
    while let Some((obj, payload, header_bytes)) = subgroup_reader.read_object_raw().await? {
        let object_id = resolve_object_id(prev_object_id, obj.object_id_delta);
        cache
            .push_object(
                group_id,
                CachedObject {
                    object_id,
                    header_bytes,
                    payload,
                },
            )
            .await;
        prev_object_id = Some(object_id);
    }

    // Mark group as complete (publisher stream FIN)
    cache.complete_group(group_id).await;

    Ok(())
}
