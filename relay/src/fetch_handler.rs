//! # fetch_handler: FETCH request handler
//!
//! Handles incoming FETCH requests by serving cached objects to subscribers.
//! Only Relative Joining Fetch is supported.

use std::sync::Arc;

use anyhow::Result;

use tokio::sync::Mutex;
use tracing::debug;

use moqt::session::MoqtSession;
use moqt::session::fetch_request::FetchRequest;
use moqt::stream::fetch_data::FetchDataStreamWriter;
use moqt::wire::fetch_ok::FetchOkMessage;
use moqt::wire::request_error::{ERROR_INVALID_JOINING_REQUEST_ID, ERROR_INVALID_RANGE};

use crate::cache::TrackCache;
use crate::state::{RelayState, SessionId};

/// Handle a FETCH request.
///
/// 1. Look up the associated SUBSCRIBE by Joining Request ID
/// 2. Compute the fetch range from the Joining Location
/// 3. Send FETCH_OK with the actual end location
/// 4. Open a uni stream with FETCH_HEADER and send cached objects
pub(crate) async fn handle_fetch(
    session_id: SessionId,
    mut request: FetchRequest,
    state: Arc<Mutex<RelayState>>,
) -> Result<()> {
    let joining_request_id = request.message.joining_request_id;
    let joining_start = request.message.joining_start;
    let fetch_request_id = request.message.request_id;

    // === Look up the associated subscription ===
    let (cache, joining_location) = {
        let s = state.lock().await;
        match s.find_subscription_by_subscriber_request(session_id, joining_request_id) {
            Some((cache, jl)) => (cache, jl),
            None => {
                request
                    .reject(
                        ERROR_INVALID_JOINING_REQUEST_ID,
                        "no subscription found for joining request ID",
                    )
                    .await?;
                return Ok(());
            }
        }
    };

    // === Get joining location ===
    let (jl_group, jl_object) = match joining_location {
        Some(loc) => loc,
        None => {
            // No objects were published when the subscriber joined
            request
                .reject(ERROR_INVALID_RANGE, "no objects published on track")
                .await?;
            return Ok(());
        }
    };

    // === Compute range ===
    let start_group = jl_group.saturating_sub(joining_start);
    let start_object: u64 = 0;
    let end_group = jl_group;
    let end_object = jl_object + 1; // exclusive (spec: "plus 1")

    debug!(
        session_id,
        start_group, end_group, end_object, "FETCH range computed"
    );

    // === Get subscriber session for opening uni stream ===
    let subscriber_session = {
        let s = state.lock().await;
        match s.sessions.get(&session_id) {
            Some(session) => session.clone(),
            None => return Ok(()), // Session gone
        }
    };

    // === Determine actual end location from cache ===
    let actual_end =
        find_actual_end_in_cache(&cache, start_group, start_object, end_group, end_object).await;

    let (actual_end_group, actual_end_object) = match actual_end {
        Some(loc) => loc,
        None => {
            request
                .reject(ERROR_INVALID_RANGE, "no objects in requested range")
                .await?;
            return Ok(());
        }
    };

    // === Send FETCH_OK ===
    let fetch_ok = FetchOkMessage {
        end_of_track: false,
        end_group: actual_end_group,
        end_object: actual_end_object + 1, // spec: end location is exclusive
        parameters: vec![],
        track_properties_raw: vec![],
    };
    request.accept(&fetch_ok).await?;

    // === Open FETCH_HEADER stream and send objects ===
    send_cached_objects(
        &cache,
        &subscriber_session,
        fetch_request_id,
        start_group,
        start_object,
        actual_end_group,
        actual_end_object,
    )
    .await?;

    Ok(())
}

/// Find the actual last object in the cache within the requested range.
/// Returns the (group, object) of the last available object, or None.
async fn find_actual_end_in_cache(
    cache: &TrackCache,
    start_group: u64,
    _start_object: u64,
    end_group: u64,
    end_object: u64,
) -> Option<(u64, u64)> {
    let mut last: Option<(u64, u64)> = None;

    for group_id in start_group..=end_group {
        let (objects, _) = cache.read_objects(group_id, 0).await;
        for (obj_id, _, _) in &objects {
            // Check if within range
            if group_id == end_group && *obj_id >= end_object {
                break;
            }
            last = Some((group_id, *obj_id));
        }
    }

    last
}

/// Send cached objects on a FETCH_HEADER unidirectional stream.
async fn send_cached_objects(
    cache: &TrackCache,
    session: &MoqtSession,
    request_id: u64,
    start_group: u64,
    start_object: u64,
    end_group: u64,
    end_object: u64,
) -> Result<()> {
    let uni = session.open_uni_stream().await?;
    let mut writer = FetchDataStreamWriter::new(uni, request_id).await?;

    for group_id in start_group..=end_group {
        // Get group header to extract subgroup_id and priority
        let header = cache.get_group_header(group_id).await;
        let subgroup_id = header.as_ref().and_then(|h| h.subgroup_id).unwrap_or(0);
        let priority = header
            .as_ref()
            .and_then(|h| h.publisher_priority)
            .unwrap_or(0);

        let (objects, _) = cache.read_objects(group_id, 0).await;
        for (obj_id, _, payload) in &objects {
            // Skip objects before start range
            if group_id == start_group && *obj_id < start_object {
                continue;
            }
            // Stop at end range
            if group_id == end_group && *obj_id > end_object {
                break;
            }

            writer
                .write_object(group_id, subgroup_id, *obj_id, priority, payload)
                .await?;
        }
    }

    writer.finish()?;
    Ok(())
}
