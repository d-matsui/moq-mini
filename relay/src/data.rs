//! # data: Data plane stream handler
//!
//! Relays unidirectional data streams from publishers to subscribers.

use std::sync::Arc;

use anyhow::Result;

use tokio::sync::Mutex;
use tracing::warn;

use moqt::session::subgroup::SubgroupReader;

use crate::state::{RelayState, SessionId};

/// Process a unidirectional data stream from a publisher and relay it to subscribers.
///
/// ## Relay flow
/// 1. Identify the target subscription from the Track Alias in the SubgroupHeader
/// 2. Open new uni streams to all subscribers
/// 3. Forward the SubgroupHeader to subscribers
/// 4. Read objects one by one and immediately forward them to subscribers
///    (relay with low latency without buffering the entire stream)
/// 5. Propagate stream termination (FIN) to subscribers
pub(crate) async fn handle_data_stream(
    sender_session: SessionId,
    mut subgroup_reader: SubgroupReader,
    state: Arc<Mutex<RelayState>>,
) -> Result<()> {
    let track_alias = subgroup_reader.track_alias();

    // === Identify subscribers and open downstream streams ===
    // Find matching subscriptions by Track Alias and sender session,
    // then open uni streams to each subscriber
    let sub_sessions = state
        .lock()
        .await
        .find_subscriber_sessions(sender_session, track_alias);

    // If there are no subscribers, drain the stream
    if sub_sessions.is_empty() {
        while let Ok(Some(_)) = subgroup_reader.read_object_raw().await {}
        return Ok(());
    }

    // === Open data streams to subscribers (writes SubgroupHeader) ===
    let mut writers = Vec::new();
    for session in &sub_sessions {
        match session.open_data_stream(subgroup_reader.header()).await {
            Ok(w) => writers.push(Arc::new(Mutex::new(w))),
            Err(e) => warn!(error = %e, "failed to open data stream to subscriber"),
        }
    }
    let mut active_writers = writers;

    // === Relay objects incrementally ===
    // Read objects one by one and immediately forward to all subscribers.
    // No buffering, so memory usage stays low even for large streams.
    // Subscribers that fail a write are removed from the active list.
    while let Some((_obj, payload, obj_header_bytes)) = subgroup_reader.read_object_raw().await? {
        let mut still_active = Vec::with_capacity(active_writers.len());
        for writer in active_writers {
            let mut w = writer.lock().await;
            if w.write_raw(&obj_header_bytes).await.is_ok() && w.write_raw(&payload).await.is_ok() {
                drop(w);
                still_active.push(writer);
            } else {
                warn!("subscriber write error, removing from relay");
            }
        }
        active_writers = still_active;
    }

    // === Propagate stream termination ===
    // When the publisher's stream ends,
    // send FIN to subscriber streams via finish()
    for writer in &active_writers {
        let mut w = writer.lock().await;
        if let Err(e) = w.finish() {
            warn!(error = %e, "subscriber finish error");
        }
    }

    Ok(())
}
