//! # control: Control plane message handlers
//!
//! Handles SUBSCRIBE messages on bidi streams.
//! Future additions (FETCH, etc.) will be added here.

use std::sync::Arc;

use anyhow::Result;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use moqt::session::subscribe_request::SubscribeRequest;
use moqt::session::{MoqtSession, RequestError};
use moqt::wire::parameter::{MessageParameter, SubscriptionFilter};
use moqt::wire::request_error::{ERROR_DOES_NOT_EXIST, ERROR_NOT_SUPPORTED};
use moqt::wire::subscribe_ok::SubscribeOkMessage;

use crate::cache::TrackCache;
use crate::state::{FullTrackName, RelayState, SessionId, SubscriberEntry};
use crate::subscriber_relay;

/// Handle a SUBSCRIBE message.
///
/// 1. Check subscription filter (NextGroupStart and LargestObject supported)
/// 2. Find publisher session by namespace (prefix match)
/// 3. Forward SUBSCRIBE to publisher via session API
/// 4. Record subscription entry with TrackCache
/// 5. Forward SUBSCRIBE_OK to subscriber (with LARGEST_OBJECT if available)
/// 6. Spawn subscriber relay task to read from cache
/// 7. Wait for PUBLISH_DONE from publisher and forward to subscriber
pub(crate) async fn handle_subscribe(
    subscriber_session: SessionId,
    request: SubscribeRequest,
    state: Arc<Mutex<RelayState>>,
) -> Result<()> {
    // === Filter check ===
    let has_unsupported_filter = request.has_unsupported_filter();
    let filter = request.subscription_filter().cloned();

    let msg = request.message.clone();
    let subscriber_request = Arc::new(Mutex::new(request));

    if has_unsupported_filter {
        subscriber_request
            .lock()
            .await
            .reject(
                ERROR_NOT_SUPPORTED,
                "only NextGroupStart and LargestObject filters are supported",
            )
            .await?;
        return Ok(());
    }

    let track_name_str = std::str::from_utf8(&msg.track_name)?;
    let track = FullTrackName {
        namespace: msg.track_namespace.clone(),
        name: track_name_str.to_string(),
    };

    // === Per-track serialization ===
    let track_lock = state
        .lock()
        .await
        .track_locks
        .entry(track.clone())
        .or_default()
        .clone();
    let track_guard = track_lock.lock().await;

    let subscriber_request_id = msg.request_id;

    // === Subscription aggregation ===
    // If there is already an upstream subscription for the same track,
    // reuse it instead of sending a new SUBSCRIBE to the publisher.
    {
        let mut s = state.lock().await;
        if let Some(existing) = s.find_existing_subscription_mut(&track) {
            let cache = existing.cache.clone();
            let ok = existing.subscribe_ok.clone();

            // Build SUBSCRIBE_OK with current LARGEST_OBJECT from cache
            let ok_with_largest = augment_subscribe_ok_with_largest(&ok, &cache).await;
            let joining_location = extract_largest_object(&ok_with_largest);

            existing.subscribers.push(SubscriberEntry {
                session_id: subscriber_session,
                request: subscriber_request.clone(),
                subscriber_request_id,
                joining_location,
            });
            drop(s);

            subscriber_request
                .lock()
                .await
                .forward_subscribe_ok(&ok_with_largest)
                .await?;

            // Determine start position and spawn relay task
            let (start_group, start_object) = compute_start_position(&filter, &cache).await;
            let sub_session = s_get_session(&state, subscriber_session).await;
            if let Some(session) = sub_session {
                tokio::spawn(subscriber_relay::relay_cache_to_subscriber(
                    cache,
                    session,
                    start_group,
                    start_object,
                ));
            }

            return Ok(());
        }
    }

    // === Find publisher session ===
    let (publisher_session_id, publisher_session) =
        match state.lock().await.find_publisher(&msg.track_namespace) {
            Some(found) => found,
            None => {
                subscriber_request
                    .lock()
                    .await
                    .reject(ERROR_DOES_NOT_EXIST, "no publisher for namespace")
                    .await?;
                return Ok(());
            }
        };

    // === Forward SUBSCRIBE to publisher via session API ===
    let mut subscription = match publisher_session
        .subscribe(
            msg.track_namespace.clone(),
            track_name_str,
            msg.parameters.clone(),
        )
        .await
    {
        Ok(sub) => sub,
        Err(e) => {
            if let Some(RequestError::Rejected {
                status_code,
                reason,
            }) = e.downcast_ref()
            {
                warn!(
                    subscriber_session,
                    status_code, reason, "publisher rejected SUBSCRIBE, forwarding to subscriber"
                );
                subscriber_request
                    .lock()
                    .await
                    .reject(*status_code, reason)
                    .await?;
                return Ok(());
            }
            return Err(e);
        }
    };

    let track_alias = subscription.track_alias();

    // === Create TrackCache and record subscription ===
    let cache = Arc::new(TrackCache::new());

    // Build SUBSCRIBE_OK with LARGEST_OBJECT (cache is empty for first subscriber)
    let ok_with_largest =
        augment_subscribe_ok_with_largest(&subscription.subscribe_ok, &cache).await;
    let joining_location = extract_largest_object(&ok_with_largest);

    state.lock().await.add_subscriber(
        track.clone(),
        publisher_session_id,
        track_alias,
        subscription.subscribe_ok.clone(),
        SubscriberEntry {
            session_id: subscriber_session,
            request: subscriber_request.clone(),
            subscriber_request_id,
            joining_location,
        },
        cache.clone(),
    );

    // Release the per-track lock now that the subscription is established.
    drop(track_guard);

    // Forward SUBSCRIBE_OK to subscriber
    subscriber_request
        .lock()
        .await
        .forward_subscribe_ok(&ok_with_largest)
        .await?;

    // === Spawn subscriber relay task ===
    let (start_group, start_object) = compute_start_position(&filter, &cache).await;
    let sub_session = s_get_session(&state, subscriber_session).await;
    if let Some(session) = sub_session {
        tokio::spawn(subscriber_relay::relay_cache_to_subscriber(
            cache.clone(),
            session,
            start_group,
            start_object,
        ));
    }

    // === Wait for PUBLISH_DONE and forward ===
    match subscription.recv_publish_done().await {
        Ok(Some(publish_done)) => {
            // Signal the cache that no more data will arrive
            cache.close().await;

            let subs_to_notify = state.lock().await.find_subscriber_requests(&track);
            for req in subs_to_notify {
                let _ = req.lock().await.forward_publish_done(&publish_done).await;
            }
        }
        Ok(None) => {
            debug!("publisher closed stream without PUBLISH_DONE");
            cache.close().await;
        }
        Err(e) => {
            warn!(error = %e, "publisher disconnected unexpectedly");
            cache.close().await;
        }
    }

    // === Close subscriber request streams ===
    let subs_to_close = state.lock().await.find_subscriber_requests(&track);
    for req in subs_to_close {
        let _ = req.lock().await.close().await;
    }

    Ok(())
}

/// Compute the start position (group, object) for a subscriber based on filter type.
async fn compute_start_position(
    filter: &Option<SubscriptionFilter>,
    cache: &TrackCache,
) -> (u64, u64) {
    match filter {
        Some(SubscriptionFilter::LargestObject) => {
            match cache.largest_object().await {
                Some((group, object)) => {
                    // Start from the object after the largest
                    (group, object + 1)
                }
                None => (0, 0), // No objects yet, start from beginning
            }
        }
        // NextGroupStart or no filter: start from next group
        _ => match cache.largest_object().await {
            Some((group, _)) => (group + 1, 0),
            None => (0, 0),
        },
    }
}

/// Augment a SUBSCRIBE_OK message with the LARGEST_OBJECT parameter from cache.
async fn augment_subscribe_ok_with_largest(
    original: &SubscribeOkMessage,
    cache: &TrackCache,
) -> SubscribeOkMessage {
    let mut ok = original.clone();

    if let Some((group, object)) = cache.largest_object().await {
        // Remove any existing LARGEST_OBJECT parameter
        ok.parameters
            .retain(|p| !matches!(p, MessageParameter::LargestObject { .. }));
        // Add current LARGEST_OBJECT
        ok.parameters
            .push(MessageParameter::LargestObject { group, object });
        // Re-sort parameters by type ID (ascending order required by spec)
        ok.parameters.sort_by_key(|p| match p {
            MessageParameter::LargestObject { .. } => 0x09u64,
            MessageParameter::Forward(_) => 0x10,
            MessageParameter::SubscriptionFilter(_) => 0x21,
        });
    }

    ok
}

/// Extract the LARGEST_OBJECT parameter from a SUBSCRIBE_OK message.
fn extract_largest_object(ok: &SubscribeOkMessage) -> Option<(u64, u64)> {
    ok.parameters.iter().find_map(|p| match p {
        MessageParameter::LargestObject { group, object } => Some((*group, *object)),
        _ => None,
    })
}

/// Helper to get a session Arc from state by session ID.
async fn s_get_session(
    state: &Arc<Mutex<RelayState>>,
    session_id: SessionId,
) -> Option<Arc<MoqtSession>> {
    state.lock().await.sessions.get(&session_id).cloned()
}
