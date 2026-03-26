//! # control: Control plane message handlers
//!
//! Handles SUBSCRIBE messages on bidi streams.
//! Future additions (FETCH, etc.) will be added here.

use std::sync::Arc;

use anyhow::Result;

use tokio::sync::Mutex;
use tracing::{debug, warn};

use moqt::session::subscribe_request::SubscribeRequest;
use moqt::session::RequestError;
use moqt::wire::request_error::{ERROR_DOES_NOT_EXIST, ERROR_NOT_SUPPORTED};

use crate::state::{FullTrackName, RelayState, SessionId, SubscriberEntry};

/// Handle a SUBSCRIBE message.
///
/// 1. Check subscription filter (only NextGroupStart supported)
/// 2. Find publisher session by namespace (prefix match)
/// 3. Forward SUBSCRIBE to publisher via session API
/// 4. Record subscription entry (used for data stream relay)
/// 5. Forward SUBSCRIBE_OK to subscriber
/// 6. Wait for PUBLISH_DONE from publisher and forward to subscriber
pub(crate) async fn handle_subscribe(
    subscriber_session: SessionId,
    request: SubscribeRequest,
    state: Arc<Mutex<RelayState>>,
) -> Result<()> {
    // === Filter check ===
    // This minimal implementation only supports NextGroupStart.
    let has_unsupported_filter = request.has_unsupported_filter();

    let msg = request.message.clone();
    let subscriber_request = Arc::new(Mutex::new(request));

    if has_unsupported_filter {
        subscriber_request
            .lock()
            .await
            .reject(
                ERROR_NOT_SUPPORTED,
                "only NextGroupStart filter is supported",
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
    // Acquire a per-track lock to prevent duplicate upstream SUBSCRIBEs
    // when multiple subscribers request the same track concurrently.
    let track_lock = state
        .lock()
        .await
        .track_locks
        .entry(track.clone())
        .or_default()
        .clone();
    let track_guard = track_lock.lock().await;

    let new_subscriber = SubscriberEntry {
        session_id: subscriber_session,
        request: subscriber_request.clone(),
    };

    // === Subscription aggregation ===
    // If there is already an upstream subscription for the same track,
    // reuse it instead of sending a new SUBSCRIBE to the publisher.
    {
        let mut s = state.lock().await;
        if let Some(existing) = s.find_existing_subscription_mut(&track) {
            let ok = existing.subscribe_ok.clone();
            existing.subscribers.push(new_subscriber);
            drop(s);
            subscriber_request
                .lock()
                .await
                .forward_subscribe_ok(&ok)
                .await?;
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

    // === Record subscription ===
    state.lock().await.add_subscriber(
        track.clone(),
        publisher_session_id,
        track_alias,
        subscription.subscribe_ok.clone(),
        new_subscriber,
    );

    // Release the per-track lock now that the subscription is established.
    // Other subscribers for this track can now proceed with aggregation.
    drop(track_guard);

    // Forward SUBSCRIBE_OK to subscriber
    subscriber_request
        .lock()
        .await
        .forward_subscribe_ok(&subscription.subscribe_ok)
        .await?;

    // === Wait for PUBLISH_DONE and forward ===
    match subscription.recv_publish_done().await {
        Ok(Some(publish_done)) => {
            let subs_to_notify = state.lock().await.find_subscriber_requests(&track);
            for req in subs_to_notify {
                let _ = req.lock().await.forward_publish_done(&publish_done).await;
            }
        }
        Ok(None) => {
            debug!("publisher closed stream without PUBLISH_DONE");
        }
        Err(e) => {
            warn!(error = %e, "publisher disconnected unexpectedly");
        }
    }

    // === Close subscriber request streams ===
    let subs_to_close = state.lock().await.find_subscriber_requests(&track);
    for req in subs_to_close {
        let _ = req.lock().await.close().await;
    }

    Ok(())
}
