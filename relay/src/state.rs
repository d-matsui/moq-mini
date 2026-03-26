//! # state: Shared relay state
//!
//! Manages sessions, namespace registrations, and subscriptions.
//! State shared across all sessions is managed via `Arc<Mutex<RelayState>>`.

use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::Mutex;

use moqt::session::subscribe_request::SubscribeRequest;
use moqt::session::MoqtSession;
use moqt::wire::subscribe_ok::SubscribeOkMessage;
use moqt::wire::track_namespace::TrackNamespace;

/// Unique identifier for a session. Assigned sequentially per connection.
pub(crate) type SessionId = u64;

/// A full track name that uniquely identifies a track (namespace + track name).
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct FullTrackName {
    pub namespace: TrackNamespace,
    pub name: String,
}

/// An active subscription for a track, from one publisher to one or more subscribers.
pub(crate) struct Subscription {
    /// Session ID of the publisher delivering data
    pub publisher_session: SessionId,
    /// Track alias assigned by the publisher (used in SubgroupHeader)
    pub publisher_track_alias: u64,
    /// The SUBSCRIBE_OK received from the publisher.
    /// Reused for subscription aggregation.
    pub subscribe_ok: SubscribeOkMessage,
    /// List of subscribers receiving data for this track
    pub subscribers: Vec<SubscriberEntry>,
}

/// A subscriber within a subscription.
pub(crate) struct SubscriberEntry {
    pub session_id: SessionId,
    /// Request handle for sending PUBLISH_DONE
    pub request: Arc<Mutex<SubscribeRequest>>,
}

/// Shared relay state. Manages sessions, namespace registrations, and subscriptions.
pub(crate) struct RelayState {
    next_session_id: u64,
    pub sessions: HashMap<SessionId, Arc<MoqtSession>>,
    namespace_to_publisher: HashMap<TrackNamespace, SessionId>,
    pub subscriptions: HashMap<FullTrackName, Subscription>,
    /// Per-track locks to serialize handle_subscribe for the same track.
    /// Prevents duplicate upstream SUBSCRIBEs when multiple subscribers
    /// request the same track concurrently.
    pub track_locks: HashMap<FullTrackName, Arc<Mutex<()>>>,
}

impl RelayState {
    pub fn new() -> Self {
        Self {
            next_session_id: 0,
            sessions: HashMap::new(),
            namespace_to_publisher: HashMap::new(),
            subscriptions: HashMap::new(),
            track_locks: HashMap::new(),
        }
    }

    /// Register a new session and return its ID.
    pub fn register_session(&mut self, session: Arc<MoqtSession>) -> SessionId {
        let id = self.next_session_id;
        self.next_session_id += 1;
        self.sessions.insert(id, session);
        id
    }

    /// Remove a session and all associated namespace registrations and subscriptions.
    pub fn remove_session(&mut self, id: SessionId) {
        self.sessions.remove(&id);
        self.namespace_to_publisher.retain(|_, v| *v != id);
        // Remove subscriber entries; drop entire subscription if publisher disconnects
        // or no subscribers remain.
        self.subscriptions.retain(|_, sub| {
            if sub.publisher_session == id {
                return false;
            }
            sub.subscribers.retain(|s| s.session_id != id);
            !sub.subscribers.is_empty()
        });
    }

    /// Register a namespace as published by the given session.
    pub fn register_namespace(&mut self, namespace: TrackNamespace, session_id: SessionId) {
        self.namespace_to_publisher.insert(namespace, session_id);
    }

    /// Find the publisher session for a namespace (prefix match).
    /// Tries progressively shorter prefixes of the given namespace
    /// against the HashMap until a match is found.
    pub fn find_publisher(&self, namespace: &TrackNamespace) -> Option<(SessionId, Arc<MoqtSession>)> {
        let mut prefix = namespace.clone();
        loop {
            if let Some(&pub_id) = self.namespace_to_publisher.get(&prefix) {
                let session = self.sessions.get(&pub_id)?.clone();
                return Some((pub_id, session));
            }
            if prefix.fields.is_empty() {
                return None;
            }
            prefix.fields.pop();
        }
    }

    /// Add a subscriber to an existing subscription, or create a new one.
    pub fn add_subscriber(
        &mut self,
        track: FullTrackName,
        publisher_session: SessionId,
        publisher_track_alias: u64,
        subscribe_ok: SubscribeOkMessage,
        subscriber: SubscriberEntry,
    ) {
        let sub = self
            .subscriptions
            .entry(track)
            .or_insert_with(|| Subscription {
                publisher_session,
                publisher_track_alias,
                subscribe_ok,
                subscribers: Vec::new(),
            });
        sub.subscribers.push(subscriber);
    }

    /// Find subscriber sessions for a given publisher's data stream.
    pub fn find_subscriber_sessions(
        &self,
        publisher_session: SessionId,
        track_alias: u64,
    ) -> Vec<Arc<MoqtSession>> {
        self.subscriptions
            .values()
            .filter(|sub| {
                sub.publisher_session == publisher_session
                    && sub.publisher_track_alias == track_alias
            })
            .flat_map(|sub| &sub.subscribers)
            .filter_map(|s| self.sessions.get(&s.session_id).cloned())
            .collect()
    }

    /// Find an existing subscription for the same track (for aggregation).
    pub fn find_existing_subscription_mut(
        &mut self,
        track: &FullTrackName,
    ) -> Option<&mut Subscription> {
        self.subscriptions.get_mut(track)
    }

    /// Find subscriber request handles for a given track (for PUBLISH_DONE forwarding).
    pub fn find_subscriber_requests(&self, track: &FullTrackName) -> Vec<Arc<Mutex<SubscribeRequest>>> {
        self.subscriptions
            .get(track)
            .map(|sub| sub.subscribers.iter().map(|s| s.request.clone()).collect())
            .unwrap_or_default()
    }
}
