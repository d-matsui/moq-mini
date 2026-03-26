//! # relay: MOQT relay server implementation
//!
//! This module implements the core logic of the MOQT relay.
//!
//! ## Architecture overview
//!
//! ```text
//! Publisher ──QUIC conn──→ [Relay Server] ←──QUIC conn── Subscriber
//!   │                              │                              │
//!   ├─ SETUP exchange              │               SETUP exchange─┤
//!   ├─ PUBLISH_NAMESPACE register  │                              │
//!   │                              │← SUBSCRIBE ──────────────────┤
//!   │← SUBSCRIBE forward ──────────┤                              │
//!   ├─ SUBSCRIBE_OK ──────────────→┤                              │
//!   │                              ├─ SUBSCRIBE_OK forward ──────→│
//!   ├─ Data stream (uni) ────────→├─ Data stream relay ──────────→│
//!   └─ PUBLISH_DONE ─────────────→├─ PUBLISH_DONE forward ──────→│
//! ```
//!
//! ## Per-connection processing flow
//! 1. Accept a new QUIC connection and assign a session ID
//! 2. Exchange SETUP messages
//! 3. Process control messages on bidi streams:
//!    - PUBLISH_NAMESPACE: register namespace and respond with REQUEST_OK
//!    - SUBSCRIBE: forward to publisher and relay response back to subscriber
//! 4. Relay data on uni streams:
//!    - Identify the subscription from the Track Alias in SubgroupHeader
//!    - Read objects one by one and forward them to subscribers

use std::sync::Arc;

use anyhow::Result;

use quinn::Endpoint;
use tokio::sync::Mutex;
use tracing::{debug, error, info};

use moqt::session::{MoqtSession, SessionEvent};

use crate::control::handle_subscribe;
use crate::data::handle_data_stream;
use crate::state::RelayState;

/// MOQT relay server. Holds a QUIC endpoint and accepts connections.
pub struct Relay {
    endpoint: Endpoint,
    /// State shared across all sessions. Protected by a Mutex.
    state: Arc<Mutex<RelayState>>,
}

impl Relay {
    pub fn new(endpoint: Endpoint) -> Self {
        Self {
            endpoint,
            state: Arc::new(Mutex::new(RelayState::new())),
        }
    }

    /// Main loop of the relay server.
    /// Accepts new QUIC connections and processes each in an async task.
    pub async fn run(&self) -> Result<()> {
        while let Some(incoming) = self.endpoint.accept().await {
            let state = self.state.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_connection(incoming, state).await {
                    error!(error = %e, "connection error");
                }
            });
        }
        Ok(())
    }
}

/// Process a single QUIC connection (session).
/// Detects the transport type via ALPN and performs the appropriate handshake:
/// - `moqt-17`: raw QUIC (wrap with Session::raw)
/// - `h3`: WebTransport (HTTP/3 CONNECT handshake via web_transport_quinn)
async fn handle_connection(incoming: quinn::Incoming, state: Arc<Mutex<RelayState>>) -> Result<()> {
    let connection = incoming.await?;

    // === Detect transport and create session ===
    let alpn = connection
        .handshake_data()
        .and_then(|d| d.downcast::<quinn::crypto::rustls::HandshakeData>().ok())
        .and_then(|d| d.protocol)
        .unwrap_or_default();

    let wt_session = if alpn.as_slice() == moqt::quic_config::ALPN_H3 {
        // WebTransport: perform HTTP/3 + CONNECT handshake
        let request = web_transport_quinn::Request::accept(connection).await?;
        request.ok().await?
    } else {
        // Raw QUIC (moqt-17 or fallback)
        let url = url::Url::parse("https://localhost").expect("static URL");
        web_transport_quinn::Session::raw(
            connection,
            url,
            web_transport_quinn::http::StatusCode::OK,
        )
    };

    // === MOQT SETUP exchange ===
    let session = Arc::new(MoqtSession::accept(wt_session).await?);

    let session_id = state.lock().await.register_session(session.clone());
    info!(session_id, "session established");

    // === Main loop: handle events until the connection closes ===
    loop {
        let event = match session.next_event().await {
            Ok(event) => event,
            Err(_) => break, // connection closed
        };

        let state = state.clone();
        match event {
            SessionEvent::Subscribe(request) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_subscribe(session_id, request, state).await {
                        error!(session_id, error = %e, "subscribe error");
                    }
                });
            }
            SessionEvent::PublishNamespace(mut request) => {
                let ns = request.message.track_namespace.clone();
                tokio::spawn(async move {
                    state
                        .lock()
                        .await
                        .register_namespace(ns.clone(), session_id);
                    info!(session_id, namespace = ?ns, "namespace registered");
                    if let Err(e) = request.accept().await {
                        error!(session_id, error = %e, "publish_namespace error");
                    }
                });
            }
            SessionEvent::DataStream(subgroup_reader) => {
                tokio::spawn(async move {
                    if let Err(e) = handle_data_stream(session_id, subgroup_reader, state).await {
                        error!(session_id, error = %e, "data stream error");
                    }
                });
            }
        }
    }

    // === Cleanup on disconnect ===
    debug!(session_id, "session disconnected");
    state.lock().await.remove_session(session_id);

    Ok(())
}
