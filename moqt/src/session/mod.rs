//! # session: MOQT session management
//!
//! Protocol logic and high-level API for MOQT sessions.
//!
//! The main entry points are `MoqtSession` (session lifecycle) and
//! `SessionEvent` (incoming events). Other types represent individual
//! protocol interactions:
//!
//! - `subgroup`: High-level Subgroup reader/writer (hides ObjectHeader)
//! - `subscribe_request`: Incoming SUBSCRIBE request handler
//! - `publish_namespace_request`: Incoming PUBLISH_NAMESPACE request handler
//! - `subscription`: Established subscription state

pub mod fetch_request;
pub mod publish_namespace_request;
pub mod published_namespace;
pub mod subgroup;
pub mod subscribe_request;
pub mod subscription;

use std::sync::atomic::{AtomicU64, Ordering};

use anyhow::Result;

use crate::session::fetch_request::FetchRequest;
use crate::session::publish_namespace_request::PublishNamespaceRequest;
use crate::session::subgroup::{SubgroupReader, SubgroupWriter};
use crate::session::subscribe_request::SubscribeRequest;
use crate::session::published_namespace::PublishedNamespace;
use crate::session::subscription::Subscription;
use crate::stream::control::{ControlStreamReader, ControlStreamWriter};
use crate::stream::data::{DataStreamReader, DataStreamWriter};
use crate::stream::request::{RequestMessage, RequestStreamReader, RequestStreamWriter};
use crate::wire::fetch::FetchMessage;
use crate::wire::fetch_ok::FetchOkMessage;
use crate::wire::parameter::MessageParameter;
use crate::wire::publish_namespace::PublishNamespaceMessage;
use crate::wire::setup::{SetupMessage, SetupOption};
use crate::wire::subgroup_header::SubgroupHeader;
use crate::wire::subscribe::SubscribeMessage;
use crate::wire::track_namespace::TrackNamespace;

// === RequestError ===

/// Error returned when a request (SUBSCRIBE, PUBLISH_NAMESPACE, etc.)
/// is rejected by the peer via REQUEST_ERROR, or an unexpected message
/// is received on a request stream.
#[derive(Debug, thiserror::Error)]
pub enum RequestError {
    /// Peer responded with REQUEST_ERROR.
    #[error("request rejected: code=0x{status_code:X}, reason={reason}")]
    Rejected { status_code: u64, reason: String },

    /// Received an unexpected message type on the request stream.
    #[error("unexpected message: expected {expected}")]
    UnexpectedMessage { expected: &'static str },
}

// === RequestIdAllocator ===

/// Request ID allocator.
///
/// MOQT assigns a unique ID to each request (SUBSCRIBE, PUBLISH_NAMESPACE, etc.).
/// Even/odd parity distinguishes the originator:
/// - Client (publisher/subscriber): even (0, 2, 4, ...)
/// - Server (relay): odd (1, 3, 5, ...)
///
/// This scheme ensures both sides can independently generate IDs without collision.
/// Uses atomic operations so it can be shared across tasks via `&self`.
struct RequestIdAllocator {
    next_id: AtomicU64,
}

impl RequestIdAllocator {
    /// Create a client allocator (even IDs: 0, 2, 4, ...).
    fn client() -> Self {
        Self {
            next_id: AtomicU64::new(0),
        }
    }

    /// Create a server allocator (odd IDs: 1, 3, 5, ...).
    fn server() -> Self {
        Self {
            next_id: AtomicU64::new(1),
        }
    }

    /// Allocate the next request ID. Increments by 2 each time.
    fn allocate(&self) -> u64 {
        self.next_id.fetch_add(2, Ordering::Relaxed)
    }
}

// === SessionEvent ===

/// An event received on the session.
pub enum SessionEvent {
    /// A SUBSCRIBE request was received on a bidi stream.
    Subscribe(SubscribeRequest),
    /// A FETCH request was received on a bidi stream.
    Fetch(FetchRequest),
    /// A PUBLISH_NAMESPACE request was received on a bidi stream.
    PublishNamespace(PublishNamespaceRequest),
    /// A data stream was received on a uni stream.
    DataStream(SubgroupReader),
}

// === MoqtSession ===

/// A MOQT session over a QUIC connection.
/// Created after SETUP exchange is complete.
///
/// Holds the two control streams (one per peer) for the session lifetime.
/// Dropping the writer would send FIN, which is a protocol violation (Section 3.3).
pub struct MoqtSession {
    session: web_transport_quinn::Session,
    request_id_alloc: RequestIdAllocator,
    /// Writer for this peer's control stream (must not be dropped).
    _ctrl_writer: ControlStreamWriter,
    /// Reader for the peer's control stream (for future GOAWAY reception).
    _ctrl_reader: ControlStreamReader,
}

impl MoqtSession {
    /// Create a MOQT session from a transport session.
    /// Performs the SETUP exchange (client side: sends Path + Authority options).
    pub async fn connect(session: web_transport_quinn::Session) -> Result<Self> {
        let ctrl_send = session.open_uni().await?;
        let mut ctrl_writer = ControlStreamWriter::new(ctrl_send);
        let setup = SetupMessage {
            setup_options: vec![
                SetupOption::Path(b"/".to_vec()),
                SetupOption::Authority(b"localhost".to_vec()),
            ],
        };
        ctrl_writer.write_setup(&setup).await?;

        let recv = session.accept_uni().await?;
        let mut ctrl_reader = ControlStreamReader::new(recv);
        let _server_setup = ctrl_reader.read_setup().await?;

        Ok(Self {
            session,
            request_id_alloc: RequestIdAllocator::client(),
            _ctrl_writer: ctrl_writer,
            _ctrl_reader: ctrl_reader,
        })
    }

    /// Accept a MOQT session from a transport session.
    /// Performs the SETUP exchange (server side: sends empty SETUP).
    pub async fn accept(session: web_transport_quinn::Session) -> Result<Self> {
        let ctrl_send = session.open_uni().await?;
        let mut ctrl_writer = ControlStreamWriter::new(ctrl_send);
        let setup = SetupMessage {
            setup_options: vec![],
        };
        ctrl_writer.write_setup(&setup).await?;

        let recv = session.accept_uni().await?;
        let mut ctrl_reader = ControlStreamReader::new(recv);
        let _client_setup = ctrl_reader.read_setup().await?;

        Ok(Self {
            session,
            request_id_alloc: RequestIdAllocator::server(),
            _ctrl_writer: ctrl_writer,
            _ctrl_reader: ctrl_reader,
        })
    }

    /// Register a namespace with the peer.
    /// Opens a bidi stream, sends PUBLISH_NAMESPACE, and waits for REQUEST_OK.
    /// Returns an error if the peer responds with REQUEST_ERROR.
    pub async fn publish_namespace(
        &self,
        namespace: TrackNamespace,
    ) -> Result<PublishedNamespace> {
        let (send, recv) = self.session.open_bi().await?;
        let mut writer = RequestStreamWriter::new(send);
        let mut reader = RequestStreamReader::new(recv);

        let msg = PublishNamespaceMessage {
            request_id: self.request_id_alloc.allocate(),
            required_request_id_delta: 0,
            track_namespace: namespace,
        };
        writer.write_publish_namespace(&msg).await?;

        let response = reader.read_message().await?;
        match response {
            RequestMessage::RequestOk(_) => Ok(PublishedNamespace::new(writer, reader)),
            RequestMessage::RequestError(err) => Err(RequestError::Rejected {
                status_code: err.error_code,
                reason: String::from_utf8_lossy(&err.reason_phrase.value).into(),
            }
            .into()),
            _ => Err(RequestError::UnexpectedMessage {
                expected: "REQUEST_OK or REQUEST_ERROR",
            }
            .into()),
        }
    }

    /// Subscribe to a track.
    /// Opens a bidi stream, sends SUBSCRIBE, and waits for SUBSCRIBE_OK.
    /// Returns a `Subscription` that can be used to receive PUBLISH_DONE.
    pub async fn subscribe(
        &self,
        namespace: TrackNamespace,
        track_name: &str,
        parameters: Vec<MessageParameter>,
    ) -> Result<Subscription> {
        let (send, recv) = self.session.open_bi().await?;
        let mut writer = RequestStreamWriter::new(send);
        let mut reader = RequestStreamReader::new(recv);

        let msg = SubscribeMessage {
            request_id: self.request_id_alloc.allocate(),
            required_request_id_delta: 0,
            track_namespace: namespace,
            track_name: track_name.as_bytes().to_vec(),
            parameters,
        };
        writer.write_subscribe(&msg).await?;

        let response = reader.read_message().await?;
        match response {
            RequestMessage::SubscribeOk(ok) => Ok(Subscription::new(ok, reader)),
            RequestMessage::RequestError(err) => Err(RequestError::Rejected {
                status_code: err.error_code,
                reason: String::from_utf8_lossy(&err.reason_phrase.value).into(),
            }
            .into()),
            _ => Err(RequestError::UnexpectedMessage {
                expected: "SUBSCRIBE_OK or REQUEST_ERROR",
            }
            .into()),
        }
    }

    /// Send a FETCH request (Relative Joining).
    /// Opens a bidi stream, sends FETCH, and waits for FETCH_OK.
    /// Returns the FETCH_OK message on success.
    pub async fn fetch(
        &self,
        request_id: u64,
        joining_request_id: u64,
        joining_start: u64,
    ) -> Result<FetchOkMessage> {
        let (send, recv) = self.session.open_bi().await?;
        let mut writer = RequestStreamWriter::new(send);
        let mut reader = RequestStreamReader::new(recv);

        let msg = FetchMessage {
            request_id,
            required_request_id_delta: 0,
            fetch_type: crate::wire::fetch::FETCH_TYPE_RELATIVE_JOINING,
            joining_request_id,
            joining_start,
            parameters: vec![],
        };
        writer.write_fetch(&msg).await?;

        let response = reader.read_message().await?;
        match response {
            RequestMessage::FetchOk(ok) => Ok(ok),
            RequestMessage::RequestError(err) => Err(RequestError::Rejected {
                status_code: err.error_code,
                reason: String::from_utf8_lossy(&err.reason_phrase.value).into(),
            }
            .into()),
            _ => Err(RequestError::UnexpectedMessage {
                expected: "FETCH_OK or REQUEST_ERROR",
            }
            .into()),
        }
    }

    /// Wait for the next event on this session.
    /// Concurrently waits for a bidi request or a uni data stream.
    /// Only `accept_bi()` / `accept_uni()` are inside the `select!`,
    /// so cancellation of the losing branch is safe (no data consumed).
    pub async fn next_event(&self) -> Result<SessionEvent> {
        tokio::select! {
            bi = self.session.accept_bi() => {
                let (send, recv) = bi?;
                let writer = RequestStreamWriter::new(send);
                let mut reader = RequestStreamReader::new(recv);
                let msg = reader.read_message().await?;
                match msg {
                    RequestMessage::Subscribe(sub) => {
                        Ok(SessionEvent::Subscribe(SubscribeRequest::new(sub, writer)))
                    }
                    RequestMessage::Fetch(fetch) => {
                        Ok(SessionEvent::Fetch(FetchRequest::new(fetch, writer)))
                    }
                    RequestMessage::PublishNamespace(pub_ns) => {
                        Ok(SessionEvent::PublishNamespace(
                            PublishNamespaceRequest::new(pub_ns, writer),
                        ))
                    }
                    _ => Err(RequestError::UnexpectedMessage {
                        expected: "SUBSCRIBE, FETCH, or PUBLISH_NAMESPACE",
                    }
                    .into()),
                }
            }
            uni = self.session.accept_uni() => {
                let recv = uni?;
                let mut reader = DataStreamReader::new(recv);
                let (header, _raw) = reader.read_subgroup_header().await?;
                Ok(SessionEvent::DataStream(SubgroupReader::new(header, reader)))
            }
        }
    }

    /// Open a subgroup for writing objects (no Object Properties).
    pub async fn open_subgroup(
        &self,
        track_alias: u64,
        group_id: u64,
        subgroup_id: u64,
    ) -> Result<SubgroupWriter> {
        self.open_subgroup_inner(track_alias, group_id, subgroup_id, false)
            .await
    }

    /// Open a subgroup with the PROPERTIES flag set.
    /// Objects must be written with `write_object_with_properties`.
    pub async fn open_subgroup_with_properties(
        &self,
        track_alias: u64,
        group_id: u64,
        subgroup_id: u64,
    ) -> Result<SubgroupWriter> {
        self.open_subgroup_inner(track_alias, group_id, subgroup_id, true)
            .await
    }

    async fn open_subgroup_inner(
        &self,
        track_alias: u64,
        group_id: u64,
        subgroup_id: u64,
        has_properties: bool,
    ) -> Result<SubgroupWriter> {
        let header = SubgroupHeader {
            track_alias,
            group_id,
            has_properties,
            end_of_group: true,
            subgroup_id: Some(subgroup_id),
            publisher_priority: None,
        };
        let writer = self.open_data_stream(&header).await?;
        Ok(SubgroupWriter::new(writer, has_properties))
    }

    /// Open an outgoing data stream (unidirectional).
    /// Writes the SubgroupHeader and returns a DataStreamWriter
    /// for writing subsequent Objects.
    /// For low-level access (e.g. relay pass-through). Prefer `open_subgroup` for clients.
    pub async fn open_data_stream(&self, header: &SubgroupHeader) -> Result<DataStreamWriter> {
        let uni = self.session.open_uni().await?;
        let mut writer = DataStreamWriter::new(uni);
        writer.write_subgroup_header(header).await?;
        Ok(writer)
    }

    /// Accept a raw unidirectional receive stream.
    /// Used for receiving FETCH_HEADER data streams.
    pub async fn accept_uni_stream(&self) -> Result<web_transport_quinn::RecvStream> {
        Ok(self.session.accept_uni().await?)
    }

    /// Open a raw unidirectional send stream.
    /// Used for FETCH_HEADER data streams.
    pub async fn open_uni_stream(&self) -> Result<web_transport_quinn::SendStream> {
        Ok(self.session.open_uni().await?)
    }

    /// Close the session.
    pub fn close(&self) {
        self.session.close(0u32, b"done");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn client_generates_even() {
        let alloc = RequestIdAllocator::client();
        assert_eq!(alloc.allocate(), 0);
        assert_eq!(alloc.allocate(), 2);
        assert_eq!(alloc.allocate(), 4);
    }

    #[test]
    fn server_generates_odd() {
        let alloc = RequestIdAllocator::server();
        assert_eq!(alloc.allocate(), 1);
        assert_eq!(alloc.allocate(), 3);
        assert_eq!(alloc.allocate(), 5);
    }
}
