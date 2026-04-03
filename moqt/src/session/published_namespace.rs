//! # published_namespace: Established PUBLISH_NAMESPACE (publisher side)
//!
//! Represents a published namespace after REQUEST_OK has been received.
//! Holds the bidi stream open so the relay does not remove the namespace.

use crate::stream::request::{RequestStreamReader, RequestStreamWriter};

/// An established published namespace (publisher side).
/// Created by `MoqtSession::publish_namespace()` after receiving REQUEST_OK.
///
/// The bidi stream stays open as long as this value is alive.
/// Dropping it closes the stream, which tells the relay to remove the namespace.
pub struct PublishedNamespace {
    _writer: RequestStreamWriter,
    _reader: RequestStreamReader,
}

impl PublishedNamespace {
    pub(crate) fn new(writer: RequestStreamWriter, reader: RequestStreamReader) -> Self {
        Self {
            _writer: writer,
            _reader: reader,
        }
    }
}
