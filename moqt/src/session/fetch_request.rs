//! # fetch_request: Incoming FETCH request (relay side)
//!
//! Represents a FETCH that has been received but not yet responded to.
//! The holder can inspect the request, then accept (FETCH_OK) or reject
//! (REQUEST_ERROR).

use anyhow::Result;

use crate::stream::request::RequestStreamWriter;
use crate::wire::fetch::FetchMessage;
use crate::wire::fetch_ok::FetchOkMessage;
use crate::wire::reason_phrase::ReasonPhrase;
use crate::wire::request_error::RequestErrorMessage;

/// An incoming FETCH request that has not yet been responded to.
pub struct FetchRequest {
    /// The received FETCH message.
    pub message: FetchMessage,
    writer: RequestStreamWriter,
}

impl FetchRequest {
    pub(crate) fn new(message: FetchMessage, writer: RequestStreamWriter) -> Self {
        Self { message, writer }
    }

    /// Accept the FETCH by sending FETCH_OK.
    pub async fn accept(&mut self, ok: &FetchOkMessage) -> Result<()> {
        self.writer.write_fetch_ok(ok).await
    }

    /// Reject the FETCH by sending REQUEST_ERROR.
    pub async fn reject(&mut self, error_code: u64, reason: &str) -> Result<()> {
        let err = RequestErrorMessage {
            error_code,
            retry_interval: 0,
            reason_phrase: ReasonPhrase::from(reason),
        };
        self.writer.write_request_error(&err).await
    }
}
