//! # subscription: Established MOQT subscription (subscriber side)
//!
//! Represents a subscription after SUBSCRIBE_OK has been received.
//! Holds the bidi stream's recv side to receive PUBLISH_DONE later.

use anyhow::Result;

use crate::session::RequestError;
use crate::stream::request::{RequestMessage, RequestStreamReader};
use crate::wire::publish_done::PublishDoneMessage;
use crate::wire::subscribe_ok::SubscribeOkMessage;

/// An established subscription (subscriber side).
/// Created by `MoqtSession::subscribe()` after receiving SUBSCRIBE_OK.
pub struct Subscription {
    /// The SUBSCRIBE_OK message received from the publisher.
    pub subscribe_ok: SubscribeOkMessage,
    reader: RequestStreamReader,
}

impl Subscription {
    pub(crate) fn new(subscribe_ok: SubscribeOkMessage, reader: RequestStreamReader) -> Self {
        Self {
            subscribe_ok,
            reader,
        }
    }

    /// Track alias assigned by the publisher.
    pub fn track_alias(&self) -> u64 {
        self.subscribe_ok.track_alias
    }

    /// Wait for PUBLISH_DONE from the publisher.
    /// Returns `Ok(None)` if the stream closed without PUBLISH_DONE (FIN).
    pub async fn recv_publish_done(&mut self) -> Result<Option<PublishDoneMessage>> {
        let msg = match self.reader.try_read_message().await? {
            Some(msg) => msg,
            None => return Ok(None),
        };
        match msg {
            RequestMessage::PublishDone(done) => Ok(Some(done)),
            _ => Err(RequestError::UnexpectedMessage {
                expected: "PUBLISH_DONE",
            }
            .into()),
        }
    }
}
