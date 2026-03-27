//! # fetch_data: FETCH_HEADER data stream writer
//!
//! Writes a FETCH_HEADER followed by objects on a unidirectional stream.
//! Used by the relay to deliver cached objects to a subscriber in response
//! to a FETCH request.

use anyhow::Result;
use web_transport_quinn::SendStream;

use crate::wire::fetch_header::{FetchObjectFields, encode_fetch_header, encode_fetch_object};

/// Writes FETCH_HEADER + objects to a unidirectional data stream.
pub struct FetchDataStreamWriter {
    stream: SendStream,
    prev_group: Option<u64>,
    prev_priority: Option<u8>,
    is_first: bool,
}

impl FetchDataStreamWriter {
    /// Create a new writer and send the FETCH_HEADER.
    pub async fn new(mut stream: SendStream, request_id: u64) -> Result<Self> {
        let mut buf = Vec::new();
        encode_fetch_header(request_id, &mut buf);
        stream.write_all(&buf).await?;
        Ok(Self {
            stream,
            prev_group: None,
            prev_priority: None,
            is_first: true,
        })
    }

    /// Write a single object to the FETCH stream.
    pub async fn write_object(
        &mut self,
        group_id: u64,
        subgroup_id: u64,
        object_id: u64,
        publisher_priority: u8,
        payload: &[u8],
    ) -> Result<()> {
        let obj = FetchObjectFields {
            group_id,
            subgroup_id,
            object_id,
            publisher_priority,
            payload: payload.to_vec(),
        };
        let mut buf = Vec::new();
        encode_fetch_object(
            &obj,
            self.is_first,
            self.prev_group,
            self.prev_priority,
            &mut buf,
        );
        self.stream.write_all(&buf).await?;

        self.prev_group = Some(group_id);
        self.prev_priority = Some(publisher_priority);
        self.is_first = false;
        Ok(())
    }

    /// Finish the stream (send FIN).
    pub fn finish(&mut self) -> Result<()> {
        self.stream.finish()?;
        Ok(())
    }
}
