//! # fetch: FETCH message (Section 9.14)
//!
//! Sent by a subscriber to request past objects. Only Relative Joining Fetch
//! (Fetch Type 0x2) is supported by this implementation.
//!
//! A Relative Joining Fetch references an existing SUBSCRIBE by its Request ID
//! (the Joining Request ID) and specifies how many groups back to fetch
//! (the Joining Start). The range is computed as:
//!   - Start Location = {Joining Location.Group - Joining Start, 0}
//!   - End Location = {Joining Location.Group, Joining Location.Object + 1}

use anyhow::{Result, ensure};

use super::parameter::{MessageParameter, decode_parameters, encode_parameters};
use super::{MSG_FETCH, decode_message, encode_message};
use crate::wire::varint::{decode_varint, encode_varint};

/// Fetch Type: Relative Joining Fetch
pub const FETCH_TYPE_RELATIVE_JOINING: u64 = 0x2;

/// FETCH message (Relative Joining Fetch only).
///
/// ```text
/// Type (vi64) = 0x16,
/// Length (u16),
/// Request ID (vi64),
/// Required Request ID Delta (vi64),
/// Fetch Type (vi64) = 0x2,
/// Joining Request ID (vi64),
/// Joining Start (vi64),
/// Number of Parameters (vi64),
/// Parameters (..) ...
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchMessage {
    /// Unique ID for this FETCH request.
    pub request_id: u64,
    /// ID delta from a dependent prior request.
    pub required_request_id_delta: u64,
    /// Fetch type. Only FETCH_TYPE_RELATIVE_JOINING (0x2) is supported.
    pub fetch_type: u64,
    /// Request ID of the SUBSCRIBE to join.
    pub joining_request_id: u64,
    /// Number of groups back from Joining Location to start fetching.
    pub joining_start: u64,
    /// Message parameters.
    pub parameters: Vec<MessageParameter>,
}

impl FetchMessage {
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let mut payload = Vec::new();
        encode_varint(self.request_id, &mut payload);
        encode_varint(self.required_request_id_delta, &mut payload);
        encode_varint(self.fetch_type, &mut payload);
        encode_varint(self.joining_request_id, &mut payload);
        encode_varint(self.joining_start, &mut payload);
        encode_parameters(&self.parameters, &mut payload)?;
        encode_message(MSG_FETCH, &payload, buf);
        Ok(())
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self> {
        let (msg_type, payload) = decode_message(buf)?;
        ensure!(
            msg_type == MSG_FETCH,
            "expected FETCH (0x{MSG_FETCH:X}), got 0x{msg_type:X}"
        );
        let mut p = payload.as_slice();
        let request_id = decode_varint(&mut p)?;
        let required_request_id_delta = decode_varint(&mut p)?;
        let fetch_type = decode_varint(&mut p)?;
        ensure!(
            fetch_type == FETCH_TYPE_RELATIVE_JOINING,
            "only Relative Joining Fetch (0x2) is supported, got 0x{fetch_type:X}"
        );
        let joining_request_id = decode_varint(&mut p)?;
        let joining_start = decode_varint(&mut p)?;
        let parameters = decode_parameters(&mut p)?;
        Ok(FetchMessage {
            request_id,
            required_request_id_delta,
            fetch_type,
            joining_request_id,
            joining_start,
            parameters,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &FetchMessage) {
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let mut slice = buf.as_slice();
        let decoded = FetchMessage::decode(&mut slice).unwrap();
        assert_eq!(msg, &decoded);
        assert!(slice.is_empty());
    }

    #[test]
    fn basic_relative_joining() {
        roundtrip(&FetchMessage {
            request_id: 2,
            required_request_id_delta: 0,
            fetch_type: FETCH_TYPE_RELATIVE_JOINING,
            joining_request_id: 0,
            joining_start: 3,
            parameters: vec![],
        });
    }

    #[test]
    fn with_nonzero_delta() {
        roundtrip(&FetchMessage {
            request_id: 4,
            required_request_id_delta: 1,
            fetch_type: FETCH_TYPE_RELATIVE_JOINING,
            joining_request_id: 2,
            joining_start: 5,
            parameters: vec![],
        });
    }

    #[test]
    fn wrong_message_type_is_error() {
        let mut buf = Vec::new();
        encode_message(0x03, &[], &mut buf); // SUBSCRIBE type
        let mut slice = buf.as_slice();
        assert!(FetchMessage::decode(&mut slice).is_err());
    }

    #[test]
    fn unsupported_fetch_type_is_error() {
        // Encode a FETCH with Standalone type (0x1)
        let mut payload = Vec::new();
        encode_varint(0, &mut payload); // request_id
        encode_varint(0, &mut payload); // delta
        encode_varint(0x1, &mut payload); // fetch_type = Standalone
        encode_varint(0, &mut payload); // dummy
        let mut buf = Vec::new();
        encode_message(MSG_FETCH, &payload, &mut buf);
        let mut slice = buf.as_slice();
        assert!(FetchMessage::decode(&mut slice).is_err());
    }

    /// Expected wire bytes for: request_id=2, delta=0, type=0x2,
    /// joining_request_id=0, joining_start=3, no params
    ///
    /// Payload (6 bytes): rid(1) + delta(1) + ftype(1) + jrid(1) + jstart(1) + params(1) = 6
    const FETCH_BASIC: &[u8] = &[
        0x16, // Type: FETCH
        0x00, 0x06, // Length: 6 bytes
        0x02, // Request ID: 2
        0x00, // Required Request ID Delta: 0
        0x02, // Fetch Type: Relative Joining
        0x00, // Joining Request ID: 0
        0x03, // Joining Start: 3
        0x00, // Number of Parameters: 0
    ];

    #[test]
    fn encode_known_bytes() {
        let msg = FetchMessage {
            request_id: 2,
            required_request_id_delta: 0,
            fetch_type: FETCH_TYPE_RELATIVE_JOINING,
            joining_request_id: 0,
            joining_start: 3,
            parameters: vec![],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf, FETCH_BASIC);
    }

    #[test]
    fn decode_known_bytes() {
        let mut slice = FETCH_BASIC;
        let decoded = FetchMessage::decode(&mut slice).unwrap();
        assert_eq!(decoded.request_id, 2);
        assert_eq!(decoded.fetch_type, FETCH_TYPE_RELATIVE_JOINING);
        assert_eq!(decoded.joining_request_id, 0);
        assert_eq!(decoded.joining_start, 3);
        assert!(slice.is_empty());
    }
}
