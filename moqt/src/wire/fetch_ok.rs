//! # fetch_ok: FETCH_OK message (Section 9.15)
//!
//! Success response to a FETCH request. Contains the actual end location
//! of the data being delivered and whether the track is complete.

use anyhow::{Result, ensure};

use super::parameter::{MessageParameter, decode_parameters, encode_parameters};
use super::{MSG_FETCH_OK, decode_message, encode_message};
use crate::wire::varint::{decode_varint, encode_varint};

/// FETCH_OK message. Success response to a FETCH request.
///
/// ```text
/// Type (vi64) = 0x18,
/// Length (u16),
/// End Of Track (u8),
/// End Location { Group (vi64), Object (vi64) },
/// Number of Parameters (vi64),
/// Parameters (..) ...,
/// Track Properties (..)
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchOkMessage {
    /// True if all Objects have been published on this Track.
    pub end_of_track: bool,
    /// End Location group (inclusive boundary).
    pub end_group: u64,
    /// End Location object.
    pub end_object: u64,
    /// Response parameters.
    pub parameters: Vec<MessageParameter>,
    /// Track Properties as raw bytes (forwarded as-is).
    pub track_properties_raw: Vec<u8>,
}

impl FetchOkMessage {
    pub fn encode(&self, buf: &mut Vec<u8>) -> Result<()> {
        let mut payload = Vec::new();
        payload.push(if self.end_of_track { 1 } else { 0 });
        encode_varint(self.end_group, &mut payload);
        encode_varint(self.end_object, &mut payload);
        encode_parameters(&self.parameters, &mut payload)?;
        payload.extend_from_slice(&self.track_properties_raw);
        encode_message(MSG_FETCH_OK, &payload, buf);
        Ok(())
    }

    pub fn decode(buf: &mut &[u8]) -> Result<Self> {
        let (msg_type, payload) = decode_message(buf)?;
        ensure!(
            msg_type == MSG_FETCH_OK,
            "expected FETCH_OK (0x{MSG_FETCH_OK:X}), got 0x{msg_type:X}"
        );
        let mut p = payload.as_slice();
        ensure!(!p.is_empty(), "FETCH_OK payload truncated");
        let end_of_track = p[0] != 0;
        p = &p[1..];
        let end_group = decode_varint(&mut p)?;
        let end_object = decode_varint(&mut p)?;
        let parameters = decode_parameters(&mut p)?;
        let track_properties_raw = p.to_vec();
        Ok(FetchOkMessage {
            end_of_track,
            end_group,
            end_object,
            parameters,
            track_properties_raw,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip(msg: &FetchOkMessage) {
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        let mut slice = buf.as_slice();
        let decoded = FetchOkMessage::decode(&mut slice).unwrap();
        assert_eq!(msg, &decoded);
        assert!(slice.is_empty());
    }

    #[test]
    fn basic() {
        roundtrip(&FetchOkMessage {
            end_of_track: false,
            end_group: 5,
            end_object: 11,
            parameters: vec![],
            track_properties_raw: vec![],
        });
    }

    #[test]
    fn end_of_track_true() {
        roundtrip(&FetchOkMessage {
            end_of_track: true,
            end_group: 10,
            end_object: 3,
            parameters: vec![],
            track_properties_raw: vec![],
        });
    }

    #[test]
    fn with_track_properties() {
        roundtrip(&FetchOkMessage {
            end_of_track: false,
            end_group: 0,
            end_object: 0,
            parameters: vec![],
            track_properties_raw: vec![0x02, 0x05, 0x00],
        });
    }

    #[test]
    fn wrong_message_type_is_error() {
        let mut buf = Vec::new();
        encode_message(0x03, &[], &mut buf);
        let mut slice = buf.as_slice();
        assert!(FetchOkMessage::decode(&mut slice).is_err());
    }

    /// Expected wire bytes for: end_of_track=false, end=(5,11), no params, no props
    ///
    /// Payload (4 bytes): eot(1) + group(1) + object(1) + params(1) = 4
    const FETCH_OK_BASIC: &[u8] = &[
        0x18, // Type: FETCH_OK
        0x00, 0x04, // Length: 4 bytes
        0x00, // End Of Track: false
        0x05, // End Group: 5
        0x0B, // End Object: 11
        0x00, // Number of Parameters: 0
    ];

    #[test]
    fn encode_known_bytes() {
        let msg = FetchOkMessage {
            end_of_track: false,
            end_group: 5,
            end_object: 11,
            parameters: vec![],
            track_properties_raw: vec![],
        };
        let mut buf = Vec::new();
        msg.encode(&mut buf).unwrap();
        assert_eq!(buf, FETCH_OK_BASIC);
    }

    #[test]
    fn decode_known_bytes() {
        let mut slice = FETCH_OK_BASIC;
        let decoded = FetchOkMessage::decode(&mut slice).unwrap();
        assert!(!decoded.end_of_track);
        assert_eq!(decoded.end_group, 5);
        assert_eq!(decoded.end_object, 11);
        assert!(slice.is_empty());
    }
}
