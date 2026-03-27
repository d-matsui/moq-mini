//! # fetch_header: FETCH_HEADER data stream (Section 10.4.4)
//!
//! Written at the beginning of a unidirectional stream carrying FETCH data.
//! After the header, objects follow with serialization flags controlling
//! which fields are present.
//!
//! This module provides encode-only support (relay → subscriber).

use crate::wire::varint::encode_varint;

/// FETCH_HEADER stream type.
pub const FETCH_HEADER_TYPE: u64 = 0x05;

// Serialization flag bits
const FLAG_SUBGROUP_EXPLICIT: u64 = 0x03; // bits 0-1: Subgroup ID field present
const FLAG_OBJECT_ID_PRESENT: u64 = 0x04; // bit 2
const FLAG_GROUP_ID_PRESENT: u64 = 0x08; // bit 3
const FLAG_PRIORITY_PRESENT: u64 = 0x10; // bit 4

/// Encode the FETCH_HEADER (written at the start of a uni stream).
///
/// ```text
/// Type (vi64) = 0x05,
/// Request ID (vi64)
/// ```
pub fn encode_fetch_header(request_id: u64, buf: &mut Vec<u8>) {
    encode_varint(FETCH_HEADER_TYPE, buf);
    encode_varint(request_id, buf);
}

/// Fields for a single object on a FETCH_HEADER stream.
pub struct FetchObjectFields {
    pub group_id: u64,
    pub subgroup_id: u64,
    pub object_id: u64,
    pub publisher_priority: u8,
    pub payload: Vec<u8>,
}

/// Encode a single object on a FETCH_HEADER stream.
///
/// Uses a simple encoding strategy: always include all fields for the
/// first object, and for subsequent objects, include only the fields
/// that differ from the previous object. For correctness and simplicity,
/// Group ID and Object ID are always included.
///
/// ```text
/// Serialization Flags (vi64),
/// [Group ID (vi64),]
/// [Subgroup ID (vi64),]
/// [Object ID (vi64),]
/// [Publisher Priority (u8),]
/// Object Payload Length (vi64),
/// [Object Payload (..)]
/// ```
pub fn encode_fetch_object(
    obj: &FetchObjectFields,
    is_first: bool,
    prev_group: Option<u64>,
    prev_priority: Option<u8>,
    buf: &mut Vec<u8>,
) {
    let group_id = obj.group_id;
    let subgroup_id = obj.subgroup_id;
    let object_id = obj.object_id;
    let publisher_priority = obj.publisher_priority;
    let payload = &obj.payload;
    let mut flags: u64 = FLAG_OBJECT_ID_PRESENT; // Object ID always present

    // Subgroup ID encoding: always explicit (0x03)
    flags |= FLAG_SUBGROUP_EXPLICIT;

    // Group ID: present if first object or group changed
    if is_first || prev_group != Some(group_id) {
        flags |= FLAG_GROUP_ID_PRESENT;
    }

    // Priority: present if first object or priority changed
    if is_first || prev_priority != Some(publisher_priority) {
        flags |= FLAG_PRIORITY_PRESENT;
    }

    encode_varint(flags, buf);

    if flags & FLAG_GROUP_ID_PRESENT != 0 {
        encode_varint(group_id, buf);
    }

    // Subgroup ID (always explicit in our encoding)
    encode_varint(subgroup_id, buf);

    // Object ID (always present)
    encode_varint(object_id, buf);

    if flags & FLAG_PRIORITY_PRESENT != 0 {
        buf.push(publisher_priority);
    }

    // No properties (flag 0x20 not set)

    encode_varint(payload.len() as u64, buf);
    buf.extend_from_slice(payload);
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_fetch_header_known_bytes() {
        let mut buf = Vec::new();
        encode_fetch_header(2, &mut buf);
        assert_eq!(buf, &[0x05, 0x02]); // type=5, request_id=2
    }

    #[test]
    fn encode_first_object_includes_all_fields() {
        let mut buf = Vec::new();
        let obj = FetchObjectFields {
            group_id: 5,
            subgroup_id: 0,
            object_id: 3,
            publisher_priority: 128,
            payload: b"hello".to_vec(),
        };
        encode_fetch_object(&obj, true, None, None, &mut buf);

        // Decode manually
        let mut slice = buf.as_slice();
        let flags = crate::wire::varint::decode_varint(&mut slice).unwrap();
        // Should have: subgroup_explicit(0x03) | object_id(0x04) | group_id(0x08) | priority(0x10)
        assert_eq!(flags, 0x1F);

        let group = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(group, 5);

        let subgroup = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(subgroup, 0);

        let object = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(object, 3);

        let priority = slice[0];
        slice = &slice[1..];
        assert_eq!(priority, 128);

        let payload_len = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(payload_len, 5);
        assert_eq!(slice, b"hello");
    }

    #[test]
    fn encode_subsequent_object_same_group_omits_group_and_priority() {
        let mut buf = Vec::new();
        let obj = FetchObjectFields {
            group_id: 5,
            subgroup_id: 0,
            object_id: 4,
            publisher_priority: 128,
            payload: b"world".to_vec(),
        };
        encode_fetch_object(&obj, false, Some(5), Some(128), &mut buf);

        let mut slice = buf.as_slice();
        let flags = crate::wire::varint::decode_varint(&mut slice).unwrap();
        // Should have: subgroup_explicit(0x03) | object_id(0x04) only
        assert_eq!(flags, 0x07);

        // No group_id field
        let subgroup = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(subgroup, 0);

        let object = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(object, 4);

        // No priority field
        let payload_len = crate::wire::varint::decode_varint(&mut slice).unwrap();
        assert_eq!(payload_len, 5);
        assert_eq!(slice, b"world");
    }

    #[test]
    fn encode_object_with_group_change() {
        let mut buf = Vec::new();
        let obj = FetchObjectFields {
            group_id: 6,
            subgroup_id: 0,
            object_id: 0,
            publisher_priority: 128,
            payload: b"new".to_vec(),
        };
        encode_fetch_object(&obj, false, Some(5), Some(128), &mut buf);

        let mut slice = buf.as_slice();
        let flags = crate::wire::varint::decode_varint(&mut slice).unwrap();
        // Should have: subgroup_explicit(0x03) | object_id(0x04) | group_id(0x08)
        assert_eq!(flags, 0x0F);
    }
}
