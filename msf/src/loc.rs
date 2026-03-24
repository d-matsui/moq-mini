//! # loc: LOC Header Extensions (Section 2.3)
//!
//! LOC (Low Overhead Container) Header Extensions carry optional metadata
//! for media payloads. They are encoded as MOQ Object Header Extensions
//! within Object Properties.
//!
//! ## Encoding rules
//! - Even ID: Value is varint (no Length field on wire)
//! - Odd ID: Length (varint) + Value (raw bytes)
//!
//! ## Defined extensions
//! | ID | Name                | Type          |
//! |----|---------------------|---------------|
//! | 2  | Capture Timestamp   | varint (even) |
//! | 4  | Video Frame Marking | varint (even) |
//! | 6  | Audio Level         | varint (even) |
//! | 13 | Video Config        | bytes (odd)   |

use anyhow::{Result, bail, ensure};
use moqt_core::wire::varint::{decode_varint, encode_varint};

/// Extension ID for Capture Timestamp (Section 2.3.1.1).
pub const EXT_CAPTURE_TIMESTAMP: u64 = 2;
/// Extension ID for Video Frame Marking (Section 2.3.2.2).
pub const EXT_VIDEO_FRAME_MARKING: u64 = 4;
/// Extension ID for Audio Level (Section 2.3.3.1).
pub const EXT_AUDIO_LEVEL: u64 = 6;
/// Extension ID for Video Config (Section 2.3.2.1).
pub const EXT_VIDEO_CONFIG: u64 = 13;

/// A single LOC Header Extension.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LocExtension {
    /// Wall-clock capture time in microseconds since Unix epoch.
    CaptureTimestamp(u64),
    /// Video frame marking flags per RFC 9626.
    VideoFrameMarking(u64),
    /// Audio level + voice activity per RFC 6464.
    AudioLevel(u64),
    /// Video codec configuration ("extradata" / WebCodecs VideoDecoderConfig description).
    VideoConfig(Vec<u8>),
}

impl LocExtension {
    /// Extension ID on the wire.
    fn id(&self) -> u64 {
        match self {
            LocExtension::CaptureTimestamp(_) => EXT_CAPTURE_TIMESTAMP,
            LocExtension::VideoFrameMarking(_) => EXT_VIDEO_FRAME_MARKING,
            LocExtension::AudioLevel(_) => EXT_AUDIO_LEVEL,
            LocExtension::VideoConfig(_) => EXT_VIDEO_CONFIG,
        }
    }
}

/// Encode a list of LOC Header Extensions into a new byte vector.
pub fn encode_extensions(extensions: &[LocExtension]) -> Result<Vec<u8>> {
    let mut buf = Vec::new();
    encode_extensions_into(extensions, &mut buf);
    Ok(buf)
}

/// Encode a list of LOC Header Extensions into an existing buffer.
///
/// Even ID: varint ID + varint value
/// Odd ID: varint ID + varint length + raw bytes
pub fn encode_extensions_into(extensions: &[LocExtension], buf: &mut Vec<u8>) {
    for ext in extensions {
        encode_varint(ext.id(), buf);
        match ext {
            LocExtension::CaptureTimestamp(v)
            | LocExtension::VideoFrameMarking(v)
            | LocExtension::AudioLevel(v) => {
                // Even ID: value is varint, no length field
                encode_varint(*v, buf);
            }
            LocExtension::VideoConfig(data) => {
                // Odd ID: length (varint) + raw bytes
                encode_varint(data.len() as u64, buf);
                buf.extend_from_slice(data);
            }
        }
    }
}

/// Decode LOC Header Extensions from bytes.
pub fn decode_extensions(buf: &[u8]) -> Result<Vec<LocExtension>> {
    let mut slice = buf;
    let mut extensions = Vec::new();

    while !slice.is_empty() {
        let id = decode_varint(&mut slice)?;
        let ext = match id {
            EXT_CAPTURE_TIMESTAMP => {
                let v = decode_varint(&mut slice)?;
                LocExtension::CaptureTimestamp(v)
            }
            EXT_VIDEO_FRAME_MARKING => {
                let v = decode_varint(&mut slice)?;
                LocExtension::VideoFrameMarking(v)
            }
            EXT_AUDIO_LEVEL => {
                let v = decode_varint(&mut slice)?;
                LocExtension::AudioLevel(v)
            }
            EXT_VIDEO_CONFIG => {
                let len = decode_varint(&mut slice)? as usize;
                ensure!(slice.len() >= len, "video config truncated");
                let data = slice[..len].to_vec();
                slice = &slice[len..];
                LocExtension::VideoConfig(data)
            }
            _ => bail!("unknown LOC extension ID: {id}"),
        };
        extensions.push(ext);
    }

    Ok(extensions)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn encode_capture_timestamp() {
        let ext = LocExtension::CaptureTimestamp(1_000_000);
        let bytes = encode_extensions(&[ext]).unwrap();
        // ID=2 (even): varint ID + varint value, no length field
        let decoded = decode_extensions(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], LocExtension::CaptureTimestamp(1_000_000));
    }

    #[test]
    fn encode_video_config() {
        let config = vec![0x01, 0x42, 0x00, 0x1e]; // example SPS bytes
        let ext = LocExtension::VideoConfig(config.clone());
        let bytes = encode_extensions(&[ext]).unwrap();
        // ID=13 (odd): varint ID + varint length + raw bytes
        let decoded = decode_extensions(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0], LocExtension::VideoConfig(config));
    }

    #[test]
    fn encode_multiple_extensions() {
        let exts = vec![
            LocExtension::CaptureTimestamp(1_700_000_000_000_000),
            LocExtension::VideoFrameMarking(0x80), // independent frame
            LocExtension::VideoConfig(vec![0xAA, 0xBB]),
        ];
        let bytes = encode_extensions(&exts).unwrap();
        let decoded = decode_extensions(&bytes).unwrap();
        assert_eq!(decoded, exts);
    }

    #[test]
    fn encode_empty_extensions() {
        let bytes = encode_extensions(&[]).unwrap();
        assert!(bytes.is_empty());
        let decoded = decode_extensions(&bytes).unwrap();
        assert!(decoded.is_empty());
    }

    #[test]
    fn encode_audio_level() {
        let ext = LocExtension::AudioLevel(0x4F); // level=79, voice active
        let bytes = encode_extensions(&[ext]).unwrap();
        let decoded = decode_extensions(&bytes).unwrap();
        assert_eq!(decoded[0], LocExtension::AudioLevel(0x4F));
    }

    /// Known bytes test: Capture Timestamp = 1000000
    /// ID=2 (varint: 0x02), Value=1000000 (varint)
    /// 1000000 = 0x0F4240
    /// MOQT varint for 1000000: 3-byte (prefix 110, 21 usable bits)
    ///   0xC0 | (0x0F4240 >> 16) = 0xC0 | 0x0F = 0xCF
    ///   (0x0F4240 >> 8) & 0xFF = 0x42
    ///   0x0F4240 & 0xFF = 0x40
    #[test]
    fn encode_capture_timestamp_known_bytes() {
        let ext = LocExtension::CaptureTimestamp(1_000_000);
        let bytes = encode_extensions(&[ext]).unwrap();
        assert_eq!(
            bytes,
            &[
                0x02, // ID: 2 (1-byte varint)
                0xCF, 0x42, 0x40, // Value: 1000000 (3-byte varint)
            ]
        );
    }

    /// Known bytes test: Video Config with 2 bytes
    /// ID=13 (varint: 0x0D), Length=2 (varint: 0x02), Value=0xAA 0xBB
    #[test]
    fn encode_video_config_known_bytes() {
        let ext = LocExtension::VideoConfig(vec![0xAA, 0xBB]);
        let bytes = encode_extensions(&[ext]).unwrap();
        assert_eq!(
            bytes,
            &[
                0x0D, // ID: 13 (1-byte varint)
                0x02, // Length: 2
                0xAA, 0xBB, // Value
            ]
        );
    }
}
