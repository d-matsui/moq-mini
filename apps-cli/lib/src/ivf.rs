//! # ivf: IVF (Indeo Video Format) container I/O
//!
//! Read VP8 frames from IVF on stdin, write IVF to stdout.
//!
//! ## IVF container structure
//! - File header: 32 bytes (signature "DKIF", codec info, resolution, etc.)
//! - Frame header: 12 bytes (frame size 4 bytes LE + timestamp 8 bytes LE)
//! - Frame data: byte sequence of the frame size

use std::io::{Read, Write};

use moqt::session::subgroup::SubgroupWriter;
use moqt::session::{MoqtSession, SessionEvent};
use tracing::{debug, warn};

/// An IVF frame with a flag indicating whether it is a keyframe.
pub struct IvfFrame {
    pub data: Vec<u8>,
    pub is_keyframe: bool,
}

/// Read IVF frames from stdin and publish as MOQT objects.
///
/// VP8 keyframe detection: if bit 0 of the first byte is 0, it is a keyframe.
/// Keyframes start a new Group (independently decodable unit).
pub async fn publish_from_stdin(session: &MoqtSession, track_alias: u64) -> anyhow::Result<u64> {
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<IvfFrame>(64);

    tokio::task::spawn_blocking(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();

        // Skip the IVF file header (32 bytes)
        let mut file_header = [0u8; 32];
        if reader.read_exact(&mut file_header).is_err() {
            warn!("failed to read IVF file header");
            return;
        }

        loop {
            // IVF frame header: 12 bytes
            //   Bytes 0-3: frame size (little-endian u32)
            //   Bytes 4-11: timestamp (little-endian u64)
            let mut frame_header = [0u8; 12];
            if reader.read_exact(&mut frame_header).is_err() {
                break; // EOF
            }
            let frame_size = u32::from_le_bytes([
                frame_header[0],
                frame_header[1],
                frame_header[2],
                frame_header[3],
            ]) as usize;

            let mut data = vec![0u8; frame_size];
            if reader.read_exact(&mut data).is_err() {
                break;
            }

            let is_keyframe = !data.is_empty() && (data[0] & 0x01) == 0;

            if frame_tx
                .blocking_send(IvfFrame { data, is_keyframe })
                .is_err()
            {
                return;
            }
        }
    });

    let mut group_id: u64 = 0;
    let mut object_id: u64 = 0;
    let mut current_group: Option<SubgroupWriter> = None;
    let mut stream_count: u64 = 0;
    let mut group_started = false;

    while let Some(frame) = frame_rx.recv().await {
        if frame.is_keyframe && group_started {
            if let Some(mut group) = current_group.take() {
                group.finish()?;
            }
            stream_count += 1;
            debug!(group_id, objects = object_id, "sent group");
            group_id += 1;
            object_id = 0;
        }

        if current_group.is_none() {
            current_group = Some(session.open_subgroup(track_alias, group_id, 0).await?);
            group_started = true;
        }

        if let Some(ref mut group) = current_group {
            group.write_object(&frame.data).await?;
            object_id += 1;
        }
    }

    if let Some(mut group) = current_group.take() {
        group.finish()?;
        stream_count += 1;
        debug!(group_id, objects = object_id, "sent group");
    }

    Ok(stream_count)
}

/// Write the IVF file header to stdout.
pub fn write_ivf_header() {
    let stdout = std::io::stdout();
    let mut ivf_hdr = [0u8; 32];
    ivf_hdr[0..4].copy_from_slice(b"DKIF");
    ivf_hdr[4..6].copy_from_slice(&0u16.to_le_bytes());
    ivf_hdr[6..8].copy_from_slice(&32u16.to_le_bytes());
    ivf_hdr[8..12].copy_from_slice(b"VP80");
    ivf_hdr[12..14].copy_from_slice(&320u16.to_le_bytes());
    ivf_hdr[14..16].copy_from_slice(&240u16.to_le_bytes());
    ivf_hdr[16..20].copy_from_slice(&30u32.to_le_bytes());
    ivf_hdr[20..24].copy_from_slice(&1u32.to_le_bytes());
    let _ = stdout.lock().write_all(&ivf_hdr);
}

/// Write a single IVF frame to stdout.
pub fn write_ivf_frame(payload: &[u8], frame_index: u64) {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    let size = payload.len() as u32;
    let _ = out.write_all(&size.to_le_bytes());
    let _ = out.write_all(&frame_index.to_le_bytes());
    let _ = out.write_all(payload);
    let _ = out.flush();
}

/// Receive MOQT objects and write as IVF frames to stdout.
///
/// Listens for DataStream events, optionally filtering by track alias.
/// Returns when the session ends.
pub async fn subscribe_to_stdout(session: &MoqtSession, track_alias_filter: Option<u64>) {
    let mut ivf_header_written = false;
    let mut frame_index: u64 = 0;

    loop {
        let mut group = match session.next_event().await {
            Ok(SessionEvent::DataStream(g)) => g,
            Ok(_) => continue,
            Err(_) => break,
        };

        if let Some(expected) = track_alias_filter
            && group.track_alias() != expected
        {
            debug!(alias = group.track_alias(), "skipping non-target stream");
            continue;
        }

        if !ivf_header_written {
            write_ivf_header();
            ivf_header_written = true;
        }

        while let Ok(Some(payload)) = group.read_object().await {
            write_ivf_frame(&payload, frame_index);
            frame_index += 1;
        }
    }
}
