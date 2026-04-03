//! # msf-publisher: MSF Publisher
//!
//! A publisher that uses MSF (MoQ Streaming Format) to advertise
//! available tracks via a catalog, then publishes media data with
//! LOC CaptureTimestamp.
//!
//! ## Flow
//! 1. Connect to relay, SETUP exchange
//! 2. PUBLISH_NAMESPACE
//! 3. Wait for SUBSCRIBE("catalog") -> send catalog JSON as Object
//! 4. Wait for SUBSCRIBE("video") -> send VP8 frames from stdin (IVF)
//!    with CaptureTimestamp in Object Properties
//!
//! ## Usage
//! ```bash
//! ffmpeg -f avfoundation -i "0" -c:v libvpx -f ivf - | cargo run --bin msf-publisher
//! ```

use std::io::Read;
use std::sync::Arc;

use moqt::session::subgroup::SubgroupWriter;
use moqt::session::{MoqtSession, SessionEvent};
use moqt::wire::track_namespace::TrackNamespace;
use msf::catalog::{CATALOG_TRACK_NAME, Catalog, Packaging, Role, Track};
use msf::loc::{LocExtension, encode_extensions};
use tracing::{debug, info, warn};

use cli_lib::client::{self, TlsConfig};

/// IVF frame with keyframe flag and timestamp.
struct IvfFrame {
    data: Vec<u8>,
    is_keyframe: bool,
    /// Timestamp in microseconds (converted from IVF timebase).
    timestamp_us: u64,
}

/// IVF file header info needed for timestamp conversion.
struct IvfHeader {
    timebase_num: u32,
    timebase_den: u32,
}

/// Read IVF file header (32 bytes) from stdin.
/// Returns the timebase for timestamp conversion.
fn read_ivf_header(reader: &mut impl Read) -> anyhow::Result<IvfHeader> {
    let mut buf = [0u8; 32];
    reader.read_exact(&mut buf)?;
    let timebase_num = u32::from_le_bytes([buf[16], buf[17], buf[18], buf[19]]);
    let timebase_den = u32::from_le_bytes([buf[20], buf[21], buf[22], buf[23]]);
    Ok(IvfHeader {
        timebase_num,
        timebase_den,
    })
}

/// Read one IVF frame from stdin.
/// Returns None on EOF.
fn read_ivf_frame(reader: &mut impl Read, header: &IvfHeader) -> anyhow::Result<Option<IvfFrame>> {
    let mut frame_header = [0u8; 12];
    if reader.read_exact(&mut frame_header).is_err() {
        return Ok(None);
    }

    let frame_size = u32::from_le_bytes([
        frame_header[0],
        frame_header[1],
        frame_header[2],
        frame_header[3],
    ]) as usize;

    let raw_timestamp = u64::from_le_bytes([
        frame_header[4],
        frame_header[5],
        frame_header[6],
        frame_header[7],
        frame_header[8],
        frame_header[9],
        frame_header[10],
        frame_header[11],
    ]);

    // IVF timebase: timebase_num is fps numerator, timebase_den is fps denominator.
    // timestamp is in units of 1/fps, so to convert to microseconds:
    // timestamp_us = raw_timestamp * 1_000_000 * timebase_den / timebase_num
    let timestamp_us = if header.timebase_num > 0 {
        raw_timestamp * 1_000_000 * header.timebase_den as u64 / header.timebase_num as u64
    } else {
        raw_timestamp
    };

    let mut data = vec![0u8; frame_size];
    reader.read_exact(&mut data)?;

    let is_keyframe = !data.is_empty() && (data[0] & 0x01) == 0;

    Ok(Some(IvfFrame {
        data,
        is_keyframe,
        timestamp_us,
    }))
}

/// Publish IVF frames from a channel with LOC CaptureTimestamp.
async fn publish_video(
    session: &MoqtSession,
    track_alias: u64,
    mut frame_rx: tokio::sync::mpsc::Receiver<IvfFrame>,
) -> anyhow::Result<u64> {
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
            current_group = Some(
                session
                    .open_subgroup_with_properties(track_alias, group_id, 0)
                    .await?,
            );
            group_started = true;
        }

        let properties = encode_extensions(&[LocExtension::CaptureTimestamp(frame.timestamp_us)])?;

        if let Some(ref mut group) = current_group {
            group
                .write_object_with_properties(&frame.data, &properties)
                .await?;
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

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    cli_lib::init::init_tracing();
    cli_lib::init::init_crypto();

    let args: Vec<String> = std::env::args().collect();
    let relay_addr = client::parse_relay_addr(&args)?;
    let namespace_raw = args
        .iter()
        .position(|a| a == "--ns")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("daiki/example");
    let video_track_name = args
        .iter()
        .position(|a| a == "--track")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("video");
    let file_path = args
        .iter()
        .position(|a| a == "--file")
        .and_then(|i| args.get(i + 1))
        .cloned();

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    let session = Arc::new(session);
    info!("connected, SETUP exchange complete");

    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());
    let _pub_ns = session.publish_namespace(ns.clone()).await?;
    info!("PUBLISH_NAMESPACE registered");

    // Build catalog
    let catalog = Catalog {
        version: 1,
        tracks: vec![Track {
            name: video_track_name.to_string(),
            packaging: Packaging::Loc,
            is_live: true,
            role: Some(Role::Video),
            codec: Some("vp8".to_string()),
            ..Track::default()
        }],
        ..Catalog::default()
    };
    let catalog_json = serde_json::to_vec(&catalog)?;

    // Handle SUBSCRIBEs (catalog=1, video=2)
    let mut catalog_request = None;
    let mut video_request = None;

    while catalog_request.is_none() || video_request.is_none() {
        let event = session.next_event().await?;
        match event {
            SessionEvent::Subscribe(mut request) => {
                let track_name = String::from_utf8_lossy(&request.message.track_name);
                info!(track = %track_name, "received SUBSCRIBE");

                if track_name == CATALOG_TRACK_NAME {
                    request.accept(1).await?;
                    info!("accepted catalog SUBSCRIBE (alias=1)");

                    let mut group = session.open_subgroup(1, 0, 0).await?;
                    group.write_object(&catalog_json).await?;
                    info!("sent catalog");

                    catalog_request = Some(request);
                } else if track_name == video_track_name {
                    request.accept(2).await?;
                    info!("accepted video SUBSCRIBE (alias=2)");
                    video_request = Some(request);
                } else {
                    warn!(track = %track_name, "unknown track, ignoring");
                }
            }
            _ => {
                debug!("ignoring non-subscribe event");
            }
        }
    }

    let mut video_request = video_request.expect("video request must be set");
    let mut catalog_request = catalog_request.expect("catalog request must be set");

    // Read IVF frames in a blocking thread.
    // --file: open file after SUBSCRIBE (no pipe buffer issue)
    // stdin: read from stdin (may have buffered data from pipe)
    let (frame_tx, frame_rx) = tokio::sync::mpsc::channel::<IvfFrame>(64);

    tokio::task::spawn_blocking(move || {
        let reader: Box<dyn Read> = if let Some(path) = file_path {
            info!(path = %path, "reading IVF from file");
            match std::fs::File::open(&path) {
                Ok(f) => Box::new(f),
                Err(e) => {
                    warn!("failed to open file: {e}");
                    return;
                }
            }
        } else {
            Box::new(std::io::stdin().lock())
        };
        let mut reader = std::io::BufReader::new(reader);

        let header = match read_ivf_header(&mut reader) {
            Ok(h) => h,
            Err(e) => {
                warn!("failed to read IVF header: {e}");
                return;
            }
        };

        loop {
            match read_ivf_frame(&mut reader, &header) {
                Ok(Some(frame)) => {
                    if frame_tx.blocking_send(frame).is_err() {
                        return;
                    }
                }
                Ok(None) => break,
                Err(e) => {
                    warn!("failed to read IVF frame: {e}");
                    break;
                }
            }
        }
    });

    let stream_count = publish_video(&session, 2, frame_rx).await?;

    video_request.send_publish_done(stream_count).await?;
    info!(stream_count, "sent PUBLISH_DONE for video");

    catalog_request.send_publish_done(1).await?;
    info!("sent PUBLISH_DONE for catalog");

    tokio::time::sleep(std::time::Duration::from_secs(1)).await;

    Ok(())
}
