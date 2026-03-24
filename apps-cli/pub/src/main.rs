//! # moqt-pub: MOQT Publisher
//!
//! A client that connects to a relay server and publishes media data.
//! Reads VP8 video in IVF container format from stdin
//! and publishes it as MOQT objects.
//!
//! ```bash
//! ffmpeg -f avfoundation -i "0" -c:v libvpx -f ivf - | cargo run --bin moqt-pub
//! ```
//!
//! ## IVF to MOQT mapping
//! - VP8 keyframe -> start of a new Group (independently decodable unit)
//! - Each VP8 frame -> one Object (individual data within a Group)

use std::io::Read;
use std::net::SocketAddr;

use moqt::client::{self, TlsConfig};
use moqt::session::subgroup::SubgroupWriter;
use moqt::session::{MoqtSession, SessionEvent};
use moqt::wire::track_namespace::TrackNamespace;
use tracing::{debug, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    rustls::crypto::ring::default_provider()
        .install_default()
        .expect("Failed to install crypto provider");

    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    let relay_addr: SocketAddr = args
        .iter()
        .find(|a| !a.starts_with('-') && a.contains(':'))
        .map(|s| s.as_str())
        .unwrap_or("127.0.0.1:4433")
        .parse()?;
    let namespace_raw = args
        .iter()
        .position(|a| a == "--ns")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("daiki/example");
    let track_name = args
        .iter()
        .position(|a| a == "--track")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("video");

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    info!("connected, SETUP exchange complete");

    // === PUBLISH_NAMESPACE ===
    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());
    session.publish_namespace(ns.clone()).await?;
    info!("PUBLISH_NAMESPACE registered");

    // === Wait for SUBSCRIBE ===
    debug!("waiting for SUBSCRIBE");
    let mut request = match session.next_event().await? {
        SessionEvent::Subscribe(r) => r,
        _ => anyhow::bail!("expected SUBSCRIBE"),
    };
    info!(
        track = %String::from_utf8_lossy(&request.message.track_name),
        "received SUBSCRIBE"
    );

    // Send SUBSCRIBE_OK (Track Alias = 1)
    request.accept(1).await?;
    info!("sent SUBSCRIBE_OK (alias=1)");

    // === Read IVF video from stdin and publish ===
    let stream_count = send_from_stdin(&session, track_name).await?;

    // Send PUBLISH_DONE
    request.send_publish_done(stream_count).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    info!(stream_count, "sent PUBLISH_DONE, exiting");

    Ok(())
}

/// An IVF frame with a flag indicating whether it is a keyframe.
struct IvfFrame {
    data: Vec<u8>,
    is_keyframe: bool,
}

/// Reads VP8 video in IVF container format from stdin and sends it as MOQT objects.
///
/// ## IVF (Indeo Video Format) container structure
/// - File header: 32 bytes (signature "DKIF", codec info, resolution, etc.)
/// - Frame header: 12 bytes (frame size 4 bytes LE + timestamp 8 bytes LE)
/// - Frame data: byte sequence of the frame size
///
/// ## VP8 keyframe detection
/// If bit 0 of the first byte of the VP8 bitstream is 0, it is a keyframe.
/// Keyframes can be decoded independently, so they are used as Group boundaries.
/// This allows subscribers to start playback from any Group.
///
/// ## Mapping to MOQT
/// - Keyframe -> new Group (FIN the previous Group's stream and open a new one)
/// - Each frame -> one Object (sequential Object IDs with delta=0)
async fn send_from_stdin(session: &MoqtSession, _track_name: &str) -> anyhow::Result<u64> {
    // Bridge blocking I/O (stdin reading) and async I/O (QUIC sending) via a channel.
    // Use spawn_blocking for blocking reads, and receive frames asynchronously
    // in the main task for sending.
    let (frame_tx, mut frame_rx) = tokio::sync::mpsc::channel::<IvfFrame>(64);

    // Blocking thread: parse IVF frames from stdin
    tokio::task::spawn_blocking(move || {
        let stdin = std::io::stdin();
        let mut reader = stdin.lock();

        // Skip the IVF file header (32 bytes)
        let mut file_header = [0u8; 32];
        if reader.read_exact(&mut file_header).is_err() {
            warn!("failed to read IVF file header");
            return;
        }

        // Read frames one by one and send them to the channel
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

            // Read the frame data
            let mut data = vec![0u8; frame_size];
            if reader.read_exact(&mut data).is_err() {
                break;
            }

            // VP8 keyframe detection:
            // If bit 0 of the first byte of the VP8 bitstream is 0 -> keyframe
            // If bit 0 is 1 -> inter-frame (depends on previous frames)
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
        // Start a new Group on each keyframe (except the very first frame)
        if frame.is_keyframe && group_started {
            if let Some(mut group) = current_group.take() {
                group.finish()?;
            }
            stream_count += 1;
            debug!(group_id, objects = object_id, "sent group");
            group_id += 1;
            object_id = 0;
        }

        // Open a new group if needed
        if current_group.is_none() {
            current_group = Some(session.open_subgroup(1, group_id, 0).await?);
            group_started = true;
        }

        // Write VP8 frame as one MOQT Object
        if let Some(ref mut group) = current_group {
            group.write_object(&frame.data).await?;
            object_id += 1;
        }
    }

    // Close the last Group
    if let Some(mut group) = current_group.take() {
        group.finish()?;
        stream_count += 1;
        debug!(group_id, objects = object_id, "sent group");
    }

    Ok(stream_count)
}
