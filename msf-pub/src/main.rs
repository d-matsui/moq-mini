//! # msf-pub: MSF Publisher
//!
//! A publisher that uses MSF (MoQ Streaming Format) to advertise
//! available tracks via a catalog, then publishes media data.
//!
//! ## Flow
//! 1. Connect to relay, SETUP exchange
//! 2. PUBLISH_NAMESPACE
//! 3. Wait for SUBSCRIBE("catalog") → send catalog JSON as Object
//! 4. Wait for SUBSCRIBE("video") → send VP8 frames from stdin (IVF)
//!
//! ## Usage
//! ```bash
//! ffmpeg -f avfoundation -i "0" -c:v libvpx -f ivf - | cargo run --bin msf-pub -- --pipe
//! ```

use std::io::Read;
use std::net::SocketAddr;
use std::sync::Arc;

use moqt_core::client::{self, TlsConfig};
use moqt_core::session::subgroup::SubgroupWriter;
use moqt_core::session::{MoqtSession, SessionEvent};
use moqt_core::wire::track_namespace::TrackNamespace;
use msf::catalog::{CATALOG_TRACK_NAME, Catalog, Packaging, Role, Track};
use tracing::{debug, info, warn};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    tracing_subscriber::fmt()
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
    let video_track_name = args
        .iter()
        .position(|a| a == "--track")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("video");

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    let session = Arc::new(session);
    info!("connected, SETUP exchange complete");

    // === PUBLISH_NAMESPACE ===
    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());
    session.publish_namespace(ns.clone()).await?;
    info!("PUBLISH_NAMESPACE registered");

    // === Build catalog ===
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

    // === Handle SUBSCRIBEs ===
    // We need to handle catalog and media subscribes.
    // Track alias assignment: catalog=1, video=2
    let mut catalog_request = None;
    let mut video_request = None;

    // Wait for both catalog and video SUBSCRIBEs
    while catalog_request.is_none() || video_request.is_none() {
        let event = session.next_event().await?;
        match event {
            SessionEvent::Subscribe(mut request) => {
                let track_name = String::from_utf8_lossy(&request.message.track_name);
                info!(track = %track_name, "received SUBSCRIBE");

                if track_name == CATALOG_TRACK_NAME {
                    request.accept(1).await?;
                    info!("accepted catalog SUBSCRIBE (alias=1)");

                    // Send catalog as Object in Group 0
                    let mut group = session.open_subgroup(1, 0, 0).await?;
                    group.write_object(&catalog_json).await?;
                    // Keep stream open for future delta updates
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

    // === Send video from stdin (IVF/VP8) ===
    let stream_count = send_from_stdin(&session, video_track_name).await?;

    // Send PUBLISH_DONE for video
    video_request.send_publish_done(stream_count).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    info!(stream_count, "sent PUBLISH_DONE for video, exiting");

    Ok(())
}

/// An IVF frame with a flag indicating whether it is a keyframe.
struct IvfFrame {
    data: Vec<u8>,
    is_keyframe: bool,
}

/// Reads VP8 video in IVF container format from stdin and sends it as MOQT objects.
async fn send_from_stdin(session: &MoqtSession, _track_name: &str) -> anyhow::Result<u64> {
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

        loop {
            let mut frame_header = [0u8; 12];
            if reader.read_exact(&mut frame_header).is_err() {
                break;
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
            // Track alias 2 for video
            current_group = Some(session.open_subgroup(2, group_id, 0).await?);
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
