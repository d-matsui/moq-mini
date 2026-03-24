//! # msf-sub: MSF Subscriber
//!
//! A subscriber that uses MSF (MoQ Streaming Format) to discover
//! available tracks via a catalog, then subscribes to media tracks.
//!
//! ## Flow
//! 1. Connect to relay, SETUP exchange
//! 2. SUBSCRIBE("catalog") → receive catalog JSON
//! 3. Parse catalog, find video track
//! 4. SUBSCRIBE(video track name) → receive VP8 frames
//! 5. Output as IVF to stdout (pipe to ffplay)
//!
//! ## Usage
//! ```bash
//! cargo run --bin msf-sub | ffplay -f ivf -
//! ```

use std::io::Write;
use std::net::SocketAddr;
use std::sync::Arc;

use moqt_core::client::{self, TlsConfig};
use moqt_core::session::SessionEvent;
use moqt_core::wire::parameter::{MessageParameter, SubscriptionFilter};
use moqt_core::wire::track_namespace::TrackNamespace;
use msf::catalog::{CATALOG_TRACK_NAME, Catalog, Packaging, Role};
use tracing::{debug, info};

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

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    let session = Arc::new(session);
    info!("connected, SETUP exchange complete");

    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());

    // === Step 1: Subscribe to catalog ===
    info!("subscribing to catalog");
    let catalog_subscription = session
        .subscribe(
            ns.clone(),
            CATALOG_TRACK_NAME,
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await?;
    info!(
        alias = catalog_subscription.track_alias(),
        "catalog subscription established"
    );

    // === Step 2: Receive catalog ===
    let catalog = match session.next_event().await? {
        SessionEvent::DataStream(mut group) => {
            let payload = group
                .read_object()
                .await?
                .ok_or_else(|| anyhow::anyhow!("catalog stream ended without data"))?;
            let catalog: Catalog = serde_json::from_slice(&payload)?;
            info!(
                version = catalog.version,
                tracks = catalog.tracks.len(),
                "received catalog"
            );
            catalog
        }
        _ => anyhow::bail!("expected data stream for catalog"),
    };

    // === Step 3: Find video track in catalog ===
    let video_track = catalog
        .tracks
        .iter()
        .find(|t| t.packaging == Packaging::Loc && t.role.as_ref() == Some(&Role::Video))
        .or_else(|| {
            // Fallback: first LOC track
            catalog
                .tracks
                .iter()
                .find(|t| t.packaging == Packaging::Loc)
        })
        .ok_or_else(|| anyhow::anyhow!("no video track found in catalog"))?;

    info!(
        track = %video_track.name,
        codec = ?video_track.codec,
        "found video track in catalog"
    );

    // === Step 4: Subscribe to video track ===
    let mut video_subscription = session
        .subscribe(
            ns,
            &video_track.name,
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await?;
    info!(
        alias = video_subscription.track_alias(),
        "video subscription established"
    );

    // === Step 5: Receive video ===
    let video_alias = video_subscription.track_alias();
    let session_recv = session.clone();
    let receive_handle = tokio::spawn(async move {
        let stdout = std::io::stdout();
        let mut ivf_header_written = false;
        let mut frame_index: u64 = 0;

        loop {
            let mut group = match session_recv.next_event().await {
                Ok(SessionEvent::DataStream(g)) => g,
                Ok(_) => continue,
                Err(_) => break,
            };

            // Skip non-video streams (e.g. catalog updates)
            if group.track_alias() != video_alias {
                debug!(alias = group.track_alias(), "skipping non-video stream");
                continue;
            }

            // Write IVF file header once
            if !ivf_header_written {
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
                ivf_header_written = true;
            }

            // Write each Object as an IVF frame
            while let Ok(Some(payload)) = group.read_object().await {
                let mut out = stdout.lock();
                let size = payload.len() as u32;
                let _ = out.write_all(&size.to_le_bytes());
                let _ = out.write_all(&frame_index.to_le_bytes());
                let _ = out.write_all(&payload);
                let _ = out.flush();
                frame_index += 1;
            }
        }
    });

    // Wait for PUBLISH_DONE on video
    match video_subscription.recv_publish_done().await? {
        Some(publish_done) => {
            info!(
                status = publish_done.status_code,
                streams = publish_done.stream_count,
                "received PUBLISH_DONE for video"
            );
        }
        None => {
            info!("publisher closed without PUBLISH_DONE");
        }
    }

    tokio::time::sleep(std::time::Duration::from_millis(200)).await;
    session.close();
    let _ = receive_handle.await;

    info!("done");
    Ok(())
}
