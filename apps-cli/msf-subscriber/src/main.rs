//! # msf-subscriber: MSF Subscriber
//!
//! A subscriber that uses MSF (MoQ Streaming Format) to discover
//! available tracks via a catalog, then subscribes to media tracks.
//!
//! ## Flow
//! 1. Connect to relay, SETUP exchange
//! 2. SUBSCRIBE("catalog") -> receive catalog JSON
//! 3. Parse catalog, find video track
//! 4. SUBSCRIBE(video track name) -> receive VP8 frames
//! 5. Output as IVF to stdout (pipe to ffplay)
//!
//! ## Usage
//! ```bash
//! cargo run --bin msf-subscriber | ffplay -f ivf -
//! ```

use std::sync::Arc;

use moqt::wire::parameter::{MessageParameter, SubscriptionFilter};
use moqt::wire::track_namespace::TrackNamespace;
use msf::catalog::{CATALOG_TRACK_NAME, Catalog, Packaging, Role};
use tracing::info;

use cli_lib::client::{self, TlsConfig};
use moqt::session::SessionEvent;

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

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    let session = Arc::new(session);
    info!("connected, SETUP exchange complete");

    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());

    // Subscribe to catalog
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

    // Receive catalog
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

    // Find video track in catalog
    let video_track = catalog
        .tracks
        .iter()
        .find(|t| t.packaging == Packaging::Loc && t.role.as_ref() == Some(&Role::Video))
        .or_else(|| {
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

    // Subscribe to video track
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

    let video_alias = video_subscription.track_alias();
    let session_recv = session.clone();
    let receive_handle = tokio::spawn(async move {
        cli_lib::ivf::subscribe_to_stdout(&session_recv, Some(video_alias)).await;
    });

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
