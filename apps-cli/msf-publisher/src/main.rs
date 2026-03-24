//! # msf-publisher: MSF Publisher
//!
//! A publisher that uses MSF (MoQ Streaming Format) to advertise
//! available tracks via a catalog, then publishes media data.
//!
//! ## Flow
//! 1. Connect to relay, SETUP exchange
//! 2. PUBLISH_NAMESPACE
//! 3. Wait for SUBSCRIBE("catalog") -> send catalog JSON as Object
//! 4. Wait for SUBSCRIBE("video") -> send VP8 frames from stdin (IVF)
//!
//! ## Usage
//! ```bash
//! ffmpeg -f avfoundation -i "0" -c:v libvpx -f ivf - | cargo run --bin msf-publisher
//! ```

use std::sync::Arc;

use moqt::session::SessionEvent;
use moqt::wire::track_namespace::TrackNamespace;
use msf::catalog::{CATALOG_TRACK_NAME, Catalog, Packaging, Role, Track};
use tracing::{debug, info, warn};

use cli_lib::client::{self, TlsConfig};

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

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    let session = Arc::new(session);
    info!("connected, SETUP exchange complete");

    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());
    session.publish_namespace(ns.clone()).await?;
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

    let stream_count = cli_lib::ivf::publish_from_stdin(&session, 2).await?;

    video_request.send_publish_done(stream_count).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    info!(stream_count, "sent PUBLISH_DONE for video, exiting");

    Ok(())
}
