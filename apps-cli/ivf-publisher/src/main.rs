//! # ivf-publisher: IVF/VP8 Publisher
//!
//! A client that connects to a relay server and publishes media data.
//! Reads VP8 video in IVF container format from stdin
//! and publishes it as MOQT objects.
//!
//! ```bash
//! ffmpeg -f avfoundation -i "0" -c:v libvpx -f ivf - | cargo run --bin ivf-publisher
//! ```
//!
//! ## IVF to MOQT mapping
//! - VP8 keyframe -> start of a new Group (independently decodable unit)
//! - Each VP8 frame -> one Object (individual data within a Group)

use moqt::session::SessionEvent;
use moqt::wire::track_namespace::TrackNamespace;
use tracing::{debug, info};

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

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    info!("connected, SETUP exchange complete");

    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());
    session.publish_namespace(ns.clone()).await?;
    info!("PUBLISH_NAMESPACE registered");

    debug!("waiting for SUBSCRIBE");
    let mut request = match session.next_event().await? {
        SessionEvent::Subscribe(r) => r,
        _ => anyhow::bail!("expected SUBSCRIBE"),
    };
    info!(
        track = %String::from_utf8_lossy(&request.message.track_name),
        "received SUBSCRIBE"
    );

    request.accept(1).await?;
    info!("sent SUBSCRIBE_OK (alias=1)");

    let stream_count = cli_lib::ivf::publish_from_stdin(&session, 1).await?;

    request.send_publish_done(stream_count).await?;
    tokio::time::sleep(std::time::Duration::from_secs(1)).await;
    info!(stream_count, "sent PUBLISH_DONE, exiting");

    Ok(())
}
