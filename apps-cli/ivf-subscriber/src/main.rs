//! # ivf-subscriber: IVF/VP8 Subscriber
//!
//! A client that connects to a relay server and receives media data.
//! Outputs received VP8 frames as an IVF container to stdout.
//!
//! ```bash
//! cargo run --bin ivf-subscriber | ffplay -f ivf -
//! ```

use moqt::wire::parameter::{MessageParameter, SubscriptionFilter};
use moqt::wire::track_namespace::TrackNamespace;
use tracing::info;

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
    let track_name = args
        .iter()
        .position(|a| a == "--track")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("video");

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    info!("connected, SETUP exchange complete");

    let ns_fields: Vec<&str> = namespace_raw.split('/').filter(|s| !s.is_empty()).collect();
    let ns = TrackNamespace::from(ns_fields.as_slice());
    let mut subscription = session
        .subscribe(
            ns,
            track_name,
            vec![MessageParameter::SubscriptionFilter(
                SubscriptionFilter::NextGroupStart,
            )],
        )
        .await?;
    info!(alias = subscription.track_alias(), "received SUBSCRIBE_OK");

    let session = std::sync::Arc::new(session);
    let session_recv = session.clone();

    let receive_handle = tokio::spawn(async move {
        cli_lib::ivf::subscribe_to_stdout(&session_recv, None).await;
    });

    match subscription.recv_publish_done().await? {
        Some(publish_done) => {
            info!(
                status = publish_done.status_code,
                streams = publish_done.stream_count,
                "received PUBLISH_DONE"
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
