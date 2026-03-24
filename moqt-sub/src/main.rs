//! # moqt-sub: MOQT Subscriber
//!
//! A client that connects to a relay server and receives media data.
//! Outputs received VP8 frames as an IVF container to stdout.
//!
//! ```bash
//! cargo run --bin moqt-sub | ffplay -f ivf -
//! ```
//!
//! ## Processing Flow
//! 1. Establish a QUIC connection to the relay and exchange SETUP
//! 2. Send SUBSCRIBE and receive SUBSCRIBE_OK
//! 3. Receive data on unidirectional streams, output as IVF
//! 4. Terminate upon receiving PUBLISH_DONE

use std::io::Write;
use std::net::SocketAddr;

use moqt_core::client::{self, TlsConfig};
use moqt_core::session::SessionEvent;
use moqt_core::wire::parameter::{MessageParameter, SubscriptionFilter};
use moqt_core::wire::track_namespace::TrackNamespace;
use tracing::info;

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
    let track_name = args
        .iter()
        .position(|a| a == "--track")
        .and_then(|i| args.get(i + 1))
        .map(|s| s.as_str())
        .unwrap_or("video");

    info!(addr = %relay_addr, "connecting to relay");
    let session = client::connect(relay_addr, "localhost", TlsConfig::Insecure).await?;
    info!("connected, SETUP exchange complete");

    // SUBSCRIBE
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

    // Wrap session in Arc so it can be shared with the receive task
    let session = std::sync::Arc::new(session);
    let session_recv = session.clone();

    // Receive Object streams and output as IVF
    let receive_handle = tokio::spawn(async move {
        let session = session_recv;
        let stdout = std::io::stdout();
        let mut ivf_header_written = false;
        let mut frame_index: u64 = 0;

        loop {
            let group = match session.next_event().await {
                Ok(SessionEvent::DataStream(g)) => g,
                Ok(_) => continue,
                Err(_) => break,
            };

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
            let mut group = group;
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

    // Wait for PUBLISH_DONE
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
