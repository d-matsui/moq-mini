//! # moqt-relay: MOQT relay server entry point
//!
//! Reads TLS certificate and key from files and starts as a QUIC server.
//! Listens on `0.0.0.0:4433` by default.
//!
//! ## Usage
//! ```bash
//! cargo run --bin moqt-relay -- --cert certs/localhost+2.pem --key certs/localhost+2-key.pem
//! ```

use relay::relay;

use std::net::SocketAddr;

use moqt::quic_config;
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

    // Parse command-line arguments
    let args: Vec<String> = std::env::args().collect();
    let cert_path = get_arg(&args, "--cert").unwrap_or_else(|| "certs/localhost+2.pem".to_string());
    let key_path =
        get_arg(&args, "--key").unwrap_or_else(|| "certs/localhost+2-key.pem".to_string());
    let addr: SocketAddr = get_arg(&args, "--addr")
        .unwrap_or_else(|| "[::]:4433".to_string())
        .parse()?;

    // Read certificate and key from PEM files
    let cert_pem = std::fs::read(&cert_path)?;
    let key_pem = std::fs::read(&key_path)?;

    let cert_der = rustls_pemfile::certs(&mut &cert_pem[..])
        .next()
        .ok_or_else(|| anyhow::anyhow!("no certificate found in {cert_path}"))??;
    let key_der = rustls_pemfile::private_key(&mut &key_pem[..])?
        .ok_or_else(|| anyhow::anyhow!("no private key found in {key_path}"))?;

    info!(cert = %cert_path, key = %key_path, "loaded TLS credentials");

    let server_config = quic_config::make_server_config(cert_der, key_der)?;
    let endpoint = quinn::Endpoint::server(server_config, addr)?;

    info!(%addr, "relay listening");
    let relay = relay::Relay::new(endpoint);
    relay.run().await?;

    Ok(())
}

fn get_arg(args: &[String], flag: &str) -> Option<String> {
    args.iter()
        .position(|a| a == flag)
        .and_then(|i| args.get(i + 1))
        .cloned()
}
