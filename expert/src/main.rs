pub mod certs;

use crate::certs::CertificateVerifier;
use anyhow::{Result, anyhow};
use common::messages::Message;
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Endpoint};
use rustls::crypto::ring::default_provider;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{error, info};
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    default_provider()
        .install_default()
        .expect("Failed to install ring provider");

    let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;

    let mut crypto = rustls::ClientConfig::builder()
        .with_root_certificates(rustls::RootCertStore::empty())
        .with_no_client_auth();

    crypto
        .dangerous()
        .set_certificate_verifier(Arc::new(CertificateVerifier));

    let conf = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
    endpoint.set_default_client_config(conf);

    let target_addr: SocketAddr = "127.0.0.1:2700".parse()?;
    let conn = endpoint.connect(target_addr, "localhost")?.await?;
    info!("Connected to {target_addr}");

    let (mut send, mut recv) = conn.open_bi().await?;
    send.write_all(&Message::PING.serialize()).await?;
    send.finish()?;
    info!("Sent PING");

    let resp_b = recv.read_to_end(1024).await?;
    let resp = Message::deserialize(&resp_b)?;

    match resp {
        Message::PONG => {
            info!("Got PONG");
        }

        other => {
            error!(?other, "Something weird happened...");
        }
    }

    Ok(())
}
