mod audio;
mod certs;
mod client;
mod discovery;

use crate::client::Client;
use anyhow::Result;
use rustls::crypto::ring::default_provider;
use secrecy::SecretString;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::new("debug").add_directive("pulseaudio=info".parse()?))
        .init();
    default_provider()
        .install_default()
        .expect("Failed to install ring provider");

    let client = Client::new("127.0.0.1:2700".to_string(), SecretString::from("amogus"))?;
    client.run().await?;

    Ok(())
}
