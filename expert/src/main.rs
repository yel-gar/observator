pub mod certs;
pub mod client;

use crate::client::Client;
use anyhow::Result;
use rustls::crypto::ring::default_provider;
use secrecy::SecretString;
use tracing_subscriber::EnvFilter;

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    default_provider()
        .install_default()
        .expect("Failed to install ring provider");

    let client = Client::new("127.0.0.1:2700".to_string(), SecretString::from("amogus"))?;
    client.run().await?;

    Ok(())
}
