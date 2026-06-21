mod server;

use crate::server::{
    DEFAULT_CERT_PATH, DEFAULT_KEY_PATH, make_config, make_config_from_certs, run_server,
};
use anyhow::{Result, anyhow};
use clap::Parser;
use std::fs;
use std::net::IpAddr;
use std::path::PathBuf;
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: IpAddr,

    #[arg(short, long, default_value_t = 2700)]
    port: u16,

    #[arg(long)]
    cert: Option<PathBuf>,

    #[arg(long)]
    key: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    let conf = match (args.cert, args.key) {
        (Some(cert_path), Some(key_path)) => {
            info!("Loading certificate from provided cli args");
            make_config_from_certs(cert_path, key_path)?
        }
        (None, None) => {
            if fs::exists(DEFAULT_CERT_PATH)? && fs::exists(DEFAULT_KEY_PATH)? {
                info!("Loading certificate from default path");
                make_config_from_certs(DEFAULT_CERT_PATH.into(), DEFAULT_KEY_PATH.into())?
            } else {
                info!("Generating new certificate");
                make_config()?
            }
        }
        _ => {
            return Err(anyhow!("You need to specify both key and cert files"));
        }
    };

    run_server(conf, args.host, args.port).await?;

    Ok(())
}
