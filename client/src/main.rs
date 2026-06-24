mod server;
mod util;

use crate::server::data::ServerData;
use crate::server::{
    DEFAULT_CERT_PATH, DEFAULT_KEY_PATH, make_config, make_config_from_certs, run_server,
};
use crate::util::get_password_hash;
use anyhow::{Result, anyhow};
use argon2::PasswordHash;
use clap::Parser;
use std::fs::{File, OpenOptions};
use std::io::Write;
use std::net::IpAddr;
use std::path::PathBuf;
use std::{fs, io};
use tracing::{debug, info};
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
struct Args {
    #[arg(long, default_value = "0.0.0.0")]
    host: IpAddr,

    #[arg(short, long, default_value_t = 2700)]
    port: u16,

    #[arg(long, help = "Path to certificate file")]
    cert: Option<PathBuf>,

    #[arg(long, help = "Path to private key file")]
    key: Option<PathBuf>,

    #[arg(
        long,
        default_value = "pwd_hash",
        help = "Path from where to load previously created password. If setting password (--set-password), saving path for passfile."
    )]
    passfile: PathBuf,

    #[arg(
        long,
        default_value_t = false,
        help = "Prompt for password and save it securely for next usage in passfile (path provided through --passfile). Do not run server."
    )]
    set_password: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()))
        .init();
    let args = Args::parse();

    if args.set_password {
        debug!("Setting password");
        main_set_password(args.passfile)?;
        return Ok(());
    }

    // Password processing
    let server_data = ServerData::new(&args.passfile)?;

    // Certificates configuration
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

    run_server(conf, args.host, args.port, server_data).await?;

    Ok(())
}

fn main_set_password(path: PathBuf) -> Result<()> {
    let conf = rpassword::ConfigBuilder::new()
        .password_feedback_mask('*')
        .build();
    print!("Enter password: ");
    io::stdout().flush()?;
    let password = rpassword::read_password_with_config(conf)?;

    let hash = get_password_hash(&password)?;
    let mut file = File::create(&path)?;
    file.write_all(hash.as_bytes())?;

    println!("Password recorded at {}", path.display());
    Ok(())
}
