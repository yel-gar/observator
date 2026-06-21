use anyhow::{Result, anyhow};
use common::messages::Message;
use common::utils::log_cert_fingerprint;
use quinn::{Connection, Endpoint, ServerConfig};
use rcgen::{CertifiedKey, KeyPair, generate_simple_self_signed};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use tracing::{debug, error, info, instrument};

pub const DEFAULT_CERT_PATH: &str = "cert.pem";
pub const DEFAULT_KEY_PATH: &str = "key.pem";

fn dump_cert(cert: &CertifiedKey<KeyPair>) -> Result<()> {
    fs::write(DEFAULT_CERT_PATH, cert.cert.pem())?;
    fs::write(DEFAULT_KEY_PATH, cert.signing_key.serialize_pem())?;

    Ok(())
}

pub fn make_config() -> Result<ServerConfig> {
    let cert = generate_simple_self_signed(vec!["localhost".to_string()])?;
    dump_cert(&cert)?;
    log_cert_fingerprint(cert.cert.der());

    let cert_der = cert.cert.der().clone();
    let key_der = cert.signing_key.serialize_der();

    let conf = ServerConfig::with_single_cert(
        vec![cert_der],
        rustls::pki_types::PrivateKeyDer::Pkcs8(key_der.into()),
    )?;

    Ok(conf)
}

pub fn make_config_from_certs(cert_path: PathBuf, key_path: PathBuf) -> Result<ServerConfig> {
    let cert_pem = fs::read_to_string(cert_path)?;
    let key_pem = fs::read_to_string(key_path)?;

    let certs = rustls_pemfile::certs(&mut cert_pem.as_bytes())
        .collect::<std::result::Result<Vec<_>, _>>()?;
    let key = rustls_pemfile::private_key(&mut key_pem.as_bytes())?
        .ok_or_else(|| anyhow!("Bad private key"))?;
    match certs.first() {
        Some(der) => {
            log_cert_fingerprint(der);
        }
        None => {
            return Err(anyhow!("Unable to compute certificate fingerprint"));
        }
    }
    let conf = ServerConfig::with_single_cert(certs, key)?;

    Ok(conf)
}

pub async fn run_server(conf: ServerConfig, host: IpAddr, port: u16) -> Result<()> {
    let addr = SocketAddr::new(host, port);
    let endpoint = Endpoint::server(conf, addr)?;

    info!("Server listening on {host}:{port}");

    while let Some(incoming) = endpoint.accept().await {
        tokio::spawn(async move {
            match incoming.await {
                Ok(connection) => {
                    info!("Accepted connection from {}", connection.remote_address());
                    handler(connection).await;
                }
                Err(e) => {
                    error!("Failed connection: {e}")
                }
            }
        });
    }

    Ok(())
}

#[instrument(skip(conn), fields(addr = %conn.remote_address()))]
async fn handler(conn: Connection) {
    while let Ok((mut send, mut recv)) = conn.accept_bi().await {
        debug!("Accepting stream");
        tokio::spawn(async move {
            let bytes = match recv.read_to_end(1024).await {
                Ok(b) => b,
                Err(e) => {
                    error!(error = %e, "Unexpected read error");
                    return;
                }
            };

            let resp = match Message::deserialize(&bytes) {
                Ok(m) => process_message(m).await,
                Err(e) => {
                    error!(error = %e, "Unable to deserialize message");
                    return;
                }
            };

            if let Some(msg) = resp {
                let res = send.write_all(&msg.serialize()).await;
                if let Err(e) = res {
                    error!(error = %e, message = ?msg, "Error sending response message");
                }
            }
            if let Err(e) = send.finish() {
                error!(error = %e, "Error finishing send stream")
            }
        });
    }
}

#[instrument]
async fn process_message(m: Message) -> Option<Message> {
    debug!(?m, "Received message");
    match m {
        Message::PING => Some(Message::PONG),
        Message::PONG => None,
        Message::ACK => None,
        Message::ERROR(_) => None,
    }
}
