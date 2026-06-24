use anyhow::{Result, anyhow};
use common::messages::{Message, recv_msg, send_msg};
use common::utils::log_cert_fingerprint;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig, VarInt};
use rcgen::{CertifiedKey, KeyPair, generate_simple_self_signed};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use tracing::{debug, error, error_span, info, info_span, instrument, Instrument};
use common::errors::MessageSendError;

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

struct Server {
    addr: SocketAddr,
    endpoint: Endpoint,
}

impl Server {
    pub fn new(host: IpAddr, port: u16, conf: ServerConfig) -> Result<Self> {
        let addr = SocketAddr::new(host, port);
        Ok(Server {
            addr,
            endpoint: Endpoint::server(conf, addr)?,
        })
    }

    pub async fn run(&self) {
        info!("Starting server...");
        self.internal_loop().await;
    }

    async fn internal_loop(&self) {
        info!("Server successfully started! Listening on {}", self.addr);
        while let Some(incoming) = self.endpoint.accept().await {
            let span = info_span!("Incoming connection", addr = %incoming.remote_address());
            tokio::spawn(async move {
                match incoming.await {
                    Ok(conn) => {
                        info!("Accepted connection");
                        let handler = ConnectionHandler::new(conn);
                        handler.handle().await;
                    }
                    Err(e) => {
                        error!(error = %e, "Error accepting connection");
                    }
                }
            }.instrument(span));
        }
    }
}

struct ConnectionHandler {
    connection: Connection,
}

impl ConnectionHandler {
    fn new(connection: Connection) -> Self {
        Self { connection }
    }

    #[instrument(skip(self), fields(addr = %self.connection.remote_address()))]
    async fn handle(self) {
        debug!("Beginning connection handling");
        while let Ok((send, recv)) = self.connection.accept_bi().await {
            debug!("Accepting stream");
            tokio::spawn(async move {
                let mut handler = StreamHandler::new(recv, send);
                handler.handle().await;
            });
        }
    }
}

struct StreamHandler {
    recv: RecvStream,
    send: SendStream,
}

impl StreamHandler {
    fn new(recv: RecvStream, send: SendStream) -> Self {
        Self { recv, send }
    }

    #[instrument(skip(self), fields(stream_id = %self.recv.id()))]
    async fn handle(mut self) {
        while let Ok(msg) = recv_msg(&mut self.recv).await {
            debug!(?msg, "Received message");
            let response = process_message(msg).await;
            if let Some(msg) = response {
                send_msg(&mut self.send, msg).await.unwrap_or_else(|e| {
                    error!(%e, "Error sending message");
                });
            }
        }
    }
}

pub async fn run_server(conf: ServerConfig, host: IpAddr, port: u16) -> Result<()> {
    let server = Server::new(host, port, conf)?;
    server.run().await;

    Ok(())
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
