pub mod data;

use crate::server::data::ServerData;
use crate::util::verify_password;
use anyhow::{Result, anyhow};
use common::messages::{Message, recv_msg, send_msg};
use common::utils::log_cert_fingerprint;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig, VarInt};
use rcgen::{CertifiedKey, KeyPair, generate_simple_self_signed};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tracing::{Instrument, debug, debug_span, error, info, info_span, instrument, warn};

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
    data: Arc<ServerData>,
}

impl Server {
    pub fn new(host: IpAddr, port: u16, conf: ServerConfig, data: ServerData) -> Result<Self> {
        let addr = SocketAddr::new(host, port);
        Ok(Server {
            addr,
            endpoint: Endpoint::server(conf, addr)?,
            data: Arc::new(data),
        })
    }

    pub async fn run(&self) {
        info!("Starting server...");
        self.internal_loop().await;
    }

    async fn internal_loop(&self) {
        info!("Server successfully started! Listening on {}", self.addr);
        while let Some(incoming) = self.endpoint.accept().await {
            let span = info_span!("connection", addr = %incoming.remote_address());
            let data = self.data.clone();
            tokio::spawn(
                async move {
                    match incoming.await {
                        Ok(conn) => {
                            info!("Accepted connection");
                            let handler = ConnectionHandler::new(conn, data);
                            handler.handle().await;
                            info!("Connection terminated")
                        }
                        Err(e) => {
                            error!(error = %e, "Error accepting connection");
                        }
                    }
                }
                .instrument(span),
            );
        }
    }
}

struct ConnectionHandler {
    connection: Connection,
    data: Arc<ServerData>,
}

impl ConnectionHandler {
    fn new(connection: Connection, data: Arc<ServerData>) -> Self {
        Self { connection, data }
    }

    #[instrument(skip(self))]
    async fn handle(mut self) {
        debug!("Beginning connection handling");
        match self.auth().await {
            Ok(_) => {
                debug!("Auth successful");
            }
            Err(e) => {
                debug!(%e, "Error authenticating");
                self.connection
                    .close(VarInt::from_u32(403), b"Failed to authenticate");
                return;
            }
        }
        while let Ok((send, recv)) = self.connection.accept_bi().await {
            debug!("Accepting stream");
            tokio::spawn(async move {
                let handler = StreamHandler::new(recv, send);
                handler.handle().await;
            });
        }
    }

    async fn auth(&mut self) -> Result<()> {
        debug!("Waiting for authentication...");
        let (mut send, mut recv) = self
            .connection
            .accept_bi()
            .await
            .map_err(|_| anyhow!("Stream closed"))?;
        let msg = recv_msg(&mut recv)
            .await
            .map_err(|_| anyhow!("Stream closed"))?;

        let err_object: anyhow::Error;
        let err_msg: &str;

        let span = debug_span!("auth", ?msg);
        let _guard = span.enter();
        match msg {
            Message::Hello(password) => match self.data.verify_password(password.as_str()) {
                Ok(true) => {
                    send_msg(&mut send, Message::Authenticated).await?;
                    return Ok(());
                }
                Ok(false) => {
                    warn!("Provided password does not match");
                    err_msg = "Invalid password";
                    err_object = anyhow!("Password mismatch");
                }
                Err(e) => {
                    warn!("Error verifying password: {e}");
                    err_msg = "Server error";
                    err_object = anyhow!("Password verification error");
                }
            },
            _ => {
                warn!("Got non-HELLO message on auth stage");
                err_msg = "You must authenticate first";
                err_object = anyhow!("Bad message");
            }
        }

        let _ = send_msg(&mut send, Message::Error(err_msg.to_string())).await;

        Err(err_object)
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
        debug!("Stream closed");
    }
}

pub async fn run_server(
    conf: ServerConfig,
    host: IpAddr,
    port: u16,
    data: ServerData,
) -> Result<()> {
    let server = Server::new(host, port, conf, data)?;
    server.run().await;

    Ok(())
}

#[instrument]
async fn process_message(m: Message) -> Option<Message> {
    debug!(?m, "Received message");
    match m {
        Message::Ping => Some(Message::Pong),
        Message::Pong => None,
        Message::Ok => None,
        Message::Error(_) => None,
        Message::Authenticated => None,
        Message::Hello(_) => Some(Message::Error("You are already authenticated".to_string())),
    }
}
