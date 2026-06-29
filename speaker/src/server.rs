pub mod data;

use crate::audio::init_audio;
use crate::errors::ConnectionLimitError;
use crate::server::data::ServerData;
use crate::util::verify_password;
use anyhow::{Result, anyhow};
use common::constants::AUDIO_QUEUE_BUFFER_SIZE;
use common::messages::{Message, VoicePacket, recv_msg, send_msg};
use common::utils::log_cert_fingerprint;
use quinn::{Connection, Endpoint, RecvStream, SendStream, ServerConfig, TransportConfig, VarInt};
use rcgen::{CertifiedKey, KeyPair, generate_simple_self_signed};
use std::fs;
use std::net::{IpAddr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tokio::task::JoinHandle;
use tracing::{Instrument, debug, debug_span, error, info, info_span, instrument, warn};

pub const DEFAULT_CERT_PATH: &str = "cert.pem";
pub const DEFAULT_KEY_PATH: &str = "key.pem";
pub const MAX_TOTAL_CONNECTIONS: usize = 1000;
pub const MAX_CONNECTIONS_PER_IP: usize = 10;
const AUTH_TIMEOUT: Duration = Duration::from_secs(30);
const IDLE_TIMEOUT: Duration = Duration::from_mins(10);

fn dump_cert(cert: &CertifiedKey<KeyPair>) -> Result<()> {
    fs::write(DEFAULT_CERT_PATH, cert.cert.pem())?;
    fs::write(DEFAULT_KEY_PATH, cert.signing_key.serialize_pem())?;

    Ok(())
}

fn set_transport(mut conf: ServerConfig) -> ServerConfig {
    let mut transport = TransportConfig::default();
    transport.max_concurrent_uni_streams(1u32.into());
    conf.transport_config(Arc::new(transport));
    conf
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

    Ok(set_transport(conf))
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

    Ok(set_transport(conf))
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
        // Init audio backend
        let (tx, rx) = tokio::sync::mpsc::channel::<VoicePacket>(AUDIO_QUEUE_BUFFER_SIZE);
        let _stream = match init_audio(rx) {
            Ok(s) => s,
            Err(e) => {
                error!(%e, "Failed to initialize audio");
                return;
            }
        };

        info!("Server successfully started! Listening on {}", self.addr);
        while let Some(incoming) = self.endpoint.accept().await {
            let span = info_span!("connection", addr = %incoming.remote_address());
            let ip = incoming.remote_address().ip();
            if let Err(e) = self.data.inc_connections(&ip).await {
                match e {
                    ConnectionLimitError::Global => {
                        error!(
                            "Global connection limit exceeded, no more connections will be accepted until some are closed"
                        );
                        incoming.refuse();
                    }
                    ConnectionLimitError::Individual => {
                        warn!(
                            "Individual connection limit exceeded, no more connections are accepted from it"
                        );
                        incoming.refuse();
                    }
                }
                continue;
            }
            let data = self.data.clone();
            let data_finalize = self.data.clone();
            let audio_tx = tx.clone();
            tokio::spawn(
                async move {
                    match incoming.await {
                        Ok(conn) => {
                            info!("Accepted connection");
                            let handler = ConnectionHandler::new(conn, data, audio_tx);
                            handler.handle().await;
                            info!("Connection terminated")
                        }
                        Err(e) => {
                            error!(error = %e, "Error accepting connection");
                        }
                    }
                    data_finalize.dec_connections(&ip).await;
                }
                .instrument(span),
            );
        }
    }
}

struct ConnectionHandler {
    connection: Connection,
    data: Arc<ServerData>,
    audio_tx: tokio::sync::mpsc::Sender<VoicePacket>,
}

impl ConnectionHandler {
    fn new(
        connection: Connection,
        data: Arc<ServerData>,
        audio_tx: tokio::sync::mpsc::Sender<VoicePacket>,
    ) -> Self {
        Self {
            connection,
            data,
            audio_tx,
        }
    }

    #[instrument(skip(self))]
    async fn handle(mut self) {
        debug!("Beginning connection handling");
        let auth = match self.auth().await {
            Ok(auth) => {
                debug!("Auth successful");
                auth
            }
            Err(e) => {
                debug!(%e, "Error authenticating");
                self.connection
                    .close(VarInt::from_u32(403), b"Failed to authenticate");
                return;
            }
        };

        let c1 = self.connection.clone();
        let c2 = self.connection.clone();

        // TODO: the client might not open any streams afterwards, so we will need to timeout
        let control = tokio::spawn(async move {
            debug!("Control handler spawned");
            while let Ok((send, recv)) = c1.accept_bi().await {
                debug!("Accepting control stream");
                tokio::spawn(async move {
                    let handler = ControlStreamHandler::new(recv, send);
                    handler.handle().await;
                });
            }
            debug!("Done accepting control streams");
        });
        let voice = tokio::spawn(async move {
            debug!("Voice handler spawned");
            while let Ok(recv) = c2.accept_uni().await {
                debug!("Accepting voice stream");
                let handler = AudioStreamHandler::new(recv, self.audio_tx.clone());
                handler.handle().await;
            }
            debug!("Done accepting voice streams");
        });

        let _ = tokio::join!(auth, control, voice);
    }

    async fn auth(&mut self) -> Result<JoinHandle<()>> {
        debug!("Waiting for authentication...");
        let (mut send, mut recv) = self
            .connection
            .accept_bi()
            .await
            .map_err(|_| anyhow!("Stream closed"))?;
        let msg = tokio::time::timeout(AUTH_TIMEOUT, recv_msg(&mut recv))
            .await?
            .map_err(|_| anyhow!("Stream closed"))?;

        let err_object: anyhow::Error;
        let err_msg: &str;

        let span = debug_span!("auth", ?msg);
        let _guard = span.enter();
        match msg {
            Message::Hello(password) => match self.data.verify_password(password.as_str()) {
                Ok(true) => {
                    send_msg(&mut send, Message::Authenticated).await?;
                    let handle = tokio::spawn(async move {
                        debug!("Delegating auth stream to controller");
                        let handler = ControlStreamHandler::new(recv, send);
                        handler.handle().await;
                    });
                    return Ok(handle);
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

struct ControlStreamHandler {
    recv: RecvStream,
    send: SendStream,
}

impl ControlStreamHandler {
    fn new(recv: RecvStream, send: SendStream) -> Self {
        Self { recv, send }
    }

    #[instrument(skip(self))]
    async fn handle(mut self) {
        debug!("Beginning handling of control stream");
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

struct AudioStreamHandler {
    recv: RecvStream,
    tx: tokio::sync::mpsc::Sender<VoicePacket>,
}

impl AudioStreamHandler {
    fn new(recv: RecvStream, tx: tokio::sync::mpsc::Sender<VoicePacket>) -> Self {
        Self { recv, tx }
    }

    #[instrument(skip(self))]
    async fn handle(self) {
        debug!("Beginning handling of audio stream");
        if let Err(e) = self.handle_internal().await {
            error!(%e, "Error handling audio stream");
        }
        debug!("Audio handler closed");
    }

    async fn handle_internal(mut self) -> Result<()> {
        loop {
            let mut buf = [0u8; 4];
            self.recv.read_exact(&mut buf).await?;
            let mut buf = vec![0u8; u32::from_be_bytes(buf) as usize];
            self.recv.read_exact(buf.as_mut()).await?;

            let packet = VoicePacket::deserialize(&buf)?;
            self.tx.send(packet).await?;
        }
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
