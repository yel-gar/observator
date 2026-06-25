use crate::certs::CertificateVerifier;
use anyhow::{Result, anyhow};
use common::messages::{Message, recv_msg, send_msg};
use quinn::crypto::rustls::QuicClientConfig;
use quinn::{ClientConfig, Connection, Endpoint, RecvStream, SendStream, VarInt};
use secrecy::{ExposeSecret, SecretString};
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{debug, info, instrument};

struct BiStream {
    pub send: SendStream,
    pub recv: RecvStream,
}

pub struct Client {
    endpoint: Endpoint,
    target_addr: SocketAddr,
    connection: Option<Connection>,
    password: SecretString,

    // Streams
    control: Option<BiStream>,
    data: Option<SendStream>,
}

impl Client {
    pub fn new(target: String, password: SecretString) -> Result<Self> {
        let mut endpoint = Endpoint::client("0.0.0.0:0".parse()?)?;

        let mut crypto = rustls::ClientConfig::builder()
            .with_root_certificates(rustls::RootCertStore::empty())
            .with_no_client_auth();

        crypto
            .dangerous()
            .set_certificate_verifier(Arc::new(CertificateVerifier));

        let conf = ClientConfig::new(Arc::new(QuicClientConfig::try_from(crypto)?));
        endpoint.set_default_client_config(conf);

        let target_addr: SocketAddr = target.parse()?;

        Ok(Self {
            endpoint,
            target_addr,
            password,
            connection: None,
            control: None,
            data: None,
        })
    }

    #[instrument(skip(self), fields(self.target_addr))]
    pub async fn run(mut self) -> Result<()> {
        info!("Starting client");
        self.connect().await?;
        self.open_streams().await?;
        self.handle().await?;

        Ok(())
    }

    async fn handle(self) -> Result<()> {
        let control = Self::handle_control(
            self.control
                .ok_or(anyhow!("Control stream not initialized"))?,
        );
        let data = Self::handle_data(self.data.ok_or(anyhow!("Data stream not initialized"))?);

        tokio::join!(control, data);

        info!("Client shutting down");
        if let Some(con) = self.connection {
            con.close(VarInt::from_u32(0), b"Finished handling");
        }
        info!("Shut down successfully");

        Ok(())
    }

    async fn handle_control(stream: BiStream) {
        todo!()
    }

    async fn handle_data(send: SendStream) {
        todo!()
    }

    async fn connect(&mut self) -> Result<()> {
        info!("Connecting...");
        let conn = self
            .endpoint
            .connect(self.target_addr, "localhost")?
            .await?;
        self.connection = Some(conn);
        Ok(())
    }

    async fn open_control(&mut self) -> Result<()> {
        debug!("Opening control stream");
        if self.control.is_some() {
            debug!("Control stream was already opened");
            return Ok(());
        }
        let (send, recv) = self.get_connection()?.open_bi().await?;
        self.control = Some(BiStream { send, recv });

        debug!("Control stream opened");
        Ok(())
    }

    async fn open_data(&mut self) -> Result<()> {
        debug!("Opening data stream");
        if self.data.is_some() {
            debug!("Data stream was already opened");
            return Ok(());
        }

        self.data = Some(self.get_connection()?.open_uni().await?);

        debug!("Data stream opened");
        Ok(())
    }

    async fn open_streams(&mut self) -> Result<()> {
        self.open_control().await?;
        self.auth_connection().await?;
        self.open_data().await?;
        Ok(())
    }

    async fn auth_connection(&mut self) -> Result<()> {
        debug!("Authenticating connection");
        let password = self.password.expose_secret().to_string();
        let BiStream { send, recv } = self.get_control()?;
        send_msg(send, Message::Hello(password)).await?;
        match recv_msg(recv).await? {
            Message::Authenticated => {
                info!("Authenticated successfully");
                Ok(())
            }
            Message::Error(e) => Err(anyhow!("Failed to authenticate: {e}")),
            other => Err(anyhow!("Unexpected message: {other:?}")),
        }
    }

    fn get_control(&mut self) -> Result<&mut BiStream> {
        self.control
            .as_mut()
            .ok_or(anyhow!("Control not initialized"))
    }

    fn get_connection(&self) -> Result<&Connection> {
        self.connection
            .as_ref()
            .ok_or(anyhow!("Connection not initialized"))
    }
}
