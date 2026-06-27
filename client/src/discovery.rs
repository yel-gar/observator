use common::constants::{DISCOVERY_HEADER, SERVER_INFO_HEADER};
use common::messages::{DiscoveryMessage, PROTOCOL_VERSION};
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::{debug, debug_span, info, instrument};

pub async fn discovery_listener(addr: SocketAddr, quic_port: u16) -> anyhow::Result<()> {
    let sock = UdpSocket::bind(addr).await?;
    info!("Discovery listener started on {addr}");

    let header_length = DISCOVERY_HEADER.len();
    let server_message = DiscoveryMessage::ServerInfo {
        version: PROTOCOL_VERSION,
        quic_port,
    }
    .serialize()?;
    let mut buf = [0u8; 1024];
    loop {
        let (len, addr) = sock.recv_from(&mut buf).await?;
        let span = debug_span!("discovery", %len, %addr);
        let _guard = span.enter();
        if len < header_length {
            debug!("Packet too short");
            continue;
        }
        if &buf[..header_length] != DISCOVERY_HEADER {
            debug!("Not a discovery packet");
            continue;
        }
        let msg_bytes = &buf[header_length..len];
        match DiscoveryMessage::deserialize(msg_bytes) {
            Ok(DiscoveryMessage::Discover { version }) => {
                if version != PROTOCOL_VERSION {
                    debug!("Version mismatch");
                    continue;
                }
                let mut send_buf = Vec::with_capacity(SERVER_INFO_HEADER.len() + msg_bytes.len());
                send_buf.extend_from_slice(&SERVER_INFO_HEADER);
                send_buf.extend(&server_message);
                sock.send_to(&send_buf, addr).await?;
                debug!("Discovery response sent");
            }
            _ => {
                debug!("Could not deserialize the message");
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
