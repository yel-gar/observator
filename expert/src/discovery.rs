use common::constants::{DISCOVERY_HEADER, SERVER_INFO_HEADER};
use common::messages::{DiscoveryMessage, PROTOCOL_VERSION};
use std::collections::HashSet;
use std::net::SocketAddr;
use std::time::Duration;
use tokio::net::UdpSocket;
use tracing::{debug, debug_span, instrument};

#[instrument]
pub async fn discover(
    broadcast_addr: SocketAddr,
    timeout: Duration,
) -> anyhow::Result<Vec<SocketAddr>> {
    debug!("Sending broadcast");
    let sock = UdpSocket::bind("0.0.0.0:0").await?;
    sock.set_broadcast(true)?;
    let msg = DiscoveryMessage::Discover {
        version: PROTOCOL_VERSION,
    }
    .serialize()?;
    let mut buf = Vec::with_capacity(DISCOVERY_HEADER.len() + msg.len());
    buf.extend_from_slice(&DISCOVERY_HEADER);
    buf.extend(msg);

    sock.send_to(&buf, broadcast_addr).await?;

    debug!("Listening for responses");
    let mut output = HashSet::<SocketAddr>::with_capacity(16);
    let _ = tokio::time::timeout(timeout, async {
        let mut buf = [0u8; 1024];
        let header_size = SERVER_INFO_HEADER.len();
        loop {
            let (len, addr) = sock.recv_from(&mut buf).await?;
            let span = debug_span!("recv", %len, %addr);
            let _guard = span.enter();
            debug!("Received packet");
            if len < header_size {
                debug!("Packet too short");
                continue;
            }
            if &buf[..header_size] != SERVER_INFO_HEADER {
                debug!("Packet doesn't match discovery header");
                continue;
            }

            let msg_bytes = &buf[header_size..len];
            match DiscoveryMessage::deserialize(msg_bytes) {
                Ok(DiscoveryMessage::ServerInfo { version, quic_port }) => {
                    debug!(%version, %quic_port, "Discovered server");
                    output.insert(SocketAddr::new(addr.ip(), quic_port));
                }
                _ => {
                    debug!("Failed to decode the message");
                }
            }
        }

        #[allow(unreachable_code)]
        return Ok::<(), anyhow::Error>(());
    })
    .await;

    Ok(output.into_iter().collect())
}
