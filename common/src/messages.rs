use crate::errors::{MessageRecvError, MessageSendError};
use quinn::{RecvStream, SendStream};
use std::time::Duration;
use tokio::time::timeout;
use wincode::{ReadResult, SchemaRead, SchemaWrite, WriteResult};

const MAX_RECV_MESSAGE_SIZE: u32 = 1024 * 1024 * 10; // 10 Mb

#[derive(SchemaWrite, SchemaRead, PartialEq, Debug)]
pub enum Message {
    // Shared
    Ping,
    Pong,
    Ok,
    Error(String),

    // Client
    Hello(String), // password

    // Server
    Authenticated,
}

impl Message {
    fn serialize(&self) -> WriteResult<Vec<u8>> {
        wincode::serialize(self)
    }

    fn deserialize(data: &[u8]) -> ReadResult<Message> {
        wincode::deserialize(data)
    }
}

pub async fn send_msg(send: &mut SendStream, msg: Message) -> Result<(), MessageSendError> {
    let serialized = msg
        .serialize()
        .map_err(MessageSendError::SerializationError)?;

    let length = serialized.len();
    if length > u32::MAX as usize {
        return Err(MessageSendError::MessageTooBig(length));
    }

    send.write_all(&(length as u32).to_be_bytes())
        .await
        .map_err(MessageSendError::NetworkError)?;
    send.write_all(&serialized)
        .await
        .map_err(MessageSendError::NetworkError)?;

    Ok(())
}

pub async fn recv_msg(recv: &mut RecvStream) -> Result<Message, MessageRecvError> {
    let mut len_buf: [u8; 4] = [0; 4];
    recv.read_exact(&mut len_buf)
        .await
        .map_err(MessageRecvError::NetworkError)?;

    let length = u32::from_be_bytes(len_buf);
    if length > MAX_RECV_MESSAGE_SIZE {
        return Err(MessageRecvError::MessageTooLarge(length));
    }

    let mut content_buf = vec![0u8; length as usize];
    recv.read_exact(&mut content_buf)
        .await
        .map_err(MessageRecvError::NetworkError)?;

    Message::deserialize(&content_buf).map_err(MessageRecvError::DeserializationError)
}

pub async fn recv_msg_timeout(
    recv: &mut RecvStream,
    duration: Duration,
) -> Result<Message, MessageRecvError> {
    timeout(duration, recv_msg(recv))
        .await
        .map_err(|_| MessageRecvError::Timeout)?
}
