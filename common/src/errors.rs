use thiserror::Error;

#[derive(Error, Debug)]
pub enum MessageSendError {
    #[error("Serialization error")]
    SerializationError(#[source] wincode::WriteError),

    #[error("Network error")]
    NetworkError(#[source] quinn::WriteError),

    #[error("Message is too big ({0} bytes)")]
    MessageTooBig(usize),
}

#[derive(Error, Debug)]
pub enum MessageRecvError {
    #[error("Network error")]
    NetworkError(#[source] quinn::ReadExactError),

    #[error("Deserialization error")]
    DeserializationError(#[source] wincode::ReadError),

    #[error("Message is too large ({0} bytes)")]
    MessageTooLarge(u32),

    #[error("Timed out")]
    Timeout,
}
