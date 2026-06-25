use thiserror::Error;

#[derive(Error, Debug)]
pub enum ConnectionLimitError {
    #[error("Connection limit for IP exceeded")]
    Individual,

    #[error("Global connection limit exceeded")]
    Global,
}
