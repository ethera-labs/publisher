//! Transport error types.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum TransportError {
    #[error("client not found: {0}")]
    ClientNotFound(String),

    #[error("TLS error: {0}")]
    Tls(String),

    #[error("QUIC error: {0}")]
    Quic(String),

    #[error("message too large: {size} > {max}")]
    MessageTooLarge { size: usize, max: usize },

    #[error("io error: {0}")]
    Io(#[from] std::io::Error),

    #[error("{0}")]
    Other(String),
}
