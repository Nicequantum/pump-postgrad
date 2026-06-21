//! Typed errors for the post-graduation trading bot.

use thiserror::Error;

#[derive(Debug, Error)]
pub enum PostGradError {
    #[error("configuration error: {0}")]
    Config(String),

    #[error("wallet error: {0}")]
    Wallet(String),

    #[error("RPC error: {0}")]
    Rpc(String),

    #[error("Jupiter swap error: {0}")]
    Jupiter(String),

    #[error("ingest error: {0}")]
    Ingest(String),

    #[error("graduation detection error: {0}")]
    Graduation(String),

    #[error("position error: {0}")]
    Position(String),

    #[error("strategy rejected entry: {0}")]
    StrategyRejected(String),

    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

pub type Result<T> = std::result::Result<T, PostGradError>;