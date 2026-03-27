use discodb_types::MessageId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum DiscordError {
    #[error("HTTP request failed: {0}")]
    RequestFailed(#[from] reqwest::Error),

    #[error("rate limited: retry after {0}ms")]
    RateLimited(u64),

    #[error("Discord API error: {0}")]
    ApiError(String),

    #[error("authentication failed")]
    AuthFailed,

    #[error("not found: {0}")]
    NotFound(String),

    #[error("message not found: {0}")]
    MessageNotFound(MessageId),

    #[error("invalid message: {0}")]
    InvalidMessage(String),

    #[error("partial success: {0}")]
    PartialSuccess(String),

    #[error("transport error: {0}")]
    TransportError(String),
}

pub type DiscordResult<T> = Result<T, DiscordError>;
