use thiserror::Error;

#[derive(Error, Debug)]
pub enum ObservabilityError {
    #[error("logging initialization failed: {0}")]
    LoggingInit(String),

    #[error("tracing initialization failed: {0}")]
    TracingInit(String),

    #[error("invalid configuration: {0}")]
    InvalidConfig(String),
}

pub type ObservabilityResult<T> = Result<T, ObservabilityError>;
