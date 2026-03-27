use thiserror::Error;

#[derive(Error, Debug)]
pub enum TypesError {
    #[error("invalid ID: expected non-zero value")]
    InvalidId,

    #[error("ID overflow")]
    IdOverflow,

    #[error("invalid JSON conversion")]
    InvalidJsonConversion,

    #[error("type mismatch: expected {expected}, got {actual}")]
    TypeMismatch { expected: String, actual: String },

    #[error("value out of range: {0}")]
    ValueOutOfRange(String),

    #[error("encoding error: {0}")]
    Encoding(String),

    #[error("decoding error: {0}")]
    Decoding(String),
}

pub type TypesResult<T> = Result<T, TypesError>;
