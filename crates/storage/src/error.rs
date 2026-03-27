use discodb_types::RowId;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum StorageError {
    #[error("row not found: {0}")]
    RowNotFound(RowId),

    #[error("encoding failed: {0}")]
    EncodingFailed(String),

    #[error("decoding failed: {0}")]
    DecodingFailed(String),

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("invalid row: {0}")]
    InvalidRow(String),

    #[error("overflow: {0}")]
    Overflow(String),

    #[error("segment full: {0}")]
    SegmentFull(String),
}

pub type StorageResult<T> = Result<T, StorageError>;
