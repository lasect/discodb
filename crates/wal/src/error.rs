use discodb_types::Lsn;
use thiserror::Error;

#[derive(Error, Debug)]
pub enum WalError {
    #[error("record too large: {0}")]
    RecordTooLarge(usize),

    #[error("checksum mismatch: expected {expected}, got {actual}")]
    ChecksumMismatch { expected: u32, actual: u32 },

    #[error("truncated record")]
    TruncatedRecord,

    #[error("unknown operation: {0}")]
    UnknownOp(u8),

    #[error("incomplete transaction: {0}")]
    IncompleteTransaction(String),

    #[error("write failed: {0}")]
    WriteFailed(String),

    #[error("LSN ordering violation: expected >= {expected}, got {actual}")]
    LsnOrderingViolation { expected: Lsn, actual: Lsn },
}

pub type WalResult<T> = Result<T, WalError>;
