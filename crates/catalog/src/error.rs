use thiserror::Error;

#[derive(Error, Debug)]
pub enum CatalogError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("table already exists: {0}")]
    TableExists(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("index not found: {0}")]
    IndexNotFound(String),

    #[error("index already exists: {0}")]
    IndexExists(String),

    #[error("schema epoch mismatch: expected {expected}, got {actual}")]
    EpochMismatch { expected: u64, actual: u64 },

    #[error("invalid schema: {0}")]
    InvalidSchema(String),
}

pub type CatalogResult<T> = Result<T, CatalogError>;
