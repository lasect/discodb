use thiserror::Error;

#[derive(Error, Debug)]
pub enum ExecutorError {
    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),

    #[error("scan failed: {0}")]
    ScanFailed(String),

    #[error("execution failed: {0}")]
    ExecutionFailed(String),

    #[error("type mismatch: {0}")]
    TypeMismatch(String),
}

pub type ExecutorResult<T> = Result<T, ExecutorError>;
