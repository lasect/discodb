use thiserror::Error;

#[derive(Error, Debug)]
pub enum SqlError {
    #[error("syntax error: {0}")]
    SyntaxError(String),

    #[error("unsupported: {0}")]
    Unsupported(String),

    #[error("planning failed: {0}")]
    PlanningFailed(String),

    #[error("type error: {0}")]
    TypeError(String),

    #[error("table not found: {0}")]
    TableNotFound(String),

    #[error("column not found: {0}")]
    ColumnNotFound(String),
}

pub type SqlResult<T> = Result<T, SqlError>;
