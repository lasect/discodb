use thiserror::Error;

#[derive(Error, Debug)]
pub enum SchedulerError {
    #[error("queue full")]
    QueueFull,

    #[error("rate limited: retry after {0}ms")]
    RateLimited(u64),

    #[error("worker crashed")]
    WorkerCrashed,

    #[error("queue not found: {0}")]
    QueueNotFound(String),
}

pub type SchedulerResult<T> = Result<T, SchedulerError>;
