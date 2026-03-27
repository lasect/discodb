#[derive(Clone, Debug)]
pub enum WorkPriority {
    Critical,
    High,
    Normal,
    Low,
}

pub struct WorkItem<T> {
    pub priority: WorkPriority,
    pub payload: T,
    pub enqueued_at: std::time::Instant,
    pub retries: u32,
}

impl<T> WorkItem<T> {
    pub fn new(payload: T, priority: WorkPriority) -> Self {
        Self {
            priority,
            payload,
            enqueued_at: std::time::Instant::now(),
            retries: 0,
        }
    }

    pub fn increment_retry(&mut self) {
        self.retries += 1;
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub enum QueueType {
    Wal,
    Heap,
    Index,
    Catalog,
}

impl QueueType {
    pub fn name(&self) -> &'static str {
        match self {
            QueueType::Wal => "wal",
            QueueType::Heap => "heap",
            QueueType::Index => "index",
            QueueType::Catalog => "catalog",
        }
    }
}
