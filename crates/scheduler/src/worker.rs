use crate::{QueueType, WorkItem, WorkPriority};
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;

pub struct RateLimiter {
    tokens: f64,
    max_tokens: f64,
    refill_rate: f64,
    last_refill: std::time::Instant,
}

impl RateLimiter {
    pub fn new(max_tokens: u32, per_second: f64) -> Self {
        Self {
            tokens: max_tokens as f64,
            max_tokens: max_tokens as f64,
            refill_rate: per_second,
            last_refill: std::time::Instant::now(),
        }
    }

    pub async fn acquire(&mut self) {
        self.refill();
        
        while self.tokens < 1.0 {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            self.refill();
        }
        
        self.tokens -= 1.0;
    }

    fn refill(&mut self) {
        let now = std::time::Instant::now();
        let elapsed = now.duration_since(self.last_refill).as_secs_f64();
        self.tokens = (self.tokens + elapsed * self.refill_rate).min(self.max_tokens);
        self.last_refill = now;
    }
}

pub struct Scheduler {
    queues: HashMap<QueueType, Arc<RwLock<crate::PriorityQueue<Box<dyn Fn() + Send + Sync>>>>>,
    rate_limiters: HashMap<QueueType, Arc<RwLock<RateLimiter>>>,
}

impl Scheduler {
    pub fn new(
        wal_queue_size: usize,
        heap_queue_size: usize,
        index_queue_size: usize,
        catalog_queue_size: usize,
        rate_limit_burst: u32,
        rate_limit_per_second: f64,
    ) -> Self {
        let mut queues = HashMap::new();
        let mut rate_limiters = HashMap::new();

        queues.insert(QueueType::Wal, Arc::new(RwLock::new(crate::PriorityQueue::new(wal_queue_size))));
        queues.insert(QueueType::Heap, Arc::new(RwLock::new(crate::PriorityQueue::new(heap_queue_size))));
        queues.insert(QueueType::Index, Arc::new(RwLock::new(crate::PriorityQueue::new(index_queue_size))));
        queues.insert(QueueType::Catalog, Arc::new(RwLock::new(crate::PriorityQueue::new(catalog_queue_size))));

        rate_limiters.insert(QueueType::Wal, Arc::new(RwLock::new(RateLimiter::new(rate_limit_burst, rate_limit_per_second))));
        rate_limiters.insert(QueueType::Heap, Arc::new(RwLock::new(RateLimiter::new(rate_limit_burst, rate_limit_per_second * 0.5))));

        Self { queues, rate_limiters }
    }

    pub async fn enqueue<F>(&self, queue: QueueType, work: F, priority: WorkPriority)
    where
        F: Fn() + Send + Sync + 'static,
    {
        if let Some(q) = self.queues.get(&queue) {
            let item = WorkItem::new(Box::new(work) as Box<dyn Fn() + Send + Sync>, priority);
            let _ = q.write().await.enqueue(item).await;
        }
    }

    pub async fn start_worker(&self, queue: QueueType) {
        if let Some(q) = self.queues.get(&queue) {
            if let Some(limiter) = self.rate_limiters.get(&queue) {
                let q = q.clone();
                let mut limiter = limiter.write().await;
                
                tokio::spawn(async move {
                    while let Some(item) = q.read().await.dequeue().await {
                        limiter.acquire().await;
                        (item.payload)();
                    }
                });
            }
        }
    }
}
