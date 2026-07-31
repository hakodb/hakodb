use tokio::sync::broadcast;
use tokio::sync::Mutex;
use tokio::time::{interval, Duration};
use std::sync::Arc;
use crate::cloud_sync::protocol::SyncOp;

/// Buffers incoming client mutations and flushes them in micro-batches
/// to prevent Replication Storms across hundreds of connected clients.
pub struct SyncBroadcaster {
    tx: broadcast::Sender<Vec<SyncOp>>,
    pending_queue: Arc<Mutex<Vec<SyncOp>>>,
}

impl SyncBroadcaster {
    pub fn new(capacity: usize, flush_interval_ms: u64) -> Self {
        let (tx, _) = broadcast::channel(capacity);
        let pending_queue = Arc::new(Mutex::new(Vec::<SyncOp>::new()));

        let queue_clone = pending_queue.clone();
        let tx_clone = tx.clone();

        // Flushes pending changes every `flush_interval_ms`
        tokio::spawn(async move {
            let mut timer = interval(Duration::from_millis(flush_interval_ms));
            loop {
                timer.tick().await;

                let mut queue = queue_clone.lock().await;
                if !queue.is_empty() {
                    let batch = std::mem::take(&mut *queue);
                    let _ = tx_clone.send(batch);
                }
            }
        });

        Self { tx, pending_queue }
    }

    pub async fn enqueue_ops(&self, ops: Vec<SyncOp>) {
        let mut queue = self.pending_queue.lock().await;
        queue.extend(ops);
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Vec<SyncOp>> {
        self.tx.subscribe()
    }
}