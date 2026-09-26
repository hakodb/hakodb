use std::sync::{Arc, RwLock};
use crate::storage::engine::{StorageEngine, Pointer};
use super::task::QueryTask;

pub fn shard_tasks(
    mut docs: Vec<(String, Pointer)>,
    workers: usize,
    plan: super::super::plan::QueryPlan,
    storage: Option<Arc<RwLock<StorageEngine>>>, // NEW: Accept storage handle
) -> Vec<QueryTask> {
    let worker_count = workers.max(1);
    let chunk_size = (docs.len() / worker_count).max(1);
    // ponytail: drain moves ownership — the old chunks().to_vec()
    // deep-cloned every id String per task split (~1 alloc/row/scan).
    let mut tasks = Vec::with_capacity(worker_count);
    while !docs.is_empty() {
        let at = chunk_size.min(docs.len());
        let chunk: Vec<(String, Pointer)> = docs.drain(..at).collect();
        tasks.push(QueryTask {
            docs: chunk,
            plan: plan.clone(),
            storage: storage.clone(),
        });
    }
    tasks
}
