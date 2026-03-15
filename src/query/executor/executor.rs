use std::thread;

use crate::document::firelite_doc::FireLiteDoc;
use crate::error::Result;
use crate::storage::engine::StorageEngine;

use super::super::plan::QueryPlan;
use super::result_stream::QueryResults;
use super::scheduler::shard_tasks;

pub struct ParallelQueryExecutor {
    workers: usize,
}

impl ParallelQueryExecutor {
    pub fn new(workers: usize) -> Self {
        Self {
            workers: workers.max(1),
        }
    }

    pub fn execute(
        &self,
        storage: &mut StorageEngine,
        plan: QueryPlan,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        let docs = storage.scan_prefix(&format!("{}:", plan.collection))?;
        let tasks = shard_tasks(docs, self.workers, plan.clone());
        let mut handles = Vec::new();
        for task in tasks {
            handles.push(thread::spawn(move || super::worker::run_task(task)));
        }

        let mut results: QueryResults = Vec::new();
        for handle in handles {
            results.extend(handle.join().unwrap_or_default());
        }

        if let Some(order) = &plan.order_by {
            results.sort_by(|(_, a), (_, b)| {
                let av = a.fields.get(&order.field);
                let bv = b.fields.get(&order.field);
                format!("{:?}", av).cmp(&format!("{:?}", bv))
            });
            if !order.ascending {
                results.reverse();
            }
        }

        if let Some(limit) = plan.limit {
            results.truncate(limit);
        }

        Ok(results)
    }
}
