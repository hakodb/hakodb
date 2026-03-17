use std::thread;

use crate::document::firelite_doc::FireLiteDoc;
use crate::error::Result;
use crate::index::manager::IndexManager;
use crate::query::plan::ScanType;
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
        storage: &StorageEngine,
        indexes: &IndexManager,
        plan: QueryPlan,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        let docs = match &plan.scan {
            ScanType::FullCollection => storage.scan_prefix(&format!("{}:", plan.collection))?,
            ScanType::CompositeIndex { fields, values } => {
                if let Some(doc_ids) = indexes.exact_match_doc_ids(&plan.collection, fields, values)
                {
                    let mut out = Vec::new();
                    for doc_id in doc_ids {
                        let key = format!("{}:{}", plan.collection, doc_id);
                        if let Some(raw) = storage.get(&key)? {
                            out.push((key, raw));
                        }
                    }
                    out
                } else {
                    storage.scan_prefix(&format!("{}:", plan.collection))?
                }
            }
        };

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
                let av = a.get(&order.field);
                let bv = b.get(&order.field);
                // let av = a.fields.get(&order.field);
                // let bv = b.fields.get(&order.field);
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
