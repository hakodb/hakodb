use std::thread;
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value; // Use your project's Value enum
use crate::error::Result;
use crate::index::manager::IndexManager;
use crate::query::plan::ScanType;
use crate::query::query::AggregateOp;
use crate::storage::engine::StorageEngine;

use super::super::plan::QueryPlan;
use super::result_stream::QueryResults;
use super::scheduler::shard_tasks;

use hashbrown::HashMap;

pub struct ParallelQueryExecutor {
    workers: usize,
}

impl ParallelQueryExecutor {
    pub fn new(workers: usize) -> Self {
        Self {
            workers: workers.max(1),
        }
    }

    /// Primary execution for fetching documents
    pub fn execute(
        &self,
        storage: &StorageEngine,
        indexes: &IndexManager,
        plan: QueryPlan,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        let docs = match &plan.scan {
            ScanType::FullCollection => storage.scan_prefix(&format!("{}:", plan.collection))?,
            ScanType::CompositeIndex { fields, values } => {
                if let Some(doc_ids) = indexes.exact_match_doc_ids(&plan.collection, fields, values) {
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

        // Apply Ordering
        if let Some(order) = &plan.order_by {
            results.sort_by(|(_, a), (_, b)| {
                let av = a.get(&order.field);
                let bv = b.get(&order.field);
                // format!("{:?}", av).cmp(&format!("{:?}", bv))
                av.cmp(&bv)
            });
            if !order.ascending {
                results.reverse();
            }
        }

        // Apply Limit
        if let Some(limit) = plan.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    /// Aggregation execution logic
    pub fn execute_aggregation(
        &self,
        storage: &StorageEngine,
        indexes: &IndexManager, // Added IndexManager to match execute
        plan: QueryPlan,
        ops: &[AggregateOp]
    ) -> Result<HashMap<String, f64>> {
        // Reuse the parallel executor to get filtered documents
        let docs = self.execute(storage, indexes, plan)?; 
        let mut results = HashMap::new();

        for op in ops {
            match op {
                AggregateOp::Count => {
                    results.insert("count".to_string(), docs.len() as f64);
                }
                AggregateOp::Sum(field) => {
                    let mut total = 0.0;
                    for (_, doc) in &docs {
                        if let Some(val) = doc.get(field) {
                            match val {
                                Value::Int(i) => total += *i as f64,
                                Value::Float(f) => total += *f,
                                _ => {} // Skip non-numeric
                            }
                        }
                    }
                    results.insert(format!("sum_{}", field), total);
                }
                AggregateOp::Avg(field) => {
                    let mut sum = 0.0;
                    let mut count = 0.0;

                    for (_, doc) in &docs {
                        if let Some(val) = doc.get(field) {
                            match val {
                                Value::Int(i) => {
                                    sum += *i as f64;
                                    count += 1.0;
                                }
                                Value::Float(f) => {
                                    sum += *f;
                                    count += 1.0;
                                }
                                _ => {}
                            }
                        }
                    }

                    let avg = if count > 0.0 { sum / count } else { 0.0 };
                    results.insert(format!("avg_{}", field), avg);
                }
            }
        }
        Ok(results)
    }
}