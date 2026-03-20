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
            },
            ScanType::CursorIndex { start_key } => {
                let mut out = Vec::new();
                // We jump to the exact spot in the B-Tree index
                // This is O(log N) instead of O(N)
                for idx in indexes.indexes_for_collection(&plan.collection) {
                    // Find the index that matches this scan
                    let end_key = { let mut e = start_key.clone(); e.push(0xFF); e };
                    let doc_ids: Vec<std::sync::Arc<str>> = idx.range_scan(&start_key, &end_key);
                    
                    for doc_id in doc_ids {
                        let key = format!("{}:{}", plan.collection, doc_id);
                        if let Some(raw) = storage.get(&key)? {
                            out.push((key, raw));
                        }
                        // Optimization: If no filters, we can stop once limit is reached
                        if plan.filters.is_empty() && plan.limit.map_or(false, |l| out.len() >= l) {
                            break;
                        }
                    }
                    if !out.is_empty() { break; }
                }
                out
            },
            ScanType::SecondaryIndex { field, value } => {
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(&plan.collection) {
                    if let Some(index) = sec_map.get(field) {
                        // Secondary indexes return Vec<String> (Doc IDs)
                        let doc_ids = index.range_scan(value, value);
                        for doc_id in doc_ids {
                            let key = format!("{}:{}", plan.collection, doc_id);
                            if let Some(raw) = storage.get(&key)? {
                                out.push((key, raw));
                            }
                        }
                    }
                }
                out
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
                let cmp = av.cmp(&bv);
                if order.ascending { cmp } else { cmp.reverse() }
            });
            // if !order.ascending {
            //     results.reverse();
            // }
        }


        // 2. Apply OFFSET (Skip N records)
        let results = if let Some(offset) = plan.offset {
            if offset >= results.len() {
                Vec::new() // Offset is larger than result set
            } else {
                results.into_iter().skip(offset).collect()
            }
        } else {
            results
        };

        // Apply Limit
        let mut final_results = results;
        if let Some(limit) = plan.limit {
            final_results.truncate(limit);
        }

        Ok(final_results)
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