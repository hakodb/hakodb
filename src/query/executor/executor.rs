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
            ScanType::UnionIndex { scans } => {
                // v0.6.1 Optimization: Run multiple index scans and merge unique results
                let mut union_map = HashMap::new();
                for scan in scans {
                    let branch_docs = self.execute_single_scan(storage, indexes, scan, &plan.collection)?;
                    for (key, raw) in branch_docs {
                        // Using a HashMap to deduplicate documents that match multiple OR branches
                        union_map.insert(key, raw);
                    }
                }
                union_map.into_iter().collect()
            }
            // Standard paths call the same helper
            _ => self.execute_single_scan(storage, indexes, &plan.scan, &plan.collection)?,
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

    /// Internal Helper: The logic for individual index/collection scans
    fn execute_single_scan(
        &self, 
        storage: &StorageEngine, 
        indexes: &IndexManager, 
        scan: &ScanType, 
        collection: &str
    ) -> Result<Vec<(String, Vec<u8>)>> {
        match scan {
            ScanType::FullCollection => storage.scan_prefix(&format!("{}:", collection)),
            
            ScanType::SecondaryIndex { field, value } => {
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        let doc_ids = index.range_scan(value, value);
                        for doc_id in doc_ids {
                            let key = format!("{}:{}", collection, doc_id);
                            if let Some(raw) = storage.get(&key)? { out.push((key, raw)); }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::CompositeIndex { fields, values } => {
                let mut out = Vec::new();
                if let Some(doc_ids) = indexes.exact_match_doc_ids(collection, fields, values) {
                    for doc_id in doc_ids {
                        let key = format!("{}:{}", collection, doc_id);
                        if let Some(raw) = storage.get(&key)? { out.push((key, raw)); }
                    }
                }
                Ok(out)
            }

            ScanType::CursorIndex { start_key } => {
                let mut out = Vec::new();
                for idx in indexes.indexes_for_collection(collection) {
                    let end_key = { let mut e = start_key.clone(); e.push(0xFF); e };
                    let doc_ids: Vec<std::sync::Arc<str>> = idx.range_scan(&start_key, &end_key);
                    for doc_id in doc_ids {
                        let key = format!("{}:{}", collection, doc_id);
                        if let Some(raw) = storage.get(&key)? { out.push((key, raw)); }
                    }
                    if !out.is_empty() { break; }
                }
                Ok(out)
            }

            ScanType::InvertedIndex { field, query } => {
                let mut out = Vec::new();
                if let Some(fts_map) = indexes.fts.get(collection) {
                    if let Some(index) = fts_map.get(field) {
                        if let Some(doc_ids) = index.search(query) {
                            for doc_id in doc_ids {
                                let key = format!("{}:{}", collection, doc_id);
                                if let Some(raw) = storage.get(&key)? {
                                    out.push((key, raw));
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }

            // UnionIndex is handled by the caller, but match must be exhaustive
            ScanType::UnionIndex { .. } => Ok(vec![]),
        }
    }

    pub fn execute_projected(
        &self,
        storage: &StorageEngine,
        indexes: &IndexManager,
        plan: QueryPlan,
    ) -> Result<Vec<(String, Vec<(String, Value)>)>> {
        
        // 1. Fast Scan (Index or Full)
        let docs = self.execute_single_scan(storage, indexes, &plan.scan, &plan.collection)?;
        
        // 2. Parallel Sharding
        let tasks = shard_tasks(docs, self.workers, plan.clone());
        let mut handles = Vec::new();
        
        for task in tasks {
            handles.push(thread::spawn(move || super::worker::run_task_projected(task)));
        }

        // FIX: Explicit type annotation for results
        let mut results: Vec<(String, Vec<(String, Value)>)> = Vec::new();
        for handle in handles {
            // FIX: help compiler with type inference on join
            let task_results: Vec<(String, Vec<(String, Value)>)> = handle.join().unwrap_or_default();
            results.extend(task_results);
        }

        // 4. Apply Post-processing (Ordering/Limit)
        if let Some(order) = &plan.order_by {
            // FIX: Explicit closure type annotations
            results.sort_by(|(_, a): &(String, Vec<(String, Value)>), (_, b)| {
                let av = a.iter().find(|(k,_)| k == &order.field).map(|(_,v)| v);
                let bv = b.iter().find(|(k,_)| k == &order.field).map(|(_,v)| v);
                
                let cmp = av.cmp(&bv);
                if order.ascending { cmp } else { cmp.reverse() }
            });
        }

        if let Some(offset) = plan.offset {
            if offset < results.len() {
                results = results.into_iter().skip(offset).collect();
            } else {
                results.clear();
            }
        }

        if let Some(limit) = plan.limit {
            results.truncate(limit);
        }

        Ok(results)
    }
}

