use std::sync::{Arc, RwLock};
use std::thread;
use hashbrown::HashMap;

use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};
use crate::document::value::Value;
use crate::error::Result;
use crate::index::manager::IndexManager;
use crate::query::plan::ScanType;
use crate::query::query::AggregateOp;
use crate::storage::engine::StorageEngine;

use super::worker::{matches_filters_view, run_task, run_task_projected};
use super::super::plan::QueryPlan;
use super::result_stream::QueryResults;
use super::scheduler::shard_tasks;

pub struct ParallelQueryExecutor {
    workers: usize,
    catalog: Arc<crate::util::catalog::Catalog>,
}

impl ParallelQueryExecutor {
    pub fn new(workers: usize, catalog: Arc<crate::util::catalog::Catalog>) -> Self {
        Self {
            workers: workers.max(1),
            catalog
        }
    }

    pub fn execute(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        
        let docs = {
            let storage = storage_arc.read().unwrap();
            match &plan.scan {
                ScanType::UnionIndex { scans } => {
                    let mut union_map = HashMap::new();
                    for scan in scans {
                        // Pass plan.limit here to ensure each branch is optimized
                        let branch_docs = self.execute_single_scan(&storage, indexes, scan, &plan.collection, plan.limit)?;
                        for (key, raw) in branch_docs {
                            union_map.insert(key, raw);
                        }
                    }
                    union_map.into_iter().collect()
                }
                _ => self.execute_single_scan(&storage, indexes, &plan.scan, &plan.collection, plan.limit)?,
            }
        };

        let tasks = shard_tasks(docs, self.workers, plan.clone(), Some(storage_arc.clone()), self.catalog.clone());
        let mut handles = Vec::new();
        for task in tasks {
            handles.push(thread::spawn(move || run_task(task)));
        }

        let mut results: QueryResults = Vec::new();
        for handle in handles {
            results.extend(handle.join().unwrap_or_default());
        }

        if let Some(order) = &plan.order_by {
            results.sort_by(|(_, a), (_, b)| {
                let av = a.get(&order.field);
                let bv = b.get(&order.field);
                let cmp = av.cmp(&bv);
                if order.ascending { cmp } else { cmp.reverse() }
            });
        }

        let mut final_results = if let Some(offset) = plan.offset {
            results.into_iter().skip(offset).collect()
        } else {
            results
        };

        if let Some(limit) = plan.limit {
            final_results.truncate(limit);
        }

        Ok(final_results)
    }

    pub fn execute_aggregation(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
        ops: &[AggregateOp]
    ) -> Result<HashMap<String, f64>> {
        
        let docs = {
            let storage = storage_arc.read().unwrap();
            // Use plan.limit
            self.execute_single_scan(&storage, indexes, &plan.scan, &plan.collection, plan.limit)?
        };

        let tasks = shard_tasks(docs, self.workers, plan.clone(), Some(storage_arc.clone()), self.catalog.clone());
        let (tx, rx) = std::sync::mpsc::channel();

        for task in tasks {
            let thread_tx = tx.clone();
            let thread_ops = ops.to_vec();
            
            thread::spawn(move || {
                let mut partial_results = HashMap::new();
                for (id, mut bytes) in task.docs {
                    if bytes.is_empty() {
                        if let Some(storage_lock) = &task.storage {
                            if let Ok(storage) = storage_lock.read() {
                                if let Ok(Some(data)) = storage.read_pointer_uncached_by_key(&id) {
                                    bytes = data;
                                }
                            }
                        }
                    }
                    if bytes.is_empty() { continue; }

                    if let Some(view) = FireLiteDocView::new(&bytes) {
                        if matches_filters_view(&bytes, &task.plan) {
                            for op in &thread_ops {
                                match op {
                                    AggregateOp::Count => {
                                        *partial_results.entry("count".to_string()).or_insert(0.0) += 1.0;
                                    }
                                    AggregateOp::Sum(field) | AggregateOp::Avg(field) => {
                                        if let Some(borrowed) = view.get_field_value(field) {
                                            if let Some(num) = borrowed.as_f64() {
                                                let key = if matches!(op, AggregateOp::Sum(_)) { 
                                                    format!("sum_{}", field) 
                                                } else { 
                                                    format!("avg_tmp_{}", field) 
                                                };
                                                *partial_results.entry(key).or_insert(0.0) += num;
                                                if matches!(op, AggregateOp::Avg(_)) {
                                                    *partial_results.entry(format!("avg_cnt_{}", field)).or_insert(0.0) += 1.0;
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                let _ = thread_tx.send(partial_results);
            });
        }

        drop(tx); 

        let mut final_results: HashMap<String, f64> = HashMap::new();
        while let Ok(partial) = rx.recv() {
            for (k, v) in partial { *final_results.entry(k).or_insert(0.0) += v; }
        }

        let keys: Vec<String> = final_results.keys().cloned().collect();
        for k in keys {
            if k.starts_with("avg_tmp_") {
                let field = &k[8..];
                let sum = final_results.remove(&k).unwrap_or(0.0);
                let count = final_results.remove(&format!("avg_cnt_{}", field)).unwrap_or(1.0);
                final_results.insert(format!("avg_{}", field), if count > 0.0 { sum / count } else { 0.0 });
            }
        }

        Ok(final_results)
    }

    pub fn execute_projected(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
    ) -> Result<Vec<(String, Vec<(String, Value)>)>> {
        
        let docs = {
            let storage = storage_arc.read().unwrap();
            // Use plan.limit
            self.execute_single_scan(&storage, indexes, &plan.scan, &plan.collection, plan.limit)?
        };
        
        let tasks = shard_tasks(docs, self.workers, plan.clone(), Some(storage_arc.clone()), self.catalog.clone());
        let mut handles = Vec::new();
        for task in tasks {
            handles.push(thread::spawn(move || run_task_projected(task)));
        }

        let mut results: Vec<(String, Vec<(String, Value)>)> = Vec::new();
        for handle in handles {
            results.extend(handle.join().unwrap_or_default());
        }

        if let Some(order) = &plan.order_by {
            results.sort_by(|(_, a), (_, b)| {
                let av = a.iter().find(|(k,_)| k == &order.field).map(|(_,v)| v);
                let bv = b.iter().find(|(k,_)| k == &order.field).map(|(_,v)| v);
                let cmp = av.cmp(&bv);
                if order.ascending { cmp } else { cmp.reverse() }
            });
        }

        let mut final_results = if let Some(offset) = plan.offset {
            results.into_iter().skip(offset).collect()
        } else {
            results
        };

        if let Some(limit) = plan.limit {
            final_results.truncate(limit);
        }

        Ok(final_results)
    }

    fn execute_single_scan(
        &self, 
        storage: &StorageEngine, 
        indexes: &IndexManager, 
        scan: &ScanType, 
        collection: &str,
        limit: Option<usize> // Passed here to allow recursive UnionIndex optimization
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let max_ids = limit.unwrap_or(usize::MAX);

        match scan {
            ScanType::FullCollection => {
                let keys = storage.scan_prefix_keys(&format!("{}:", collection));
                // We don't apply limit here for FullCollection because we want workers 
                // to handle it to support filters. 
                Ok(keys.into_iter().map(|k| (k, Vec::new())).collect())
            }
            
            ScanType::SecondaryIndex { field, value } => {
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        for doc_id in index.range_scan(value, value).iter().take(max_ids) {
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
                    for doc_id in doc_ids.iter().take(max_ids) {
                        let key = format!("{}:{}", collection, doc_id);
                        if let Some(raw) = storage.get(&key)? { out.push((key, raw)); }
                    }
                }
                Ok(out)
            }

            ScanType::CursorIndex { start, end } => {
                let mut out = Vec::new();
                for idx in indexes.indexes_for_collection(collection) {
                    let doc_ids: Vec<_> = idx.tree
                        .range((start.clone(), end.clone()))
                        .map(|(_, id)| id.clone())
                        .take(max_ids)
                        .collect();

                    for doc_id in doc_ids {
                        let key = format!("{}:{}", collection, doc_id);
                        
                        // Use 'storage' (the parameter) to resolve the pointer immediately
                        // while we are already holding the read lock.
                        if let Some(pointer) = storage.index.get(&key) {
                            if let Ok(Some(bytes)) = storage.read_pointer_internal(pointer, true) {
                                out.push((key, bytes));
                            }
                        }
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
                            for doc_id in doc_ids.iter().take(max_ids) {
                                let key = format!("{}:{}", collection, doc_id);
                                out.push((key, Vec::new()));
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::UnionIndex { .. } => Ok(vec![]),
        }
    }
}