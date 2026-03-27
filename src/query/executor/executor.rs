use hashbrown::HashMap;
use std::sync::{Arc, RwLock};
use std::thread;

use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView, BorrowedValue};
use crate::document::value::Value;
use crate::error::Result;
use crate::index::manager::IndexManager;
use crate::query::plan::ScanType;
use crate::query::query::AggregateOp;
use crate::storage::engine::StorageEngine;

use super::super::plan::QueryPlan;
use super::result_stream::QueryResults;
use super::scheduler::shard_tasks;
use super::task::QueryTask;
use super::worker::{matches_filters_view, run_task, run_task_projected};

use crate::index::index_key::decode_scalar_as_f64;

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
                        let branch_docs = self.execute_single_scan(
                            &storage,
                            indexes,
                            scan,
                            &plan.collection,
                            plan.scan_limit,
                        )?;
                        for (key, raw) in branch_docs {
                            union_map.insert(key, raw);
                        }
                    }
                    union_map.into_iter().collect()
                }
                _ => self.execute_single_scan(
                    &storage,
                    indexes,
                    &plan.scan,
                    &plan.collection,
                    plan.scan_limit,
                )?,
            }
        };

        let mut results: QueryResults = Vec::new();
        let doc_count = docs.len();

        if doc_count < 1000 || self.workers <= 1 {
            let task = QueryTask {
                docs,
                plan: plan.clone(),
                storage: Some(storage_arc.clone()),
            };
            results = run_task(task);
        } else {
            let optimal_workers = self.workers.min((doc_count / 500).max(1));
            let tasks = shard_tasks(
                docs,
                optimal_workers,
                plan.clone(),
                Some(storage_arc.clone()),
            );

            let mut handles = Vec::new();
            for task in tasks {
                handles.push(thread::spawn(move || run_task(task)));
            }

            for handle in handles {
                results.extend(handle.join().unwrap_or_default());
            }
        }

        // ONLY SORT IF NOT SATISFIED BY INDEX
        if !plan.order_by_satisfied {
            if let Some(order) = &plan.order_by {
                // This block is very expensive for large documents!
                results.sort_by(|(_, a), (_, b)| {
                    let av = a.get(&order.field);
                    let bv = b.get(&order.field);
                    let cmp = av.cmp(&bv);
                    if order.ascending { cmp } else { cmp.reverse() }
                });
            }
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
        ops: &[AggregateOp],
    ) -> Result<HashMap<String, f64>> {

        if ops.len() == 1 && plan.or_groups.is_empty() {
            if let AggregateOp::Sum(ref sum_field) = ops[0] {
                let mut filter_ids: Option<hashbrown::HashSet<String>> = None;
                let mut can_use_index_filter = true;

                for filter in &plan.filters {
                    if filter.op == crate::query::filter::Operator::Eq {
                        let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                        if let Some(ids) = indexes.lookup_secondary(&plan.collection, &filter.field, &val_bytes) {
                            let id_set: hashbrown::HashSet<_> = ids.into_iter().collect();
                            if let Some(ref mut existing_set) = filter_ids {
                                existing_set.retain(|id| id_set.contains(id));
                            } else {
                                filter_ids = Some(id_set);
                            }
                        } else { can_use_index_filter = false; break; }
                    } else { can_use_index_filter = false; break; }
                }

                if can_use_index_filter {
                    if let Some(sec_map) = indexes.secondary.get(&plan.collection) {
                        if let Some(sum_index) = sec_map.get(sum_field) {
                            let mut total = 0.0;
                            for (val_bytes, doc_ids) in sum_index.get_map() {
                                if let Some(val) = decode_scalar_as_f64(val_bytes) {
                                    let match_count = if let Some(ref allowed_ids) = filter_ids {
                                        doc_ids.iter().filter(|id| allowed_ids.contains(*id)).count()
                                    } else {
                                        doc_ids.len()
                                    };
                                    total += val * (match_count as f64);
                                }
                            }
                            let mut res = HashMap::new();
                            res.insert(format!("sum_{}", sum_field), total);
                            return Ok(res);
                        }
                    }
                }
            }
        }

        let docs = {
            let storage = storage_arc.read().unwrap();
            self.execute_single_scan(
                &storage,
                indexes,
                &plan.scan,
                &plan.collection,
                plan.scan_limit,
            )?
        };

        let doc_count = docs.len();
        let mut final_results: HashMap<String, f64> = HashMap::new();

        let process_task = |task: QueryTask, thread_ops: Vec<AggregateOp>| -> HashMap<String, f64> {
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
                                    // PERFORMANCE: Iterate the view to find the requested field without decoding the whole doc
                                    if let Some((_, tag, data)) = view.iter().find(|(k, _, _)| k == field) {
                                        let borrowed = BorrowedValue { tag, data };
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
            partial_results
        };

        if doc_count < 1000 || self.workers <= 1 {
            let task = QueryTask {
                docs,
                plan: plan.clone(),
                storage: Some(storage_arc.clone()),
            };
            final_results = process_task(task, ops.to_vec());
        } else {
            let optimal_workers = self.workers.min((doc_count / 500).max(1));
            let tasks = shard_tasks(
                docs,
                optimal_workers,
                plan.clone(),
                Some(storage_arc.clone()),
            );
            let (tx, rx) = std::sync::mpsc::channel();

            for task in tasks {
                let thread_tx = tx.clone();
                let thread_ops = ops.to_vec();
                thread::spawn(move || {
                    let partial = process_task(task, thread_ops);
                    let _ = thread_tx.send(partial);
                });
            }
            drop(tx);

            while let Ok(partial) = rx.recv() {
                for (k, v) in partial {
                    *final_results.entry(k).or_insert(0.0) += v;
                }
            }
        }

        let keys: Vec<String> = final_results.keys().cloned().collect();
        for k in keys {
            if k.starts_with("avg_tmp_") {
                let field = &k[8..];
                let sum = final_results.remove(&k).unwrap_or(0.0);
                let count = final_results.remove(&format!("avg_cnt_{}", field)).unwrap_or(1.0);
                final_results.insert(
                    format!("avg_{}", field),
                    if count > 0.0 { sum / count } else { 0.0 },
                );
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
            self.execute_single_scan(
                &storage,
                indexes,
                &plan.scan,
                &plan.collection,
                plan.scan_limit,
            )?
        };

        let mut results: Vec<(String, Vec<(String, Value)>)> = Vec::new();
        let doc_count = docs.len();

        if doc_count < 1000 || self.workers <= 1 {
            let task = QueryTask {
                docs,
                plan: plan.clone(),
                storage: Some(storage_arc.clone()),
            };
            results = run_task_projected(task);
        } else {
            let optimal_workers = self.workers.min((doc_count / 500).max(1));
            let tasks = shard_tasks(
                docs,
                optimal_workers,
                plan.clone(),
                Some(storage_arc.clone()),
            );

            let mut handles = Vec::new();
            for task in tasks {
                handles.push(thread::spawn(move || run_task_projected(task)));
            }

            for handle in handles {
                results.extend(handle.join().unwrap_or_default());
            }
        }

        if let Some(order) = &plan.order_by {
            results.sort_by(|(_, a), (_, b)| {
                let av = a.iter().find(|(k, _)| k == &order.field).map(|(_, v)| v);
                let bv = b.iter().find(|(k, _)| k == &order.field).map(|(_, v)| v);
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


    #[inline]
    fn make_key(collection: &str, doc_id: &str) -> String {
        format!("{}:{}", collection, doc_id)
    }

    fn execute_single_scan(
        &self,
        storage: &StorageEngine,
        indexes: &IndexManager,
        scan: &ScanType,
        collection: &str,
        limit: Option<usize>,
    ) -> Result<Vec<(String, Vec<u8>)>> {
        let max_ids = limit.unwrap_or(usize::MAX);

        match scan {
            ScanType::FullCollection => {
                let keys = storage.scan_prefix_keys(&format!("{}:", collection));
                Ok(keys
                    .into_iter()
                    .take(max_ids)
                    .map(|k| (k, Vec::new()))
                    .collect())
            }

            ScanType::SecondaryIndex { field, value } => {
                let mut out = Vec::new();

                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        out.extend(
                            index
                                .range_scan(value, value)
                                .iter()
                                .take(max_ids)
                                .map(|doc_id| (Self::make_key(collection, doc_id), Vec::new()))
                        );
                    }
                }

                Ok(out)
            }

            ScanType::CompositeIndex { fields, values, reverse } => {
                let mut out = Vec::new();
                if let Some(doc_ids) = indexes.exact_match_doc_ids(collection, fields, values) {
                    // Apply the reverse logic if the planner requested it
                    let iter: Box<dyn Iterator<Item = _>> = if *reverse {
                        Box::new(doc_ids.iter().rev())
                    } else {
                        Box::new(doc_ids.iter())
                    };

                    for doc_id in iter.take(max_ids) {
                        let key = format!("{}:{}", collection, doc_id);
                        // Return empty bytes to let parallel workers handle decryption/decompression
                        out.push((key, Vec::new()));
                    }
                }
                Ok(out)
            }

            ScanType::CompositeIndexRange { index_id, ranges } => {
                let mut out = Vec::new();

                if let Some(idx) = indexes.composite.get(*index_id) {
                    for (start, end) in ranges {
                        let remaining = max_ids.saturating_sub(out.len());

                        if remaining == 0 {
                            break;
                        }

                        out.extend(
                            idx.tree
                                .range((start.clone(), end.clone()))
                                .take(remaining)
                                .map(|(_, doc_id)| (Self::make_key(collection, doc_id), Vec::new()))
                        );
                    }
                }

                Ok(out)
            }

            ScanType::CursorIndex { start, end } => {
                let mut out = Vec::new();

                for idx in indexes.indexes_for_collection(collection) {
                    let remaining = max_ids.saturating_sub(out.len());

                    if remaining == 0 {
                        break;
                    }

                    out.extend(
                        idx.tree
                            .range((start.clone(), end.clone()))
                            .take(remaining)
                            .map(|(_, doc_id)| (Self::make_key(collection, doc_id), Vec::new()))
                    );

                    if !out.is_empty() {
                        break;
                    }
                }

                Ok(out)
            }

            ScanType::InvertedIndex { field, query } => {
                let mut out = Vec::new();

                if let Some(fts_map) = indexes.fts.get(collection) {
                    if let Some(index) = fts_map.get(field) {
                        if let Some(doc_ids) = index.search(query) {
                            out.extend(
                                doc_ids
                                    .iter()
                                    .take(max_ids)
                                    .map(|doc_id| (Self::make_key(collection, doc_id), Vec::new()))
                            );
                        }
                    }
                }

                Ok(out)
            }

            ScanType::UnionIndex { .. } => Ok(vec![]),
        }
    }
}