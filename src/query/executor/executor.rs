use hashbrown::HashMap;
use rayon::prelude::*;
use std::sync::{Arc, RwLock};
use std::thread;

use crate::document::firelite_doc::{BorrowedValue, FireLiteDoc, FireLiteDocView};
use crate::document::value::Value;
use crate::error::Result;
use crate::index::manager::IndexManager;
use crate::query::plan::ScanType;
use crate::query::query::AggregateOp;
use crate::storage::engine::{Pointer, StorageEngine};

use super::super::plan::QueryPlan;
// use super::result_stream::QueryResults;
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
        // 1. PHASE 1: INDEX SCAN
        // Fetch physical pointers from the RAM Index
        let mut keys_from_index = {
            let storage = storage_arc.read().unwrap();
            self.execute_single_scan(
                &storage,
                indexes,
                &plan.scan,
                &plan.collection,
                plan.scan_limit,
            )?
        };

        if keys_from_index.is_empty() {
            return Ok(Vec::new());
        }

        // 2. PHASE 2: RAM-LEVEL OPTIMIZATION (Limit/Offset)
        let mut offset_to_apply_later = plan.offset.unwrap_or(0);

        // If the index already provides the correct sort order,
        // we can discard pointers in RAM before touching the disk.
        if plan.order_by_satisfied && offset_to_apply_later > 0 {
            let skip_count = offset_to_apply_later.min(keys_from_index.len());
            keys_from_index.drain(0..skip_count);
            offset_to_apply_later = 0;
        }

        if plan.order_by_satisfied && plan.filters_satisfied_by_index {
            if let Some(limit) = plan.limit {
                keys_from_index.truncate(limit);
            }
        }

        // 3. PHASE 3: PHYSICAL SORT (The "Sweep" optimization)
        // Record logical position to restore order later
        let mut work_items: Vec<(usize, String, Pointer)> = keys_from_index
            .into_iter()
            .enumerate()
            .map(|(i, (k, v))| (i, k, v))
            .collect();

        // Sort by file offset to ensure sequential disk reads (minimizes seek time)
        work_items.sort_by_key(|(_, _, ptr)| match ptr {
            Pointer::Segment { offset, .. } => *offset,
            Pointer::Blob { offset, .. } => *offset,
            Pointer::Inlined(_) | Pointer::BlobPending(_) | Pointer::BlobPendingData { .. } => 0,
            _ => 0,
        });

        let docs_to_fetch: Vec<(String, Pointer)> = work_items
            .iter()
            .map(|(_, k, p)| (k.clone(), p.clone()))
            .collect();

        // 4. PHASE 4: ADAPTIVE WORKER DISPATCH
        let doc_count = docs_to_fetch.len();

        // Decide how many workers to use based on result set density
        // Threshold: 1 worker per 150 documents, capped by global config.
        let target_workers = match doc_count {
            0..=150 => 1,      // Small set: Stay on current thread (Fairness to Get)
            151..=500 => 2,    // Medium: Parallelize across 2 cores
            _ => self.workers, // Large: Full system power
        }
        .min(self.workers)
        .max(1);

        let tasks = shard_tasks(
            docs_to_fetch,
            target_workers,
            plan.clone(),
            Some(storage_arc.clone()),
        );

        let processed_docs: Vec<(String, FireLiteDoc)> = if target_workers == 1 {
            // SHORT-CIRCUIT: Avoid Rayon task-scheduling overhead for small results.
            // This ensures a query for 50 docs is as fast as 50 individual Get calls.
            tasks.into_iter().flat_map(|task| run_task(task)).collect()
        } else {
            // PARALLEL PATH: Use full CPU power for heavy workloads
            tasks
                .into_par_iter()
                .flat_map(|task| run_task(task))
                .collect()
        };

        // 5. PHASE 5: LOGICAL ORDER RESTORATION
        let mut processed_map: HashMap<String, FireLiteDoc> = processed_docs.into_iter().collect();
        let mut ordered_results = Vec::with_capacity(doc_count);

        for (original_pos, key, _) in work_items {
            if let Some(doc) = processed_map.remove(&key) {
                ordered_results.push((original_pos, key, doc));
            }
        }

        // Restore the order provided by the index (or original insertion order)
        ordered_results.sort_by_key(|(pos, _, _)| *pos);

        let mut results: Vec<(String, FireLiteDoc)> = ordered_results
            .into_iter()
            .map(|(_, k, d)| (k, d))
            .collect();

        // 6. PHASE 6: FINAL SORTING & SLICING
        // Manual sort if the Index couldn't satisfy the order_by clause
        if !plan.order_by_satisfied {
            if let Some(order) = &plan.order_by {
                results.sort_by(|(id_a, doc_a), (id_b, doc_b)| {
                    let cmp = match order.field.as_str() {
                        // 1. Sort by the naked ID (String comparison)
                        "id" => id_a.cmp(id_b),

                        // 2. Sort by the native timestamp (i64 comparison)
                        "_time" => doc_a._time.cmp(&doc_b._time),

                        // 3. Fallback to regular document fields
                        _ => {
                            let av = doc_a.get(&order.field);
                            let bv = doc_b.get(&order.field);
                            av.cmp(&bv)
                        }
                    };

                    if order.ascending {
                        cmp
                    } else {
                        cmp.reverse()
                    }
                });
            }
        }

        // Apply final Offset/Limit for non-indexed queries
        if offset_to_apply_later > 0 {
            results = results.into_iter().skip(offset_to_apply_later).collect();
        }
        if let Some(limit) = plan.limit {
            results.truncate(limit);
        }

        Ok(results)
    }

    #[inline]
    fn make_key(_collection: &str, doc_id: &str) -> String {
        doc_id.to_string()
    }

    pub fn execute_aggregation(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
        ops: &[AggregateOp],
    ) -> Result<HashMap<String, f64>> {
        // --- 1. UNIFIED FAST PATH (Index-Only Aggregation) ---
        // Conditions: Only 1 op, no OR groups, and simple Equality filters
        if ops.len() == 1 && plan.or_groups.is_empty() {
            let op = &ops[0];

            // Determine if the operation is eligible for RAM-only execution
            let mut filter_ids: Option<hashbrown::HashSet<String>> = None;
            let mut can_use_fast_path = true;

            // Step A: Attempt to resolve matching Document IDs using RAM indexes
            for filter in &plan.filters {
                if filter.op == crate::query::filter::Operator::Eq {
                    let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                    if let Some(ids) =
                        indexes.lookup_secondary(&plan.collection, &filter.field, &val_bytes)
                    {
                        let id_set: hashbrown::HashSet<_> = ids.into_iter().collect();
                        if let Some(ref mut existing_set) = filter_ids {
                            existing_set.retain(|id| id_set.contains(id));
                        } else {
                            filter_ids = Some(id_set);
                        }
                    } else {
                        can_use_fast_path = false;
                        break;
                    }
                } else {
                    can_use_fast_path = false;
                    break;
                }
            }

            if can_use_fast_path {
                match op {
                    // CASE 1: COUNT
                    AggregateOp::Count => {
                        let count = if plan.filters.is_empty() {
                            // Instant O(1) count from storage metadata if no filters
                            let storage = storage_arc.read().unwrap();
                            storage.count_prefix("")
                        } else {
                            // O(Index) count from intersection results
                            filter_ids.map(|ids| ids.len()).unwrap_or(0)
                        };

                        let mut res = HashMap::new();
                        res.insert("count".to_string(), count as f64);
                        return Ok(res);
                    }

                    // CASE 2: SUM / AVG
                    AggregateOp::Sum(target_field) | AggregateOp::Avg(target_field) => {
                        if let Some(sec_map) = indexes.secondary.get(&plan.collection) {
                            if let Some(index) = sec_map.get(target_field) {
                                let mut total_sum = 0.0;
                                let mut total_count = 0.0;
                                let is_avg = matches!(op, AggregateOp::Avg(_));

                                for (val_bytes, doc_ids) in index.get_map() {
                                    if let Some(val) = decode_scalar_as_f64(val_bytes) {
                                        let match_count = if let Some(ref allowed_ids) = filter_ids
                                        {
                                            doc_ids
                                                .iter()
                                                .filter(|id| allowed_ids.contains(*id))
                                                .count()
                                        } else {
                                            doc_ids.len()
                                        };

                                        total_sum += val * (match_count as f64);
                                        total_count += match_count as f64;
                                    }
                                }

                                let mut res = HashMap::new();
                                if is_avg {
                                    res.insert(
                                        format!("avg_{}", target_field),
                                        if total_count > 0.0 {
                                            total_sum / total_count
                                        } else {
                                            0.0
                                        },
                                    );
                                } else {
                                    res.insert(format!("sum_{}", target_field), total_sum);
                                }
                                return Ok(res);
                            }
                        }
                    }
                }
            }
        }

        // --- 2. SLOW PATH (Physical Disk Sweep) ---
        // Fallback for complex queries, multiple ops, or range filters
        let docs: Vec<(String, Pointer)> = {
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
            let storage_engine = task.storage.as_ref().unwrap().read().unwrap();

            for (id, pointer) in task.docs {
                if let Ok(Some(bytes)) = storage_engine.read_pointer(&pointer) {
                    // Check if the document matches the WHERE filters first
                    if matches_filters_view(&id, &bytes, &task.plan) {
                        if let Some(view) = FireLiteDocView::new(&bytes) {
                            for op in &thread_ops {
                                match op {
                                    AggregateOp::Count => {
                                        *partial_results.entry("count".to_string()).or_insert(0.0) += 1.0;
                                    }
                                    AggregateOp::Sum(field) | AggregateOp::Avg(field) => {
                                        // IMPROVEMENT: Instead of failing if index is missing, 
                                        // we scan the document view for the field.
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
            }
            partial_results
        };

        // --- 3. PARALLEL EXECUTION ---
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
                    let _ = thread_tx.send(process_task(task, thread_ops));
                });
            }
            drop(tx);
            while let Ok(partial) = rx.recv() {
                for (k, v) in partial {
                    *final_results.entry(k).or_insert(0.0) += v;
                }
            }
        }

        // [4. Final Processing for Averages]
        let keys: Vec<String> = final_results.keys().cloned().collect();
        for k in keys {
            if k.starts_with("avg_tmp_") {
                let field = &k[8..];
                let sum = final_results.remove(&k).unwrap_or(0.0);
                let count = final_results
                    .remove(&format!("avg_cnt_{}", field))
                    .unwrap_or(1.0);
                final_results.insert(
                    format!("avg_{}", field),
                    if count > 0.0 { sum / count } else { 0.0 },
                );
            }
        }

        // Ensure default keys exist for requested operations
        for op in ops {
            match op {
                AggregateOp::Count => {
                    final_results.entry("count".to_string()).or_insert(0.0);
                }
                AggregateOp::Sum(field) => {
                    final_results.entry(format!("sum_{}", field)).or_insert(0.0);
                }
                AggregateOp::Avg(field) => {
                    final_results.entry(format!("avg_{}", field)).or_insert(0.0);
                }
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
        let docs: Vec<(String, Pointer)> = {
            // Explicitly set the type
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
            results.sort_by(|(id_a, fields_a), (id_b, fields_b)| {
                let cmp = match order.field.as_str() {
                    "id" => id_a.cmp(id_b),

                    // In projected results, _time is already injected into the fields vec
                    // by FireLiteDoc::decode_projected
                    _ => {
                        let av = fields_a
                            .iter()
                            .find(|(k, _)| k == &order.field)
                            .map(|(_, v)| v);
                        let bv = fields_b
                            .iter()
                            .find(|(k, _)| k == &order.field)
                            .map(|(_, v)| v);
                        av.cmp(&bv)
                    }
                };

                if order.ascending {
                    cmp
                } else {
                    cmp.reverse()
                }
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
        limit: Option<usize>,
    ) -> Result<Vec<(String, Pointer)>> {
        // Change from Vec<u8> to Pointer
        let max_ids = limit.unwrap_or(usize::MAX);

        match scan {
            ScanType::FullCollection => {
                Ok(storage
                    .index
                    .iter()
                    // FIX: Added check to skip Pointer::Deleted
                    .filter(|(_, p)| !matches!(p, Pointer::Deleted { .. }))
                    .take(max_ids)
                    .map(|(k, p)| (k.clone(), p.clone()))
                    .collect())
            }

            ScanType::SecondaryIndex { field, value } => {
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        for doc_id in index.range_scan(value, value).iter() {
                            let key = Self::make_key(collection, doc_id);
                            if let Some(ptr) = storage.index.get(&key) {
                                // FIX: Ensure we don't return a deleted pointer found in secondary index
                                if !matches!(ptr, Pointer::Deleted { .. }) {
                                    out.push((key, ptr.clone()));
                                    if out.len() >= max_ids {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::CompositeIndex {
                fields,
                values,
                reverse,
            } => {
                let mut out = Vec::new();
                if let Some(doc_ids) = indexes.exact_match_doc_ids(collection, fields, values) {
                    let iter: Box<dyn Iterator<Item = _>> = if *reverse {
                        Box::new(doc_ids.iter().rev())
                    } else {
                        Box::new(doc_ids.iter())
                    };

                    for doc_id in iter.take(max_ids) {
                        let key = Self::make_key(collection, &doc_id);
                        if let Some(ptr) = storage.index.get(&key) {
                            // out.push((key, ptr.clone()));
                            if !matches!(ptr, Pointer::Deleted { .. }) {
                                out.push((key, ptr.clone()));
                                if out.len() >= max_ids {
                                    break;
                                }
                            }
                        }
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

                        for (_, doc_id) in
                            idx.tree.range((start.clone(), end.clone())).take(remaining)
                        {
                            let key = Self::make_key(collection, &doc_id);
                            if let Some(ptr) = storage.index.get(&key) {
                                // out.push((key, ptr.clone()));
                                if !matches!(ptr, Pointer::Deleted { .. }) {
                                    out.push((key, ptr.clone()));
                                    if out.len() >= max_ids {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::SecondaryIndexRange {
                field,
                start,
                end,
                reverse,
            } => {
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        let remaining = max_ids.saturating_sub(out.len());

                        // Perform range scan on the BTreeMap
                        let range_iter = index.get_map().range((start.clone(), end.clone()));

                        // Collect doc IDs based on direction
                        let doc_ids: Vec<String> = if *reverse {
                            range_iter
                                .rev()
                                .flat_map(|(_, ids)| ids.iter().cloned())
                                .collect()
                        } else {
                            range_iter
                                .flat_map(|(_, ids)| ids.iter().cloned())
                                .collect()
                        };

                        // Map Doc IDs to Physical Pointers
                        for doc_id in doc_ids.into_iter().take(remaining) {
                            if let Some(ptr) = storage.index.get(&doc_id) {
                                if !matches!(ptr, Pointer::Deleted { .. }) {
                                    out.push((doc_id, ptr.clone()));
                                    if out.len() >= max_ids {
                                        break;
                                    }
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::CursorIndex {
                index_id,
                start,
                end,
                reverse,
            } => {
                let mut out = Vec::new();
                if let Some(idx) = indexes.composite.get(*index_id) {
                    let remaining = max_ids.saturating_sub(out.len());
                    let iter = idx.tree.range((start.clone(), end.clone()));

                    let doc_ids: Vec<_> = if *reverse {
                        iter.rev().map(|(_, id)| id.clone()).collect()
                    } else {
                        iter.map(|(_, id)| id.clone()).collect()
                    };

                    for doc_id in doc_ids.into_iter().take(remaining) {
                        let key = Self::make_key(collection, &doc_id);
                        if let Some(ptr) = storage.index.get(&key) {
                            // out.push((key, ptr.clone()));
                            if !matches!(ptr, Pointer::Deleted { .. }) {
                                out.push((key, ptr.clone()));
                                if out.len() >= max_ids {
                                    break;
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::InvertedIndex { field, query } => {
                let mut out = Vec::new();
                if let Some(fts_map) = indexes.fts.get(collection) {
                    if let Some(index) = fts_map.get(field) {
                        if let Some(doc_ids) = index.search(query) {
                            for doc_id in doc_ids.iter().take(max_ids) {
                                let key = Self::make_key(collection, doc_id);
                                if let Some(ptr) = storage.index.get(&key) {
                                    // out.push((key, ptr.clone()));
                                    if !matches!(ptr, Pointer::Deleted { .. }) {
                                        out.push((key, ptr.clone()));
                                        if out.len() >= max_ids {
                                            break;
                                        }
                                    }
                                }
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
