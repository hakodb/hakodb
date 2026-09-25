use hashbrown::HashMap;
use rayon::prelude::*;
use std::collections::BinaryHeap;
use std::sync::{Arc, RwLock};
use std::thread;

use crate::document::hako_doc::{BorrowedValue, DocView, HakoDoc, HakoDocView};
use crate::document::value::Value;
use crate::error::Result;
use crate::index::manager::IndexManager;
use crate::query::order::OrderBy;
use crate::query::plan::ScanType;
use crate::query::query::AggregateOp;
use crate::storage::engine::{Pointer, StorageEngine};

use super::super::plan::QueryPlan;
// use super::result_stream::QueryResults;
use super::scheduler::shard_tasks;
use super::task::QueryTask;
use super::worker::{matches_filters_view, run_task, run_task_projected};

/// TopN lane firings since process start: operational visibility and test
/// engagement proof (the parity suite asserts this moves). One Relaxed
/// increment per TopN query — negligible next to a scan.
static TOPN_RUNS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many queries took the TopN lane (see `execute_topn`).
pub fn topn_runs() -> usize {
    TOPN_RUNS.load(std::sync::atomic::Ordering::Relaxed)
}

use crate::index::index_key::decode_scalar_as_f64;

pub struct ParallelQueryExecutor {
    workers: usize,
}

/// One ORDER BY slot, resolved per direction at compare time.
/// `Field(None)` = missing (sorts below every value — same as the full
/// sort's `Option` compare).
#[derive(Debug, Clone, PartialEq, Eq)]
enum TopKey {
    Id,
    Time(i64),
    Field(Option<Value>),
}

/// Heap candidate: owned sort keys + scan position + identity. The pointer
/// is re-read at fetch (never compared).
struct TopEntry {
    keys: Vec<TopKey>,
    seq: usize,
    id: String,
    ptr: Pointer,
}

/// Heap item borrows the order spec (directions live in the plan).
struct TopItem<'a> {
    entry: TopEntry,
    orders: &'a [OrderBy],
}

impl<'a> PartialEq for TopItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        cmp_top(&self.entry, &other.entry, self.orders) == std::cmp::Ordering::Equal
    }
}
impl<'a> Eq for TopItem<'a> {}
impl<'a> PartialOrd for TopItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(cmp_top(&self.entry, &other.entry, self.orders))
    }
}
impl<'a> Ord for TopItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        cmp_top(&self.entry, &other.entry, self.orders)
    }
}

/// The phase-6 comparator, factored for the heap: per-slot direction, then
/// scan-seq (a stable sort over scan order is exactly (keys…, input-seq),
/// so this reproduces the legacy page bit-for-bit, ties included).
fn cmp_top(a: &TopEntry, b: &TopEntry, orders: &[OrderBy]) -> std::cmp::Ordering {
    use std::cmp::Ordering;
    for (o, (ka, kb)) in orders.iter().zip(a.keys.iter().zip(b.keys.iter())) {
        let cmp = match (o.field.as_str(), ka, kb) {
            ("id", _, _) => a.id.cmp(&b.id),
            ("_time", TopKey::Time(x), TopKey::Time(y)) => x.cmp(y),
            (_, TopKey::Field(x), TopKey::Field(y)) => x.cmp(y),
            // Unreachable: keys are built from this same spec. Equal keeps
            // the heap total without inventing order.
            _ => Ordering::Equal,
        };
        if cmp != Ordering::Equal {
            return if o.ascending { cmp } else { cmp.reverse() };
        }
    }
    a.seq.cmp(&b.seq)
}

/// Pull one ORDER BY slot per doc from the view: header time, id compare
/// needs nothing, fields decode a single value (never the whole doc).
fn top_keys(view: &HakoDocView, orders: &[OrderBy]) -> Vec<TopKey> {
    orders
        .iter()
        .map(|o| match o.field.as_str() {
            "id" => TopKey::Id,
            "_time" => TopKey::Time(view._time),
            f => TopKey::Field(
                view.iter()
                    .find(|(k, _, _)| *k == f)
                    .and_then(|(_, tag, data)| {
                        crate::document::hako_doc::decode_value(tag, data)
                    }),
            ),
        })
        .collect()
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
    ) -> Result<Vec<(String, HakoDoc)>> {
        // 1. PHASE 1: INDEX SCAN
        // Fetch physical pointers from the RAM Index
        let keys_from_index = {
            let storage = storage_arc.read().unwrap();
            self.execute_single_scan(
                &storage,
                indexes,
                &plan.scan,
                &plan.collection,
                plan.scan_limit,
                plan.offset,
            )?
        };

        if keys_from_index.is_empty() {
            return Ok(Vec::new());
        }

        // 2. PHASE 2: Limit / Offset
        // ponytail: v0.7.2 hard-coded this to 0 which silently dropped the
        // user's offset for FullCollection and every other scan that didn't
        // apply it internally. SortedKeys above already applied offset via
        // sorted_key_range, so its skip path is a no-op (`0`).
        let offset_to_apply_later = match plan.scan {
            ScanType::SortedKeys { .. } => 0,
            _ => plan.offset.unwrap_or(0),
        };

        let doc_count = keys_from_index.len();

        // --- TOPN FAST LANE: unsatisfied ORDER BY + LIMIT, no cursor bounds.
        // Full scans used to decode every doc + full sort + truncate (~10ms
        // per 2000 docs). The heap keeps limit+offset candidates by sort
        // keys pulled from views (no full decode); only the final page
        // decodes. Cursor shapes keep the legacy path (bounds need the
        // contract predicate; the gateway post-filters them anyway).
        // Guard: limit >= scanned rows means no eviction is possible — the
        // heap would be pure overhead, so those keep the legacy full sort
        // (deep offsets with small limits still qualify: skipping is cheap,
        // decoding the skipped prefix is not).
        let topn = !plan.order_by.is_empty()
            && !plan.order_by_satisfied
            && !plan.has_cursor_bounds
            && matches!(plan.limit, Some(l) if l < keys_from_index.len());
        if topn {
            return self.execute_topn(storage_arc, &plan, keys_from_index);
        }

        // --- THE FAST PATH: satisfied scans skip the heavy machinery ---
        // Bypasses physical sorting, String re-clones, rayon dispatch, and
        // HashMap recreation overhead. Small queries always qualify
        // (<= 250 rows); satisfied scans up to the sequential cap decode in
        // scan order (that IS the result); bigger satisfied scans go
        // parallel below. Unordered/unfiltered shapes keep the general path.
        // ponytail: 2000 rows x ~1us decode ~= 2ms — past that, rayon earns
        // its dispatch. Typical pages (<= 1000) stay sequential.
        const SATISFIED_SEQUENTIAL_CAP: usize = 2000;
        if doc_count <= 250
            || ((plan.order_by_satisfied && plan.filters_satisfied_by_index)
                && doc_count <= SATISFIED_SEQUENTIAL_CAP)
        {
            let storage_guard = storage_arc.read().unwrap();
            let blob_manager = storage_guard.blob_manager.as_ref();
            let optimized_plan = crate::query::executor::worker::prepare_optimized_plan(&plan);
            let mut results = Vec::with_capacity(doc_count);

            // ponytail: shared decode helper for both byte sources below.
            // Decodes skeletons ONLY — blob inflation happens once below,
            // concurrently across all rows (positional blob reads are
            // thread-safe). With plan.defer_blobs the placeholders are
            // returned as-is; resolve later per doc if needed.
            let defer = plan.defer_blobs;
            let push_row = |id: String, bytes: &[u8], results: &mut Vec<(String, HakoDoc)>| {
                if plan.filters_satisfied_by_index {
                    if let Some(doc) = HakoDoc::decode(bytes) {
                        results.push((id, doc));
                    }
                } else if let Some(doc) = crate::query::executor::worker::unified_match_decode(&id, bytes, &optimized_plan) {
                    results.push((id, doc));
                }
            };

            for (id, ptr) in keys_from_index {
                // ponytail: Inlined bytes are Arc-shared — decode borrows the
                // shared buffer instead of cloning ~1KB per row. The owned Arc
                // keeps the buffer alive, so no guard lifetime is involved.
                match ptr {
                    Pointer::Inlined(shared) => push_row(id, &shared, &mut results),
                    other => {
                        if let Ok(Some(bytes)) = storage_guard.read_pointer(&other) {
                            push_row(id, &bytes, &mut results);
                        }
                    }
                }
            }

            // ponytail: parallel blob inflation. Skeletons are all decoded
            // above; resolve every BlobLink concurrently (positional preads
            // are thread-safe, no shared state). Sequential 20x50KB disk
            // reads serialize on I/O latency; concurrent reads queue in the
            // OS. Link-free results (the common small-doc case) skip the
            // pass via one branchy scan — no rayon dispatch, no per-doc
            // overhead, same speed as before.
            if !defer {
                if let Some(bm) = blob_manager {
                    use rayon::prelude::*;
                    use crate::query::executor::worker::doc_has_links;
                    if results.iter().any(|(_, doc)| doc_has_links(doc)) {
                        results.par_iter_mut().for_each(|(_, doc)| {
                            let _ = crate::query::executor::worker::inflate_blobs(doc, bm);
                        });
                    }
                }
            }

            // 6. PHASE 6: FINAL SORTING & SLICING
            if !plan.order_by_satisfied && !plan.order_by.is_empty() {
                results.sort_by(|(id_a, doc_a), (id_b, doc_b)| {
                    for order in &plan.order_by {
                        let cmp = match order.field.as_str() {
                            "id" => id_a.cmp(id_b),
                            "_time" => doc_a._time.cmp(&doc_b._time),
                            _ => doc_a.get(&order.field).cmp(&doc_b.get(&order.field)),
                        };
                        if cmp != std::cmp::Ordering::Equal {
                            return if order.ascending { cmp } else { cmp.reverse() };
                        }
                    }
                    std::cmp::Ordering::Equal
                });
            }

            if offset_to_apply_later > 0 {
                results = results.into_iter().skip(offset_to_apply_later).collect();
            }
            if let Some(limit) = plan.limit {
                results.truncate(limit);
            }

            return Ok(results);
        }

        // Large satisfied scans (> SEQUENTIAL cap below): same skip-the-
        // machinery deal, but decode in parallel. Staged as (id, shared
        // bytes) under one guard, then order-preserving par-decode — no
        // String re-clones, no restore-order map. Without this, big
        // satisfied scans would fall into phases 3-6 and regress vs the
        // old rayon path they used to take.
        if plan.order_by_satisfied && plan.filters_satisfied_by_index {
            return self.execute_satisfied_parallel(storage_arc, indexes, plan, keys_from_index);
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

        let processed_docs: Vec<(String, HakoDoc)> = if target_workers == 1 {
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
        let mut processed_map: HashMap<String, HakoDoc> = processed_docs.into_iter().collect();
        let mut ordered_results = Vec::with_capacity(doc_count);

        for (original_pos, key, _) in work_items {
            if let Some(doc) = processed_map.remove(&key) {
                ordered_results.push((original_pos, key, doc));
            }
        }

        // Restore the order provided by the index (or original insertion order)
        ordered_results.sort_by_key(|(pos, _, _)| *pos);

        let mut results: Vec<(String, HakoDoc)> = ordered_results
            .into_iter()
            .map(|(_, k, d)| (k, d))
            .collect();

        // 6. PHASE 6: FINAL SORTING & SLICING
        // Manual sort if the Index couldn't satisfy the order_by clause
        if !plan.order_by_satisfied && !plan.order_by.is_empty() {
            results.sort_by(|(id_a, doc_a), (id_b, doc_b)| {
                for order in &plan.order_by {
                    let cmp = match order.field.as_str() {
                        "id" => id_a.cmp(id_b),
                        "_time" => doc_a._time.cmp(&doc_b._time),
                        _ => {
                            let av = doc_a.get(&order.field);
                            let bv = doc_b.get(&order.field);
                            av.cmp(&bv)
                        }
                    };

                    if cmp != std::cmp::Ordering::Equal {
                        return if order.ascending { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
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

    /// TopN heap for unsatisfied ORDER BY + LIMIT (see the hook in
    /// [`Self::execute`]). Scan order in, globally-correct top page out:
    ///
    /// - Key-only scan: sort keys come from [`HakoDocView`] (header `_time`,
    ///   id compare, single-field view pull) — never a full decode, except
    ///   the final page. Filter matching is view-based too, or skipped when
    ///   the scan already constrained to matches (same trust as the fast path).
    /// - Exact parity with the legacy full stable sort: the heap orders by
    ///   (keys…, scan-seq), and a stable sort is exactly (keys…, input-seq).
    ///   Same scan order in → identical page out, ties included.
    /// - Corrupt rows (view ok, full decode fails) drop at fetch like the
    ///   legacy paths drop undecodables; offset applies over successes.
    ///   Storage writes validated docs, so this is bitrot-only territory.
    fn execute_topn(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        plan: &QueryPlan,
        keys: Vec<(String, Pointer)>,
    ) -> Result<Vec<(String, HakoDoc)>> {
        let limit = plan.limit.unwrap_or(usize::MAX);
        let offset = plan.offset.unwrap_or(0);
        let k = limit.saturating_add(offset);
        TOPN_RUNS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        if k == 0 {
            return Ok(Vec::new());
        }
        let orders = &plan.order_by;
        let need_match = !plan.filters.is_empty() || !plan.or_groups.is_empty();
        let trust_scan = plan.filters_satisfied_by_index;
        let storage = storage_arc.read().unwrap();
        // Max-heap of the K best so far: peek is the worst kept; a better
        // arrival evicts it. (NOT Reverse: that would peak the best and
        // freeze the heap on the first K scanned.)
        let mut heap: BinaryHeap<TopItem> = BinaryHeap::new();
        for (seq, (id, ptr)) in keys.iter().enumerate() {
            let Some(shared) = storage.read_pointer_shared(ptr)? else {
                continue;
            };
            let bytes: &[u8] = &shared;
            let Some(view) = HakoDocView::new(bytes) else {
                continue;
            };
            if need_match && !trust_scan && !matches_filters_view(&id, bytes, plan) {
                continue;
            }
            let entry = TopEntry {
                keys: top_keys(&view, orders),
                seq,
                id: id.clone(),
                ptr: ptr.clone(),
            };
            let item = TopItem { entry, orders };
            if heap.len() < k {
                heap.push(item);
            } else if let Some(worst) = heap.peek() {
                if item < *worst {
                    heap.pop();
                    heap.push(item);
                }
            }
        }
        // Best-first, then offset over successes + take limit (mirrors the
        // legacy slice order: offset first, then limit).
        let mut entries: Vec<TopEntry> =
            heap.into_iter().map(|item| item.entry).collect();
        entries.sort_by(|a, b| cmp_top(a, b, orders));
        let blob_manager = storage.blob_manager.as_ref();
        let mut out = Vec::new();
        let mut skipped = 0usize;
        for e in entries {
            // ponytail: decode lazily in order; corrupt rows drop and the
            // page backfills from the heap remainder (same rows the legacy
            // full-decode-then-sort would have kept).
            let bytes = match storage.read_pointer(&e.ptr)? {
                Some(b) => b,
                None => continue,
            };
            let Some(doc) = HakoDoc::decode(&bytes) else {
                continue;
            };
            if skipped < offset {
                skipped += 1;
                continue;
            }
            if out.len() >= limit {
                break;
            }
            out.push((e.id, doc));
        }
        // Blob inflation for the final page only (mirrors the fast path).
        if !plan.defer_blobs {
            if let Some(bm) = blob_manager {
                use crate::query::executor::worker::{doc_has_links, inflate_blobs};
                if out.iter().any(|(_, doc)| doc_has_links(doc)) {
                    out.par_iter_mut().for_each(|(_, doc)| {
                        let _ = inflate_blobs(doc, bm);
                    });
                }
            }
        }
        Ok(out)
    }

    /// Raw scan: phase-1 index walk with byte materialization — no decode,
    /// no filter re-verify, no blob inflation, no rayon. `Inlined` docs
    /// share the RAM buffer (Arc bump, zero copies); segment/blob pointers
    /// do one positional read each. Returns storage-encoded bytes: opaque
    /// and version-scoped — decode with `HakoDoc::decode`, do not
    /// persist or compare across versions.
    ///
    /// Requires index-satisfied filters AND ordering (raw cannot match or
    /// sort without decoding) — otherwise `QueryError`. Single-threaded by
    /// design: the work is Arc bumps + memcpys, rayon dispatch would only
    /// add overhead. Scan order is preserved (no re-sort).
    pub fn execute_raw(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
    ) -> Result<Vec<(String, Arc<Vec<u8>>)>> {
        use crate::error::HakoError;
        if (!plan.filters.is_empty() || !plan.or_groups.is_empty())
            && !plan.filters_satisfied_by_index
        {
            return Err(HakoError::QueryError(
                "raw queries require index-satisfied filters (decode to match)".into(),
            ));
        }
        if !plan.order_by_satisfied && !plan.order_by.is_empty() {
            return Err(HakoError::QueryError(
                "raw queries require index-satisfied ordering (decode to sort)".into(),
            ));
        }

        let storage = storage_arc.read().unwrap();
        let found = self.execute_single_scan(
            &storage,
            indexes,
            &plan.scan,
            &plan.collection,
            plan.scan_limit,
            plan.offset,
        )?;

        let mut out = Vec::with_capacity(found.len());
        for (id, ptr) in found {
            // ponytail: shared read — Inlined hands back the live Arc, no
            // per-row copy; Deleted defensively skipped (phase 1 already
            // filters it).
            if let Some(bytes) = storage.read_pointer_shared(&ptr)? {
                out.push((id, bytes));
            }
        }

        // Phase-6-style truncation for scans that didn't pre-apply it
        // (mirrors `execute`: SortedKeys pre-applied offset via the range).
        let skip = match plan.scan {
            ScanType::SortedKeys { .. } => 0,
            _ => plan.offset.unwrap_or(0),
        };
        let mut out = out;
        if skip > 0 {
            out = out.into_iter().skip(skip).collect();
        }
        if let Some(limit) = plan.limit {
            out.truncate(limit);
        }
        Ok(out)
    }

    /// Zero-alloc scan walk: the engine lends each row (`&str` id borrowed
    /// from the sorted key vec, bytes borrowed from the resident Arc or a
    /// single reused scratch buffer) and the caller decides per row.
    /// Returning `false` stops early. Returns rows visited.
    ///
    /// Currently covers the SortedKeys fast path (order-by-id / no-order
    /// scans — the hot shape); anything else is `QueryError`, extend
    /// arm-by-arm on demand. Same index-satisfied requirements as raw.
    ///
    /// CONTRACT: the storage read lock is held for the whole walk, so the
    /// callback MUST NOT re-enter the engine (a waiting writer + second
    /// read-lock attempt deadlocks, same as nested MDBX txns). Keep it
    /// short: count, hash, copy, compare — not queries.
    pub fn execute_walk<F>(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
        callback: &mut F,
    ) -> Result<usize>
    where
        F: FnMut(&str, &[u8]) -> bool,
    {
        use crate::error::HakoError;
        // ponytail: reserved for the non-SortedKeys arms when the walk
        // grows beyond order-by-id (underscore until then, not dead).
        let _ = indexes;
        if (!plan.filters.is_empty() || !plan.or_groups.is_empty())
            && !plan.filters_satisfied_by_index
        {
            return Err(HakoError::QueryError(
                "walk requires index-satisfied filters (decode to match)".into(),
            ));
        }
        if !plan.order_by_satisfied && !plan.order_by.is_empty() {
            return Err(HakoError::QueryError(
                "walk requires index-satisfied ordering (decode to sort)".into(),
            ));
        }
        let (reverse, start_key, start_exclusive) = match &plan.scan {
            ScanType::SortedKeys { start_key, start_exclusive, reverse } => {
                (*reverse, start_key.clone(), *start_exclusive)
            }
            _ => {
                return Err(HakoError::QueryError(
                    "walk supports SortedKeys scans only (order by id)".into(),
                ))
            }
        };

        // The guard lives to the end of the function — every borrow below
        // (keys, resident Arcs) is covered by it. ponytail: one reused
        // scratch buffer for the rare owned reads (segment/blob pointers)
        // instead of a Vec per row.
        let storage = storage_arc.read().unwrap();
        if storage.sorted_keys.is_empty() {
            return Ok(0);
        }
        let (start, end) = Self::sorted_walk_bounds(
            &storage,
            start_key.as_deref(),
            start_exclusive,
            plan.offset,
            plan.limit,
            reverse,
        );
        let limit = plan.limit.unwrap_or(usize::MAX);
        let mut scratch = Vec::new();
        Self::drive_sorted_keys(&storage, start, end, reverse, limit, &mut |key, ptr| {
            match ptr {
                Pointer::Inlined(shared) => Ok(callback(key, shared)),
                other => match storage.read_pointer_internal(other, true)? {
                    Some(bytes) => {
                        scratch.clear();
                        scratch.extend_from_slice(&bytes);
                        Ok(callback(key, &scratch))
                    }
                    None => Ok(true),
                },
            }
        })
    }

    /// View walk: like [`Self::execute_walk`] but lends each row as a
    /// `DocView` (lazy per-field reads) instead of raw bytes. Same guards,
    /// same driver. Construction validates the framing header; per-field
    /// access is bounds-checked and `to_owned_doc` decodes strictly, so
    /// corrupt rows surface as empty pulls, never panics.
    pub fn execute_walk_view<F>(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        indexes: &IndexManager,
        plan: QueryPlan,
        callback: &mut F,
    ) -> Result<usize>
    where
        F: FnMut(&str, &DocView) -> bool,
    {
        use crate::error::HakoError;
        let _ = indexes;
        if (!plan.filters.is_empty() || !plan.or_groups.is_empty())
            && !plan.filters_satisfied_by_index
        {
            return Err(HakoError::QueryError(
                "view walk requires index-satisfied filters (decode to match)".into(),
            ));
        }
        if !plan.order_by_satisfied && !plan.order_by.is_empty() {
            return Err(HakoError::QueryError(
                "view walk requires index-satisfied ordering (decode to sort)".into(),
            ));
        }
        let (reverse, start_key, start_exclusive) = match &plan.scan {
            ScanType::SortedKeys { start_key, start_exclusive, reverse } => {
                (*reverse, start_key.clone(), *start_exclusive)
            }
            _ => {
                return Err(HakoError::QueryError(
                    "view walk supports SortedKeys scans only (order by id)".into(),
                ))
            }
        };

        let storage = storage_arc.read().unwrap();
        if storage.sorted_keys.is_empty() {
            return Ok(0);
        }
        let (start, end) = Self::sorted_walk_bounds(
            &storage,
            start_key.as_deref(),
            start_exclusive,
            plan.offset,
            plan.limit,
            reverse,
        );
        let limit = plan.limit.unwrap_or(usize::MAX);
        Self::drive_sorted_keys(&storage, start, end, reverse, limit, &mut |key, ptr| {
            let shared = match ptr {
                Pointer::Inlined(shared) => Arc::clone(shared),
                Pointer::BlobPendingData { data, .. } => Arc::clone(data),
                Pointer::BlobPending(doc) => Arc::new(doc.encode()),
                Pointer::Deleted { .. } => return Ok(true),
                other => match storage.read_pointer_internal(other, true)? {
                    Some(bytes) => Arc::new(bytes),
                    None => return Ok(true),
                },
            };
            match DocView::new(shared) {
                Some(view) => Ok(callback(key, &view)),
                None => Ok(true),
            }
        })
    }

    #[inline]
    fn make_key(_collection: &str, doc_id: &str) -> String {
        doc_id.to_string()
    }

    /// Bounds for a SortedKeys walk in either direction: binary-search the
    /// optional anchor, apply offset from the top, take limit downward.
    /// Returns an empty range (never None-shaped) when there is nothing.
    fn sorted_walk_bounds(
        storage: &StorageEngine,
        start_key: Option<&str>,
        start_exclusive: bool,
        offset: Option<usize>,
        limit: Option<usize>,
        reverse: bool,
    ) -> (usize, usize) {
        if reverse {
            storage
                .sorted_key_range_reverse(start_key, start_exclusive, offset, limit)
                .unwrap_or((0, 0))
        } else {
            storage
                .sorted_key_range(start_key, start_exclusive, offset, limit)
                .unwrap_or((0, 0))
        }
    }

    /// Drives a SortedKeys slice in either direction, invoking `f` per live
    /// (non-deleted, indexed) row. `f` returns false to stop early. Returns
    /// rows PRESENTED to `f`. Shared by the byte walk and the view walk —
    /// one loop to maintain instead of per-arm duplicates.
    fn drive_sorted_keys<F>(
        storage: &StorageEngine,
        start: usize,
        end: usize,
        reverse: bool,
        limit: usize,
        f: &mut F,
    ) -> Result<usize>
    where
        F: FnMut(&str, &Pointer) -> Result<bool>,
    {
        let mut visited = 0usize;
        if reverse {
            for key in storage.sorted_keys[start..end].iter().rev() {
                if visited >= limit {
                    break;
                }
                let Some(ptr) = storage.index.get(key) else { continue };
                if matches!(ptr, Pointer::Deleted { .. }) {
                    continue;
                }
                visited += 1;
                if !f(key.as_str(), ptr)? {
                    return Ok(visited);
                }
            }
        } else {
            for key in &storage.sorted_keys[start..end] {
                if visited >= limit {
                    break;
                }
                let Some(ptr) = storage.index.get(key) else { continue };
                if matches!(ptr, Pointer::Deleted { .. }) {
                    continue;
                }
                visited += 1;
                if !f(key.as_str(), ptr)? {
                    return Ok(visited);
                }
            }
        }
        Ok(visited)
    }

    /// Parallel twin of the satisfied fast path above, for scans past the
    /// sequential cap. Stage 1 resolves pointers to shared bytes under one
    /// guard (Arc bumps, zero copies for inlined); stage 2 is an
    /// order-preserving parallel decode; stage 3 reuses the standard blob
    /// inflation + truncation tail. Filters are index-satisfied by the
    /// caller gate, so no per-row re-verify.
    fn execute_satisfied_parallel(
        &self,
        storage_arc: Arc<RwLock<StorageEngine>>,
        _indexes: &IndexManager,
        plan: QueryPlan,
        keys_from_index: Vec<(String, Pointer)>,
    ) -> Result<Vec<(String, HakoDoc)>> {
        use crate::query::executor::worker::{doc_has_links, inflate_blobs};
        let (staged, blob_manager) = {
            let storage = storage_arc.read().unwrap();
            let mut v = Vec::with_capacity(keys_from_index.len());
            for (id, ptr) in keys_from_index {
                if let Some(bytes) = storage.read_pointer_shared(&ptr)? {
                    v.push((id, bytes));
                }
            }
            (v, storage.blob_manager.clone())
        };
        let mut results: Vec<(String, HakoDoc)> = staged
            .into_par_iter()
            .filter_map(|(id, bytes)| HakoDoc::decode(&bytes).map(|doc| (id, doc)))
            .collect();
        if !plan.defer_blobs {
            if let Some(bm) = blob_manager.as_ref() {
                if results.iter().any(|(_, doc)| doc_has_links(doc)) {
                    results.par_iter_mut().for_each(|(_, doc)| {
                        let _ = inflate_blobs(doc, bm);
                    });
                }
            }
        }
        let offset_to_apply_later = match plan.scan {
            ScanType::SortedKeys { .. } => 0,
            _ => plan.offset.unwrap_or(0),
        };
        if offset_to_apply_later > 0 {
            results = results.into_iter().skip(offset_to_apply_later).collect();
        }
        if let Some(limit) = plan.limit {
            results.truncate(limit);
        }
        Ok(results)
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
            let mut filter_ids: Option<hashbrown::HashSet<std::sync::Arc<str>>> = None;
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
                None,
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
                        if let Some(view) = HakoDocView::new(&bytes) {
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
                plan.offset,
            )?
        };

        let mut results: Vec<(String, Vec<(String, Value)>)> = Vec::new();
        // let doc_count = docs.len();

        // if doc_count < 1000 || self.workers <= 1 {
        //     let task = QueryTask {
        //         docs,
        //         plan: plan.clone(),
        //         storage: Some(storage_arc.clone()),
        //     };
        //     results = run_task_projected(task);
        // } else {
        //     let optimal_workers = self.workers.min((doc_count / 500).max(1));
        //     let tasks = shard_tasks(
        //         docs,
        //         optimal_workers,
        //         plan.clone(),
        //         Some(storage_arc.clone()),
        //     );

        //     let mut handles = Vec::new();
        //     for task in tasks {
        //         handles.push(thread::spawn(move || run_task_projected(task)));
        //     }

        //     for handle in handles {
        //         results.extend(handle.join().unwrap_or_default());
        //     }
        // }

        // Apply Offset/Limit to Index results early if satisfied
        let mut docs_to_process = docs;
        let mut offset_to_apply_later = plan.offset.unwrap_or(0);
        if plan.order_by_satisfied && offset_to_apply_later > 0 {
            let skip = offset_to_apply_later.min(docs_to_process.len());
            docs_to_process.drain(0..skip);
            offset_to_apply_later = 0;
        }
        if plan.order_by_satisfied && plan.filters_satisfied_by_index {
            if let Some(limit) = plan.limit { docs_to_process.truncate(limit); }
        }
        let doc_count = docs_to_process.len();

        // --- FAST PATH ---
        if doc_count <= 250 {
            let storage_guard = storage_arc.read().unwrap();
            let blob_manager = storage_guard.blob_manager.clone();
            let optimized_plan = crate::query::executor::worker::prepare_optimized_plan(&plan);

            for (id, pointer) in docs_to_process {
                if let Ok(Some(bytes)) = storage_guard.read_pointer(&pointer) {
                    if let Some(mut fields) = crate::query::executor::worker::unified_match_projected(&id, &bytes, &optimized_plan) {
                        if let Some(ref manager) = blob_manager {
                            for (_, val) in fields.iter_mut() {
                                if let Value::BlobLink { offset, len } = *val {
                                    *val = crate::query::executor::worker::resolve_single_blob_in_worker(manager, offset, len);
                                }
                            }
                        }
                        results.push((id, fields));
                    }
                }
            }
        } else {
            // SLOW PATH (Rayon Tasks)
            let optimal_workers = self.workers.min((doc_count / 500).max(1));
            let tasks = shard_tasks(docs_to_process, optimal_workers, plan.clone(), Some(storage_arc.clone()));
            let mut handles = Vec::new();
            for task in tasks { handles.push(thread::spawn(move || run_task_projected(task))); }
            for handle in handles { results.extend(handle.join().unwrap_or_default()); }
        }

        if !plan.order_by_satisfied && !plan.order_by.is_empty() {
            results.sort_by(|(id_a, fields_a), (id_b, fields_b)| {
                for order in &plan.order_by {
                    let cmp = match order.field.as_str() {
                        "id" => id_a.cmp(id_b),
                        "_time" => { /* handle time if projected */ id_a.cmp(id_b) } // Placeholder
                        _ => {
                            let av = fields_a.iter().find(|(k, _)| k == &order.field).map(|(_, v)| v);
                            let bv = fields_b.iter().find(|(k, _)| k == &order.field).map(|(_, v)| v);
                            av.cmp(&bv)
                        }
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.ascending { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }

        // let mut final_results = if let Some(offset) = plan.offset {
        //     results.into_iter().skip(offset).collect()
        // } else {
        //     results
        // };

        // if let Some(limit) = plan.limit {
        //     final_results.truncate(limit);
        // }
        let mut final_results = if offset_to_apply_later > 0 {
            results.into_iter().skip(offset_to_apply_later).collect()
        } else { results };

        if let Some(limit) = plan.limit { final_results.truncate(limit); }

        Ok(final_results)
    }

    fn execute_single_scan(
        &self,
        storage: &StorageEngine,
        indexes: &IndexManager,
        scan: &ScanType,
        collection: &str,
        limit: Option<usize>,
        offset: Option<usize>,
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

            // Direct slice over sorted_keys. Used by the planner for queries
            // that order by `id` (or no order at all) and have no usable
            // filter index — turns offset-of-N from O(N) into O(log N + limit).
            ScanType::SortedKeys { start_key, start_exclusive, reverse } => {
                let total = storage.sorted_keys.len();
                if total == 0 {
                    return Ok(Vec::new());
                }
                // Descending, optional upper cursor bound (planner
                // guarantee): binary-search the anchor, walk backward —
                // O(log N + limit), no sort pass needed.
                if *reverse {
                    let (start, end) = match storage.sorted_key_range_reverse(
                        start_key.as_deref(),
                        *start_exclusive,
                        offset,
                        limit,
                    ) {
                        Some(r) => r,
                        None => return Ok(Vec::new()),
                    };
                    let mut out = Vec::with_capacity(end.saturating_sub(start));
                    for key in storage.sorted_keys[start..end].iter().rev() {
                        if let Some(ptr) = storage.index.get(key) {
                            if !matches!(ptr, Pointer::Deleted { .. }) {
                                out.push((key.clone(), ptr.clone()));
                                if out.len() >= max_ids { break; }
                            }
                        }
                    }
                    return Ok(out);
                }

                // Ascending, optional cursor bound: binary search + slice.
                let (start_pos, end_pos) = match storage.sorted_key_range(
                    start_key.as_deref(),
                    *start_exclusive,
                    offset,
                    limit,
                ) {
                    Some(r) => r,
                    None => return Ok(Vec::new()),
                };
                let mut out = Vec::with_capacity(end_pos.saturating_sub(start_pos));
                for key in &storage.sorted_keys[start_pos..end_pos] {
                    if let Some(ptr) = storage.index.get(key) {
                        if !matches!(ptr, Pointer::Deleted { .. }) {
                            out.push((key.clone(), ptr.clone()));
                            if out.len() >= max_ids { break; }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::SecondaryIndex { field, value } => {
                // let mut out = Vec::new();
                // if let Some(sec_map) = indexes.secondary.get(collection) {
                //     if let Some(index) = sec_map.get(field) {
                //         for doc_id in index.range_scan(value, value).iter() {
                //             let key = Self::make_key(collection, doc_id);
                //             if let Some(ptr) = storage.index.get(&key) {
                //                 // FIX: Ensure we don't return a deleted pointer found in secondary index
                //                 if !matches!(ptr, Pointer::Deleted { .. }) {
                //                     out.push((key, ptr.clone()));
                //                     if out.len() >= max_ids {
                //                         break;
                //                     }
                //                 }
                //             }
                //         }
                //     }
                // }
                // Ok(out)
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        
                        // CRITICAL FIX: Direct `get()` on the BTreeMap prevents array allocation
                        if let Some(doc_ids) = index.get_map().get(value) {
                            for doc_id in doc_ids {
                                let key = Self::make_key(collection, doc_id);
                                if let Some(ptr) = storage.index.get(&key) {
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

            ScanType::CompositeIndex {
                index_id,
                fields: _,
                values,
                reverse,
            } => {
                let mut out = Vec::new();
                if let Some(idx) = indexes.composite.get(*index_id) {
                    let range = crate::index::composite::range_builder::build_prefix_range(&idx.definition, values);
                    // ponytail: bounded walk — the old code materialized the
                    // ENTIRE prefix (e.g. 666 keys for a hot value) and only
                    // then truncated via max_ids below.
                    let doc_ids = idx.range_scan_limit(&range.start, &range.end, max_ids, *reverse);

                    // ponytail: range_scan_limit already yields rows in scan
                    // order (ascending, or descending when reverse), so no
                    // re-reversal — the old code rev'd because it always
                    // materialized ascending first.
                    for doc_id in doc_ids.iter() {
                        if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                            if !matches!(ptr, Pointer::Deleted { .. }) {
                                out.push((doc_id.to_string(), ptr.clone()));
                            }
                        }
                        if out.len() >= max_ids { break; }
                    }
                }
                Ok(out)
            }

            ScanType::CompositeIndexRange { index_id, ranges, reverse } => {
                let mut out = Vec::new();
                if let Some(idx) = indexes.composite.get(*index_id) {
                    for (start_bound, end_bound) in ranges {
                        let iter = idx.tree.range((start_bound.clone(), end_bound.clone()));
                        
                        if *reverse {
                            for (_, doc_id) in iter.rev() {
                                if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                                    if !matches!(ptr, Pointer::Deleted { .. }) {
                                        out.push((doc_id.to_string(), ptr.clone()));
                                    }
                                }
                                if out.len() >= max_ids { break; }
                            }
                        } else {
                            for (_, doc_id) in iter {
                                if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                                    if !matches!(ptr, Pointer::Deleted { .. }) {
                                        out.push((doc_id.to_string(), ptr.clone()));
                                    }
                                }
                                if out.len() >= max_ids { break; }
                            }
                        }
                        if out.len() >= max_ids { break; }
                    }
                }
                Ok(out)
            }

            // ScanType::SecondaryIndexRange { field, start, end, reverse } => {
            //     let mut out = Vec::new();
            //     if let Some(sec_map) = indexes.secondary.get(collection) {
            //         if let Some(index) = sec_map.get(field) {
            //             let range_iter = index.get_map().range((start.clone(), end.clone()));
                        
            //             // CRITICAL FIX: Iterate lazily
            //             if *reverse {
            //                 for (_, ids) in range_iter.rev() {
            //                     for doc_id in ids {
            //                         if let Some(ptr) = storage.index.get(doc_id) {
            //                             if !matches!(ptr, Pointer::Deleted { .. }) {
            //                                 out.push((doc_id.clone(), ptr.clone()));
            //                                 if out.len() >= max_ids { break; }
            //                             }
            //                         }
            //                     }
            //                     if out.len() >= max_ids { break; }
            //                 }
            //             } else {
            //                 for (_, ids) in range_iter {
            //                     for doc_id in ids {
            //                         if let Some(ptr) = storage.index.get(doc_id) {
            //                             if !matches!(ptr, Pointer::Deleted { .. }) {
            //                                 out.push((doc_id.clone(), ptr.clone()));
            //                                 if out.len() >= max_ids { break; }
            //                             }
            //                         }
            //                     }
            //                     if out.len() >= max_ids { break; }
            //                 }
            //             }
            //         }
            //     }
            //     Ok(out)
            // }

            ScanType::SecondaryIndexRange { field, start, end, reverse } => {
                let mut out = Vec::new();
                if let Some(sec_map) = indexes.secondary.get(collection) {
                    if let Some(index) = sec_map.get(field) {
                        let range_iter = index.get_map().range((start.clone(), end.clone()));
                        
                        if *reverse {
                            for (_, ids) in range_iter.rev() {
                                for doc_id in ids.iter().rev() { 
                                    if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                                        if !matches!(ptr, Pointer::Deleted { .. }) {
                                            out.push((doc_id.to_string(), ptr.clone()));
                                            if out.len() >= max_ids { break; }
                                        }
                                    }
                                }
                                if out.len() >= max_ids { break; }
                            }
                        } else {
                            for (_, ids) in range_iter {
                                for doc_id in ids {
                                    if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                                        if !matches!(ptr, Pointer::Deleted { .. }) {
                                            out.push((doc_id.to_string(), ptr.clone()));
                                            if out.len() >= max_ids { break; }
                                        }
                                    }
                                }
                                if out.len() >= max_ids { break; }
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::CursorIndex { index_id, start, end, reverse } => {
                let mut out = Vec::new();
                if let Some(idx) = indexes.composite.get(*index_id) {
                    let iter = idx.tree.range((start.clone(), end.clone()));
                    
                    // CRITICAL FIX: Iterate lazily to prevent massive memory allocation
                    if *reverse {
                        for (_, doc_id) in iter.rev() {
                            if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                                if !matches!(ptr, Pointer::Deleted { .. }) {
                                    out.push((doc_id.to_string(), ptr.clone()));
                                    if out.len() >= max_ids { break; }
                                }
                            }
                        }
                    } else {
                        for (_, doc_id) in iter {
                            if let Some(ptr) = storage.index.get(doc_id.as_ref()) {
                                if !matches!(ptr, Pointer::Deleted { .. }) {
                                    out.push((doc_id.to_string(), ptr.clone()));
                                    if out.len() >= max_ids { break; }
                                }
                            }
                        }
                    }
                }
                Ok(out)
            }

            ScanType::InvertedIndex { field, query, prefix } => {
                let mut out = Vec::new();
                if let Some(fts_map) = indexes.fts.get(collection) {
                    if let Some(index) = fts_map.get(field) {
                        let doc_ids = if *prefix {
                            index.search_prefix(query)
                        } else {
                            index.search(query)
                        };
                        if let Some(doc_ids) = doc_ids {
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

            ScanType::UnionIndex { scans } => {
                let mut unique_results = HashMap::new();
                for sub_scan in scans {
                    // Recursively execute sub-scans (Eq lookups for each item in the "IN" array)
                    let results = self.execute_single_scan(storage, indexes, sub_scan, collection, None, None)?;
                    for (id, ptr) in results {
                        unique_results.insert(id, ptr); 
                    }
                }
                let mut out: Vec<_> = unique_results.into_iter().collect();
                // Re-apply limits after union
                if let Some(l) = limit { out.truncate(l); }
                Ok(out)
            }

        }
    }
}
