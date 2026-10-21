use crate::document::value::Value;
use crate::index::vector::Metric;
use smallvec::SmallVec;
use std::ops::Bound;

use super::filter::{Filter, Operator};
use super::order::OrderBy;
use super::query::ElemMatch;

#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    FullCollection,
    /// Direct slice over the storage engine's sorted_keys vec. Used when the
    /// planner can determine that the query's natural key order matches the
    /// requested order (or no order is requested) — i.e. the executor would
    /// otherwise materialise the entire index and skip N entries by hand.
    /// `start_key` is the inclusive start; combined with the plan's
    /// `offset` and `limit` to derive the actual slice.
    SortedKeys {
        start_key: Option<String>,
        /// true = `start_after` (exclusive bound), false = `start_at`
        /// (inclusive) or no bound. Meaningful whenever `start_key` is
        /// Some, in either direction.
        start_exclusive: bool,
        /// true = descending walk (highest key first). Carries the same
        /// optional upper `start_key` bound as the ascending path; `None`
        /// means from the top.
        reverse: bool,
    },
    CompositeIndex {
        index_id: u32,
        fields: Vec<String>,
        values: Vec<Value>,
        reverse: bool,
    },
    CompositeIndexRange {
        index_id: u32,
        ranges: Vec<(Bound<SmallVec<[u8; 32]>>, Bound<SmallVec<[u8; 32]>>)>,
        reverse: bool,
    },
    SecondaryIndexRange {
        field: String,
        start: Bound<Vec<u8>>,
        end: Bound<Vec<u8>>,
        reverse: bool,
    },
    SecondaryIndex {
        field: String,
        value: Vec<u8>,
    },
    UnionIndex {
        scans: Vec<ScanType>,
    },
    InvertedIndex {
        field: String,
        query: String,
        prefix: bool,
    },
    /// Exact KNN (P0 brute-force; P1 swaps the scan internals for HNSW).
    /// `query` is the raw LE-f32 Binary (mirrors the filter value); the
    /// arm scores every doc and returns ids nearest-first. `metric` is
    /// fixed by the index definition at plan time. `extra` carries the
    /// plan's remaining (non-Near) filters — the arm verifies them
    /// itself (the plan is marked satisfied, and `execute_single_scan`
    /// takes no filter list, so they must ride the scan).
    VectorKnn {
        field: String,
        query: Vec<u8>,
        metric: Metric,
        extra: Vec<Filter>,
    },
    /// Graph BFS from one anchor (issue #53). `start_col`/`start_id`
    /// ride the marker filter's field/value; `spec` carries direction,
    /// edge field, depth, limit. `extra` like VectorKnn (verified by the
    /// executor itself). Hits are filtered to the queried collection —
    /// traversal crosses collections internally, results don't (the
    /// pipeline's rows are single-collection; cross-collection hits ride
    /// the `find_paths` engine API instead).
    Traverse {
        start_col: String,
        start_id: String,
        spec: crate::index::traverse::TraverseSpec,
        extra: Vec<Filter>,
    },
    // UPDATED: CursorIndex now defines a strict range
    CursorIndex {
        start: Bound<SmallVec<[u8; 32]>>,
        end: Bound<SmallVec<[u8; 32]>>,
        index_id: u32,
        reverse: bool,
    },
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub collection: String,
    pub scan: ScanType,
    pub filters: Vec<Filter>,
    pub or_groups: Vec<Vec<crate::query::filter::Filter>>, // <--- ADD THIS
    /// hakodb#71 F3 — elemMatch clauses riding the plan. Non-empty
    /// plans always decode (the clauses are matcher passes), so
    /// `make_plan` forces `filters_satisfied_by_index` false and the
    /// raw/walk gate refuses them.
    pub elem_match: Vec<ElemMatch>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub projection: Vec<String>,
    pub scan_limit: Option<usize>,
    pub order_by_satisfied: bool,
    pub filters_satisfied_by_index: bool,
    /// ponytail: mirrors `Query::defer_blobs` — part of the plan (and the
    /// plan-cache key) because it changes what the executor returns.
    pub defer_blobs: bool,
    /// ponytail: mirrors `Query::raw` — the executor's raw path reads this,
    /// and it keys the plan cache (a raw query must never reuse a decoded
    /// plan's output shape or vice versa).
    pub raw: bool,
    /// True when the query carries cursor bounds. Unsatisfied-order scans
    /// cannot apply them natively (the driver post-filters), so the TopN
    /// lane refuses such plans and they keep the legacy full path.
    pub has_cursor_bounds: bool,
}

impl ScanType {
    /// Human-readable scan kind for explain and metrics labels.
    pub fn describe(&self) -> String {
        match self {
            ScanType::FullCollection => "full_collection".into(),
            ScanType::SortedKeys { .. } => "sorted_keys".into(),
            ScanType::CompositeIndex { fields, .. } => {
                format!("composite_index({})", fields.join(","))
            }
            ScanType::CompositeIndexRange { index_id, .. } => {
                format!("composite_index_range({index_id})")
            }
            ScanType::SecondaryIndex { field, .. } => format!("secondary_index({field})"),
            ScanType::SecondaryIndexRange { field, .. } => {
                format!("secondary_index_range({field})")
            }
            ScanType::UnionIndex { scans } => {
                let parts: Vec<String> = scans.iter().map(|s| s.describe()).collect();
                format!("union({})", parts.join("|"))
            }
            ScanType::InvertedIndex { field, .. } => format!("inverted_index({field})"),
            ScanType::VectorKnn { field, .. } => format!("vector_knn({field})"),
            ScanType::Traverse { .. } => "traverse".into(),
            ScanType::CursorIndex { index_id, .. } => format!("cursor_index({index_id})"),
        }
    }

    /// The index a scan rides, or None for plain collection/key walks.
    /// This is the label the per-index hit counters use.
    pub fn index_label(&self) -> Option<String> {
        match self {
            ScanType::FullCollection | ScanType::SortedKeys { .. } | ScanType::Traverse { .. } => {
                None
            }
            other => Some(other.describe()),
        }
    }
}

impl QueryPlan {
    /// hakodb#71 F6 / hakodb#22 - index-only (covering) projection. The
    /// projected values must be known from the plan itself; the executor
    /// can then answer without reading a single document.
    ///
    /// Supported shapes:
    /// - composite point lookup whose prefix covers the projection;
    /// - secondary equality where the projection is the indexed field;
    /// - `id` alone on an index-backed scan (the row id is the output key).
    ///
    /// Fail-closed by construction: anything not provably covered returns
    /// None and the normal decode path runs.
    pub fn covering_projection(&self) -> Option<Vec<(String, Value)>> {
        if !self.filters_satisfied_by_index
            || !self.order_by_satisfied
            || self.has_cursor_bounds
            || self.raw
            || self.projection.is_empty()
            || !self.elem_match.is_empty()
        {
            return None;
        }
        let mut out = Vec::with_capacity(self.projection.len());
        for path in &self.projection {
            if path == "id" {
                // The executor already carries the row id; projection APIs
                // return it outside the field list.
                if self.scan.index_label().is_none() {
                    return None;
                }
                continue;
            }
            if path.contains('.') {
                // Nested rebuild from an index prefix is a later step; the
                // decode path handles it correctly today.
                return None;
            }
            let value = match &self.scan {
                ScanType::CompositeIndex { fields, values, .. } => {
                    let pos = fields.iter().position(|f| f == path)?;
                    values.get(pos)?.clone()
                }
                ScanType::SecondaryIndex { field, .. } => {
                    // The planner only marks a secondary scan satisfied for
                    // a single equality filter on this field.
                    if field != path {
                        return None;
                    }
                    let mut filters = self
                        .filters
                        .iter()
                        .filter(|f| f.field == *field && f.op == Operator::Eq);
                    let filter = filters.next()?;
                    if filters.next().is_some() {
                        return None;
                    }
                    filter.value.clone()
                }
                _ => return None,
            };
            out.push((path.clone(), value));
        }
        Some(out)
    }
}
