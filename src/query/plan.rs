use crate::document::value::Value;
use smallvec::SmallVec;
use std::ops::Bound;

use super::filter::Filter;
use super::order::OrderBy;

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
}
