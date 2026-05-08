use crate::document::value::Value;
use smallvec::SmallVec;
use std::ops::Bound;

use super::filter::Filter;
use super::order::OrderBy;

#[derive(Debug, Clone, PartialEq)]
pub enum ScanType {
    FullCollection,
    CompositeIndex {
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
}
