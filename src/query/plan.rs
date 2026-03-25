use smallvec::SmallVec;
use std::ops::Bound;
use crate::document::value::Value;

use super::filter::Filter;
use super::order::OrderBy;

#[derive(Debug, Clone)]
pub enum ScanType {
    FullCollection,
    CompositeIndex {
        fields: Vec<String>,
        values: Vec<Value>,
    },
    // CursorIndex { start_key: SmallVec<[u8; 32]> },
    SecondaryIndex { field: String, value: Vec<u8> },
    UnionIndex { scans: Vec<ScanType> },
    InvertedIndex { field: String, query: String },
    // UPDATED: CursorIndex now defines a strict range
    CursorIndex { 
        start: Bound<SmallVec<[u8; 32]>>, 
        end: Bound<SmallVec<[u8; 32]>>
    },
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub collection: String,
    pub scan: ScanType,
    pub filters: Vec<Filter>,
    pub or_groups: Vec<Vec<crate::query::filter::Filter>>, // <--- ADD THIS
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>, 
    pub projection: Vec<String>,
    pub scan_limit: Option<usize>,
}
