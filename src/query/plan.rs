use smallvec::SmallVec;

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
    CursorIndex { start_key: SmallVec<[u8; 32]> },
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
}
