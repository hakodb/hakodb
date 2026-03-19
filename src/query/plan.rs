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
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub collection: String,
    pub scan: ScanType,
    pub filters: Vec<Filter>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub projection: Vec<String>,
}
