use super::query::Filter;

#[derive(Debug, Clone)]
pub enum ScanType {
    FullCollection,
    CompositeIndex,
}

#[derive(Debug, Clone)]
pub struct QueryPlan {
    pub collection: String,
    pub scan: ScanType,
    pub filters: Vec<Filter>,
    pub limit: Option<usize>,
}
