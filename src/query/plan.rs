use crate::query::query::Filter;

#[derive(Debug)]
pub enum ScanType {

    CollectionScan,

    IndexScan {
        field: String
    }
}

#[derive(Debug)]
pub struct QueryPlan {

    pub collection_id: u32,

    pub scan: ScanType,

    pub filters: Vec<Filter>,

    pub limit: Option<usize>,
}
