use super::super::plan::QueryPlan;
use std::sync::{Arc, RwLock};
use crate::storage::engine::StorageEngine;

#[derive(Clone)]
pub struct QueryTask {
    pub docs: Vec<(String, Vec<u8>)>,
    pub plan: QueryPlan,
    pub storage: Option<Arc<RwLock<StorageEngine>>>,
}
