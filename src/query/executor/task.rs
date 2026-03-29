use super::super::plan::QueryPlan;
use std::sync::{Arc, RwLock};
use crate::storage::engine::{StorageEngine,Pointer};

#[derive(Clone)]
pub struct QueryTask {
    pub docs: Vec<(String, Pointer)>,
    pub plan: QueryPlan,
    pub storage: Option<Arc<RwLock<StorageEngine>>>,
}
