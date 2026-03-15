use super::super::plan::QueryPlan;

#[derive(Clone)]
pub struct QueryTask {
    pub docs: Vec<(String, Vec<u8>)>,
    pub plan: QueryPlan,
}
