use crate::index::manager::IndexManager;

use super::composite_planner::has_matching_composite_index;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(query: &Query, indexes: &IndexManager) -> QueryPlan {
        let scan = if has_matching_composite_index(query, indexes) {
            ScanType::CompositeIndex
        } else {
            ScanType::FullCollection
        };

        QueryPlan {
            collection: query.collection.clone(),
            scan,
            filters: query.filters.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit,
        }
    }
}
