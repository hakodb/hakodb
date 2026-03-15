use crate::index::manager::IndexManager;

use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(query: &Query, indexes: &IndexManager) -> QueryPlan {
        let fields = query.index_fields();
        let scan = if indexes.has_index(&query.collection, &fields) {
            ScanType::CompositeIndex
        } else {
            ScanType::FullCollection
        };
        QueryPlan {
            collection: query.collection.clone(),
            scan,
            filters: query.filters.clone(),
            limit: query.limit,
        }
    }
}
