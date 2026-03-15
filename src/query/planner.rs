use crate::index::manager::IndexManager;

use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(query: &Query, indexes: &IndexManager) -> QueryPlan {
        let fields = query.composite_fields();
        let all_eq = query.filters.iter().all(|f| matches!(f.op, Operator::Eq));
        let scan = if all_eq && indexes.has_index(&query.collection, &fields) {
            ScanType::CompositeIndex {
                fields,
                values: query.filters.iter().map(|f| f.value.clone()).collect(),
            }
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
