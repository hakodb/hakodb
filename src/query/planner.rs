use crate::query::query::Query;
use crate::query::plan::{QueryPlan, ScanType};
use crate::index::manager::IndexManager;

pub struct QueryPlanner;

impl QueryPlanner {

    pub fn plan(
        query: &Query,
        collection_id: u32,
        index_manager: &IndexManager
    ) -> QueryPlan {

        for filter in &query.filters {

            if index_manager.has_index(
                collection_id,
                &filter.field
            ) {

                return QueryPlan {

                    collection_id,

                    scan: ScanType::IndexScan {
                        field: filter.field.clone()
                    },

                    filters: query.filters.clone(),

                    limit: query.limit
                };
            }
        }

        QueryPlan {

            collection_id,

            scan: ScanType::CollectionScan,

            filters: query.filters.clone(),

            limit: query.limit
        }
    }
}
