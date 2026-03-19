use crate::index::manager::IndexManager;

use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(query: &Query, indexes: &IndexManager, collection_rows: usize) -> QueryPlan {
        let fields = query.composite_fields();
        
        // let all_eq = query.filters.iter().all(|f| matches!(f.op, Operator::Eq));

        // let scan = if all_eq {
        //     let values: Vec<_> = query.filters.iter().map(|f| f.value.clone()).collect();
        //     let candidates = indexes
        //         .exact_match_doc_ids(&query.collection, &fields, &values)
        //         .unwrap_or_default();

        //     // simple cost model: prefer index when candidate set is meaningfully smaller.
        //     if !candidates.is_empty() && candidates.len() <= collection_rows.max(1) {
        //         ScanType::CompositeIndex { fields, values }
        //     } else {
        //         ScanType::FullCollection
        //     }
        // } else {
        //     ScanType::FullCollection
        // };

        // NEW LOGIC: Support Eq, Gt, Gte, Lt, Lte for index scanning
        let is_index_compatible = query.filters.iter().all(|f| {
            matches!(f.op, Operator::Eq | Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte)
        });

        let scan = if is_index_compatible {
            let values: Vec<_> = query.filters.iter().map(|f| f.value.clone()).collect();
            
            // Check if we have a matching composite index
            let candidates = indexes
                .exact_match_doc_ids(&query.collection, &fields, &values);

            if let Some(ids) = candidates {
                 if !ids.is_empty() && ids.len() <= collection_rows.max(1) {
                    ScanType::CompositeIndex { fields, values }
                 } else {
                    ScanType::FullCollection
                 }
            } else {
                ScanType::FullCollection
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
            projection: query.projection.clone()
        }
    }
}
