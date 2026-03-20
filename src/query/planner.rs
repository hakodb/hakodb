use crate::index::manager::IndexManager;
use crate::index::composite::range_builder::build_cursor_range;
use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(query: &Query, indexes: &IndexManager, collection_rows: usize) -> QueryPlan {

        // If we have an OrderBy and a Cursor, try to jump!
        if let (Some(order), Some(cursor)) = (&query.order_by, &query.start_after) {
            // Find an index that starts with our Sort field
            for idx in indexes.indexes_for_collection(&query.collection) {
                if idx.definition.fields[0].field == order.field {
                    let start_key = build_cursor_range(&idx.definition, cursor, true);
                    return QueryPlan {
                        collection: query.collection.clone(),
                        scan: ScanType::CursorIndex { start_key },
                        filters: query.filters.clone(),
                        or_groups: query.or_groups.clone(), // <--- ADD THIS
                        order_by: query.order_by.clone(),
                        limit: query.limit,
                        offset: None, // Cursor replaces Offset!
                        projection: query.projection.clone(),
                    };
                }
            }
        }

        // NEW: Check Secondary Indexes
        for filter in &query.filters {
            if matches!(filter.op, Operator::Eq) {
                if indexes.secondary.get(&query.collection)
                    .map_or(false, |m| m.contains_key(&filter.field)) 
                {
                    // Found a single-field index!
                    let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                    return QueryPlan {
                        collection: query.collection.clone(),
                        scan: ScanType::SecondaryIndex { 
                            field: filter.field.clone(), 
                            value: val_bytes 
                        },
                        filters: query.filters.clone(),
                        or_groups: query.or_groups.clone(), // <--- ADD THIS
                        order_by: query.order_by.clone(),
                        limit: query.limit,
                        offset: None, // Cursor replaces Offset!
                        projection: query.projection.clone(),
                    };
                }
            }
        }

        let fields = query.composite_fields();
        
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
            or_groups: query.or_groups.clone(), // <--- ADD THIS
            order_by: query.order_by.clone(),
            limit: query.limit,
            offset: query.offset,  
            projection: query.projection.clone()
        }
    }
}
