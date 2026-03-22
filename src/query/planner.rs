use crate::index::manager::IndexManager;
use crate::index::composite::range_builder::build_cursor_range;
use crate::document::value::Value; // Added missing import
use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(query: &Query, indexes: &IndexManager, collection_rows: usize) -> QueryPlan {
        // 1. Try to build a Union Scan for OR / IN logic (v0.6.1 Optimization)
        if !query.or_groups.is_empty() || query.filters.iter().any(|f| matches!(f.op, Operator::In)) {
            // Call as associated function using Self::
            if let Some(union_scan) = Self::try_plan_union(query, indexes) {
                return QueryPlan {
                    collection: query.collection.clone(),
                    scan: union_scan,
                    filters: query.filters.clone(),
                    or_groups: query.or_groups.clone(),
                    order_by: query.order_by.clone(),
                    limit: query.limit,
                    offset: query.offset,
                    projection: query.projection.clone(),
                };
            }
        }

        // 2. If we have an OrderBy and a Cursor, try to jump!
        if let (Some(order), Some(cursor)) = (&query.order_by, &query.start_after) {
            for idx in indexes.indexes_for_collection(&query.collection) {
                if idx.definition.fields[0].field == order.field {
                    let start_key = build_cursor_range(&idx.definition, cursor, true);
                    return QueryPlan {
                        collection: query.collection.clone(),
                        scan: ScanType::CursorIndex { start_key },
                        filters: query.filters.clone(),
                        or_groups: query.or_groups.clone(),
                        order_by: query.order_by.clone(),
                        limit: query.limit,
                        offset: query.offset, // Carry over requested offset
                        projection: query.projection.clone(),
                    };
                }
            }
        }

        // 3. Check Secondary Indexes
        for filter in &query.filters {
            if matches!(filter.op, Operator::Eq) {
                if indexes.secondary.get(&query.collection)
                    .map_or(false, |m| m.contains_key(&filter.field)) 
                {
                    let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                    return QueryPlan {
                        collection: query.collection.clone(),
                        scan: ScanType::SecondaryIndex { 
                            field: filter.field.clone(), 
                            value: val_bytes 
                        },
                        filters: query.filters.clone(),
                        or_groups: query.or_groups.clone(),
                        order_by: query.order_by.clone(),
                        limit: query.limit,
                        offset: query.offset,
                        projection: query.projection.clone(),
                    };
                }
            }
        }

        // 4. Default: Composite Index check or Full Scan
        let fields = query.composite_fields();
        let is_index_compatible = query.filters.iter().all(|f| {
            matches!(f.op, Operator::Eq | Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte)
        });

        let scan = if is_index_compatible {
            let values: Vec<_> = query.filters.iter().map(|f| f.value.clone()).collect();
            let candidates = indexes.exact_match_doc_ids(&query.collection, &fields, &values);

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
            or_groups: query.or_groups.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit,
            offset: query.offset,  
            projection: query.projection.clone()
        }
    }
    
    // Changed from &self to associated function
    fn try_plan_union(query: &Query, indexes: &IndexManager) -> Option<ScanType> {
        let mut scans = Vec::new();

        // Handle IN operator: Convert "id IN [1,2]" to "id==1 UNION id==2"
        for filter in &query.filters {
            if matches!(filter.op, Operator::In) {
                if let Value::Array(vals) = &filter.value {
                    for v in vals {
                        scans.push(Self::get_single_filter_scan(&query.collection, &filter.field, v, indexes)?);
                    }
                }
            }
        }

        // Handle OR groups
        for group in &query.or_groups {
            // Optimization only for simple single-filter OR groups for now
            if group.len() == 1 {
                let f = &group[0];
                scans.push(Self::get_single_filter_scan(&query.collection, &f.field, &f.value, indexes)?);
            } else {
                return None; 
            }
        }

        if scans.is_empty() { None } else { Some(ScanType::UnionIndex { scans }) }
    }

    // Changed from &self to associated function
    fn get_single_filter_scan(col: &str, field: &str, val: &Value, indexes: &IndexManager) -> Option<ScanType> {
        // Check Secondary Index
        if indexes.secondary.get(col).map_or(false, |m| m.contains_key(field)) {
            let val_bytes = crate::index::index_key::encode_scalar(val);
            return Some(ScanType::SecondaryIndex { field: field.to_string(), value: val_bytes });
        }
        // Check Composite Index (Single field prefix)
        if indexes.has_index(col, &[field.to_string()]) {
            return Some(ScanType::CompositeIndex { fields: vec![field.to_string()], values: vec![val.clone()] });
        }
        None
    }
}