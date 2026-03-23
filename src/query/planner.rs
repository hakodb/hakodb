use crate::index::manager::IndexManager;
use crate::index::composite::range_builder::build_cursor_range;
use crate::document::value::Value;
use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;
use std::ops::Bound; // Required for range definitions

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(
        query: &Query, 
        indexes: &IndexManager, 
        collection_rows: usize,
        worker_count: usize 
    ) -> QueryPlan {
        
        
        // 1. PRIORITY 1: Full-Text Search (Match operator)
        // This is prioritized because it is usually the most selective index.
        for filter in &query.filters {
            if matches!(filter.op, Operator::Match) {
                if let Value::String(q_text) = &filter.value {
                    if indexes.fts.get(&query.collection).map_or(false, |m| m.contains_key(&filter.field)) {
                        return Self::make_plan(query, ScanType::InvertedIndex { 
                            field: filter.field.clone(), 
                            query: q_text.clone() 
                        });
                    }
                }
            }
        }

        // 2. PRIORITY 2: Range/Cursor Detection (v0.7.5 Update)
        // If the user provided any start/end bounds, we MUST use a CursorIndex scan 
        // to avoid O(N) performance degradation on large datasets.
        if let Some(order) = &query.order_by {
            let has_start = query.start_at.is_some() || query.start_after.is_some();
            let has_end = query.end_at.is_some() || query.end_before.is_some();

            if has_start || has_end {
                // Find an index that starts with the field we are ordering by
                for idx in indexes.indexes_for_collection(&query.collection) {
                    if !idx.definition.fields.is_empty() && idx.definition.fields[0].field == order.field {
                        
                        // Calculate Start Bound
                        let start = match (&query.start_at, &query.start_after) {
                            (Some(v), _) => Bound::Included(build_cursor_range(&idx.definition, v, false)),
                            (_, Some(v)) => Bound::Excluded(build_cursor_range(&idx.definition, v, false)),
                            _ => Bound::Unbounded, // Start from the beginning
                        };

                        // Calculate End Bound
                        let end = match (&query.end_at, &query.end_before) {
                            (Some(v), _) => Bound::Included(build_cursor_range(&idx.definition, v, false)),
                            (_, Some(v)) => Bound::Excluded(build_cursor_range(&idx.definition, v, false)),
                            _ => Bound::Unbounded, // Go up to the end
                        };

                        return Self::make_plan(query, ScanType::CursorIndex { start, end });
                    }
                }
            }
        }

        // 3. INTELLIGENCE: Cost-Based Heuristic
        // If the work per thread is low, we ignore standard indexes and use parallel full scan.
        let work_per_thread = collection_rows / worker_count.max(1);
        let use_index_heuristic = work_per_thread > 1000 && collection_rows > 15000;

        // 4. Try Union/OR/IN Logic
        if !query.or_groups.is_empty() || query.filters.iter().any(|f| matches!(f.op, Operator::In)) {
            if let Some(union_scan) = Self::try_plan_union(query, indexes) {
                return Self::make_plan(query, union_scan);
            }
        }

        // 5. Conditional Secondary Index (Equality)
        if use_index_heuristic {
            for filter in &query.filters {
                if matches!(filter.op, Operator::Eq) {
                    if indexes.secondary.get(&query.collection)
                        .map_or(false, |m| m.contains_key(&filter.field)) 
                    {
                        let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                        return Self::make_plan(query, ScanType::SecondaryIndex { 
                            field: filter.field.clone(), 
                            value: val_bytes 
                        });
                    }
                }
            }
        }

        // 6. Default: Composite Index check or Full Scan
        let fields = query.composite_fields();
        let is_index_compatible = query.filters.iter().all(|f| {
            matches!(f.op, Operator::Eq | Operator::Gt | Operator::Gte | Operator::Lt | Operator::Lte)
        });

        let scan = if is_index_compatible && use_index_heuristic {
            let values: Vec<_> = query.filters.iter().map(|f| f.value.clone()).collect();
            if indexes.has_index(&query.collection, &fields) {
                ScanType::CompositeIndex { fields, values }
            } else {
                ScanType::FullCollection
            }
        } else {
            ScanType::FullCollection
        };

        Self::make_plan(query, scan)
    }
    
    fn make_plan(query: &Query, scan: ScanType) -> QueryPlan {
        QueryPlan {
            collection: query.collection.clone(),
            scan,
            filters: query.filters.clone(),
            or_groups: query.or_groups.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit,
            offset: query.offset,
            projection: query.projection.clone(),
        }
    }

    fn try_plan_union(query: &Query, indexes: &IndexManager) -> Option<ScanType> {
        let mut scans = Vec::new();
        for filter in &query.filters {
            if matches!(filter.op, Operator::In) {
                if let Value::Array(vals) = &filter.value {
                    for v in vals {
                        scans.push(Self::get_single_filter_scan(&query.collection, &filter.field, v, indexes)?);
                    }
                }
            }
        }
        for group in &query.or_groups {
            if group.len() == 1 {
                let f = &group[0];
                scans.push(Self::get_single_filter_scan(&query.collection, &f.field, &f.value, indexes)?);
            } else {
                return None; 
            }
        }
        if scans.is_empty() { None } else { Some(ScanType::UnionIndex { scans }) }
    }

    fn get_single_filter_scan(col: &str, field: &str, val: &Value, indexes: &IndexManager) -> Option<ScanType> {
        if indexes.secondary.get(col).map_or(false, |m| m.contains_key(field)) {
            let val_bytes = crate::index::index_key::encode_scalar(val);
            return Some(ScanType::SecondaryIndex { field: field.to_string(), value: val_bytes });
        }
        if indexes.has_index(col, &[field.to_string()]) {
            return Some(ScanType::CompositeIndex { fields: vec![field.to_string()], values: vec![val.clone()] });
        }
        None
    }
}