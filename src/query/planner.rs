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
        let work_per_thread = collection_rows / worker_count.max(1);
        let use_index_heuristic = work_per_thread > 500; 

        // 1. PRIORITY 1: Full-Text Search (Match operator)
        for filter in &query.filters {
            if matches!(filter.op, Operator::Match) {
                if let Value::String(q_text) = &filter.value {
                    if indexes.fts.get(&query.collection).map_or(false, |m| m.contains_key(&filter.field)) {
                        return Self::make_plan(query, ScanType::InvertedIndex { 
                            field: filter.field.clone(), query: q_text.clone() 
                        }, query.limit); // Limit safe here if no order_by
                    }
                }
            }
        }

        // 2. PRIORITY 2: Range/Cursor Detection
        if let Some(order) = &query.order_by {
            let has_bounds = query.start_at.is_some() || query.start_after.is_some() || 
                             query.end_at.is_some() || query.end_before.is_some();
            if has_bounds {
                for idx in indexes.indexes_for_collection(&query.collection) {
                    if !idx.definition.fields.is_empty() && idx.definition.fields[0].field == order.field {
                        let start = match (&query.start_at, &query.start_after) {
                            (Some(v), _) => Bound::Included(build_cursor_range(&idx.definition, v, false)),
                            (_, Some(v)) => Bound::Excluded(build_cursor_range(&idx.definition, v, false)),
                            _ => Bound::Unbounded,
                        };
                        let end = match (&query.end_at, &query.end_before) {
                            (Some(v), _) => Bound::Included(build_cursor_range(&idx.definition, v, false)),
                            (_, Some(v)) => Bound::Excluded(build_cursor_range(&idx.definition, v, false)),
                            _ => Bound::Unbounded,
                        };
                        return Self::make_plan(query, ScanType::CursorIndex { start, end }, query.limit);
                    }
                }
            }
        }

        // 3. PRIORITY 3: Composite Index (Filters + OrderBy)
        if use_index_heuristic {
            let mut target_fields = query.composite_fields();
            if let Some(order) = &query.order_by {
                target_fields.push(order.field.clone());
            }

            // Checks if index covers both filters and sort
            if indexes.has_index(&query.collection, &target_fields) {
                let is_eq_only = query.filters.iter().all(|f| matches!(f.op, Operator::Eq));
                if is_eq_only {
                    // FIX: We only extract values for the FILTER fields, not the OrderBy field
                    let values: Vec<_> = query.filters.iter().map(|f| f.value.clone()).collect();
                    return Self::make_plan(query, ScanType::CompositeIndex { 
                        fields: target_fields, 
                        values 
                    }, query.limit); // Limit safe because B-Tree natively sorts
                }
            }
        }

        // 4. Try Union/OR/IN Logic
        if !query.or_groups.is_empty() || query.filters.iter().any(|f| matches!(f.op, Operator::In)) {
            if let Some(union_scan) = Self::try_plan_union(query, indexes) {
                let safe_limit = if query.order_by.is_some() { None } else { query.limit };
                return Self::make_plan(query, union_scan, safe_limit);
            }
        }

        // 5. PRIORITY 4: Secondary Index (Equality)
        // Ensure we DO NOT pass the limit down to the scan if we have an ORDER BY
        let safe_limit = if query.order_by.is_some() { None } else { query.limit };

        if use_index_heuristic {
            for filter in &query.filters {
                if matches!(filter.op, Operator::Eq) {
                    if indexes.secondary.get(&query.collection).map_or(false, |m| m.contains_key(&filter.field)) {
                        let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                        return Self::make_plan(query, ScanType::SecondaryIndex { 
                            field: filter.field.clone(), value: val_bytes 
                        }, safe_limit);
                    }
                }
            }
        }

        // 6. Default: Full Scan
        Self::make_plan(query, ScanType::FullCollection, safe_limit)
    }
    
    fn make_plan(query: &Query, scan: ScanType, scan_limit: Option<usize>) -> QueryPlan {
        QueryPlan {
            collection: query.collection.clone(),
            scan,
            filters: query.filters.clone(),
            or_groups: query.or_groups.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit,
            scan_limit, // Used specifically for disk retrieval
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