use crate::index::manager::IndexManager;
use crate::index::composite::range_builder::build_cursor_range;
use crate::document::value::Value; // Added missing import
use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(
        query: &Query, 
        indexes: &IndexManager, 
        collection_rows: usize,
        worker_count: usize // NEW: Consider hardware resources
    ) -> QueryPlan {
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
        // --- INTELLIGENCE: Cost-Based Heuristic ---
        // A parallel full scan is extremely fast if we have many workers and few rows.
        // We calculate if the "work per thread" is low enough to ignore the index.

        let work_per_thread = collection_rows / worker_count.max(1);
        
        // If work per thread is low (e.g., < 1000 docs), 
        // a parallel full scan is usually faster than index traversal overhead.
        // let use_index_heuristic = work_per_thread > 800 || collection_rows > 5000;
        let use_index_heuristic = work_per_thread > 1000 && collection_rows > 15000;

        // 1. Force Index for Cursor/Pagination (Algorithmically necessary)
        if let (Some(order), Some(cursor)) = (&query.order_by, &query.start_after) {
            for idx in indexes.indexes_for_collection(&query.collection) {
                if idx.definition.fields[0].field == order.field {
                    let start_key = build_cursor_range(&idx.definition, cursor, true);
                    return Self::make_plan(query, ScanType::CursorIndex { start_key });
                }
            }
        }

        // 2. Try Union/OR/IN Logic
        if !query.or_groups.is_empty() || query.filters.iter().any(|f| matches!(f.op, Operator::In)) {
            if let Some(union_scan) = Self::try_plan_union(query, indexes) {
                return Self::make_plan(query, union_scan);
            }
        }

        // 3. Conditional Secondary Index (Only if collection is large enough)
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

        // 4. Default: Composite Index check or Full Scan
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
    
    // Helper to reduce boilerplate
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