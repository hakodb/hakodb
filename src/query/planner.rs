use super::filter::Operator;
use super::plan::{QueryPlan, ScanType};
use super::query::Query;
use crate::document::value::Value;
use crate::index::composite::definition::SortDirection;
use crate::index::composite::range_builder::build_cursor_range;
use crate::index::composite::range_builder::{
    build_prefix_key, build_prefix_open_end, build_value_key, key_with_upper_sentinel,
};
use crate::index::manager::IndexManager;
use std::ops::Bound; // Required for range definitions

pub struct QueryPlanner;

impl QueryPlanner {
    pub fn plan(
        query: &Query,
        indexes: &IndexManager,
        collection_rows: usize,
        worker_count: usize,
        index_ready: bool,
    ) -> QueryPlan {
        if !index_ready {
            return Self::make_plan(query, ScanType::FullCollection, None, false, false);
        }

        let work_per_thread = collection_rows / worker_count.max(1);
        let use_index_heuristic = work_per_thread > 50;

        // 1. PRIORITY 1: Full-Text Search
        // SAFEGUARD: Skip FTS for 'id' and '_time' as they are never tokenized as text.
        for filter in &query.filters {
            if matches!(filter.op, Operator::Match | Operator::MatchPrefix) && filter.field != "_time" {
                if let Value::String(q_text) = &filter.value {
                    if indexes.fts.get(&query.collection).map_or(false, |m| m.contains_key(&filter.field)) {
                        return Self::make_plan(
                            query,
                            ScanType::InvertedIndex {
                                field: filter.field.clone(),
                                query: q_text.clone(),
                                prefix: filter.op == Operator::MatchPrefix,
                            },
                            query.limit,
                            false,
                            false,
                        );
                    }
                }
            }
        }

        // 2. PRIORITY 2: Composite Index (Now correctly handles id/_time via updated IndexManager)
        if let Some((eq_scan, satisfied, filters_done)) = Self::try_plan_composite_eq(query, indexes) {
            let safe_limit = if satisfied { query.limit } else { None };
            return Self::make_plan(
                query, 
                eq_scan, 
                safe_limit, 
                satisfied, 
                filters_done
            );
        }

        // 3. PRIORITY 3: Range / Cursor / Pure Pagination Detection
        if let Some(first_order) = query.order_by.first() {
            // PONYTAIL: special-case the `id` field. When the user orders by
            // `id` with no cursor bounds and no filters, the storage
            // sorted_keys vec is already sorted by id. We can slice it
            // directly (O(log N + limit)) instead of walking any index.
            // fl_engine_create_index("id") registers `id` as a composite
            // index with one field, so this short-circuit has to live
            // BEFORE the composite-index loop below or it never fires.
            if first_order.field == "id"
                && query.start_at.is_none() && query.start_after.is_none()
                && query.end_at.is_none() && query.end_before.is_none()
            {
                let order_satisfied = query.order_by.len() == 1;
                let filters_empty = query.filters.is_empty() && query.or_groups.is_empty();
                let safe_limit = if order_satisfied && filters_empty {
                    query.limit
                } else { None };
                return Self::make_plan(
                    query,
                    ScanType::SortedKeys { start_key: None },
                    safe_limit,
                    order_satisfied,
                    filters_empty,
                );
            }
            // Check Composite Indices for both Cursors AND pure OrderBy Pagination
            for idx in indexes.indexes_for_collection(&query.collection) {
                if !idx.definition.fields.is_empty() && idx.definition.fields[0].field == first_order.field {
                    let mut matches_all = true;
                    let mut reverse_scan = false;
                    for (i, q_order) in query.order_by.iter().enumerate() {
                        if let Some(idx_f) = idx.definition.fields.get(i) {
                            if idx_f.field != q_order.field { matches_all = false; break; }
                            if i == 0 && (idx_f.direction == SortDirection::Asc) != q_order.ascending {
                                reverse_scan = true;
                            }
                            if (idx_f.direction == SortDirection::Asc) != (q_order.ascending != reverse_scan) {
                                matches_all = false; break;
                            }
                        } else { matches_all = false; break; }
                    }

                    if matches_all {
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

                        let filters_empty = query.filters.is_empty() && query.or_groups.is_empty();
                        let safe_limit = if filters_empty {
                            query.limit.map(|l| l + query.offset.unwrap_or(0))
                        } else { None };

                        return Self::make_plan(
                            query, 
                            ScanType::CompositeIndexRange { index_id: idx.definition.id, ranges: vec![(start, end)], reverse: reverse_scan }, 
                            safe_limit, 
                            true, // order_satisfied
                            filters_empty
                        );
                    }
                }
            }

            // Secondary Index Range Fallback
            if let Some(sec_map) = indexes.secondary.get(&query.collection) {
                if sec_map.contains_key(&first_order.field) {
                    let start = match (&query.start_at, &query.start_after) {
                        (Some(v), _) if !v.is_empty() => Bound::Included(crate::index::index_key::encode_scalar(&v[0])),
                        (_, Some(v)) if !v.is_empty() => Bound::Excluded(crate::index::index_key::encode_scalar(&v[0])),
                        _ => Bound::Unbounded,
                    };
                    let end = match (&query.end_at, &query.end_before) {
                        (Some(v), _) if !v.is_empty() => Bound::Included(crate::index::index_key::encode_scalar(&v[0])),
                        (_, Some(v)) if !v.is_empty() => Bound::Excluded(crate::index::index_key::encode_scalar(&v[0])),
                        _ => Bound::Unbounded,
                    };

                    let order_satisfied = query.order_by.len() == 1;
                    let filters_empty = query.filters.is_empty() && query.or_groups.is_empty();
                    let safe_limit = if order_satisfied && filters_empty {
                        query.limit.map(|l| l + query.offset.unwrap_or(0))
                    } else { None };

                    return Self::make_plan(
                        query,
                        ScanType::SecondaryIndexRange { field: first_order.field.clone(), start, end, reverse: !first_order.ascending },
                        safe_limit,
                        order_satisfied,
                        filters_empty
                    );
                }
            }
        }

        // 4. PRIORITY 4: Composite Range (e.g. WHERE price > 100)
        if use_index_heuristic {
            if let Some(range_scan) = Self::try_plan_composite_range(query, indexes) {
                return Self::make_plan(
                    query,
                    range_scan,
                    None,
                    false,
                    false
                );
            }
        }

        // 5. PRIORITY 5: Union/OR/IN Logic
        if !query.or_groups.is_empty() || query.filters.iter().any(|f| matches!(f.op, Operator::In)) {
            if let Some(union_scan) = Self::try_plan_union(query, indexes) {
                return Self::make_plan(
                    query,
                    union_scan,
                    None,
                    false,
                    false
                );
            }
        }

        // 6. PRIORITY 6: Simple Secondary Index (Equality)
        if use_index_heuristic {
            for filter in &query.filters {
                if matches!(filter.op, Operator::Eq) {
                    if let Some(sec_map) = indexes.secondary.get(&query.collection) {
                        if sec_map.contains_key(&filter.field) {
                            let is_ambiguous = match &filter.value {
                                Value::String(s) => s.parse::<i64>().is_ok(),
                                _ => false
                            };

                            if !is_ambiguous {
                                let val_bytes = crate::index::index_key::encode_scalar(&filter.value);
                                
                                // CRITICAL FIX: Mark filters satisfied if this is the ONLY filter
                                let filters_satisfied = query.filters.len() == 1 && query.or_groups.is_empty();
                                let safe_limit = if filters_satisfied { 
                                    query.limit.map(|l| l + query.offset.unwrap_or(0)) 
                                } else { None };

                                return Self::make_plan(
                                    query, 
                                    ScanType::SecondaryIndex { field: filter.field.clone(), value: val_bytes }, 
                                    safe_limit, 
                                    false, 
                                    filters_satisfied
                                );
                            }
                        }
                    }
                }
            }
        }

        // 7. DEFAULT FALLBACK: Full Collection Scan
        // This is the "Safety Net". If no index was found for 'id:eq',
        // it lands here and the Worker checks the storage keys manually.
        let no_filters = query.filters.is_empty() && query.or_groups.is_empty();
        Self::make_plan(query, ScanType::FullCollection, None, false, no_filters)
    }

    /// Helper to build the plan object.
    /// Now takes 'order_satisfied' as an argument.
    fn make_plan(
        query: &Query,
        scan: ScanType,
        scan_limit: Option<usize>,
        order_satisfied: bool,
        filters_satisfied: bool,
    ) -> QueryPlan {
        // If there's no specific sort requested, then the index's natural order is acceptable
        let actual_order_satisfied = query.order_by.is_empty() || order_satisfied;

        QueryPlan {
            collection: query.collection.clone(),
            scan,
            filters: query.filters.clone(),
            or_groups: query.or_groups.clone(),
            order_by: query.order_by.clone(),
            limit: query.limit,
            scan_limit,
            offset: query.offset,
            projection: query.projection.clone(),
            order_by_satisfied: actual_order_satisfied,
            filters_satisfied_by_index: filters_satisfied,
        }
    }

    fn try_plan_union(query: &Query, indexes: &IndexManager) -> Option<ScanType> {
        let mut scans = Vec::new();
        for filter in &query.filters {
            if matches!(filter.op, Operator::In) {
                if let Value::Array(vals) = &filter.value {
                    let mut scans = Vec::new();
                    for v in vals {
                        // This ensures each item in the IN array checks for 
                        // Composite Indexes first, then Secondary Indexes.
                        if let Some(scan) = Self::get_single_filter_scan(
                            &query.collection, &filter.field, v, indexes, query
                        ) {
                            scans.push(scan);
                        } else {
                            // If one item can't hit an index, the whole IN might as well full scan
                            return None; 
                        }
                    }
                    return Some(ScanType::UnionIndex { scans });
                }
            }
        }
        if !query.or_groups.is_empty() {
            for group in &query.or_groups {
                if group.len() == 1 {
                    let f = &group[0];
                    scans.push(Self::get_single_filter_scan(
                        &query.collection,
                        &f.field,
                        &f.value,
                        indexes,
                        query
                    )?);
                } else {
                    // Complex OR groups (multiple filters in one OR) still fallback to Full Scan
                    return None;
                }
            }
            return Some(ScanType::UnionIndex { scans });
        }

        None
    }

    fn get_single_filter_scan(
        col: &str,
        field: &str,
        val: &Value,
        indexes: &IndexManager,
        _query: &Query,
    ) -> Option<ScanType> {
        let mut alternatives = Vec::new();
        alternatives.push(val.clone());

        // 1. Generate alternatives for number-like strings
        match val {
            Value::String(s) => {
                if let Ok(i) = s.parse::<i64>() { alternatives.push(Value::Int(i)); }
                if let Ok(f) = s.parse::<f64>() { alternatives.push(Value::Float(f)); }
            }
            Value::Int(i) => { alternatives.push(Value::String(i.to_string())); }
            _ => {}
        }

        let mut sub_scans = Vec::new();
        for alt_val in &alternatives {
            let mut found_for_this_type = false;
            // Look for Composite Index first
            for idx in indexes.indexes_for_collection(col) {
                if idx.definition.fields.first().map_or(false, |f| f.field == field) {
                    sub_scans.push(ScanType::CompositeIndex {
                        index_id: idx.definition.id,
                        fields: vec![field.to_string()],
                        values: vec![alt_val.clone()],
                        reverse: false,
                    });
                    found_for_this_type = true;
                    break; // Found an index for this specific alternative
                }
            }
            
            // If no composite scan was added for this alternative, check Secondary Index
            if !found_for_this_type { // Logic: if we didn't just add a composite scan
                if let Some(sec_map) = indexes.secondary.get(col) {
                    if sec_map.contains_key(field) {
                        sub_scans.push(ScanType::SecondaryIndex {
                            field: field.to_string(),
                            value: crate::index::index_key::encode_scalar(alt_val),
                        });
                    }
                }
            }
        }

        // Return a Union if multiple types are valid, a Single scan if only one, or None
        if sub_scans.is_empty() {
            None
        } else if sub_scans.len() == 1 {
            sub_scans.pop()
        } else {
            // This is the magic: It will check the index for String AND Int versions
            Some(ScanType::UnionIndex { scans: sub_scans })
        }
    }


    fn try_plan_composite_range(query: &Query, indexes: &IndexManager) -> Option<ScanType> {
        
        let is_point_lookup = |f: &crate::query::filter::Filter| {
            matches!(f.op, Operator::Eq) || 
            (matches!(f.op, Operator::In) && matches!(&f.value, Value::Array(a) if a.len() == 1))
        };

        if query.filters.is_empty() || !query.filters.iter().all(is_point_lookup) {
            return None;
        }
        
        for idx in indexes.indexes_for_collection(&query.collection) {
            let mut eq_prefix = Vec::new();
            let mut range_filter = None;
            let mut supported = true;

            let mut matched_fields = Vec::new();
            let mut matched_values = Vec::new();

            for idx_field in &idx.definition.fields {
                if let Some(filter) = query.filters.iter().find(|f| f.field == idx_field.field) {
                    matched_fields.push(idx_field.field.clone());
                    
                    // Extract value: if array of 1, take the first element
                    let v = match &filter.value {
                        Value::Array(a) if a.len() == 1 => a[0].clone(),
                        other => other.clone(),
                    };
                    matched_values.push(v);
                } else { break; }
            }

            for field in &idx.definition.fields {
                if let Some(filter) = query.filters.iter().find(|f| f.field == field.field) {
                    match filter.op {
                        Operator::Eq => eq_prefix.push(filter.value.clone()),
                        Operator::Gt
                        | Operator::Gte
                        | Operator::Lt
                        | Operator::Lte
                        | Operator::Ne => {
                            range_filter = Some((filter.op, filter.value.clone()));
                            break;
                        }
                        _ => {
                            supported = false;
                            break;
                        }
                    }
                } else {
                    break;
                }
            }

            if !supported || (eq_prefix.is_empty() && range_filter.is_none()) {
                continue;
            }

            let eq_prefix_key = build_prefix_key(&idx.definition, &eq_prefix);
            let eq_prefix_end = build_prefix_open_end(&eq_prefix_key);

            let ranges = match range_filter {
                None => vec![(
                    Bound::Included(eq_prefix_key),
                    Bound::Included(eq_prefix_end),
                )],
                Some((op, value)) => {
                    let value_key = build_value_key(&idx.definition, &eq_prefix, &value)?;
                    let value_key_end = key_with_upper_sentinel(&value_key);
                    match op {
                        Operator::Gt => vec![(
                            Bound::Excluded(value_key_end),
                            Bound::Included(eq_prefix_end),
                        )],
                        Operator::Gte => {
                            vec![(Bound::Included(value_key), Bound::Included(eq_prefix_end))]
                        }
                        Operator::Lt => {
                            vec![(Bound::Included(eq_prefix_key), Bound::Excluded(value_key))]
                        }
                        Operator::Lte => vec![(
                            Bound::Included(eq_prefix_key),
                            Bound::Included(value_key_end),
                        )],
                        Operator::Ne => vec![
                            (
                                Bound::Included(eq_prefix_key),
                                Bound::Excluded(value_key.clone()),
                            ),
                            (
                                Bound::Excluded(value_key_end),
                                Bound::Included(eq_prefix_end),
                            ),
                        ],
                        _ => return None,
                    }
                }
            };

            return Some(ScanType::CompositeIndexRange {
                index_id: idx.definition.id,
                ranges,
                reverse: false
            });
        }

        None
    }

    fn try_plan_composite_eq(query: &Query, indexes: &IndexManager) -> Option<(ScanType, bool, bool)> {
        let is_point_lookup = |f: &crate::query::filter::Filter| {
            matches!(f.op, Operator::Eq) || 
            (matches!(f.op, Operator::In) && matches!(&f.value, Value::Array(a) if a.len() == 1))
        };

        // We only use this optimization if ALL filters are Equality/Point-lookups
        if query.filters.is_empty() || !query.filters.iter().all(is_point_lookup) {
            return None;
        }

        // 1. Probing: Prepare alternate types for the FIRST filter field
        let first_filter = &query.filters[0];
        let mut alternatives = Vec::new();
        alternatives.push(first_filter.value.clone());
        match &first_filter.value {
            Value::String(s) => { if let Ok(i) = s.parse::<i64>() { alternatives.push(Value::Int(i)); } }
            Value::Int(i) => { alternatives.push(Value::String(i.to_string())); }
            _ => {}
        }

        let mut sub_scans = Vec::new();

        // 2. Try to find a matching index for each alternative type
        for alt_val in alternatives {
            for idx in indexes.indexes_for_collection(&query.collection) {
                let mut matched_fields = Vec::new();
                let mut matched_values = Vec::new();

                // Match query filters to index prefix
                for idx_field in &idx.definition.fields {
                    let val_opt = if matched_fields.is_empty() {
                        if idx_field.field == first_filter.field { Some(&alt_val) } else { None }
                    } else {
                        query.filters.iter().find(|f| f.field == idx_field.field).map(|f| &f.value)
                    };

                    if let Some(v) = val_opt {
                        matched_fields.push(idx_field.field.clone());
                        matched_values.push(match v {
                            Value::Array(a) if a.len() == 1 => a[0].clone(),
                            other => other.clone(),
                        });
                    } else { break; }
                }

                // If this index covers ALL our filters
                if matched_fields.len() == query.filters.len() {
                    let mut order_satisfied = false;
                    let mut reverse_scan = false;

                    // 3. Sorting Logic: Does the index cover the ORDER BY clause?
                    if !query.order_by.is_empty() {
                        let start_idx = matched_fields.len();
                        let mut matches_all_sorts = true;
                        
                        for (i, q_order) in query.order_by.iter().enumerate() {
                            if let Some(idx_f) = idx.definition.fields.get(start_idx + i) {
                                if idx_f.field != q_order.field {
                                    matches_all_sorts = false;
                                    break;
                                }
                                // Logic: If the first sort field direction is different from index, 
                                // we scan the B-Tree backwards (reverse_scan).
                                if i == 0 && (idx_f.direction == SortDirection::Asc) != q_order.ascending {
                                    reverse_scan = true;
                                }
                                // Subsequent fields must follow the same flip
                                if (idx_f.direction == SortDirection::Asc) != (q_order.ascending != reverse_scan) {
                                    matches_all_sorts = false;
                                    break;
                                }
                            } else {
                                matches_all_sorts = false;
                                break;
                            }
                        }
                        order_satisfied = matches_all_sorts;
                    }

                    sub_scans.push((
                        ScanType::CompositeIndex { 
                            index_id: idx.definition.id,
                            fields: matched_fields, 
                            values: matched_values, 
                            reverse: reverse_scan 
                        },
                        order_satisfied
                    ));
                    break; // Found the best index for this specific alternative
                }
            }
        }

        if sub_scans.is_empty() {
            None
        } else if sub_scans.len() == 1 {
            let (scan, ordered) = sub_scans.pop().unwrap();
            // Some((scan, ordered, true))
            let has_in_filter = query.filters.iter().any(|f| f.op == Operator::In);
            let filters_done = query.or_groups.is_empty() && !has_in_filter;
            
            Some((scan, ordered, filters_done))
        } else {
            // We hit multiple types (String and Int). 
            // We MUST sort in RAM because Union merges different ranges.
            let scans = sub_scans.into_iter().map(|(s, _)| s).collect();
            Some((ScanType::UnionIndex { scans }, false, false))
        }
    }
}

