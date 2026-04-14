// use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};
// use crate::document::value::Value;
// use super::task::QueryTask;
// use crate::query::filter::Operator;


// pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
//     let mut out = Vec::new();
//     let storage_guard = task.storage.as_ref().unwrap().read().unwrap();
//     let blob_manager = storage_guard.blob_manager.as_ref();

//     for (id, pointer) in task.docs {
//         if let Ok(Some(bytes)) = storage_guard.read_pointer(&pointer) {
//             // 1. Zero-allocation filter check
//             // if task.plan.filters_satisfied_by_index || matches_filters_view(&id, &bytes, &task.plan) {
//             //     if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
                    
//             //         // 2. Call the helper function (This removes the warning)
//             //         if let Some(bm) = blob_manager {
//             //             // We ignore errors here so a single bad blob doesn't crash the whole query
//             //             let _ = inflate_blobs(&mut doc, bm);
//             //         }
                    
//             //         out.push((id, doc));
//             //     }
//             // }
//             if let Some(mut doc) = unified_match_decode(&id, &bytes, &task.plan) {
//                 if let Some(bm) = blob_manager {
//                     let _ = inflate_blobs(&mut doc, bm);
//                 }
//                 out.push((id, doc));
//             }
//         }
//     }
//     out
// }

// fn inflate_blobs(doc: &mut FireLiteDoc, blob_manager: &crate::storage::blob::BlobManager) -> Result<(), crate::error::FireLiteError> {
//     // Collect references to avoid multiple scans of doc.fields
//     let links: Vec<&mut Value> = doc.fields.iter_mut()
//         .map(|(_, v)| v)
//         .filter(|v| matches!(v, Value::BlobLink { .. }))
//         .collect();

//     if links.is_empty() { return Ok(()); }

//     // Sequential because we are already inside a Rayon thread for the Query Task
//     for val in links {
//         if let Value::BlobLink { offset, len } = *val {
//             let data = blob_manager.read_at(offset, len)?;
//             *val = match String::from_utf8(data) {
//                 Ok(s) => Value::String(s),
//                 Err(e) => Value::Binary(e.into_bytes()),
//             };
//         }
//     }
//     Ok(())
// }


// pub fn run_task_projected(task: QueryTask) -> Vec<(String, Vec<(String, Value)>)> {
//     let mut out = Vec::new();
    
//     // 1. Pull the handles from the Shard (StorageEngine)
//     // We do this once per task (worker thread)
//     let blob_manager = {
//         let guard = task.storage.as_ref().unwrap().read().unwrap();
//         guard.blob_manager.clone()
//     };

//     let storage_engine = task.storage.as_ref().unwrap().read().unwrap();
//     let projection = &task.plan.projection;

//     for (id, pointer) in task.docs {
//         if let Ok(Some(bytes)) = storage_engine.read_pointer(&pointer) {
//             if task.plan.filters_satisfied_by_index || matches_filters_view(&id, &bytes, &task.plan) {
//                 let mut fields_out = Vec::new();
//                 if let Some(view) = FireLiteDocView::new(&bytes) {
//                     for field_name in projection {
//                         if let Some((_, tag, data)) = view.iter().find(|(k, _, _)| k == field_name) {
//                             if let Some(mut val) = crate::document::firelite_doc::decode_value(tag, data) {
                                
//                                 // PARALLEL BLOB RESOLUTION for Projected Fields
//                                 if let Value::BlobLink { offset, len } = val {
//                                     if let Some(ref manager) = blob_manager {
//                                         val = resolve_single_blob_in_worker(manager, offset, len);
//                                     }
//                                 }
                                
//                                 fields_out.push((field_name.clone(), val));
//                             }
//                         }
//                     }
//                     out.push((id, fields_out));
//                 }
//             }
//         }
//     }
//     out
// }

// // Helper for projected resolution
// fn resolve_single_blob_in_worker(
//     blob_manager: &crate::storage::blob::BlobManager,
//     offset: u64, 
//     len: u32,
// ) -> Value {
//     // If read fails, return Null rather than panicking the worker
//     let Ok(data) = blob_manager.read_at(offset, len) else {
//         return Value::Null;
//     };

//     match String::from_utf8(data) {
//         Ok(s) => Value::String(s),
//         Err(e) => Value::Binary(e.into_bytes()),
//     }
// }

// /// The high-performance core: Scans document bytes ONCE and performs 
// /// comparisons without allocating memory for document values.
// pub(crate) fn matches_filters_view(
//     doc_id :&str,
//     bytes: &[u8],
//     plan: &crate::query::plan::QueryPlan,
// ) -> bool {
//     let doc_time = i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]));
//     if plan.filters.is_empty() && plan.or_groups.is_empty() { return true; }
    
//     let mut and_matches = vec![false; plan.filters.len()];
//     let mut or_group_results = vec![false; plan.or_groups.len()];

//     for (i, f) in plan.filters.iter().enumerate() {
//         if f.field == "id" {
//             // FLEXIBLE ID CHECK:
//             // If the user provided a string, compare directly.
//             // If the user provided a number, stringify it first.
//             let is_match = match &f.value {
//                 Value::String(s) => doc_id == s,
//                 Value::Int(i) => doc_id == i.to_string(),
//                 Value::Float(f_val) => doc_id == f_val.to_string(),
//                 _ => false,
//             };

//             // Use a custom evaluator for the operator since we 
//             // are handling the comparison manually here.
//             if !evaluate_virtual_op(is_match, doc_id, &f.op, &f.value) {
//                 return false; 
//             }
//         } else if f.field == "_time" {
//             // Compare i64 directly
//             if crate::query::filter::compare_values(&Value::Int(doc_time), &f.op, &f.value) {
//                 and_matches[i] = true;
//             }
//         }
//     }
    
//     let Some(view) = FireLiteDocView::new(bytes) else { return false; };
//     // CRITICAL PERFORMANCE FIX: Single linear pass over fields
//     for (key, tag, data) in view.iter() {
//         // 1. Check ANDs
//         for (i, f) in plan.filters.iter().enumerate() {
//             if !and_matches[i] && key == f.field {
//                 if compare_raw_bytes(tag, data, &f.op, &f.value) {
//                     and_matches[i] = true;
//                 }
//             }
//         }
//         // 2. Check OR groups
//         for (gi, group) in plan.or_groups.iter().enumerate() {
//             if or_group_results[gi] { continue; }
//             for f in group {
//                 if key == f.field {
//                     if compare_raw_bytes(tag, data, &f.op, &f.value) {
//                         or_group_results[gi] = true;
//                     }
//                 }
//             }
//         }
//     }

//     let and_final = plan.filters.is_empty() || and_matches.iter().all(|&m| m);
//     let or_final = plan.or_groups.is_empty() || or_group_results.iter().any(|&m| m);

//     and_final && or_final
// }


// fn unified_match_decode(doc_id: &str, bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> Option<FireLiteDoc> {
//     let view = FireLiteDocView::new(bytes)?;
    
//     // 1. Virtual Field Check (id and _time)
//     // We check these first because they require zero field scanning.
//     let mut and_matches = vec![false; plan.filters.len()];
//     let mut or_group_results = vec![false; plan.or_groups.len()];

//     for (i, f) in plan.filters.iter().enumerate() {
//         if f.field == "id" {
//             // FLEXIBLE ID CHECK:
//             // If the user provided a string, compare directly.
//             // If the user provided a number, stringify it first.
//             let is_match = match &f.value {
//                 Value::String(s) => doc_id == s,
//                 Value::Int(i) => doc_id == i.to_string(),
//                 Value::Float(f_val) => doc_id == f_val.to_string(),
//                 _ => false,
//             };

//             // Use a custom evaluator for the operator since we 
//             // are handling the comparison manually here.
//             if !evaluate_virtual_op(is_match, doc_id, &f.op, &f.value) {
//                 return false; 
//             }
//         } else if f.field == "_time" {
//             if crate::query::filter::compare_values(&Value::Int(view._time), &f.op, &f.value) {
//                 and_matches[i] = true;
//             }
//         }
//     }

//     // 2. Body Field Scanning + Eager Decoding
//     let mut fields = Vec::with_capacity(view.iter().count()); 
    
//     for (key, tag, data) in view.iter() {
//         // Decode the value once
//         let val = crate::document::firelite_doc::decode_value(tag, data)?;

//         // Update AND matches
//         for (i, f) in plan.filters.iter().enumerate() {
//             if !and_matches[i] && key == f.field {
//                 if crate::query::filter::compare_values(&val, &f.op, &f.value) {
//                     and_matches[i] = true;
//                 }
//             }
//         }

//         // Update OR matches
//         for (gi, group) in plan.or_groups.iter().enumerate() {
//             if or_group_results[gi] { continue; }
//             for f in group {
//                 if key == f.field {
//                     if crate::query::filter::compare_values(&val, &f.op, &f.value) {
//                         or_group_results[gi] = true;
//                     }
//                 }
//             }
//         }

//         // Add to our document structure while we have it
//         fields.push((std::sync::Arc::from(key), val));
//     }

//     // 3. Final Validation
//     let and_final = plan.filters.is_empty() || and_matches.iter().all(|&m| m);
//     let or_final = plan.or_groups.is_empty() || or_group_results.iter().any(|&m| m);

//     if and_final && or_final {
//         Some(FireLiteDoc { fields, _time: view._time })
//     } else {
//         None
//     }
// }

// fn evaluate_virtual_op(is_eq: bool, doc_id: &str, op: &Operator, filter_val: &Value) -> bool {
//     match op {
//         Operator::Eq => is_eq,
//         Operator::Ne => !is_eq,
//         Operator::StartsWith => {
//             if let Value::String(s) = filter_val { doc_id.starts_with(s) } 
//             else if let Value::Int(i) = filter_val { doc_id.starts_with(&i.to_string()) }
//             else { false }
//         },
//         // 'match' and 'contains' are handled by standard string logic later
//         _ => crate::query::filter::compare_values(&Value::String(doc_id.to_string()), op, filter_val),
//     }
// }

// /// Helper function to perform comparisons directly on byte slices.
// /// This avoids the overhead of creating 'Value' enum instances for every field.
// fn compare_raw_bytes(tag: u8, data: &[u8], op: &Operator, b: &Value) -> bool {
//     match tag {
//         3 => { // Int (stored as 8 bytes LE)
//             if let Ok(val) = data.try_into().map(i64::from_le_bytes) {
//                 return crate::query::filter::compare_values(&Value::Int(val), op, b);
//             }
//         }
//         4 => { // Float (stored as 8 bytes LE)
//             if let Ok(bits) = data.try_into().map(f64::from_le_bytes) {
//                 return crate::query::filter::compare_values(&Value::Float(bits), op, b);
//             }
//         }
//         5 => { // String (Zero-allocation check for Eq and StartsWith)
//             if let Ok(s) = std::str::from_utf8(data) {
//                 if let Value::String(target) = b {
//                     match op {
//                         Operator::Eq => return s == target,
//                         Operator::StartsWith => return s.starts_with(target),
//                         _ => {} // Fall through for complex ops like Match/Contains
//                     }
//                 }
//                 // Fallback for complex string logic
//                 return crate::query::filter::compare_values(&Value::String(s.to_string()), op, b);
//             }
//         }
//         2 => { // Bool
//             let val = data.first().map_or(false, |&v| v == 1);
//             return crate::query::filter::compare_values(&Value::Bool(val), op, b);
//         }
//         _ => {
//             // For complex types (Map/Array/Ref), decode the specific value
//             if let Some(v) = crate::document::firelite_doc::decode_value(tag, data) {
//                 return crate::query::filter::compare_values(&v, op, b);
//             }
//         }
//     }
//     false
// }

use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};
use crate::document::value::Value;
use super::task::QueryTask;
use crate::query::filter::Operator;
use std::sync::Arc;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    let storage_guard = task.storage.as_ref().unwrap().read().unwrap();
    let blob_manager = storage_guard.blob_manager.as_ref();

    // OPTIMIZATION: Pre-stringify any numeric ID filters once per task
    let optimized_plan = prepare_optimized_plan(&task.plan);

    for (id, pointer) in task.docs {
        if let Ok(Some(bytes)) = storage_guard.read_pointer(&pointer) {
            // HIGH PERFORMANCE: Single-pass filtering and full decoding
            if let Some(mut doc) = unified_match_decode(&id, &bytes, &optimized_plan) {
                if let Some(bm) = blob_manager {
                    let _ = inflate_blobs(&mut doc, bm);
                }
                out.push((id, doc));
            }
        }
    }
    out
}

pub fn run_task_projected(task: QueryTask) -> Vec<(String, Vec<(String, Value)>)> {
    let mut out = Vec::new();
    let storage_guard = task.storage.as_ref().unwrap().read().unwrap();
    let blob_manager = storage_guard.blob_manager.clone();

    let optimized_plan = prepare_optimized_plan(&task.plan);

    for (id, pointer) in task.docs {
        if let Ok(Some(bytes)) = storage_guard.read_pointer(&pointer) {
            // HIGH PERFORMANCE: Single-pass filtering and partial decoding
            if let Some(fields) = unified_match_projected(&id, &bytes, &optimized_plan) {
                let mut finalized_fields = fields;
                
                if let Some(ref manager) = blob_manager {
                    for (_, val) in finalized_fields.iter_mut() {
                        if let Value::BlobLink { offset, len } = *val {
                            *val = resolve_single_blob_in_worker(manager, offset, len);
                        }
                    }
                }
                out.push((id, finalized_fields));
            }
        }
    }
    out
}

/// Pre-converts numeric 'id' filters into strings to avoid allocations in the document loop.
fn prepare_optimized_plan(plan: &crate::query::plan::QueryPlan) -> crate::query::plan::QueryPlan {
    let mut p = plan.clone();
    let stringify_id = |f: &mut crate::query::filter::Filter| {
        if f.field == "id" {
            match f.value {
                Value::Int(i) => f.value = Value::String(i.to_string()),
                Value::Float(fv) => f.value = Value::String(fv.to_string()),
                _ => {}
            }
        }
    };

    for f in &mut p.filters { stringify_id(f); }
    for group in &mut p.or_groups {
        for f in group { stringify_id(f); }
    }
    p
}

/// Unified decoder for full documents. Combines filter checking with object construction.
fn unified_match_decode(doc_id: &str, bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> Option<FireLiteDoc> {
    let view = FireLiteDocView::new(bytes)?;
    
    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    // 1. Metadata check (id and _time)
    if !check_metadata_filters(doc_id, view._time, plan, &mut and_matches, &mut or_group_results) {
        return None; 
    }

    // 2. Body Field Scanning (Decodes every field for the final object)
    let mut fields = Vec::with_capacity(view.iter().count()); 
    for (key, tag, data) in view.iter() {
        let val = crate::document::firelite_doc::decode_value(tag, data)?;

        apply_filter_logic(key, &val, plan, &mut and_matches, &mut or_group_results);
        fields.push((Arc::from(key), val));
    }

    if validate_final_match(plan, &and_matches, &or_group_results) {
        Some(FireLiteDoc { fields, _time: view._time })
    } else {
        None
    }
}

/// Unified decoder for projected queries. Only decodes fields needed for filters or results.
fn unified_match_projected(doc_id: &str, bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> Option<Vec<(String, Value)>> {
    let view = FireLiteDocView::new(bytes)?;
    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    if !check_metadata_filters(doc_id, view._time, plan, &mut and_matches, &mut or_group_results) {
        return None;
    }

    let mut extracted = Vec::with_capacity(plan.projection.len());
    for (key, tag, data) in view.iter() {
        let is_needed_for_filter = plan.filters.iter().any(|f| f.field == key) || 
                                   plan.or_groups.iter().any(|g| g.iter().any(|f| f.field == key));
        let is_needed_for_proj = plan.projection.contains(&key.to_string());

        if is_needed_for_filter || is_needed_for_proj {
            let val = crate::document::firelite_doc::decode_value(tag, data)?;
            apply_filter_logic(key, &val, plan, &mut and_matches, &mut or_group_results);
            if is_needed_for_proj {
                extracted.push((key.to_string(), val));
            }
        }
    }

    if validate_final_match(plan, &and_matches, &or_group_results) {
        Some(extracted)
    } else {
        None
    }
}

/// Internal helper to check ID and Timestamp without scanning document body.
fn check_metadata_filters(
    doc_id: &str, 
    doc_time: i64, 
    plan: &crate::query::plan::QueryPlan,
    and_matches: &mut [bool],
    or_group_results: &mut [bool],
) -> bool {
    // Check ANDs
    for (i, f) in plan.filters.iter().enumerate() {
        if f.field == "id" {
            if matches_id_flexible(doc_id, &f.op, &f.value) { and_matches[i] = true; }
            else { return false; } // Early exit for AND failure
        } else if f.field == "_time" {
            if crate::query::filter::compare_values(&Value::Int(doc_time), &f.op, &f.value) { and_matches[i] = true; }
            else { return false; }
        }
    }
    // Check ORs
    for (gi, group) in plan.or_groups.iter().enumerate() {
        for f in group {
            if f.field == "id" && matches_id_flexible(doc_id, &f.op, &f.value) { or_group_results[gi] = true; }
            else if f.field == "_time" && crate::query::filter::compare_values(&Value::Int(doc_time), &f.op, &f.value) { or_group_results[gi] = true; }
        }
    }
    true
}

#[inline]
fn apply_filter_logic(key: &str, val: &Value, plan: &crate::query::plan::QueryPlan, and_matches: &mut [bool], or_group_results: &mut [bool]) {
    for (i, f) in plan.filters.iter().enumerate() {
        if !and_matches[i] && key == f.field {
            if crate::query::filter::compare_values(val, &f.op, &f.value) { and_matches[i] = true; }
        }
    }
    for (gi, group) in plan.or_groups.iter().enumerate() {
        if or_group_results[gi] { continue; }
        for f in group {
            if key == f.field && crate::query::filter::compare_values(val, &f.op, &f.value) {
                or_group_results[gi] = true;
            }
        }
    }
}

#[inline]
fn validate_final_match(plan: &crate::query::plan::QueryPlan, and_matches: &[bool], or_group_results: &[bool]) -> bool {
    let and_final = plan.filters.is_empty() || and_matches.iter().all(|&m| m);
    let or_final = plan.or_groups.is_empty() || or_group_results.iter().any(|&m| m);
    and_final && or_final
}

fn matches_id_flexible(doc_id: &str, op: &Operator, filter_val: &Value) -> bool {
    match filter_val {
        Value::String(s) => match op {
            Operator::Eq => doc_id == s,
            Operator::Ne => doc_id != s,
            Operator::StartsWith => doc_id.starts_with(s),
            _ => crate::query::filter::compare_values(&Value::String(doc_id.to_string()), op, filter_val),
        },
        _ => crate::query::filter::compare_values(&Value::String(doc_id.to_string()), op, filter_val),
    }
}

/// Remaining logic (inflate_blobs, matches_filters_view, etc.) kept for internal use...

fn inflate_blobs(doc: &mut FireLiteDoc, blob_manager: &crate::storage::blob::BlobManager) -> Result<(), crate::error::FireLiteError> {
    let links: Vec<&mut Value> = doc.fields.iter_mut()
        .map(|(_, v)| v)
        .filter(|v| matches!(v, Value::BlobLink { .. }))
        .collect();

    if links.is_empty() { return Ok(()); }
    for val in links {
        if let Value::BlobLink { offset, len } = *val {
            let data = blob_manager.read_at(offset, len)?;
            *val = match String::from_utf8(data) {
                Ok(s) => Value::String(s),
                Err(e) => Value::Binary(e.into_bytes()),
            };
        }
    }
    Ok(())
}

pub(crate) fn matches_filters_view(doc_id: &str, bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> bool {
    let doc_time = i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]));
    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    if !check_metadata_filters(doc_id, doc_time, plan, &mut and_matches, &mut or_group_results) {
        return false;
    }
    
    let Some(view) = FireLiteDocView::new(bytes) else { return false; };
    for (key, tag, data) in view.iter() {
        for (i, f) in plan.filters.iter().enumerate() {
            if !and_matches[i] && key == f.field {
                if compare_raw_bytes(tag, data, &f.op, &f.value) { and_matches[i] = true; }
            }
        }
        for (gi, group) in plan.or_groups.iter().enumerate() {
            if or_group_results[gi] { continue; }
            for f in group {
                if key == f.field && compare_raw_bytes(tag, data, &f.op, &f.value) {
                    or_group_results[gi] = true;
                }
            }
        }
    }
    validate_final_match(plan, &and_matches, &or_group_results)
}

fn compare_raw_bytes(tag: u8, data: &[u8], op: &Operator, b: &Value) -> bool {
    match tag {
        3 => data.try_into().map(i64::from_le_bytes).map(|v| crate::query::filter::compare_values(&Value::Int(v), op, b)).unwrap_or(false),
        4 => data.try_into().map(f64::from_le_bytes).map(|v| crate::query::filter::compare_values(&Value::Float(v), op, b)).unwrap_or(false),
        5 => if let Ok(s) = std::str::from_utf8(data) {
            match (op, b) {
                (Operator::Eq, Value::String(t)) => s == t,
                (Operator::StartsWith, Value::String(t)) => s.starts_with(t),
                _ => crate::query::filter::compare_values(&Value::String(s.to_string()), op, b),
            }
        } else { false },
        2 => crate::query::filter::compare_values(&Value::Bool(data.first().map_or(false, |&v| v == 1)), op, b),
        _ => crate::document::firelite_doc::decode_value(tag, data).map(|v| crate::query::filter::compare_values(&v, op, b)).unwrap_or(false),
    }
}

fn resolve_single_blob_in_worker(
    blob_manager: &crate::storage::blob::BlobManager,
    offset: u64, 
    len: u32,
) -> Value {
    // If read fails, return Null rather than panicking the worker
    let Ok(data) = blob_manager.read_at(offset, len) else {
        return Value::Null;
    };

    match String::from_utf8(data) {
        Ok(s) => Value::String(s),
        Err(e) => Value::Binary(e.into_bytes()),
    }
}