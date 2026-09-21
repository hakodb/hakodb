use crate::document::hako_doc::{HakoDoc, HakoDocView};
use crate::document::value::Value;
use super::task::QueryTask;
use crate::query::filter::Operator;
use std::cell::RefCell;

thread_local! {
    /// Scratch buffer for encoding filter values during byte-compare matching.
    /// Reused across rows so the fast match pass allocates nothing.
    static ENC_SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(256));
}

/// Value types with canonical byte encoding: same value <=> same bytes.
/// Float excluded (-0.0 == 0.0 but different bits); Map/Array excluded
/// (field order not canonical). Everything else memcmps exactly.
fn byte_safe(v: &Value) -> bool {
    !matches!(v, Value::Float(_) | Value::Map(_) | Value::Array(_))
}

pub fn run_task(task: QueryTask) -> Vec<(String, HakoDoc)> {
    let mut out = Vec::new();
    let storage_guard = task.storage.as_ref().unwrap().read().unwrap();
    let blob_manager = storage_guard.blob_manager.as_ref();

    let optimized_plan = prepare_optimized_plan(&task.plan);

    for (id, pointer) in task.docs {
        // ponytail: same borrowed-Inlined fast path as execute() — decode
        // borrows the Arc-shared buffer, zero copy per row. Skeletons only;
        // blob inflation happens once below, concurrently (see execute()).
        match pointer {
            crate::storage::engine::Pointer::Inlined(shared) => {
                decode_row(&id, &shared, &task.plan, &optimized_plan, &mut out);
            }
            other => {
                if let Ok(Some(bytes)) = storage_guard.read_pointer(&other) {
                    decode_row(&id, &bytes, &task.plan, &optimized_plan, &mut out);
                }
            }
        }
    }

    // ponytail: parallel blob inflation (same shape as execute(): skip when
    // deferred or link-free). run_task already runs on worker threads;
    // rayon nests via work-stealing, no oversubscription deadlock.
    if !task.plan.defer_blobs {
        if let Some(bm) = blob_manager {
            if out.iter().any(|(_, doc)| doc_has_links(doc)) {
                use rayon::prelude::*;
                out.par_iter_mut().for_each(|(_, doc)| {
                    let _ = inflate_blobs(doc, bm);
                });
            }
        }
    }
    out
}

/// Shared decode+push helper for run_task's borrowed/owned sources.
/// Inflation is the caller's job (once, concurrently — see above).
fn decode_row(
    id: &str,
    bytes: &[u8],
    plan: &crate::query::plan::QueryPlan,
    optimized_plan: &crate::query::plan::QueryPlan,
    out: &mut Vec<(String, HakoDoc)>,
) {
    // CRITICAL FIX: If index handled everything, bypass `unified_match_decode` completely.
    // Skeletons only — the caller inflates once, concurrently (see above).
    if plan.filters_satisfied_by_index {
        if let Some(doc) = HakoDoc::decode(bytes) {
            out.push((id.to_string(), doc));
        }
    } else {
        // Slower Path: Query contains filters that the index couldn't verify
        if let Some(doc) = unified_match_decode(id, bytes, optimized_plan) {
            out.push((id.to_string(), doc));
        }
    }
}

pub fn run_task_projected(task: QueryTask) -> Vec<(String, Vec<(String, Value)>)> {
    let mut out = Vec::new();
    let storage_guard = task.storage.as_ref().unwrap().read().unwrap();
    let blob_manager = storage_guard.blob_manager.clone();

    let optimized_plan = prepare_optimized_plan(&task.plan);

    for (id, pointer) in task.docs {
        if let Ok(Some(bytes)) = storage_guard.read_pointer(&pointer) {
            // HIGH PERFORMANCE: Single-pass filtering and partial decoding.
            if let Some(fields) = unified_match_projected(&id, &bytes, &optimized_plan) {
                out.push((id, fields));
            }
        }
    }

    // ponytail: parallel blob inflation over rows (same shape as run_task).
    // Link-free rows skip via one scan — no rayon dispatch when idle.
    if !task.plan.defer_blobs {
        if let Some(ref manager) = blob_manager {
            let has_links = out.iter().any(|(_, fields)| {
                fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }))
            });
            if has_links {
                use rayon::prelude::*;
                out.par_iter_mut().for_each(|(_, fields)| {
                    for (_, val) in fields.iter_mut() {
                        if let Value::BlobLink { offset, len } = *val {
                            *val = resolve_single_blob_in_worker(manager, offset, len);
                        }
                    }
                });
            }
        }
    }
    out
}

/// ponytail: fast BlobLink probe — lets callers skip the inflate pass
/// entirely for link-free rows (the common small-doc case) instead of
/// paying per-doc scan+Vec overhead inside `inflate_blobs`.
#[inline]
pub(crate) fn doc_has_links(doc: &HakoDoc) -> bool {
    doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }))
}

/// Pre-converts numeric 'id' filters into strings to avoid allocations in the document loop.
pub(crate) fn prepare_optimized_plan(plan: &crate::query::plan::QueryPlan) -> crate::query::plan::QueryPlan {
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
///
/// ponytail: two-pass shape. When every undecided filter is a byte-comparable
/// Eq/Ne on a body field (no OR groups), pass 1 memcmps the row's encoded
/// bytes against the filter's encoding — zero allocation — and only rows
/// that match pay for pass 2 (full decode). Non-matching rows previously paid
/// a full decode (~15 allocs) just to be discarded.
pub(crate) fn unified_match_decode(doc_id: &str, bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> Option<HakoDoc> {
    let view = HakoDocView::new(bytes)?;
    
    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    // 1. Metadata check (id and _time)
    if !check_metadata_filters(doc_id, view._time, plan, &mut and_matches, &mut or_group_results) {
        return None; 
    }

    // 2. Fast path eligibility: no ORs, every undecided filter byte-checkable.
    let mut fast = plan.or_groups.is_empty();
    if fast {
        for (i, f) in plan.filters.iter().enumerate() {
            if and_matches[i] || f.field == "id" || f.field == "_time" { continue; }
            if !(matches!(f.op, Operator::Eq | Operator::Ne) && byte_safe(&f.value)) {
                fast = false;
                break;
            }
        }
    }

    if fast {
        // PASS 1: match on encoded bytes, no decode, no allocation, no
        // UTF-8 validation (raw key bytes compared directly; rows this
        // accepts are validated by the owned body scan below, rejected
        // rows are discarded either way — same outcomes, less work).
        ENC_SCRATCH.with(|scratch| {
            let mut enc = scratch.borrow_mut();
            for (key, tag, data) in view.iter_raw() {
                for (i, f) in plan.filters.iter().enumerate() {
                    if and_matches[i] || key != f.field.as_bytes() { continue; }
                    enc.clear();
                    HakoDoc::encode_value_to(&f.value, &mut enc);
                    let hit = enc.first().copied() == Some(tag) && &enc[1..] == data;
                    if (hit && f.op == Operator::Eq) || (!hit && f.op == Operator::Ne) {
                        and_matches[i] = true;
                    }
                }
            }
        });
        // Missing field => stays false => rejected, same as the old path.
        if !validate_final_match(plan, &and_matches, &or_group_results) {
            return None;
        }
    }

    // 2/3. Body Field Scanning (Decodes every field for the final object).
    // Slow path reaches here directly; fast path only for proven winners.
    let mut fields = Vec::with_capacity(view.field_count()); 
    for (key, tag, data) in view.iter() {
        let val = crate::document::hako_doc::decode_value(tag, data)?;

        if !fast {
            apply_filter_logic(key, &val, plan, &mut and_matches, &mut or_group_results);
        }
        fields.push((crate::document::hako_doc::intern_field(key), val));
    }

    if fast || validate_final_match(plan, &and_matches, &or_group_results) {
        Some(HakoDoc { fields, _time: view._time })
    } else {
        None
    }
}

/// Unified decoder for projected queries. Only decodes fields needed for filters or results.
pub(crate) fn unified_match_projected(doc_id: &str, bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> Option<Vec<(String, Value)>> {
    let view = HakoDocView::new(bytes)?;
    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    if !check_metadata_filters(doc_id, view._time, plan, &mut and_matches, &mut or_group_results) {
        return None;
    }

    let mut extracted = Vec::with_capacity(plan.projection.len());
    for (key, tag, data) in view.iter() {
        let is_needed_for_filter = plan.filters.iter().any(|f| f.field == key) ||
                                   plan.or_groups.iter().any(|g| g.iter().any(|f| f.field == key));
        // ponytail: borrow-compare the projection list — the old
        // `projection.contains(&key.to_string())` allocated one String per
        // field per row just to compare.
        let is_needed_for_proj = plan.projection.iter().any(|p| p == key);

        if is_needed_for_filter || is_needed_for_proj {
            let val = crate::document::hako_doc::decode_value(tag, data)?;
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
    // Optimization: Create Value objects once per document check
    // let id_val = Value::String(doc_id.to_string());
    // let time_val = Value::Int(doc_time);

    // // 1. Check AND Filters
    // for (i, f) in plan.filters.iter().enumerate() {
    //     if f.field == "id" {
    //         if crate::query::filter::compare_values(&id_val, &f.op, &f.value) { 
    //             and_matches[i] = true; 
    //         } else { return false; } // Early exit
    //     } else if f.field == "_time" {
    //         if crate::query::filter::compare_values(&time_val, &f.op, &f.value) { 
    //             and_matches[i] = true; 
    //         } else { return false; }
    //     }
    // }

    // // 2. Check OR Groups
    // for (gi, group) in plan.or_groups.iter().enumerate() {
    //     if or_group_results[gi] { continue; }
    //     for f in group {
    //         if f.field == "id" {
    //             if crate::query::filter::compare_values(&id_val, &f.op, &f.value) { 
    //                 or_group_results[gi] = true; 
    //                 break;
    //             }
    //         } else if f.field == "_time" {
    //             if crate::query::filter::compare_values(&time_val, &f.op, &f.value) { 
    //                 or_group_results[gi] = true; 
    //                 break;
    //             }
    //         }
    //     }
    // }
    // true
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
            _ => compare_default_filter(doc_id, op, filter_val),
        },
        Value::Array(items) => {
            // Handing "IN" logic: where id in ["6", "9"]
            if matches!(op, Operator::In) {
                return items.iter().any(|v| {
                    if let Value::String(s) = v { s == doc_id } else { false }
                });
            } else if matches!(op, Operator::NotIn) {
                return !items.iter().any(|v| {
                    if let Value::String(s) = v { s == doc_id } else { false }
                });
            } else {
                return compare_default_filter(doc_id, op, filter_val)
            }
        },
        _ => compare_default_filter(doc_id, op, filter_val)
    }
}

fn compare_default_filter (doc_id: &str, op: &Operator, filter_val: &Value) -> bool {
    let id_val = Value::String(doc_id.to_string());
    crate::query::filter::compare_values(&id_val, op, filter_val)
}

/// Remaining logic (inflate_blobs, matches_filters_view, etc.) kept for internal use...

pub(crate) fn inflate_blobs(doc: &mut HakoDoc, blob_manager: &crate::storage::blob::BlobManager) -> Result<(), crate::error::HakoError> {
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
    
    let Some(view) = HakoDocView::new(bytes) else { return false; };
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

// fn compare_raw_bytes(tag: u8, data: &[u8], op: &Operator, b: &Value) -> bool {
//     // --- 1. Handle Super Tags (Zero-length headers) ---
//     if tag == 0xC0 { return crate::query::filter::compare_values(&Value::Null, op, b); }
//     if tag == 0xC2 { return crate::query::filter::compare_values(&Value::Bool(true), op, b); }
//     if tag == 0xC3 { return crate::query::filter::compare_values(&Value::Bool(false), op, b); }
//     if (tag & 0xF0) == 0x10 { 
//         return crate::query::filter::compare_values(&Value::Int((tag & 0x0F) as i64), op, b); 
//     }
//     if (tag & 0xF8) == 0x40 { 
//         let len = (tag & 0x07) as usize;
//         if let Ok(s) = std::str::from_utf8(&data[..len]) {
//             return crate::query::filter::compare_values(&Value::String(s.to_string()), op, b);
//         }
//         return false;
//     }

//     // --- 2. Handle Standard Tags (4-byte length prefix + Body) ---
//     if data.len() < 4 { return false; }
    
//     // The actual payload starts after the 4-byte length header
//     let body = &data[4..];

//     let physical_val = match tag {
//         3 | 7 => { // Int or Timestamp (Zigzag Varint)
//             let mut p = 0; 
//             if let Some(val) = crate::util::varint::decode_varint(body, &mut p) {
//                 let decoded = crate::util::varint::zigzag_decode(val);
//                 let val_obj = if tag == 3 { Value::Int(decoded) } else { Value::Timestamp(decoded) };
//                 return crate::query::filter::compare_values(&val_obj, op, b);
//             }
//         }
//         4 => { // Float (8 bytes LE)
//             if let Ok(bits) = body.try_into().map(f64::from_le_bytes) {
//                 return crate::query::filter::compare_values(&Value::Float(bits), op, b);
//             }
//         }
//         5 => { // String
//             // Fix: We must respect the length provided in the header for exact slices
//             let v_len = u32::from_le_bytes(data[0..4].try_into().unwrap()) as usize;
//             if let Some(s_bytes) = body.get(..v_len) {
//                 if let Ok(s) = std::str::from_utf8(s_bytes) {
//                     match (op, b) {
//                         (Operator::Eq, Value::String(t)) => return s == t,
//                         (Operator::StartsWith, Value::String(t)) => return s.starts_with(t),
//                         _ => return crate::query::filter::compare_values(&Value::String(s.to_string()), op, b),
//                     }
//                 }
//             }
//         }
//         _ => {
//             // Fallback for complex types (Map, Array, Reference)
//             if let Some(v) = crate::document::hako_doc::decode_value(tag, data) {
//                 return crate::query::filter::compare_values(&v, op, b);
//             }
//         }
//     }
//     if let Some(val) = physical_val {
//         // Use the flexible compare_values we just fixed
//         return crate::query::filter::compare_values(&val, op, query_val);
//     }
//     false
// }

fn compare_raw_bytes(tag: u8, data: &[u8], op: &Operator, b: &Value) -> bool {
    // --- 1. Handle Super Tags (Zero-length headers / Tags with inlined data) ---
    if tag == 0xC0 { return crate::query::filter::compare_values(&Value::Null, op, b); }
    if tag == 0xC2 { return crate::query::filter::compare_values(&Value::Bool(true), op, b); }
    if tag == 0xC3 { return crate::query::filter::compare_values(&Value::Bool(false), op, b); }
    
    // Tag 0x10 - 0x1F: Small Integers
    if (tag & 0xF0) == 0x10 { 
        return crate::query::filter::compare_values(&Value::Int((tag & 0x0F) as i64), op, b); 
    }
    
    // Tag 0x40 - 0x47: Small Strings
    if (tag & 0xF8) == 0x40 { 
        let len = (tag & 0x07) as usize;
        if let Ok(s) = std::str::from_utf8(&data[..len]) {
            return crate::query::filter::compare_values(&Value::String(s.to_string()), op, b);
        }
        return false;
    }

    // --- 2. Handle Standard Tags (4-byte length prefix + Body) ---
    if data.len() < 4 { return false; }
    
    // Payload starts after 4-byte header
    let body = &data[4..];

    let physical_val = match tag {
        3 | 7 => { // Int or Timestamp (Zigzag Varint)
            let mut p = 0; 
            crate::util::varint::decode_varint(body, &mut p).map(|val| {
                let decoded = crate::util::varint::zigzag_decode(val);
                if tag == 3 { Value::Int(decoded) } else { Value::Timestamp(decoded) }
            })
        }
        4 => { // Float (8 bytes LE)
            body.get(..8)
                .and_then(|slice| slice.try_into().ok())
                .map(|arr| Value::Float(f64::from_le_bytes(arr)))
        }
        5 => { // String
            let v_len = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0;4])) as usize;
            body.get(..v_len)
                .and_then(|s_bytes| std::str::from_utf8(s_bytes).ok())
                .map(|s| Value::String(s.to_string()))
        }
        6 => { // Binary
            let v_len = u32::from_le_bytes(data[0..4].try_into().unwrap_or([0;4])) as usize;
            body.get(..v_len).map(|bytes| Value::Binary(bytes.to_vec()))
        }
        _ => {
            // Fallback for complex types (Map, Array, Reference, BlobLink)
            crate::document::hako_doc::decode_value(tag, data)
        }
    };

    // If we successfully resolved a value from the disk, 
    // run it through the flexible comparison logic.
    if let Some(val) = physical_val {
        return crate::query::filter::compare_values(&val, op, b);
    }
    
    false
}

pub(crate) fn resolve_single_blob_in_worker(
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