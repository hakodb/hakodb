use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};
use crate::document::value::Value;
use super::task::QueryTask;
use crate::query::filter::Operator;
// use crate::engine::engine::resolve_doc_static;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    let blob_manager = {
        let guard = task.storage.as_ref().unwrap().read().unwrap();
        guard.blob_manager.clone()
    };
    let storage_engine = task.storage.as_ref().unwrap().read().unwrap();

    for (id, pointer) in task.docs {
        if let Ok(Some(bytes)) = storage_engine.read_pointer(&pointer) {
            if task.plan.filters_satisfied_by_index || matches_filters_view(&bytes, &task.plan) {
                if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
                    
                    if let Some(ref manager) = blob_manager {
                        let _ = inflate_blobs(&mut doc, manager);
                    }
                    
                    out.push((id, doc));
                }
            }
        }
    }
    out
}

fn inflate_blobs(doc: &mut FireLiteDoc, blob_manager: &crate::storage::blob::BlobManager) -> Result<(), crate::error::FireLiteError> {
    for (_, value) in &mut doc.fields {
        if let Value::BlobLink { offset, len } = *value {
            let data = blob_manager.read_at(offset, len)?;

            // Convert back to original type (Simple heuristic for the benchmark)
            if let Ok(s) = String::from_utf8(data.clone()) {
                *value = Value::String(s);
            } else {
                *value = Value::Binary(data);
            }
        }
    }
    Ok(())
}


pub fn run_task_projected(task: QueryTask) -> Vec<(String, Vec<(String, Value)>)> {
    let mut out = Vec::new();
    
    // 1. Pull the handles from the Shard (StorageEngine)
    // We do this once per task (worker thread)
    let blob_manager = {
        let guard = task.storage.as_ref().unwrap().read().unwrap();
        guard.blob_manager.clone()
    };

    let storage_engine = task.storage.as_ref().unwrap().read().unwrap();
    let projection = &task.plan.projection;

    for (id, pointer) in task.docs {
        if let Ok(Some(bytes)) = storage_engine.read_pointer(&pointer) {
            if task.plan.filters_satisfied_by_index || matches_filters_view(&bytes, &task.plan) {
                let mut fields_out = Vec::new();
                if let Some(view) = FireLiteDocView::new(&bytes) {
                    for field_name in projection {
                        if let Some((_, tag, data)) = view.iter().find(|(k, _, _)| k == field_name) {
                            if let Some(mut val) = crate::document::firelite_doc::decode_value(tag, data) {
                                
                                // PARALLEL BLOB RESOLUTION for Projected Fields
                                if let Value::BlobLink { offset, len } = val {
                                    if let Some(ref manager) = blob_manager {
                                        val = resolve_single_blob_in_worker(manager, offset, len);
                                    }
                                }
                                
                                fields_out.push((field_name.clone(), val));
                            }
                        }
                    }
                    out.push((id, fields_out));
                }
            }
        }
    }
    out
}

// Helper for projected resolution
fn resolve_single_blob_in_worker(
    blob_manager: &crate::storage::blob::BlobManager,
    offset: u64, 
    len: u32,
) -> Value {
    let data = blob_manager.read_at(offset, len).unwrap_or_default();

    if let Ok(s) = String::from_utf8(data.clone()) {
        Value::String(s)
    } else {
        Value::Binary(data)
    }
}

/// The high-performance core: Scans document bytes ONCE and performs 
/// comparisons without allocating memory for document values.
pub(crate) fn matches_filters_view(
    bytes: &[u8],
    plan: &crate::query::plan::QueryPlan,
) -> bool {
    let Some(view) = FireLiteDocView::new(bytes) else { return false; };
    if plan.filters.is_empty() && plan.or_groups.is_empty() { return true; }

    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    // CRITICAL PERFORMANCE FIX: Single linear pass over fields
    for (key, tag, data) in view.iter() {
        // 1. Check ANDs
        for (i, f) in plan.filters.iter().enumerate() {
            if !and_matches[i] && key == f.field {
                if compare_raw_bytes(tag, data, &f.op, &f.value) {
                    and_matches[i] = true;
                }
            }
        }
        // 2. Check OR groups
        for (gi, group) in plan.or_groups.iter().enumerate() {
            if or_group_results[gi] { continue; }
            for f in group {
                if key == f.field {
                    if compare_raw_bytes(tag, data, &f.op, &f.value) {
                        or_group_results[gi] = true;
                    }
                }
            }
        }
    }

    let and_final = plan.filters.is_empty() || and_matches.iter().all(|&m| m);
    let or_final = plan.or_groups.is_empty() || or_group_results.iter().any(|&m| m);

    and_final && or_final
}

/// Helper function to perform comparisons directly on byte slices.
/// This avoids the overhead of creating 'Value' enum instances for every field.
fn compare_raw_bytes(tag: u8, data: &[u8], op: &Operator, b: &Value) -> bool {
    match tag {
        3 => { // Int (stored as 8 bytes LE)
            if let Ok(val) = data.try_into().map(i64::from_le_bytes) {
                return crate::query::filter::compare_values(&Value::Int(val), op, b);
            }
        }
        4 => { // Float (stored as 8 bytes LE)
            if let Ok(bits) = data.try_into().map(f64::from_le_bytes) {
                return crate::query::filter::compare_values(&Value::Float(bits), op, b);
            }
        }
        5 => { // String (Zero-allocation check for Eq and StartsWith)
            if let Ok(s) = std::str::from_utf8(data) {
                if let Value::String(target) = b {
                    match op {
                        Operator::Eq => return s == target,
                        Operator::StartsWith => return s.starts_with(target),
                        _ => {} // Fall through for complex ops like Match/Contains
                    }
                }
                // Fallback for complex string logic
                return crate::query::filter::compare_values(&Value::String(s.to_string()), op, b);
            }
        }
        2 => { // Bool
            let val = data.first().map_or(false, |&v| v == 1);
            return crate::query::filter::compare_values(&Value::Bool(val), op, b);
        }
        _ => {
            // For complex types (Map/Array/Ref), decode the specific value
            if let Some(v) = crate::document::firelite_doc::decode_value(tag, data) {
                return crate::query::filter::compare_values(&v, op, b);
            }
        }
    }
    false
}
