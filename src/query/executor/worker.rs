use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};

// use super::super::filter::compare_values;
use super::task::QueryTask;
use crate::document::value::Value;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    let projection = &task.plan.projection;
    for (id, mut bytes) in task.docs {
        if bytes.is_empty() {
            if let Some(storage_lock) = &task.storage {
                if let Ok(storage) = storage_lock.read() {
                    if let Ok(Some(data)) = storage.get(&id) {
                        bytes = data;
                    }
                }
            }
        }
        if bytes.is_empty() {
            continue;
        }

        if matches_filters_view(&bytes, &task.plan, Some(&task.catalog)) {
            let decoded = if projection.is_empty() {
                FireLiteDoc::decode(&bytes, Some(&task.catalog))
            } else {
                FireLiteDoc::decode_projected(&bytes, projection, Some(&task.catalog))
            };
            if let Some(doc) = decoded {
                out.push((id, doc));
            }
        }
    }
    out
}

/// Optimized projected worker: Extracts only requested fields without full doc decoding.
pub fn run_task_projected(task: QueryTask) -> Vec<(String, Vec<(String, Value)>)> {
    let mut out = Vec::new();
    let projection = &task.plan.projection;

    for (id, mut bytes) in task.docs {
        if bytes.is_empty() {
            if let Some(storage_lock) = &task.storage {
                if let Ok(storage) = storage_lock.read() {
                    if let Ok(Some(data)) = storage.get(&id) {
                        bytes = data;
                    }
                }
            }
        }
        if bytes.is_empty() {
            continue;
        }

        // FIX: Removed super::super::worker:: because the function is in this file
        if let Some(view) = FireLiteDocView::new(&bytes) {
            if matches_filters_view(&bytes, &task.plan, Some(&task.catalog)) {
                let mut fields_out = Vec::new();

                if projection.is_empty() {
                    if let Some(doc) = FireLiteDoc::decode(&bytes, Some(&task.catalog)) {
                        for (k, v) in doc.fields {
                            fields_out.push((k.to_string(), v)); // Convert Arc to String
                        }
                    }
                } else {
                    for field_name in projection {
                        if let Some(borrowed) =
                            view.get_field_value(field_name, Some(&task.catalog))
                        {
                            if let Some(val) = borrowed.to_owned_value() {
                                fields_out.push((field_name.clone(), val));
                            }
                        }
                    }
                }
                out.push((id, fields_out)); // <--- id and fields_out are now in scope
            }
        }
    }
    out
}

pub(crate) fn matches_filters_view(
    bytes: &[u8],
    plan: &crate::query::plan::QueryPlan,
    catalog: Option<&crate::util::catalog::Catalog>, // <--- ADD THIS
) -> bool {
    let Some(view) = FireLiteDocView::new(bytes) else {
        return false;
    };
    if plan.filters.is_empty() && plan.or_groups.is_empty() {
        return true;
    }

    let mut and_matches = vec![false; plan.filters.len()];
    let mut or_group_results = vec![false; plan.or_groups.len()];

    // CRITICAL FIX: One single loop over the fields
    for (key, val) in view.iter(catalog) {
        // 1. Check ANDs
        for (i, f) in plan.filters.iter().enumerate() {
            if !and_matches[i] && &*key == &f.field {
                if let Some(v) = val.to_owned_value() {
                    if crate::query::filter::compare_values(&v, &f.op, &f.value) {
                        and_matches[i] = true;
                    }
                }
            }
        }
        // 2. Check ORs
        for (gi, group) in plan.or_groups.iter().enumerate() {
            if or_group_results[gi] {
                continue;
            }
            for f in group {
                if &*key == &f.field {
                    if let Some(v) = val.to_owned_value() {
                        if crate::query::filter::compare_values(&v, &f.op, &f.value) {
                            or_group_results[gi] = true;
                        }
                    }
                }
            }
        }
    }

    let and_final = plan.filters.is_empty() || and_matches.iter().all(|&m| m);
    let or_final = plan.or_groups.is_empty() || or_group_results.iter().any(|&m| m);

    and_final && or_final
}

// fn check_single_filter(view: &FireLiteDocView, f: &Filter, catalog: Option<&crate::util::catalog::Catalog>) -> bool {
//     for (k, v) in view.iter(catalog) {
//         if &*k == f.field {
//             return v.to_owned_value()
//                 .map(|val| compare_values(&val, &f.op, &f.value))
//                 .unwrap_or(false);
//         }
//     }
//     false
// }
