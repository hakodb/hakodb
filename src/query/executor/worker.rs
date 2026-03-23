use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};

use super::super::filter::{compare_values, Filter};
use crate::document::value::Value;
use super::task::QueryTask;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    let projection = &task.plan.projection; 
    for (id, bytes) in task.docs {
        if matches_filters_view(&bytes, &task.plan) {

            // OPTIMIZATION: Use decode_projected instead of decode
            if let Some(doc) = FireLiteDoc::decode_projected(&bytes, projection) {
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

    for (id, bytes) in task.docs {
        let Some(view) = FireLiteDocView::new(&bytes) else { continue; };

        // FIX: Removed super::super::worker:: because the function is in this file
        if matches_filters_view(&bytes, &task.plan) {
            
            let mut fields = Vec::with_capacity(projection.len());
            
            if projection.is_empty() {
                if let Some(doc) = crate::document::firelite_doc::FireLiteDoc::decode(&bytes) {
                    fields = doc.fields;
                }
            } else {
                for field_name in projection {
                    if let Some(borrowed) = view.get_field_value(field_name) {
                        if let Some(val) = borrowed.to_owned_value() {
                            fields.push((field_name.clone(), val));
                        }
                    }
                }
            }
            out.push((id, fields));
        }
    }
    out
}

fn matches_filters_view(bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> bool {
    let Some(view) = FireLiteDocView::new(bytes) else { return false; };

    // 1. Check main AND filters (The Base Group)
    // If these match, we return true immediately (OR short-circuit)
    if !plan.filters.is_empty() {
        let and_match = plan.filters.iter().all(|f| check_single_filter(&view, f));
        if and_match {
            return true;
        }
    }

    // 2. Check OR groups
    // If any group matches, the whole document matches
    let or_match = plan.or_groups.iter().any(|group| {
        // Each group is an AND-block
        group.iter().all(|f| check_single_filter(&view, f))
    });

    if or_match {
        return true;
    }

    // 3. Fallback: If there are NO filters at all, it's a "Select All"
    if plan.filters.is_empty() && plan.or_groups.is_empty() {
        return true;
    }

    false
}

fn check_single_filter(view: &FireLiteDocView, f: &Filter) -> bool {
    for (k, v) in view.iter() {
        if k == f.field {
            return v.to_owned_value()
                .map(|val| compare_values(&val, &f.op, &f.value))
                .unwrap_or(false);
        }
    }
    false
}
