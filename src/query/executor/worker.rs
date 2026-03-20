use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};

use super::super::filter::{compare_values, Filter};
use super::task::QueryTask;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    let projection = &task.plan.projection; 
    for (id, bytes) in task.docs {
        if matches_filters_view(&bytes, &task.plan) {
            // if let Some(doc) = FireLiteDoc::decode(&bytes) {
            //     out.push((id, doc));
            // }
            // OPTIMIZATION: Use decode_projected instead of decode
            if let Some(doc) = FireLiteDoc::decode_projected(&bytes, projection) {
                out.push((id, doc));
            }
        }
    }
    out
}

fn matches_filters_view(bytes: &[u8], plan: &crate::query::plan::QueryPlan) -> bool {
    let Some(view) = FireLiteDocView::new(bytes) else { return false; };

    // 1. Check main AND filters
    let and_match = plan.filters.iter().all(|f| {
        check_single_filter(&view, f)
    });

    if !and_match && !plan.filters.is_empty() {
        return false;
    }

    // 2. Check OR groups (If any group matches, the whole thing matches)
    if plan.or_groups.is_empty() {
        return and_match;
    }

    plan.or_groups.iter().any(|group: &Vec<crate::query::filter::Filter>| { // Added explicit type hint
        group.iter().all(|f| check_single_filter(&view, f))
    })
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
