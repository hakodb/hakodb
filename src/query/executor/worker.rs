use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};

use super::super::filter::{compare_values, Filter};
use super::task::QueryTask;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    let projection = &task.plan.projection; 
    for (id, bytes) in task.docs {
        if matches_filters_view(&bytes, &task.plan.filters) {
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

fn matches_filters_view(bytes: &[u8], filters: &[Filter]) -> bool {
    if filters.is_empty() {
        return true;
    }

    let Some(view) = FireLiteDocView::new(bytes) else {
        return false;
    };

    filters.iter().all(|f| {
        let mut matched = None;
        for (k, v) in view.iter() {
            if k == f.field {
                matched = v.to_owned_value();
                break;
            }
        }
        matched
            .as_ref()
            .map(|value| compare_values(value, &f.op, &f.value))
            .unwrap_or(false)
    })
}
