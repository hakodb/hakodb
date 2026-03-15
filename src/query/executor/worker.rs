use crate::document::firelite_doc::FireLiteDoc;

use super::super::filter::{compare_values, Filter};
use super::task::QueryTask;

pub fn run_task(task: QueryTask) -> Vec<(String, FireLiteDoc)> {
    let mut out = Vec::new();
    for (id, bytes) in task.docs {
        if let Some(doc) = FireLiteDoc::decode(&bytes) {
            if matches_filters(&doc, &task.plan.filters) {
                out.push((id, doc));
            }
        }
    }
    out
}

fn matches_filters(doc: &FireLiteDoc, filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        doc.fields
            .get(&f.field)
            .map(|v| compare_values(v, &f.op, &f.value))
            .unwrap_or(false)
    })
}
