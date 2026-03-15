use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;

use super::super::filter::{Filter, Operator};
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
            .map(|v| compare(v, &f.op, &f.value))
            .unwrap_or(false)
    })
}

fn compare(a: &Value, op: &Operator, b: &Value) -> bool {
    match (a, b) {
        (Value::Int(a), Value::Int(b)) => match op {
            Operator::Eq => a == b,
            Operator::Gt => a > b,
            Operator::Gte => a >= b,
            Operator::Lt => a < b,
            Operator::Lte => a <= b,
        },
        (Value::String(a), Value::String(b)) => match op {
            Operator::Eq => a == b,
            Operator::Gt => a > b,
            Operator::Gte => a >= b,
            Operator::Lt => a < b,
            Operator::Lte => a <= b,
        },
        _ => false,
    }
}
