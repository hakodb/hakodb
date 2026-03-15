use std::sync::Arc;
use std::thread;

use crate::document::firelite_doc::{FireLiteDoc, Value};
use crate::error::Result;
use crate::storage::engine::StorageEngine;

use super::plan::QueryPlan;
use super::query::{Filter, Operator};

pub struct ParallelQueryExecutor {
    workers: usize,
}

impl ParallelQueryExecutor {
    pub fn new(workers: usize) -> Self {
        Self {
            workers: workers.max(1),
        }
    }

    pub fn execute(
        &self,
        storage: &mut StorageEngine,
        plan: QueryPlan,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        let docs = storage.scan_prefix(&format!("{}:", plan.collection))?;
        let chunk_size = (docs.len() / self.workers).max(1);
        let filters = Arc::new(plan.filters);
        let mut handles = Vec::new();
        for chunk in docs.chunks(chunk_size) {
            let local = chunk.to_vec();
            let filters = filters.clone();
            handles.push(thread::spawn(move || {
                let mut out = Vec::new();
                for (key, bytes) in local {
                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                        if matches_filters(&doc, &filters) {
                            out.push((key, doc));
                        }
                    }
                }
                out
            }));
        }
        let mut out = Vec::new();
        for h in handles {
            out.extend(h.join().unwrap_or_default());
        }
        if let Some(limit) = plan.limit {
            out.truncate(limit);
        }
        Ok(out)
    }
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
