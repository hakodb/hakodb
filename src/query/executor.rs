use crate::query::plan::{QueryPlan, ScanType};
use crate::document::firelite_doc::{FireLiteDoc, Value};
use crate::storage::engine::FireLiteEngine;
use crate::index::manager::IndexManager;

pub struct QueryExecutor;

impl QueryExecutor {

    pub fn execute(
        engine: &mut FireLiteEngine,
        indexes: &IndexManager,
        plan: QueryPlan
    ) -> Vec<(String, FireLiteDoc)> {

        match plan.scan {

            ScanType::CollectionScan => {

                Self::collection_scan(engine, plan)
            }

            ScanType::IndexScan { field } => {

                Self::index_scan(engine, indexes, plan, &field)
            }
        }
    }

    fn collection_scan(
        engine: &mut FireLiteEngine,
        plan: QueryPlan
    ) -> Vec<(String, FireLiteDoc)> {

        let mut results = Vec::new();

        for (doc_id, raw_doc) in
            engine.scan_collection(plan.collection_id)
        {

            if let Some(doc) = FireLiteDoc::decode(&raw_doc) {

                if Self::matches_filters(&doc, &plan.filters) {

                    results.push((doc_id, doc));
                }
            }

            if let Some(limit) = plan.limit {

                if results.len() >= limit {
                    break;
                }
            }
        }

        results
    }

    fn index_scan(
        engine: &mut FireLiteEngine,
        indexes: &IndexManager,
        plan: QueryPlan,
        field: &str
    ) -> Vec<(String, FireLiteDoc)> {

        let mut results = Vec::new();

        let index =
            indexes.get_index(plan.collection_id, field)
            .unwrap();

        let doc_ids =
            index.find_all();

        for doc_id in doc_ids {

            if let Some(raw) =
                engine.get_by_id(plan.collection_id, &doc_id)
            {

                if let Some(doc) = FireLiteDoc::decode(&raw) {

                    if Self::matches_filters(&doc, &plan.filters) {

                        results.push((doc_id.clone(), doc));
                    }
                }
            }

            if let Some(limit) = plan.limit {

                if results.len() >= limit {
                    break;
                }
            }
        }

        results
    }

    fn matches_filters(
        doc: &FireLiteDoc,
        filters: &Vec<crate::query::query::Filter>
    ) -> bool {

        for f in filters {

            let val = match doc.fields.get(&f.field) {
                Some(v) => v,
                None => return false
            };

            if !compare(val, &f.op, &f.value) {
                return false;
            }
        }

        true
    }
}

fn compare(
    a: &Value,
    op: &crate::query::query::Operator,
    b: &Value
) -> bool {

    match (a,b) {

        (Value::Int(a), Value::Int(b)) => {

            match op {

                crate::query::query::Operator::Eq => a == b,
                crate::query::query::Operator::Gt => a > b,
                crate::query::query::Operator::Gte => a >= b,
                crate::query::query::Operator::Lt => a < b,
                crate::query::query::Operator::Lte => a <= b,
            }
        }

        _ => false
    }
}
