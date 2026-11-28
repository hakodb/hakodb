//! Cached plans must be rebound to the live range bound on every page.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::order::OrderBy;
use hakodb::query::query::Query;
use std::time::Duration;

fn dir() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("hako-cursor-rebind-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn keyset_pages_rebind_cached_plan_bounds() {
    let d = dir();
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(&d, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..200usize {
        let mut doc = HakoDoc::default();
        doc.insert("key", Value::String(format!("k{i:03}")));
        doc.insert("score", Value::Int(i as i64));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("k{i:03}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db.create_index("records", "key").expect("key index");
    assert!(db.await_quiescent(Duration::from_secs(30)));

    let mut anchor = String::new();
    let mut total = 0usize;
    for _ in 0..4 {
        let mut q = Query::new("records");
        if !anchor.is_empty() {
            q.filters.push(Filter {
                field: "key".into(),
                op: Operator::Gt,
                value: Value::String(anchor.clone()),
            });
        }
        q.order_by.push(OrderBy { field: "key".into(), ascending: true });
        q.limit = Some(50);
        let page = db.query(q).expect("page");
        assert_eq!(page.len(), 50);
        anchor = page.last().expect("last").0.clone();
        total += page.len();
    }
    assert_eq!(total, 200);
    assert_eq!(anchor, "k199");
    let _ = std::fs::remove_dir_all(&d);
}
