//! Index-only id collection for `key IN [...]` mass delete shapes.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::query::Query;
use std::time::Duration;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("hako-delete-in-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn open(d: &std::path::Path) -> Hako {
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(d, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..100usize {
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
    db
}

fn in_query() -> Query {
    let mut q = Query::new("records");
    q.filters.push(Filter {
        field: "key".into(),
        op: Operator::In,
        value: Value::Array(vec![
            Value::String("k001".into()),
            Value::String("k017".into()),
            Value::String("k099".into()),
        ]),
    });
    q
}

#[test]
fn delete_in_soft_is_restoreable_and_scoped() {
    let d = dir("soft");
    let db = open(&d);
    assert_eq!(db.delete_where(in_query()).expect("soft delete"), 3);
    assert!(db.get("records", "k001").expect("get").is_none());
    assert!(db.get("records", "k002").expect("get").is_some());
    assert_eq!(db.restore_available(Some("records")).expect("restore list").len(), 3);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn delete_in_hard_leaves_no_restore_record() {
    let d = dir("hard");
    let db = open(&d);
    assert_eq!(db.delete_where_hard(in_query()).expect("hard delete"), 3);
    assert!(db.get("records", "k001").expect("get").is_none());
    assert!(db.get("records", "k002").expect("get").is_some());
    assert!(db.restore_available(Some("records")).expect("restore list").is_empty());
    let _ = std::fs::remove_dir_all(&d);
}
