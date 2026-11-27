//! Index-only id collection for `key IN [...]` patch shapes.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::query::Query;
use std::time::Duration;

fn dir() -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!("hako-patch-in-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn patch_by_in_uses_index_ids_and_updates_exact_rows() {
    let d = dir();
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(&d, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..100usize {
        let mut doc = HakoDoc::default();
        doc.insert("key", Value::String(format!("k{i:03}")));
        doc.insert("active", Value::Bool(true));
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

    let keys = vec![
        Value::String("k001".into()),
        Value::String("k017".into()),
        Value::String("k099".into()),
    ];
    let mut query = Query::new("records");
    query.filters.push(Filter {
        field: "key".into(),
        op: Operator::In,
        value: Value::Array(keys),
    });
    let updated = db.patch_where(query, vec![("active".into(), Value::Bool(false))]).expect("patch");
    assert_eq!(updated, 3);

    for key in ["k001", "k017", "k099"] {
        let doc = db.get("records", key).expect("get").expect("doc exists");
        assert_eq!(doc.get("active"), Some(&Value::Bool(false)), "{key} not patched");
    }
    let untouched = db.get("records", "k002").expect("get").expect("doc exists");
    assert_eq!(untouched.get("active"), Some(&Value::Bool(true)), "unrelated row changed");

    let _ = std::fs::remove_dir_all(&d);
}
