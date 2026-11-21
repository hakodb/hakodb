//! Projected FFI path must sort on fields outside the projection (issue #237).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::query::Query;

fn temp_dir() -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "hako-proj-order-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&path);
    path
}

#[test]
fn projected_query_sorts_on_unselected_field() {
    let dir = temp_dir();
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&dir, cfg).expect("open");

    let mut batch = Vec::new();
    for (id, name, age) in [("u1", "alice", 30), ("u2", "bob", 40)] {
        let mut doc = HakoDoc::default();
        doc.insert("name", Value::String(name.into()));
        doc.insert("age", Value::Int(age));
        doc.insert("secret", Value::String("hidden".into()));
        batch.push(BatchMutation::Put {
            collection: "users".into(),
            doc_id: id.into(),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");

    let query = Query::new("users")
        .order_by("age", false)
        .select("name")
        .limit(10);
    let rows = db
        .query_projected_zero_copy(query, &["name".into()])
        .expect("projected query");

    assert_eq!(rows.len(), 2);
    assert_eq!(rows[0].0, "u2", "older user must rank first");
    let fields = &rows[0].1;
    assert!(fields.iter().any(|(k, v)| k == "name" && v == &Value::String("bob".into())));
    assert!(!fields.iter().any(|(k, _)| k == "age"), "sort field must be pruned");
    assert!(!fields.iter().any(|(k, _)| k == "secret"), "non-selected field must be pruned");
}

#[test]
fn projected_query_uses_topn_for_large_candidate_set() {
    let dir = temp_dir();
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&dir, cfg).expect("open");

    let mut batch = Vec::new();
    for i in 0..300 {
        let mut doc = HakoDoc::default();
        doc.insert("name", Value::String(format!("n{i:03}")));
        doc.insert("score", Value::Int(i));
        doc.insert("secret", Value::String("hidden".into()));
        batch.push(BatchMutation::Put {
            collection: "scores".into(),
            doc_id: format!("d{i:03}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");

    let query = Query::new("scores")
        .order_by("score", false)
        .select("name")
        .limit(5);
    let rows = db
        .query_projected_zero_copy(query, &["name".into()])
        .expect("projected topn query");

    assert_eq!(rows.len(), 5);
    for (rank, (id, fields)) in rows.iter().enumerate() {
        let expected = 299 - rank;
        assert_eq!(id, &format!("d{expected:03}"), "wrong top-N rank {rank}");
        assert!(fields.iter().any(|(k, v)| k == "name" && v == &Value::String(format!("n{expected:03}"))));
        assert!(!fields.iter().any(|(k, _)| k == "score" || k == "secret"));
    }
}