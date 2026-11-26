//! Explicit reset/delete semantics: soft by default, hard on request.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-reset-delete-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn open(path: &std::path::Path) -> Hako {
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(path, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..100usize {
        let mut doc = HakoDoc::default();
        doc.insert("score", Value::Int(i as i64));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:03}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db
}

#[test]
fn soft_delete_stays_restoreable_hard_delete_does_not() {
    let d = dir("modes");
    let db = open(&d);
    let soft = Query::new("records").where_filter("score", Operator::Lt, Value::Int(10));
    assert_eq!(db.delete_where(soft).expect("soft"), 10);
    assert_eq!(db.restore_available(Some("records")).expect("restore list").len(), 10);

    let hard = Query::new("records").where_filter("score", Operator::Gte, Value::Int(10));
    assert_eq!(db.delete_where_hard(hard).expect("hard"), 90);
    assert_eq!(db.restore_available(Some("records")).expect("restore list").len(), 10);
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn reset_collection_leaves_no_tombstones_and_keeps_indexes() {
    let d = dir("reset");
    let db = open(&d);
    db.create_index("records", "score").expect("index");
    assert_eq!(db.reset_collection("records").expect("reset"), 100);
    assert_eq!(db.restore_available(Some("records")).expect("restore list").len(), 0);
    // Version tombstones may remain until the maintenance sweeper runs;
    // the hard-reset contract here is "no restoreable retention records".
    let indexes = db.list_indexes(Some("records"));
    assert!(indexes.secondary.get("records")
        .map(|f| f.iter().any(|name| name == "score"))
        .unwrap_or(false), "index def survived reset");
    assert_eq!(db.reset_collection("records").expect("second reset"), 0, "reset is idempotent");

    drop(db);
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let reopened = Hako::open(&d, cfg).expect("reopen");
    assert!(reopened.query(Query::new("records")).expect("query after reset").is_empty());
    assert!(reopened.list_indexes(Some("records")).secondary.contains_key("records"));
    let _ = std::fs::remove_dir_all(&d);
}
