//! hakodb#71 Release D: F7 `explain_report` + operational counters on the
//! existing write-stats hook, and F8 optional per-collection schema
//! validation. Plan-only explain is pinned against actual routing; schema
//! checks cover defaults, required/type violations, batch atomicity,
//! persistence across reopen and cleanup on drop.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::engine::write_stats_report;
use hakodb::engine::Hako;
use hakodb::error::ErrorKind;
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::query::Query;
use hakodb::schema::{CollectionSchema, FieldKind, FieldRule};

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71d-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn doc(status: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("status", Value::String(status.into()));
    d
}

#[test]
fn explain_reports_chosen_index_and_candidates() {
    let db = Hako::open(&tmp_dir("explain"), HakoConfig::default()).unwrap();
    db.create_index("orders", "status").unwrap();
    // Above the small-collection heuristic (planner scans small tables on
    // purpose) so the index actually routes.
    let mut batch = Vec::new();
    for i in 0..300 {
        let mut d = HakoDoc::default();
        d.insert("status", Value::String(if i % 2 == 0 { "paid" } else { "open" }.into()));
        batch.push(hakodb::engine::BatchMutation::Put {
            collection: "orders".into(),
            doc_id: format!("o{i:04}"),
            doc: d,
        });
    }
    db.write_batch(batch).unwrap();
    db.await_quiescent(std::time::Duration::from_secs(5));

    let q = Query::new("orders").where_eq("status", Value::String("paid".into()));
    let rep = db.explain_report(q).unwrap();
    assert_eq!(rep.scan, "secondary_index(status)");
    assert_eq!(rep.index.as_deref(), Some("secondary_index(status)"));
    assert_eq!(rep.collection_rows, 300);
    assert!(rep.available_indexes.iter().any(|l| l == "secondary_index(status)"));
    assert!(rep.to_string().contains("index: secondary_index(status)"));

    let unindexed = db
        .explain_report(Query::new("orders").where_eq("name", Value::String("x".into())))
        .unwrap();
    assert_eq!(unindexed.index, None, "no index on `name`");
    assert!(unindexed.available_indexes.iter().any(|l| l.contains("status")));
    assert!(unindexed.to_string().contains("[no index]"));
}

#[test]
fn counters_cover_unique_violations_and_tx_conflicts() {
    let db = Hako::open(&tmp_dir("counters"), HakoConfig::default()).unwrap();
    write_stats_report();

    db.create_composite_index_ex(
        "accounts",
        vec![("platform".into(), SortDirection::Asc)],
        true,
        false,
        None,
    )
    .unwrap();
    let mut a = HakoDoc::default();
    a.insert("platform", Value::String("ios".into()));
    db.put("accounts", "a1", &a).unwrap();
    assert_eq!(
        db.put("accounts", "a2", &a).unwrap_err().kind(),
        ErrorKind::UniqueViolation
    );

    let mut tx = hakodb::engine::SerializableTransaction::default();
    tx.get(&db, "orders", "a1").unwrap();
    let mut bumped = a.clone();
    bumped.insert("note", Value::String("bumped".into()));
    db.put("orders", "a1", &bumped).unwrap();
    let err = tx.commit(&db).unwrap_err();
    assert_eq!(err.kind(), ErrorKind::RetryableConflict);

    db.query(Query::new("accounts")).unwrap();

    let rep = write_stats_report();
    let ops = rep
        .lines()
        .find(|l| l.starts_with("ops:"))
        .unwrap_or_else(|| panic!("no ops line in report:\n{rep}"));
    let nums: Vec<u64> = ops
        .split_whitespace()
        .filter_map(|t| t.parse::<u64>().ok())
        .collect();
    assert_eq!(nums.len(), 4, "ops line: {ops}");
    assert!(nums[0] >= 1, "query counter: {ops}");
    assert!(nums[1] >= 1, "conflict counter: {ops}");
    assert!(nums[2] == 0, "no helper retry in a manual commit: {ops}");
    assert!(nums[3] >= 1, "unique counter: {ops}");
}

fn schema() -> CollectionSchema {
    CollectionSchema::new(vec![
        FieldRule::new("status", FieldKind::String).required(),
        FieldRule::new("qty", FieldKind::Int).with_default(Value::Int(1)),
        FieldRule::new("note", FieldKind::Any),
    ])
}

#[test]
fn schema_required_type_and_defaults() {
    let db = Hako::open(&tmp_dir("schema"), HakoConfig::default()).unwrap();
    db.set_schema("orders", schema()).unwrap();

    assert_eq!(
        db.put("orders", "missing", &HakoDoc::default()).unwrap_err().kind(),
        ErrorKind::ConstraintMismatch
    );

    let mut wrong = HakoDoc::default();
    wrong.insert("status", Value::Int(3));
    assert_eq!(
        db.put("orders", "wrong", &wrong).unwrap_err().kind(),
        ErrorKind::ConstraintMismatch
    );

    let id = db.put("orders", "ok", &doc("paid")).unwrap();
    assert_eq!(id, "ok");
    let stored = db.get("orders", "ok").unwrap().unwrap();
    assert_eq!(stored.get("qty"), Some(&Value::Int(1)), "default filled in storage");
    assert_eq!(stored.get("status"), Some(&Value::String("paid".into())));

    // Undeclared fields stay allowed â€” schema is opt-in per field.
    let mut extra = doc("open");
    extra.insert("whatever", Value::Bool(true));
    db.put("orders", "extra", &extra).unwrap();
}

#[test]
fn schema_rejects_bad_declarations_and_batch_atomically() {
    let db = Hako::open(&tmp_dir("schema-batch"), HakoConfig::default()).unwrap();
    assert!(db
        .set_schema(
            "orders",
            CollectionSchema::new(vec![FieldRule::new("a.b", FieldKind::String)]),
        )
        .is_err());
    let bad_default = CollectionSchema::new(vec![FieldRule::new("n", FieldKind::Int)
        .with_default(Value::String("x".into()))]);
    assert!(db.set_schema("orders", bad_default).is_err());

    db.set_schema("orders", schema()).unwrap();
    let batch = vec![
        hakodb::engine::BatchMutation::Put {
            collection: "orders".into(),
            doc_id: "good".into(),
            doc: doc("paid"),
        },
        hakodb::engine::BatchMutation::Put {
            collection: "orders".into(),
            doc_id: "bad".into(),
            doc: HakoDoc::default(),
        },
    ];
    assert!(db.write_batch(batch).is_err());
    assert!(db.get("orders", "good").unwrap().is_none(), "whole batch rejected");
    assert!(db.get("orders", "bad").unwrap().is_none());
}

#[test]
fn schema_persists_across_reopen_and_clears_on_drop() {
    let dir = tmp_dir("schema-persist");
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        db.set_schema("orders", schema()).unwrap();
        db.put("orders", "o1", &doc("paid")).unwrap();
    }
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        assert!(db.schema("orders").is_some(), "schema restored from disk");
        assert_eq!(
            db.put("orders", "o2", &HakoDoc::default()).unwrap_err().kind(),
            ErrorKind::ConstraintMismatch
        );
        db.clear_schema("orders").unwrap();
        db.put("orders", "o2", &HakoDoc::default()).unwrap();
        db.set_schema("orders", schema()).unwrap();
        db.drop_collection("orders").unwrap();
        assert!(db.schema("orders").is_none(), "drop clears the schema");
        db.put("orders", "fresh", &HakoDoc::default()).unwrap();
    }
}
