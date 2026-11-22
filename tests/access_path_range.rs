//! #250 follow-up: filter range pushdown into index bounds, and filter
//! index + TopN for filter+ORDER BY shapes.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(label: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let p = std::env::temp_dir().join(format!(
        "hako-range-{}-{}-{}", label, std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn setup(path: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(path, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..1000 {
        let mut doc = HakoDoc::default();
        doc.insert("category", Value::String(if i % 5 == 0 { "alpha" } else { "beta" }.into()));
        doc.insert("score", Value::Int(i));
        batch.push(BatchMutation::Put {
            collection: "records".into(), doc_id: format!("r{i:04}"), doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db.create_index("records", "category").expect("category index");
    db.create_index("records", "score").expect("score index");
    while !db.is_indexes_ready() { std::thread::sleep(std::time::Duration::from_millis(10)); }
    db.await_quiescent(std::time::Duration::from_secs(5));
    db
}

#[test]
fn range_filter_pushed_into_secondary_bounds() {
    let path = dir("range");
    let db = setup(&path);
    let query = Query::new("records")
        .where_filter("score", Operator::Gte, Value::Int(300))
        .where_filter("score", Operator::Lte, Value::Int(320))
        .order_by("score", true)
        .limit(10);
    let plan = db.explain(query.clone()).expect("explain");
    assert!(matches!(plan.scan, hakodb::query::plan::ScanType::SecondaryIndexRange { .. }),
        "expected secondary range, got {:?}", plan.scan);
    assert!(plan.filters_satisfied_by_index, "range bounds must satisfy the filter");

    let rows = db.query(query).expect("query");
    assert_eq!(rows.len(), 10);
    for (_, doc) in &rows {
        let v = match doc.get("score") { Some(Value::Int(v)) => *v, o => panic!("{o:?}") };
        assert!((300..=320).contains(&v), "out of range: {v}");
    }
}

#[test]
fn filter_index_with_order_by_still_correct() {
    let path = dir("filter-order");
    let db = setup(&path);
    let query = Query::new("records")
        .where_filter("category", Operator::Eq, Value::String("alpha".into()))
        .order_by("score", true)
        .limit(10);
    let rows = db.query(query).expect("query");
    assert_eq!(rows.len(), 10);
    for (_, doc) in &rows {
        assert_eq!(doc.get("category"), Some(&Value::String("alpha".into())));
    }
}

#[test]
fn filter_index_with_order_uses_filter_posting() {
    let path = dir("filter-topn");
    let db = setup(&path);
    let query = Query::new("records")
        .where_filter("category", Operator::Eq, Value::String("alpha".into()))
        .order_by("score", true)
        .limit(10);
    let plan = db.explain(query.clone()).expect("explain");
    assert!(matches!(plan.scan, hakodb::query::plan::ScanType::SecondaryIndex { .. }),
        "expected filter index, got {:?}", plan.scan);
    assert!(!plan.order_by_satisfied, "TopN must sort, not the index scan");

    let rows = db.query(query).expect("query");
    assert_eq!(rows.len(), 10);
    let mut scores = Vec::new();
    for (_, doc) in &rows {
        assert_eq!(doc.get("category"), Some(&Value::String("alpha".into())));
        scores.push(match doc.get("score") { Some(Value::Int(v)) => *v, o => panic!("{o:?}") });
    }
    assert!(scores.windows(2).all(|w| w[0] <= w[1]), "scores must be sorted: {scores:?}");
}