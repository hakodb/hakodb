//! #250 follow-up: composite prefix equality + ORDER BY must use the
//! composite index (and keep results correct).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(label: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let p = std::env::temp_dir().join(format!(
        "hako-access-path-{}-{}-{}",
        label,
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn composite_prefix_order_plan_and_results() {
    let path = dir("composite");
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&path, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..300 {
        let mut doc = HakoDoc::default();
        doc.insert("tenant", Value::String(if i % 2 == 0 { "t07" } else { "t08" }.into()));
        doc.insert("score", Value::Int(i));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:04}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db.create_index("records", "score").expect("score index");
    db.create_composite_index("records", vec![("tenant".into(), hakodb::index::composite::definition::SortDirection::Asc), ("score".into(), hakodb::index::composite::definition::SortDirection::Asc)])
        .expect("composite");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    db.await_quiescent(std::time::Duration::from_secs(5));

    let query = Query::new("records")
        .where_filter("tenant", Operator::Eq, Value::String("t07".into()))
        .where_filter("score", Operator::Gte, Value::Int(200))
        .order_by("score", true)
        .limit(10);
    let rep = db.explain_report(query.clone()).expect("report");
    eprintln!("AVAILABLE={:?} SCAN={}", rep.available_indexes, rep.scan);
    let plan = db.explain(query.clone()).expect("explain");
    assert!(
        matches!(plan.scan, hakodb::query::plan::ScanType::CompositeIndexRange { .. }),
        "expected composite range, got {:?}", plan.scan
    );
    assert!(plan.order_by_satisfied, "composite must satisfy ORDER BY score");

    let rows = db.query(query).expect("query");
    assert_eq!(rows.len(), 10);
    let mut scores: Vec<i64> = rows.iter().map(|(_, d)| match d.get("score") {
        Some(Value::Int(v)) => *v,
        other => panic!("unexpected score {other:?}"),
    }).collect();
    scores.sort_unstable();
    assert_eq!(scores, vec![200, 202, 204, 206, 208, 210, 212, 214, 216, 218]);
}
