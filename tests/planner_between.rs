//! BETWEEN bounds on a composite prefix must keep the walk bounded and
//! return the same page as the secondary range path.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection::Asc;
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::order::OrderBy;
use hakodb::query::query::Query;
use std::time::Duration;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-between-{tag}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn between_on_composite_prefix_stays_bounded() {
    let d = dir("range");
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(&d, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..1000usize {
        let mut doc = HakoDoc::default();
        doc.insert("key", Value::String(format!("{:010}", i + 1)));
        doc.insert("score", Value::Int(i as i64));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("{:010}", i + 1),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db.create_index("records", "score").expect("secondary");
    db.create_composite_index("records", vec![("score".into(), Asc), ("key".into(), Asc)])
        .expect("covering composite");
    assert!(db.await_quiescent(Duration::from_secs(30)));

    let mut q = Query::new("records");
    q.filters.push(Filter { field: "score".into(), op: Operator::Gte, value: Value::Int(200) });
    q.filters.push(Filter { field: "score".into(), op: Operator::Lte, value: Value::Int(299) });
    q.order_by.push(OrderBy { field: "score".into(), ascending: true });
    q.limit = Some(100);

    let report = db.explain_report(q.clone()).expect("explain").to_string();
    assert!(
        !report.contains("satisfied_by_index: false"),
        "BETWEEN must keep filters index-satisfied: {report}"
    );

    let rows = db.query(q).expect("query");
    assert_eq!(rows.len(), 100);
    let sum: i64 = rows.into_iter().map(|(_, doc)| match doc.get("score") {
        Some(Value::Int(v)) => *v,
        _ => 0,
    }).sum();
    assert_eq!(sum, 24950);
    let _ = std::fs::remove_dir_all(&d);
}
