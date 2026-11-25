//! Equivalence of batched index declaration with per-index declaration.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-index-batch-{}-{}-{}",
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

fn config() -> HakoConfig {
    HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() }
}

fn seed(path: &std::path::Path) -> Hako {
    let db = Hako::open(path, config()).expect("open");
    let mut batch = Vec::new();
    for i in 0..2000usize {
        let mut doc = HakoDoc::default();
        doc.insert("category", Value::String(format!("cat-{}", i % 5)));
        doc.insert("tenant", Value::String(format!("t{:02}", i % 12)));
        doc.insert("score", Value::Int(i as i64));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:04}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db
}

fn ready(db: &Hako) {
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
}

fn ids(db: &Hako, query: Query) -> Vec<String> {
    db.query(query).expect("query").into_iter().map(|(id, _)| id).collect()
}

#[test]
fn batched_index_declaration_matches_per_index() {
    let d1 = dir("batch");
    let d2 = dir("single");
    let a = seed(&d1);
    let b = seed(&d2);

    let fields: Vec<String> = ["category", "tenant", "score"].iter().map(|s| s.to_string()).collect();
    let composite: Vec<Vec<(String, SortDirection)>> = vec![vec![
        ("tenant".to_string(), SortDirection::Asc),
        ("score".to_string(), SortDirection::Asc),
    ]];

    a.create_indexes("records", &fields, &composite).expect("batch create");
    ready(&a);

    for field in &fields {
        b.create_index("records", field).expect("single secondary");
    }
    b.create_composite_index("records", composite[0].clone()).expect("single composite");
    ready(&b);

    let cases = [
        Query::new("records")
            .where_filter("category", Operator::Eq, Value::String("cat-1".into()))
            .order_by("score", true)
            .limit(100),
        Query::new("records")
            .where_filter("tenant", Operator::Eq, Value::String("t03".into()))
            .where_filter("score", Operator::Gte, Value::Int(900))
            .order_by("score", true)
            .limit(100),
        Query::new("records")
            .where_filter("score", Operator::Gte, Value::Int(100))
            .where_filter("score", Operator::Lte, Value::Int(160))
            .order_by("score", true)
            .limit(100),
    ];
    for q in cases {
        let want = ids(&a, q.clone());
        let got = ids(&b, q.clone());
        assert_eq!(want, got, "query mismatch: {q:?}");
    }

    drop(a);
    drop(b);
    let _ = std::fs::remove_dir_all(&d1);
    let _ = std::fs::remove_dir_all(&d2);
}
