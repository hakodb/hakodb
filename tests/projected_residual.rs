//! Equivalence for the fused residual-filter path used by projected pages.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-proj-residual-{}-{}-{}",
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
    for i in 0..4000usize {
        let mut doc = HakoDoc::default();
        doc.insert("key", Value::String(format!("{:010}", i)));
        doc.insert("name", Value::String(format!("name-{}", i % 25)));
        doc.insert("score", Value::Int(i as i64));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:04}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
    db
}

fn base(limit: usize, offset: usize) -> Query {
    Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-7".into()))
        .limit(limit)
        .offset(offset)
}

fn reference(db: &Hako, q: &Query) -> Vec<(String, Option<Value>)> {
    db.query(q.clone())
        .expect("decoded")
        .into_iter()
        .map(|(id, doc)| (id, doc.get("key").cloned()))
        .collect()
}

fn projected(db: &Hako, q: &Query) -> Vec<(String, Option<Value>)> {
    db.query_projected(q.clone(), &["key".to_string()])
        .expect("projected")
        .into_iter()
        .map(|(id, fields)| {
            (
                id,
                fields.into_iter().find(|(key, _)| key == "key").map(|(_, value)| value),
            )
        })
        .collect()
}

#[test]
fn fused_projected_page_matches_decoded_reference() {
    let d = dir("fused");
    let db = open(&d);

    for (limit, offset) in [(1usize, 0usize), (10, 0), (100, 0), (10, 5), (50, 50)] {
        let q = base(limit, offset);
        assert_eq!(projected(&db, &q), reference(&db, &q), "limit={limit} offset={offset}");
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn declined_projected_shape_keeps_its_rows() {
    let d = dir("declined");
    let db = open(&d);
    let q = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-7".into()))
        .where_filter("score", Operator::Gte, Value::Int(1000))
        .limit(50);
    assert_eq!(projected(&db, &q), reference(&db, &q));
    let _ = std::fs::remove_dir_all(&d);
}
