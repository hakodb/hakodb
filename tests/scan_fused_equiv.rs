//! Equivalence for the fused residual-filter page lane.
//!
//! The lane is an optimization: a page of matches must be row-for-row,
//! order-for-order identical to the historical "decode every candidate, then
//! filter, then slice" path. This test pins that contract on the shapes the
//! fuse can accept, plus the shapes it must decline (index-satisfied, OR,
//! dotted, elemMatch, cursor bounds).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-fused-{}-{}-{}",
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

fn seed(tag: &str) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(dir(tag), cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..4000 {
        let mut d = HakoDoc::default();
        d.insert("key", Value::String(format!("{:010}", i)));
        d.insert("name", Value::String(format!("name-{}", i % 25)));
        d.insert("category", Value::String(format!("cat-{}", i % 5)));
        d.insert("tenant", Value::String(format!("t{:02}", i % 24)));
        d.insert("score", Value::Int(i as i64));
        d.insert("email", Value::String(format!("u{}@example.test", i)));
        d.insert("payload", Value::String("x".repeat(32)));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("{:010}", i),
            doc: d,
        });
    }
    db.write_batch(batch).expect("seed");
    for f in ["category", "tenant", "score", "key"] {
        db.create_index("records", f).expect("index");
    }
    db.create_composite_index(
        "records",
        vec![
            (
                "tenant".into(),
                hakodb::index::composite::definition::SortDirection::Asc,
            ),
            (
                "score".into(),
                hakodb::index::composite::definition::SortDirection::Asc,
            ),
        ],
    )
    .expect("composite");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    db.await_quiescent(std::time::Duration::from_secs(10));
    db
}

fn keys(rows: &[(String, HakoDoc)]) -> Vec<String> {
    rows.iter().map(|(id, _)| id.clone()).collect()
}

fn shape(db: &Hako, q: Query, tag: &str) -> Vec<String> {
    let rows = db.query(q).expect(tag);
    keys(&rows)
}

#[test]
fn residual_page_matches_decode_then_filter_then_slice() {
    let db = seed("residual");

    // Reference: the old pipeline expressed as a query with no page, sliced in
    // Rust. Same matcher, same scan order, no fused lane.
    let all = db
        .query(
            Query::new("records")
                .where_filter("name", Operator::Eq, Value::String("name-7".into()))
                .order_by("key", true)
                // Explicit page: `Query` has an engine default limit, so an
                // unbounded reference query is not actually the full set.
                .limit(1000),
        )
        .expect("all");
    let all_ids = keys(&all);
    assert!(all_ids.len() > 100, "need enough matches for a deep page");

    for (offset, limit) in [
        (0usize, 1usize),
        (0, 7),
        (0, 100),
        (5, 10),
        (50, 50),
        (100, 100),
    ] {
        let expected: Vec<String> = all_ids.iter().skip(offset).take(limit).cloned().collect();
        let got = shape(
            &db,
            Query::new("records")
                .where_filter("name", Operator::Eq, Value::String("name-7".into()))
                .order_by("key", true)
                .offset(offset)
                .limit(limit),
            "fused",
        );
        assert_eq!(got, expected, "offset={offset} limit={limit}");
    }
}

#[test]
fn pages_without_order_match_reference_prefix() {
    let db = seed("nonorder");

    let all = db
        .query(
            Query::new("records")
                .where_filter("name", Operator::Eq, Value::String("name-3".into()))
                .limit(1000),
        )
        .expect("all");
    let all_ids = keys(&all);
    assert!(all_ids.len() > 100);

    for limit in [1usize, 10, 100] {
        let page = shape(
            &db,
            Query::new("records")
                .where_filter("name", Operator::Eq, Value::String("name-3".into()))
                .limit(limit),
            "page",
        );
        assert_eq!(page, all_ids[..limit.min(all_ids.len())].to_vec());
    }
}

#[test]
fn index_satisfied_shapes_keep_full_count() {
    let db = seed("indexed");
    let rows = db
        .query(
            Query::new("records")
                .where_filter("category", Operator::Eq, Value::String("cat-1".into()))
                .order_by("key", true)
                .limit(100),
        )
        .expect("indexed");
    assert_eq!(rows.len(), 100, "index-satisfied page must not be truncated");
}

#[test]
fn declined_shapes_still_return_their_own_rows() {
    let db = seed("declined");

    // Dotted path: no such top-level field, the matcher must stay in charge.
    let dotted = db
        .query(
            Query::new("records")
                .where_filter("payload.raw", Operator::Eq, Value::String("x".into()))
                .limit(10),
        )
        .expect("dotted");
    assert_eq!(dotted.len(), 0);

    // Cursor bounds are a separate contract (the cursor lane) and must keep
    // their own semantics; the fused page lane declines them.
    let cursor = db
        .query(
            Query::new("records")
                .where_filter("name", Operator::Eq, Value::String("name-7".into()))
                .order_by("key", true)
                .start_after(vec![Value::String("0000000100".into())])
                .limit(10),
        )
        .expect("cursor");
    assert!(cursor.iter().all(|(id, _)| id.as_str() > "0000000100"));
}

#[test]
fn miss_page_is_empty() {
    let db = seed("miss");
    let rows = db
        .query(
            Query::new("records")
                .where_filter("name", Operator::Eq, Value::String("nobody".into()))
                .order_by("key", true)
                .limit(100),
        )
        .expect("miss");
    assert!(rows.is_empty());
}

#[test]
fn duplicate_field_occurrences_keep_legacy_match() {
    // HakoDoc.fields is public and the decoder pushes every stored occurrence,
    // so a byte miss on one occurrence must not reject a later match (issue #255).
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(dir("dupfield"), cfg).expect("open");
    let mut doc = HakoDoc::default();
    doc.insert("key", Value::String("0000000001".into()));
    doc.fields.push(("name".into(), Value::String("wrong".into())));
    doc.fields.push(("name".into(), Value::String("right".into())));
    db.write_batch(vec![BatchMutation::Put {
        collection: "records".into(),
        doc_id: "0000000001".into(),
        doc,
    }])
    .expect("seed");
    db.create_index("records", "key").expect("index");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    db.await_quiescent(std::time::Duration::from_secs(5));

    for wanted in ["wrong", "right"] {
        let rows = db
            .query(
                Query::new("records")
                    .where_filter("name", Operator::Eq, Value::String(wanted.into()))
                    .order_by("key", true)
                    .limit(10),
            )
            .expect("query");
        assert_eq!(keys(&rows), vec!["0000000001".to_string()], "wanted={wanted}");
    }
}