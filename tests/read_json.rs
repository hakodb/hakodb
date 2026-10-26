//! P1 read paths: JSON rows from raw bytes and shared-cache point reads.

use std::collections::HashMap;
use std::sync::Arc;

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::query::query::Query;

fn open(label: &str, threshold: usize) -> (std::path::PathBuf, Hako) {
    let dir = std::env::temp_dir().join(format!(
        "hako-read-json-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let cfg = HakoConfig {
        durability_mode: DurabilityMode::Manual,
        value_blob_threshold_bytes: threshold,
        ..Default::default()
    };
    let db = Hako::open(&dir, cfg).expect("open");
    (dir, db)
}

fn seed(db: &Hako) {
    let mut a = HakoDoc::default();
    a.insert("tag", Value::String("a".into()));
    a.insert("n", Value::Int(1));
    a.insert("nested", Value::Map(vec![
        (Arc::from("z"), Value::Int(2)),
        (Arc::from("a"), Value::Array(vec![Value::String("x".into()), Value::Null])),
    ]));
    db.put("c", "a", &a).expect("put a");

    let mut b = HakoDoc::default();
    b.insert("tag", Value::String("b".into()));
    b.insert("n", Value::Int(2));
    b.insert("arr", Value::Array(vec![Value::Int(1), Value::Bool(true)]));
    db.put("c", "b", &b).expect("put b");

    let mut c = HakoDoc::default();
    c.insert("tag", Value::String("a".into()));
    c.insert("n", Value::Int(3));
    db.put("c", "c", &c).expect("put c");
}

fn expected(db: &Hako, q: Query) -> HashMap<String, Vec<u8>> {
    db.query(q)
        .expect("decoded query")
        .into_iter()
        .map(|(id, doc)| (id, doc.to_json_bytes()))
        .collect()
}

#[test]
fn query_json_matches_decoded_query_and_walk() {
    let (dir, db) = open("query", usize::MAX);
    seed(&db);

    let q = Query::new("c");
    let want = expected(&db, q.clone());
    let got: HashMap<_, _> = db.query_json(q.clone()).expect("query_json").into_iter().collect();
    assert_eq!(got, want);

    let mut walked = HashMap::new();
    let n = db
        .walk_json(q, &mut |id, json| {
            walked.insert(id.to_string(), json.to_vec());
            true
        })
        .expect("walk_json");
    assert_eq!(n, want.len());
    assert_eq!(walked, want);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn query_json_falls_back_for_filters_and_projection() {
    let (dir, db) = open("fallback", usize::MAX);
    seed(&db);

    // No index on `tag`: query_raw refuses, query_json must decode instead.
    let filtered = Query::new("c").where_eq("tag", Value::String("a".into()));
    let got: HashMap<_, _> = db.query_json(filtered.clone()).expect("filtered").into_iter().collect();
    assert_eq!(got, expected(&db, filtered));

    // Projection is not a raw-row feature; decoded query semantics win.
    let projected = Query::new("c").select_fields(vec!["n".into()]);
    let got: HashMap<_, _> = db.query_json(projected.clone()).expect("projected").into_iter().collect();
    assert_eq!(got, expected(&db, projected));

    std::fs::remove_dir_all(&dir).ok();
}

/// `query_json_into` must emit the same rows, in the same order, as the
/// per-row `query_json` path it replaces: plain pages, filter/projection
/// fallbacks, and an empty result all end up as one JSON array.
#[test]
fn query_json_into_matches_query_json() {
    let (dir, db) = open("json-into", usize::MAX);
    seed(&db);

    fn assembled(db: &Hako, q: &Query) -> (usize, Vec<u8>) {
        let rows = db.query_json(q.clone()).expect("query_json");
        let mut want = Vec::new();
        want.push(b'[');
        for (i, (_, json)) in rows.iter().enumerate() {
            if i > 0 {
                want.push(b',');
            }
            want.extend_from_slice(json);
        }
        want.push(b']');
        (rows.len(), want)
    }

    for (label, q) in [
        ("page", Query::new("c")),
        (
            "filtered",
            Query::new("c").where_eq("tag", Value::String("a".into())),
        ),
        ("projected", Query::new("c").select_fields(vec!["n".into()])),
    ] {
        let mut out = Vec::new();
        let n = db.query_json_into(q.clone(), &mut out).expect("into");
        let (want_n, want) = assembled(&db, &q);
        assert_eq!(n, want_n, "row count mismatch on {label}");
        assert_eq!(out, want, "bytes mismatch on {label}");
    }

    // Empty result must still be a valid empty array.
    let none = Query::new("c").where_eq("tag", Value::String("missing".into()));
    let mut out = Vec::new();
    let n = db.query_json_into(none, &mut out).expect("into empty");
    assert_eq!((n, out.as_slice()), (0, b"[]".as_slice()));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn query_json_resolves_blobs() {
    let (dir, db) = open("blob", 1024);
    let mut doc = HakoDoc::default();
    doc.insert("small", Value::String("ok".into()));
    doc.insert("payload", Value::String("x".repeat(4096)));
    db.put("c", "blob", &doc).expect("put blob");
    db.flush().expect("flush blobs");

    let q = Query::new("c");
    let got: HashMap<_, _> = db.query_json(q.clone()).expect("query_json").into_iter().collect();
    assert_eq!(got, expected(&db, q));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn get_arc_shares_cached_doc_without_clone() {
    let (dir, db) = open("arc", 1024);
    let mut doc = HakoDoc::default();
    doc.insert("n", Value::Int(7));
    doc.insert("payload", Value::String("x".repeat(4096)));
    db.put("c", "k", &doc).expect("put");
    db.flush().expect("flush blobs");

    let expected = db.get("c", "k").expect("get").expect("doc").to_json_bytes();
    let first = db.get_arc("c", "k").expect("first").expect("doc");
    let second = db.get_arc("c", "k").expect("second").expect("doc");
    assert!(Arc::ptr_eq(&first, &second), "cache hit must reuse the Arc");
    assert_eq!(first.to_json_bytes(), expected);

    std::fs::remove_dir_all(&dir).ok();
}

/// `get` probes the hot cache with a reused per-thread key buffer: the buffer
/// must never mix two collections that share a doc id, and a write must still
/// invalidate the cached version.
#[test]
fn get_reuses_the_cache_key_buffer_without_crosstalk() {
    let (dir, db) = open("get-key-buffer", 1024);
    for (col, n) in [("c1", 1i64), ("c2", 2)] {
        let mut doc = HakoDoc::default();
        doc.insert("n", Value::Int(n));
        db.put(col, "k", &doc).expect("put");
    }

    // Warm both entries, then interleave: same id, different collection.
    for _ in 0..3 {
        for (col, want) in [("c1", 1i64), ("c2", 2)] {
            let doc = db.get(col, "k").expect("get").expect("doc");
            match doc.get("n") {
                Some(Value::Int(got)) => assert_eq!(*got, want, "crosstalk on {col}"),
                other => panic!("unexpected value in {col}: {other:?}"),
            }
        }
    }

    // Version bump + cache eviction on write: the next get must see the new doc.
    let mut updated = HakoDoc::default();
    updated.insert("n", Value::Int(3));
    db.put("c1", "k", &updated).expect("update");
    let doc = db.get("c1", "k").expect("get").expect("doc");
    match doc.get("n") {
        Some(Value::Int(got)) => assert_eq!(*got, 3, "stale hit after write"),
        other => panic!("unexpected value after update: {other:?}"),
    }

    std::fs::remove_dir_all(&dir).ok();
}
