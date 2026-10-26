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

/// Query row cache (#142 Q1): a second identical query rides the cache, so
/// an update or delete between two queries must be reflected immediately â€”
/// the version check on probe, not luck, keeps the page fresh.
#[test]
fn row_cache_serves_fresh_rows_after_writes() {
    let (dir, db) = open("rowcache-writes", usize::MAX);
    seed(&db);
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)), "quiescent");

    fn ns(db: &Hako) -> Vec<i64> {
        let mut got: Vec<i64> = db
            .query(Query::new("c"))
            .expect("query")
            .into_iter()
            .map(|(_, doc)| match doc.get("n") {
                Some(Value::Int(n)) => *n,
                other => panic!("unexpected n: {other:?}"),
            })
            .collect();
        got.sort_unstable();
        got
    }

    assert_eq!(ns(&db), vec![1, 2, 3], "first query decodes");
    assert_eq!(ns(&db), vec![1, 2, 3], "second query rides the cache");

    let mut updated = HakoDoc::default();
    updated.insert("tag", Value::String("a".into()));
    updated.insert("n", Value::Int(99));
    db.put("c", "a", &updated).expect("update a");
    assert_eq!(ns(&db), vec![2, 3, 99], "stale hit after update");

    db.delete("c", "b").expect("delete b");
    let mut ids: Vec<String> = db.query(Query::new("c")).expect("query").into_iter().map(|(id, _)| id).collect();
    ids.sort();
    assert_eq!(ids, vec!["a".to_string(), "c".to_string()], "deleted row must not resurrect");

    std::fs::remove_dir_all(&dir).ok();
}

/// Blob rows never enter the cache: they are skeletons until the inflate
/// pass runs, and a cached skeleton would serve an uninflated page forever.
#[test]
fn row_cache_never_serves_uninflated_blob_rows() {
    let (dir, db) = open("rowcache-blob", 1024);
    let mut doc = HakoDoc::default();
    doc.insert("n", Value::Int(1));
    doc.insert("payload", Value::String("x".repeat(4096)));
    db.put("c", "blob", &doc).expect("put blob");
    db.flush().expect("flush blobs");
    // best-effort settle: pending blob bytes keep the engine formally
    // non-quiescent in Manual mode, so the verdict is not asserted here.
    let _ = db.await_quiescent(std::time::Duration::from_secs(2));

    fn payload(db: &Hako, defer: bool) -> usize {
        let doc = &db
            .query(Query::new("c").defer_blobs(defer))
            .expect("query")
            .remove(0)
            .1;
        match doc.get("payload") {
            Some(Value::String(s)) => s.len(),
            Some(Value::BlobLink { .. }) => 0, // placeholder, not resolved
            other => panic!("unexpected payload: {other:?}"),
        }
    }

    // Same page three times: deferred rows are placeholders, resolved rows
    // are full â€” the gate keeps the two shapes from ever mixing.
    assert_eq!(payload(&db, false), 4096, "first query inflates");
    assert_eq!(payload(&db, false), 4096, "second query must be full too");
    assert_ne!(payload(&db, true), 4096, "deferred row is a placeholder");
    assert_eq!(payload(&db, false), 4096, "cache poisoned by deferred rows");

    std::fs::remove_dir_all(&dir).ok();
}

/// query_arc (#142 Q3): warm rows come back as the same `Arc` (zero-copy
/// sharing with the engine's cache); an update bumps the version, so the
/// next call decodes fresh into a new `Arc` with the new value.
#[test]
fn query_arc_shares_cache_hit_rows() {
    let (dir, db) = open("query-arc", usize::MAX);
    seed(&db);

    assert!(db.await_quiescent(std::time::Duration::from_secs(60)), "quiescent");

    let q = Query::new("c");
    db.query(q.clone()).expect("warm query");

    let first = db.query_arc(q.clone()).expect("arc 1");
    let second = db.query_arc(q.clone()).expect("arc 2");
    assert_eq!(first.len(), second.len());
    let pick = |rows: &[(String, Arc<HakoDoc>)], id: &str| {
        rows.iter().find(|(i, _)| i == id).map(|(_, a)| a).expect("row").clone()
    };
    for id in ["a", "b", "c"] {
        let (a, b) = (pick(&first, id), pick(&second, id));
        assert!(Arc::ptr_eq(&a, &b), "cache-hit rows must share the Arc ({id})");
    }

    let mut updated = HakoDoc::default();
    updated.insert("tag", Value::String("a".into()));
    updated.insert("n", Value::Int(99));
    db.put("c", "a", &updated).expect("update a");

    let fresh = db.query_arc(q).expect("arc 3");
    let (fa, da) = fresh.iter().find(|(id, _)| id == "a").expect("row a");
    match da.get("n") {
        Some(Value::Int(99)) => {}
        other => panic!("stale arc row after update: {other:?}"),
    }
    let old = first.iter().find(|(id, _)| id == fa).expect("old a");
    assert!(!Arc::ptr_eq(&old.1, da), "updated row must not reuse the old Arc");

    std::fs::remove_dir_all(&dir).ok();
}

/// Projected and plain queries share the executor's fast path but keep
/// separate cache gates; interleaving them must never change what either
/// returns (projection trimming happens above the executor, so both shapes
/// see the same full documents here).
#[test]
fn row_cache_respects_projection_gate() {
    let (dir, db) = open("rowcache-projection", usize::MAX);
    seed(&db);
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)), "quiescent");

    let expected = vec![1, 2, 3];
    let q = Query::new("c").select_fields(vec!["n".into()]);
    for round in 0..3 {
        let mut projected: Vec<i64> = db
            .query(q.clone())
            .expect("projected")
            .into_iter()
            .map(|(_, doc)| match doc.get("n") {
                Some(Value::Int(n)) => *n,
                other => panic!("unexpected n: {other:?}"),
            })
            .collect();
        projected.sort_unstable();
        assert_eq!(projected, expected, "projected round {round}");
        let mut plain: Vec<i64> = db
            .query(Query::new("c"))
            .expect("plain")
            .into_iter()
            .map(|(_, doc)| match doc.get("n") {
                Some(Value::Int(n)) => *n,
                other => panic!("unexpected n: {other:?}"),
            })
            .collect();
        plain.sort_unstable();
        assert_eq!(plain, expected, "plain round {round}");
    }

    std::fs::remove_dir_all(&dir).ok();
}
