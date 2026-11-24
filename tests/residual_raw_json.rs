//! Residual raw/JSON egress (v0.21.10 follow-up).
//!
//! The decoded fused page lane proved that a byte-safe `Eq` predicate can be
//! decided on the borrowed storage encoding. This suite pins the matching
//! egress contract for raw rows and JSON: same ids, same order, same bytes as
//! `query()` + `to_json_bytes()`, including offset/limit, misses and the shapes
//! that must keep declining. It also drives the write path (put, batch,
//! delete, reopen) and checks that residual reads observe it.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-residual-raw-{}-{}-{}",
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
    HakoConfig {
        durability_mode: DurabilityMode::Manual,
        ..Default::default()
    }
}

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    while !db.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(db.await_quiescent(std::time::Duration::from_secs(30)), "quiescent");
}

fn doc(i: usize) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("key", Value::String(format!("{:010}", i)));
    d.insert("name", Value::String(format!("name-{}", i % 25)));
    d.insert("category", Value::String(format!("cat-{}", i % 5)));
    d.insert("score", Value::Int(i as i64));
    d.insert("payload", Value::String("x".repeat(32)));
    d
}

fn seed(db: &Hako, n: usize) {
    let batch: Vec<BatchMutation> = (0..n)
        .map(|i| BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("{:010}", i),
            doc: doc(i),
        })
        .collect();
    db.write_batch(batch).expect("seed");
    for f in ["category", "score", "key"] {
        db.create_index("records", f).expect("index");
    }
    wait_ready(db);
}

fn ids(rows: &[(String, HakoDoc)]) -> Vec<String> {
    rows.iter().map(|(id, _)| id.clone()).collect()
}

fn residual_q(limit: usize, offset: usize) -> Query {
    Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-7".into()))
        .order_by("key", true)
        .limit(limit)
        .offset(offset)
}

#[test]
fn raw_residual_matches_decoded_ids_and_order() {
    let d = dir("raw-ids");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 4000);

    for (limit, offset) in [
        (1usize, 0usize),
        (7, 0),
        (100, 0),
        (10, 5),
        (50, 50),
        (100, 100),
        (4000, 0),
    ] {
        let q = residual_q(limit, offset);
        let want = ids(&db.query(q.clone()).expect("decoded"));
        let got = db.query_raw(q).expect("raw");
        let got_ids: Vec<String> = got.into_iter().map(|(id, _)| id).collect();
        assert_eq!(got_ids, want, "limit={limit} offset={offset}");
    }

    let miss = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("nobody".into()))
        .order_by("key", true)
        .limit(100);
    assert!(db.query_raw(miss).unwrap().is_empty());

    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn raw_residual_without_order_is_a_scan_prefix() {
    let d = dir("raw-noorder");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 3000);

    let q = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-3".into()))
        .limit(100);
    let want = ids(&db.query(q.clone()).expect("decoded"));
    let got: Vec<String> = db
        .query_raw(q)
        .expect("raw")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(got, want);

    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn raw_residual_unbounded_page_matches_decoded() {
    let d = dir("raw-unbounded");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 2000);

    let q = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-9".into()))
        .order_by("key", true);
    let decoded = db.query(q.clone()).expect("decoded");
    assert!(decoded.len() > 51, "need a multi-page result set");
    let want = ids(&decoded);

    let raw = db.query_raw(q.clone()).expect("raw");
    let got: Vec<String> = raw.into_iter().map(|(id, _)| id).collect();
    assert_eq!(got, want);

    let mut out = Vec::new();
    let n = db.query_json_into(q, &mut out).expect("json");
    assert_eq!(n, want.len());
    let arg: serde_json::Value = serde_json::from_slice(&out).expect("json parse");
    assert_eq!(arg.as_array().expect("array").len(), want.len());

    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn query_json_into_is_byte_identical_to_decoded_query_json() {
    let d = dir("json-bytes");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 4000);

    for (limit, offset) in [(1usize, 0usize), (7, 0), (100, 0), (10, 5), (100, 100)] {
        let q = residual_q(limit, offset);
        let rows = db.query(q.clone()).expect("decoded");

        let mut want = vec![b'['];
        for (i, (_, doc)) in rows.iter().enumerate() {
            if i > 0 {
                want.push(b',');
            }
            want.extend_from_slice(&doc.to_json_bytes());
        }
        want.push(b']');

        let mut got = Vec::new();
        let n = db.query_json_into(q, &mut got).expect("json into");
        assert_eq!(n, rows.len(), "limit={limit} offset={offset} count");
        assert_eq!(got, want, "limit={limit} offset={offset} bytes");
    }

    let q = residual_q(50, 7);
    let json_rows = db.query_json(q.clone()).expect("json rows");
    let decoded = db.query(q).expect("decoded rows");
    let want: Vec<_> = decoded
        .into_iter()
        .map(|(id, doc)| (id, doc.to_json_bytes()))
        .collect();
    assert_eq!(json_rows, want);

    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn index_satisfied_raw_and_json_keep_their_counts() {
    let d = dir("indexed");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 2000);

    // No ORDER BY: the index-satisfied scan is still the raw fast lane.
    let q = Query::new("records")
        .where_filter("category", Operator::Eq, Value::String("cat-1".into()))
        .limit(100);
    let decoded = ids(&db.query(q.clone()).expect("decoded"));
    assert_eq!(decoded.len(), 100);
    let raw: Vec<String> = db
        .query_raw(q.clone())
        .expect("indexed raw")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(raw, decoded);

    let mut out = Vec::new();
    let n = db.query_json_into(q, &mut out).expect("indexed json");
    assert_eq!(n, 100);

    // ORDER BY a non-indexed field keeps the historical decoded JSON path; the
    // residual raw egress must not silently claim it.
    let ordered = Query::new("records")
        .where_filter("category", Operator::Eq, Value::String("cat-1".into()))
        .order_by("key", true)
        .limit(100);
    assert!(db.query_raw(ordered.clone()).is_err());
    let mut out = Vec::new();
    assert_eq!(
        db.query_json_into(ordered, &mut out).expect("ordered json"),
        100
    );

    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn residual_raw_declines_projection_cursor_and_non_eq() {
    let d = dir("decline");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 1000);

    let projected = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-4".into()))
        .select_fields(vec!["key".into()]);
    assert!(db.query_raw(projected.clone()).is_err());
    let json = db.query_json(projected).expect("projected json");
    assert!(!json.is_empty());

    let cursor = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-4".into()))
        .order_by("key", true)
        .start_after(vec![Value::String("0000000100".into())])
        .limit(10);
    assert!(db.query_raw(cursor).is_err());

    let ne = Query::new("records")
        .where_filter("name", Operator::Ne, Value::String("name-4".into()))
        .order_by("key", true)
        .limit(10);
    assert!(db.query_raw(ne).is_err());

    std::fs::remove_dir_all(&d).ok();
}

#[test]
fn write_path_updates_are_visible_to_residual_raw_reads() {
    let d = dir("write-read");
    let db = Hako::open(&d, config()).expect("open");
    seed(&db, 1000);

    let mut updated = doc(7);
    updated.insert("name", Value::String("renamed".into()));
    db.put("records", "0000000007", &updated).expect("put");
    wait_ready(&db);

    let hit = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("renamed".into()))
        .order_by("key", true)
        .limit(10);
    let raw = db.query_raw(hit.clone()).expect("raw after put");
    assert_eq!(raw.len(), 1, "exactly the overwritten row");
    assert_eq!(raw[0].0, "0000000007");
    let mut out = Vec::new();
    db.query_json_into(hit, &mut out).expect("json after put");
    let parsed: serde_json::Value = serde_json::from_slice(&out).expect("json parse");
    assert_eq!(parsed[0]["name"], "renamed");

    let delete = BatchMutation::Delete {
        collection: "records".into(),
        doc_id: "0000000008".into(),
    };
    let insert = BatchMutation::Put {
        collection: "records".into(),
        doc_id: "0000000999".into(),
        doc: doc(999),
    };
    db.write_batch(vec![delete, insert]).expect("batch");
    wait_ready(&db);

    let gone = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-8".into()))
        .limit(1000);
    let gone_rows = db.query_raw(gone).expect("raw after delete");
    assert!(
        gone_rows.iter().all(|(id, _)| id != "0000000008"),
        "deleted row must not leak through residual raw"
    );
    assert!(!gone_rows.is_empty(), "other name-8 rows must survive");

    db.flush().expect("flush before reopen");
    drop(db);
    let db = Hako::open(&d, config()).expect("reopen");
    wait_ready(&db);
    let q = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-7".into()))
        .order_by("key", true)
        .limit(100);
    let decoded = db.query(q.clone()).expect("after reopen");
    assert_eq!(decoded.len(), 39, "one name-7 row was renamed");
    assert_eq!(db.query_raw(q).expect("raw after reopen").len(), decoded.len());

    std::fs::remove_dir_all(&d).ok();
}
