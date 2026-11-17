//! Wave 1 hybrid retrieval (#193): FTS (sparse) + vector KNN (dense)
//! fused with Reciprocal Rank Fusion via `Hako::hybrid_search`.
//! Covers: fused ordering with both signals, empty-side fallback to the
//! other side's order, k/rrf_k validation, and `query()` staying
//! byte-identical for single-signal plans.
//!
//! Run with: cargo test --test hybrid_search

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::vector::{encode_f32s, Metric};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-hyb-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn item_doc(name: &str, emb: [f32; 2]) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("name", Value::String(name.into()));
    doc.insert("emb", Value::Binary(encode_f32s(&emb)));
    doc
}

/// Corpus: d1 text-matches best AND is the vector bullseye; d2 matches
/// both but weaker; d3 vector-far and text-out.
fn seed(db: &Hako) {
    db.create_fts_index("items", "name").unwrap();
    db.create_vector_index("items", "emb", 2, Metric::Cosine).unwrap();
    db.put("items", "d1", &item_doc("alpha beta gamma", [1.0, 0.0])).unwrap();
    db.put("items", "d2", &item_doc("alpha beta delta", [0.9, 0.1])).unwrap();
    db.put("items", "d3", &item_doc("alpha epsilon", [0.0, 1.0])).unwrap();
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
}

fn ids(rows: &[(String, f64, HakoDoc)]) -> Vec<String> {
    rows.iter().map(|(id, _, _)| id.clone()).collect()
}

#[test]
fn hybrid_fuses_both_signals() {
    let dir = tmp("fuse");
    let db = open_db(&dir);
    seed(&db);
    // d1 tops both lists (text tie -> id order, vector bullseye), so RRF
    // must put it first; d2 rides both ranks; d3 only rides the vector
    // list and lands last.
    let rows = db
        .hybrid_search(
            "items", "name", "alpha beta", "emb", &[1.0, 0.0], 3, 60,
        )
        .unwrap();
    assert_eq!(ids(&rows), vec!["d1", "d2", "d3"]);
    assert!(rows[0].1 > rows[1].1, "two-signal hit must outscore one-signal: {:?}", rows);
    assert!(rows[1].1 > rows[2].1, "two ranks beat one rank");
    // Deterministic repeat.
    assert_eq!(
        ids(&db.hybrid_search("items", "name", "alpha beta", "emb", &[1.0, 0.0], 3, 60).unwrap()),
        ids(&rows)
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_empty_side_falls_back() {
    let dir = tmp("empty");
    let db = open_db(&dir);
    seed(&db);
    // No text match: pure vector order (distance asc), which for this
    // corpus is d1, d2, d3.
    let rows = db
        .hybrid_search("items", "name", "zzz", "emb", &[1.0, 0.0], 3, 60)
        .unwrap();
    assert_eq!(ids(&rows), vec!["d1", "d2", "d3"]);
    assert_eq!(
        rows[0].1,
        1.0 / 61.0,
        "single-signal fused score is the bare RRF term"
    );
    // Both empty is unreachable on an indexed vector field (there is
    // always a nearest), so the near-dead case: query far from everything
    // still ranks d2 first (cosine to [0.9,0.1] beats the others).
    let rows = db
        .hybrid_search("items", "name", "zzz", "emb", &[5.0, 5.0], 3, 60)
        .unwrap();
    assert_eq!(ids(&rows), vec!["d2", "d1", "d3"]);
    let rows = db
        .hybrid_search("items", "name", "zzz", "nope", &[1.0, 0.0], 3, 60)
        .unwrap();
    assert!(rows.is_empty(), "both sides dead -> empty");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_validates_and_caps() {
    let dir = tmp("valid");
    let db = open_db(&dir);
    seed(&db);
    // k = 0 -> empty; rrf_k = 0 -> hard error (division).
    assert!(db
        .hybrid_search("items", "name", "alpha", "emb", &[1.0, 0.0], 0, 60)
        .unwrap()
        .is_empty());
    assert!(db
        .hybrid_search("items", "name", "alpha", "emb", &[1.0, 0.0], 3, 0)
        .is_err());
    // k caps: 1 hit only.
    let rows = db
        .hybrid_search("items", "name", "alpha beta", "emb", &[1.0, 0.0], 1, 60)
        .unwrap();
    assert_eq!(ids(&rows), vec!["d1"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn query_stays_single_signal() {
    let dir = tmp("plan");
    let db = open_db(&dir);
    seed(&db);
    // A query carrying both a Match and a Near filter keeps its existing
    // fail-closed semantics: Near is only answerable by the VectorKnn
    // scan, and the planner's FTS arm claims the Match filter first, so
    // the combined query returns nothing (pre-#193 behavior, preserved —
    // fusion lives in hybrid_search only).
    let q = Query::new("items")
        .where_filter("name", Operator::Match, Value::String("alpha beta".into()))
        .where_filter(
            "emb",
            Operator::Near,
            Value::Binary(encode_f32s(&[1.0, 0.0])),
        )
        .limit(10);
    let rows = db.query(q).unwrap();
    assert!(rows.is_empty(), "Near outside its scan answers false");
    // Single-signal plans unchanged: Match alone still routes to FTS.
    let q = Query::new("items")
        .where_filter("name", Operator::Match, Value::String("alpha beta".into()))
        .limit(10);
    let rows = db.query(q).unwrap();
    assert_eq!(rows.len(), 2, "d1 and d2 contain both words");
    let _ = std::fs::remove_dir_all(&dir);
}

// ===== #217 P0-A: filtered hybrid + find_near_scored_filtered =====

use hakodb::query::filter::Filter;

fn user_doc(user: &str, name: &str, emb: [f32; 2]) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("user_id", Value::String(user.into()));
    doc.insert("name", Value::String(name.into()));
    doc.insert("emb", Value::Binary(encode_f32s(&emb)));
    doc
}

fn eq_filter(field: &str, val: &str) -> Filter {
    Filter { field: field.into(), op: Operator::Eq, value: Value::String(val.into()) }
}

/// 10 alice docs clustered near [1,0], 10 bob docs near [0,1]; every doc
/// text-matches "alpha" (so the text side alone cannot separate tenants).
fn seed_users(db: &Hako) {
    db.create_fts_index("docs", "name").unwrap();
    db.create_vector_index("docs", "emb", 2, Metric::Cosine).unwrap();
    for i in 0..10 {
        db.put("docs", &format!("a{i}"), &user_doc("alice", &format!("alpha aa{i}"), [1.0, i as f32 * 0.01])).unwrap();
        db.put("docs", &format!("b{i}"), &user_doc("bob", &format!("alpha bb{i}"), [0.0, 1.0 + i as f32 * 0.01])).unwrap();
    }
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
}

/// kajian test 1: full k, every row passes the filter.
#[test]
fn filtered_hybrid_returns_full_k_subset() {
    let dir = tmp("pk");
    let db = open_db(&dir);
    seed_users(&db);
    let f = [eq_filter("user_id", "alice")];
    let rows = db
        .hybrid_search_filtered("docs", "name", "alpha", "emb", &[1.0, 0.0], 10, 60, &f)
        .unwrap();
    assert_eq!(rows.len(), 10, "full k: 10 alice docs exist");
    assert!(rows
        .iter()
        .all(|(id, _, d)| id.starts_with('a')
            && d.get("user_id") == Some(&Value::String("alice".into()))));
    let _ = std::fs::remove_dir_all(&dir);
}

/// kajian test 2: filters work on FTS-indexed and plain fields alike.
#[test]
fn filter_works_on_fts_field_and_plain_field() {
    let dir = tmp("idx");
    let db = open_db(&dir);
    seed_users(&db);
    let rows = db
        .hybrid_search_filtered(
            "docs", "name", "alpha", "emb", &[1.0, 0.0], 5, 60,
            &[eq_filter("name", "alpha aa3")], // name is the FTS field
        )
        .unwrap();
    assert_eq!(rows.iter().map(|(id, ..)| id.clone()).collect::<Vec<_>>(), vec!["a3"]);
    let rows = db
        .hybrid_search_filtered(
            "docs", "name", "alpha", "emb", &[1.0, 0.0], 3, 60,
            &[eq_filter("user_id", "bob")], // user_id has no index
        )
        .unwrap();
    assert!(rows.iter().all(|(id, ..)| id.starts_with('b')));
    let _ = std::fs::remove_dir_all(&dir);
}

/// kajian test 3: zero matching candidates -> empty, no panic.
#[test]
fn zero_candidates_answer_empty() {
    let dir = tmp("zero");
    let db = open_db(&dir);
    seed_users(&db);
    let rows = db
        .hybrid_search_filtered(
            "docs", "name", "alpha", "emb", &[1.0, 0.0], 10, 60,
            &[eq_filter("user_id", "carol")],
        )
        .unwrap();
    assert!(rows.is_empty());
    let rows = db
        .find_near_scored_filtered(
            "docs", "emb", &[1.0, 0.0], 10, None,
            &[eq_filter("user_id", "carol")],
        )
        .unwrap();
    assert!(rows.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// kajian test 4: widening + exact fallback reach a filtered region the
/// beam never visits — alice's docs are the FAR cluster for this query.
#[test]
fn filtered_widening_reaches_sparse_region() {
    let dir = tmp("wide");
    let db = open_db(&dir);
    seed_users(&db);
    // Vector-only: query sits in bob's cluster; k=10 must fill with the
    // 10 alice docs (the unfiltered top-10 would all be bob).
    let rows = db
        .find_near_scored_filtered(
            "docs", "emb", &[0.0, 1.0], 10, None,
            &[eq_filter("user_id", "alice")],
        )
        .unwrap();
    assert_eq!(
        rows.len(),
        10,
        "widening/fallback must surface the far cluster in full"
    );
    assert!(rows.iter().all(|(id, _, _)| id.starts_with('a')));
    let _ = std::fs::remove_dir_all(&dir);
}

/// kajian test 5: no other tenant's id anywhere — neither id list nor
/// the returned docs.
#[test]
fn filtered_results_leak_no_other_tenant() {
    let dir = tmp("leak");
    let db = open_db(&dir);
    seed_users(&db);
    let f = [eq_filter("user_id", "alice")];
    let rows = db
        .hybrid_search_filtered("docs", "name", "alpha", "emb", &[0.0, 1.0], 10, 60, &f)
        .unwrap();
    assert!(!rows.is_empty());
    assert!(rows.iter().all(|(id, _, d)| id.starts_with('a')
        && d.get("user_id") == Some(&Value::String("alice".into()))));
    let rows = db
        .find_near_scored_filtered("docs", "emb", &[0.0, 1.0], 5, None, &f)
        .unwrap();
    assert!(rows.iter().all(|(id, _, _)| id.starts_with('a')));
    let _ = std::fs::remove_dir_all(&dir);
}

/// kajian test 6: empty filters are byte-identical to the unfiltered APIs.
#[test]
fn empty_filters_are_identical_to_unfiltered() {
    let dir = tmp("parity");
    let db = open_db(&dir);
    seed_users(&db);
    let a = db.hybrid_search("docs", "name", "alpha", "emb", &[1.0, 0.0], 7, 60).unwrap();
    let b = db
        .hybrid_search_filtered("docs", "name", "alpha", "emb", &[1.0, 0.0], 7, 60, &[])
        .unwrap();
    assert_eq!(a, b, "hybrid_search must be hybrid_search_filtered(&[])");
    let c = db.find_near_scored("docs", "emb", &[1.0, 0.0], 7, None).unwrap();
    let d = db
        .find_near_scored_filtered("docs", "emb", &[1.0, 0.0], 7, None, &[])
        .unwrap();
    assert_eq!(c, d, "find_near_scored must be find_near_scored_filtered(&[])");
    let _ = std::fs::remove_dir_all(&dir);
}
