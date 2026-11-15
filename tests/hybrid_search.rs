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
