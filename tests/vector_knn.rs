//! P0 exact KNN: routing, ordering, hybrid filters, fail-closed shapes,
//! plan-cache isolation, restart persistence of vector defs.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::vector::{cosine_distance, decode_f32s, encode_f32s, HnswParams, Metric};
use hakodb::query::filter::Operator;
use hakodb::query::plan::ScanType;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-vec-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn vdoc(emb: &[f32], grp: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("emb", Value::Binary(encode_f32s(emb)));
    doc.insert("grp", Value::String(grp.into()));
    doc
}

/// Recovery runs on a background thread and flips `indexes_ready` when
/// done; until then the planner fail-closes to FullCollection (same rule
/// as FTS). Index creation backfills HNSW on a second background thread
/// (covered by `index_backfills`), and live writes index through a third
/// (covered by `pending_index_ops` — the RAM graph lags storage by that
/// window, same eventual-consistency rule as every other RAM family).
/// Every test waits past all three windows before asserting.
fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        let q = db.quiescence_status();
        if db.is_indexes_ready() && q.index_backfills == 0 && q.pending_index_ops == 0 {
            return;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(60),
            "indexes never ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

// 2D points: a=(1,0) b=(0,1) c=(1,1) d=(10,0). Query q=(1,0).
// cosine: a=0, c≈0.293, b=1, d=0 (collinear! d is same direction as a).
// Use q=(1,0.5) instead: cosine order a,c,b,d... compute in test via
// ground truth below. L2-squared from q: a=0.25, c=0.25, b=1.25, d=81.25.
fn seed(db: &Hako) {
    let pts: &[(&str, [f32; 2], &str)] = &[
        ("a", [1.0, 0.0], "x"),
        ("b", [0.0, 1.0], "x"),
        ("c", [1.0, 1.0], "y"),
        ("d", [10.0, 0.0], "y"),
    ];
    let batch = pts
        .iter()
        .map(|(id, p, g)| BatchMutation::Put {
            collection: "pts".into(),
            doc_id: id.to_string(),
            doc: vdoc(p, g),
        })
        .collect();
    db.write_batch(batch).expect("seed");
    db.create_vector_index("pts", "emb", 2, Metric::Cosine)
        .expect("create");
}

#[test]
fn routes_to_vector_knn() {
    let dir = tmp("route");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    let q = Query::new("pts")
        .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(&[1.0, 0.5])))
        .limit(2);
    match db.explain(q).expect("explain").scan {
        ScanType::VectorKnn { field, metric, .. } => {
            assert_eq!(field, "emb");
            assert_eq!(metric, Metric::Cosine);
        }
        other => panic!("expected VectorKnn, got {other:?}"),
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn exact_cosine_order_and_k() {
    let dir = tmp("cos");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    let got: Vec<String> = db
        .find_near("pts", "emb", &[1.0, 0.5], 10)
        .expect("knn")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    // Ground truth (cosine distance from (1,0.5)):
    // a: 1-1/sqrt(1.25)≈0.1056; c: 1-1.5/sqrt(2*1.25)≈0.0513;
    // b: 1-0.5/sqrt(1.25)≈0.5528; d: same direction as a ≈0.1056.
    // Order: c, then a/d tie (doc-id breaks: a before d), then b.
    assert_eq!(got, vec!["c", "a", "d", "b"], "nearest-first, ties by id");

    let top2: Vec<String> = db
        .find_near("pts", "emb", &[1.0, 0.5], 2)
        .expect("knn2")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(top2, vec!["c", "a"], "k truncates after ordering");
    let _ = std::fs::remove_dir_all(&dir);
}

/// #141-8 (Phase B): Inner Product — distance = -dot, so magnitude matters
/// where cosine is scale-invariant. Same 3 docs: cosine ties a/c ahead of
/// b; IP orders c, a, b.
#[test]
fn inner_product_orders_by_dot() {
    let dir = tmp("ip");
    let db = open_db(&dir);
    db.create_vector_index("ips", "emb", 2, Metric::InnerProduct)
        .expect("create ip");
    let mut batch = Vec::new();
    for (id, p) in [
        ("a", vec![2.0, 0.0]),
        ("b", vec![1.0, 1.0]),
        ("c", vec![3.0, 0.0]),
    ] {
        let mut doc = HakoDoc::default();
        doc.insert("emb", Value::Binary(encode_f32s(&p)));
        batch.push(BatchMutation::Put {
            collection: "ips".into(),
            doc_id: id.into(),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
    let got: Vec<String> = db
        .find_near("ips", "emb", &[1.0, 0.0], 10)
        .expect("ip knn")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(got, vec!["c", "a", "b"], "inner product: magnitude matters");
    // Scored surface: distance = negated dot, exactly.
    let scores: Vec<f64> = db
        .find_near_scored("ips", "emb", &[1.0, 0.0], 10, None)
        .expect("ip scored")
        .into_iter()
        .map(|(_, s, _)| s)
        .collect();
    assert_eq!(scores, vec![-3.0, -2.0, -1.0]);
    let _ = std::fs::remove_dir_all(&dir);
}

/// #141-1 (Phase B): explicit HNSW params persist in the index definition
/// and survive reopen (describe surface carries them through).
#[test]
fn hnsw_params_persist_through_reopen() {
    let dir = tmp("params");
    {
        let db = open_db(&dir);
        db.create_vector_index_with_params(
            "ptuned",
            "emb",
            2,
            Metric::Cosine,
            HnswParams { m: Some(32), ef_construction: Some(400), ef_search: Some(1024) },
        )
        .expect("create with params");
        db.write_batch(vec![BatchMutation::Put {
            collection: "ptuned".into(),
            doc_id: "v1".into(),
            doc: vdoc(&[1.0, 0.5], "x"),
        }])
        .expect("seed");
        assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
    }
    let db = open_db(&dir);
    let info = &db.list_indexes(Some("ptuned")).vector["ptuned"][0];
    assert_eq!(info.m, Some(32), "M persists");
    assert_eq!(info.ef_construction, Some(400), "ef_construction persists");
    assert_eq!(info.ef_search, Some(1024), "ef_search persists");
    let _ = std::fs::remove_dir_all(&dir);
}

/// #141-5 (Phase B): batched find_near — per-query results match the
/// single-query path 1:1.
#[test]
fn find_near_batch_matches_single() {
    let dir = tmp("batch");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    let q1 = [1.0f32, 0.5];
    let q2 = [0.0f32, 1.0];
    let batch = db
        .find_near_batch("pts", "emb", &[&q1, &q2], 3)
        .expect("batch");
    assert_eq!(batch.len(), 2);
    let single: Vec<Vec<String>> = [&q1, &q2]
        .iter()
        .map(|q| {
            db.find_near("pts", "emb", *q, 3)
                .expect("single")
                .into_iter()
                .map(|(id, _)| id)
                .collect()
        })
        .collect();
    for (b, s) in batch.iter().zip(single.iter()) {
        let bids: Vec<String> = b.iter().map(|(id, _)| id.clone()).collect();
        assert_eq!(&bids, s, "batch result matches single-query result");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// #141-3 / #141-11 (Phase A): `find_near_scored` returns the exact
/// distance per hit (ascending) and honors the `max_distance` threshold.
#[test]
fn find_near_scored_distances_and_threshold() {
    let dir = tmp("scored");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    let hits = db
        .find_near_scored("pts", "emb", &[1.0, 0.5], 10, None)
        .expect("scored knn");
    let got: Vec<String> = hits.iter().map(|(id, _, _)| id.clone()).collect();
    assert_eq!(got, vec!["c", "a", "d", "b"], "same order as find_near");
    // Scores are exact distances, ascending, re-derivable from the stored
    // vectors.
    let mut prev = 0.0f64;
    for (id, score, doc) in &hits {
        assert!(*score >= prev - 1e-12, "scores ascending: {hits:?}");
        prev = *score;
        let stored = match doc.get("emb") {
            Some(Value::Binary(b)) => decode_f32s(b).expect("stored vector"),
            other => panic!("expected binary vector, got {other:?}"),
        };
        let expected = cosine_distance(&[1.0, 0.5], &stored);
        assert!((score - expected).abs() < 1e-9, "exact rescore for {id}");
    }
    // Threshold: b (~0.55) drops out, c/a/d stay.
    let kept: Vec<String> = db
        .find_near_scored("pts", "emb", &[1.0, 0.5], 10, Some(0.2))
        .expect("threshold")
        .into_iter()
        .map(|(id, _, _)| id)
        .collect();
    assert_eq!(kept, vec!["c", "a", "d"], "b (~0.55) filtered out");
    // Unindexed field: fail-closed empty (mirrors find_near).
    assert!(db.find_near_scored("pts", "nope", &[1.0], 5, None).unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn l2_metric_orders_by_euclidean() {
    let dir = tmp("l2");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    db.create_vector_index("pts", "emb2", 2, Metric::L2)
        .expect("create l2");
    // Mirror the embeddings into emb2 for an L2-indexed field.
    let mut batch = Vec::new();
    for (id, p) in [("a", [1.0, 0.0]), ("b", [0.0, 1.0]), ("c", [1.0, 1.0]), ("d", [10.0, 0.0])] {
        let mut doc = vdoc(&p, if id == "a" || id == "b" { "x" } else { "y" });
        doc.insert("emb2", Value::Binary(encode_f32s(&p)));
        batch.push(BatchMutation::Put {
            collection: "pts".into(),
            doc_id: id.to_string(),
            doc,
        });
    }
    db.write_batch(batch).expect("reseed");
    wait_ready(&db);
    let got: Vec<String> = db
        .find_near("pts", "emb2", &[1.0, 0.5], 10)
        .expect("knn")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    // L2-squared: a=0.25, c=0.25 (tie → a first), b=1.25, d=81.25.
    assert_eq!(got, vec!["a", "c", "b", "d"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_eq_plus_near() {
    let dir = tmp("hybrid");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    // grp=y restricts to {c, d}; nearest-first within that set.
    let q = Query::new("pts")
        .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(&[1.0, 0.5])))
        .where_filter("grp", Operator::Eq, Value::String("y".into()))
        .limit(10);
    let got: Vec<String> = db.query(q).expect("q").into_iter().map(|(id, _)| id).collect();
    assert_eq!(got, vec!["c", "d"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fail_closed_shapes() {
    let dir = tmp("closed");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    // No index on field → empty, not unranked rows.
    let q = Query::new("pts")
        .where_filter("nope", Operator::Near, Value::Binary(encode_f32s(&[1.0, 0.5])))
        .limit(10);
    assert!(db.query(q).expect("q").is_empty());
    // Dim mismatch via find_near → hard error.
    assert!(db.find_near("pts", "emb", &[1.0, 0.5, 0.0], 5).is_err());
    // Dim mismatch via raw Query → empty (planner gate).
    let q = Query::new("pts")
        .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(&[1.0, 0.5, 0.0])))
        .limit(10);
    assert!(db.query(q).expect("q").is_empty());
    // Non-binary value → empty.
    let q = Query::new("pts")
        .where_filter("emb", Operator::Near, Value::String("junk".into()))
        .limit(10);
    assert!(db.query(q).expect("q").is_empty());
    // Unknown collection-field → empty.
    assert!(db.find_near("pts", "missing", &[1.0, 0.5], 5).expect("q").is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn plan_cache_isolates_query_vectors() {
    let dir = tmp("cacheiso");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    // Same shape, same dim, different vectors — pre-fix these shared one
    // cached plan (with the FIRST vector) and returned identical rows.
    let r1: Vec<String> = db
        .find_near("pts", "emb", &[1.0, 0.0], 1)
        .expect("q1")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    let r2: Vec<String> = db
        .find_near("pts", "emb", &[0.0, 1.0], 1)
        .expect("q2")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert_eq!(r1, vec!["a"]);
    assert_eq!(r2, vec!["b"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn zero_vector_and_offset() {
    let dir = tmp("zero");
    let db = open_db(&dir);
    seed(&db);
    wait_ready(&db);
    db.put("pts", "z", &vdoc(&[0.0, 0.0], "x")).expect("put zero");
    // The RAM graph lags storage by the async index-worker window —
    // wait it out (same rule as every other RAM family).
    wait_ready(&db);
    // Zero vector scores finite (1.0) — returned when k covers it.
    let got: Vec<String> = db
        .find_near("pts", "emb", &[1.0, 0.5], 10)
        .expect("q")
        .into_iter()
        .map(|(id, _)| id)
        .collect();
    assert!(got.contains(&"z".to_string()), "zero vector must be finite-ranked, got {got:?}");
    // Offset applies after distance ordering: skip c, take a.
    let q = Query::new("pts")
        .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(&[1.0, 0.5])))
        .limit(1)
        .offset(1);
    let page: Vec<String> = db.query(q).expect("q").into_iter().map(|(id, _)| id).collect();
    assert_eq!(page, vec!["a"], "offset(1) skips nearest c");
    let _ = std::fs::remove_dir_all(&dir);
}

// ===== Phase D (#156): 2-stage hybrid ANN + #141-7 index stats =====

#[test]
fn vector_index_info_reports_live_stats() {
    let dir = tmp("stats");
    {
        let db = open_db(&dir);
        seed(&db);
        wait_ready(&db);
        let info = &db.list_indexes(Some("pts")).vector["pts"][0];
        assert_eq!(info.indexed, 4, "all seeded vectors are in the graph");
        assert!(info.complete, "backfill finished → ANN authoritative");
        assert!(info.mem_estimate > 0, "upper-bound estimate present");
    }
    {
        // Graphs rebuild on open; stats must converge again.
        let db = open_db(&dir);
        wait_ready(&db);
        let info = &db.list_indexes(Some("pts")).vector["pts"][0];
        assert_eq!(info.indexed, 4);
        assert!(info.complete);
        assert!(info.mem_estimate > 0);
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_2stage_ann_matches_filter_and_stays_exact() {
    let dir = tmp("hybrid2");
    let db = open_db(&dir);
    // 200 docs, d000..d199, even = "a", odd = "b"; deterministic 8-dim
    // vectors so the test can compute the exact ground truth itself.
    let q = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8];
    let emb = |i: usize| -> [f32; 8] {
        core::array::from_fn(|j| (((i * 13 + j * 7) % 11) as f32) - 5.5)
    };
    let mutations: Vec<BatchMutation> = (0..200)
        .map(|i| BatchMutation::Put {
            collection: "pts".into(),
            doc_id: format!("d{i:03}"),
            doc: vdoc(&emb(i), if i % 2 == 0 { "a" } else { "b" }),
        })
        .collect();
    db.write_batch(mutations).expect("seed");
    db.create_vector_index("pts", "emb", 8, Metric::Cosine).expect("create");
    wait_ready(&db);

    // (1) Filtered ANN (2-stage): every row matches, exact distances are
    // non-decreasing, count == limit.
    let rows = db
        .query(
            Query::new("pts")
                .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(&q)))
                .where_filter("grp", Operator::Eq, Value::String("b".into()))
                .limit(10),
        )
        .expect("hybrid query");
    assert_eq!(rows.len(), 10, "fills to the limit, never strands");
    let mut last: Option<f64> = None;
    for (id, doc) in &rows {
        assert_eq!(doc.get("grp"), Some(&Value::String("b".into())), "{id} must match grp=b");
        let dv = match doc.get("emb") {
            Some(Value::Binary(b)) => decode_f32s(b).expect("f32"),
            other => panic!("expected binary vector, got {other:?}"),
        };
        let d = cosine_distance(&q, &dv);
        if let Some(prev) = last {
            assert!(d >= prev, "rows must stay nearest-first: {id} {d} after {prev}");
        }
        last = Some(d);
    }

    // (2) Under-fill (grp="rare": 4 docs of 200 — d007/d057/d107/d157):
    // widening ×4/×16 cannot fill 10, so the exact fallback serves —
    // result must EQUAL the ground truth for that filter (full recall
    // by construction).
    let rare: Vec<usize> = (0..200).filter(|i| i % 50 == 7).collect();
    assert_eq!(rare, vec![7, 57, 107, 157], "fixture: sparse filter band");
    let rare_ids: Vec<String> = rare.iter().map(|i| format!("d{i:03}")).collect();
    // Stamp the rare half with grp="rare" (keep the rest for ANN density).
    let stamps: Vec<BatchMutation> = rare
        .iter()
        .map(|&i| BatchMutation::Put {
            collection: "pts".into(),
            doc_id: format!("d{i:03}"),
            doc: vdoc(&emb(i), "rare"),
        })
        .collect();
    db.write_batch(stamps).expect("stamp");
    wait_ready(&db);
    let rows = db
        .query(
            Query::new("pts")
                .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(&q)))
                .where_filter("grp", Operator::Eq, Value::String("rare".into()))
                .limit(10),
        )
        .expect("underfilled query");
    let mut got: Vec<String> = rows.into_iter().map(|(id, _)| id).collect();
    got.sort();
    assert_eq!(got, rare_ids, "fallback must surface every rare match");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn def_survives_restart() {
    let dir = tmp("restart");
    {
        let db = open_db(&dir);
        seed(&db);
    wait_ready(&db);
        let n = db.find_near("pts", "emb", &[1.0, 0.5], 2).expect("q").len();
        assert_eq!(n, 2);
    }
    {
        let db = open_db(&dir);
        wait_ready(&db);
        let listed = db.list_indexes(Some("pts"));
        assert!(
            listed.vector.get("pts").is_some_and(|v| v.iter().any(|i| i.field == "emb" && i.dim == 2)),
            "vector def must persist across restart"
        );
        let got: Vec<String> = db
            .find_near("pts", "emb", &[1.0, 0.5], 2)
            .expect("q2")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(got, vec!["c", "a"]);
    }
    let _ = std::fs::remove_dir_all(&dir);
}
