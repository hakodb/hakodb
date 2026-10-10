//! P0 exact KNN: routing, ordering, hybrid filters, fail-closed shapes,
//! plan-cache isolation, restart persistence of vector defs.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::vector::{encode_f32s, Metric};
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
/// (covered by `index_backfills`). Every test waits past both windows
/// before asserting.
fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        if db.is_indexes_ready() && db.quiescence_status().index_backfills == 0 {
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
