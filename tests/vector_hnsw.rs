//! P1 HNSW: the ANN path engages (complete flag), matches the P0 exact
//! oracle, deletes stay invisible, updates move rankings, graphs rebuild
//! on reopen. The oracle is brute force computed in-test — P0 stays the
//! ground truth for every ANN assertion.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::vector::{encode_f32s, Metric};

const DIM: usize = 8;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-hnsw-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        let q = db.quiescence_status();
        if db.is_indexes_ready() && q.index_backfills == 0 && q.pending_index_ops == 0 {
            return;
        }
        assert!(t0.elapsed() < std::time::Duration::from_secs(60), "never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn wait_complete(db: &Hako, col: &str, field: &str) {
    wait_ready(db);
    let t0 = std::time::Instant::now();
    loop {
        if db.vector_index_complete(col, field) {
            return;
        }
        assert!(t0.elapsed() < std::time::Duration::from_secs(60), "graph never completes");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

/// Deterministic fixture RNG (same xorshift shape as the engine's).
struct Rng(u64);
impl Rng {
    fn next_f32(&mut self) -> f32 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        ((self.0 >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
    }
}

/// Seed n docs; returns the point list (index i == doc d{i:04}).
fn seed(db: &Hako, n: usize, seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    let pts: Vec<Vec<f32>> = (0..n).map(|_| (0..DIM).map(|_| rng.next_f32()).collect()).collect();
    let batch = pts
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let mut doc = HakoDoc::default();
            doc.insert("emb", Value::Binary(encode_f32s(p)));
            doc.insert("grp", Value::String(if i % 2 == 0 { "ev" } else { "od" }.into()));
            BatchMutation::Put {
                collection: "pts".into(),
                doc_id: format!("d{i:04}"),
                doc,
            }
        })
        .collect();
    db.write_batch(batch).expect("seed");
    db.create_vector_index("pts", "emb", DIM as u32, Metric::Cosine)
        .expect("create");
    pts
}

/// #190 fixture: same shape as `seed` but the index opts into binary
/// stage-1 (popcount traversal, int8/f32 exact rescore unchanged), with
/// a 64-dim clustered layout — an 8-bit stage-1 over uniform randoms
/// can't rank (hamming 0..8; randoms concentrate at ~90°), while real
/// embeddings are anisotropic and sign bits track cluster direction.
fn seed_binary(db: &Hako, n: usize, seed: u64) -> Vec<Vec<f32>> {
    const BDIM: usize = 64;
    let mut rng = Rng(seed);
    let center = |c: usize, j: usize| -> f32 {
        ((c * 7919 + j * 104729) % 1000) as f32 / 500.0 - 1.0
    };
    let pts: Vec<Vec<f32>> = (0..n)
        .map(|i| {
            (0..BDIM)
                .map(|j| center(i % 25, j) + rng.next_f32() * 0.05)
                .collect()
        })
        .collect();
    let batch = pts
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let mut doc = HakoDoc::default();
            doc.insert("emb", Value::Binary(encode_f32s(p)));
            doc.insert("grp", Value::String(if i % 2 == 0 { "ev" } else { "od" }.into()));
            BatchMutation::Put {
                collection: "pts".into(),
                doc_id: format!("d{i:04}"),
                doc,
            }
        })
        .collect();
    db.write_batch(batch).expect("seed");
    db.create_vector_index_with_params(
        "pts",
        "emb",
        BDIM as u32,
        Metric::Cosine,
        hakodb::index::vector::HnswParams {
            m: None,
            ef_construction: None,
            ef_search: None,
            binary_stage1: true,
        },
    )
    .expect("create binary-stage1 index");
    pts
}

/// Queries that sit inside the binary fixture's clusters (a fresh
/// center + small noise), so top-10 = own cluster + nearest boundary.
fn binary_queries(seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    let center = |c: usize, j: usize| -> f32 {
        ((c * 7919 + j * 104729) % 1000) as f32 / 500.0 - 1.0
    };
    (0..10)
        .map(|qi| {
            (0..64)
                .map(|j| center(qi * 2 % 25, j) + rng.next_f32() * 0.05)
                .collect()
        })
        .collect()
}

fn cosine(a: &[f32], b: &[f32]) -> f64 {
    let (mut dot, mut na, mut nb) = (0.0f64, 0.0, 0.0);
    for i in 0..a.len() {
        let (x, y) = (a[i] as f64, b[i] as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let d = na.sqrt() * nb.sqrt();
    if d == 0.0 { 1.0 } else { 1.0 - dot / d }
}

/// Exact oracle over a live set (deleted ids excluded by the caller).
fn brute_top(q: &[f32], pts: &[Vec<f32>], live: &[bool], k: usize) -> Vec<String> {
    let mut scored: Vec<(f64, usize)> = pts
        .iter()
        .enumerate()
        .filter(|(i, _)| live[*i])
        .map(|(i, p)| (cosine(q, p), i))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, i)| format!("d{i:04}")).collect()
}

fn queries(seed: u64) -> Vec<Vec<f32>> {
    let mut rng = Rng(seed);
    (0..10).map(|_| (0..DIM).map(|_| rng.next_f32()).collect()).collect()
}

fn knn_ids(db: &Hako, q: &[f32], k: usize) -> Vec<String> {
    db.find_near("pts", "emb", q, k).expect("knn").into_iter().map(|(id, _)| id).collect()
}

#[test]
fn ann_matches_exact() {
    let dir = tmp("match");
    let db = open_db(&dir);
    let pts = seed(&db, 300, 0xA1);
    wait_complete(&db, "pts", "emb");
    let live = vec![true; pts.len()];
    for q in &queries(0xB2) {
        assert_eq!(knn_ids(&db, q, 10), brute_top(q, &pts, &live, 10));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn delete_stays_invisible() {
    let dir = tmp("del");
    let db = open_db(&dir);
    let pts = seed(&db, 100, 0xC3);
    wait_complete(&db, "pts", "emb");
    // Delete the exact top-1 for the first query.
    let q = &queries(0xD4)[0];
    let mut live = vec![true; pts.len()];
    let top = brute_top(q, &pts, &live, 1).pop().expect("top");
    let idx: usize = top[1..].parse().expect("id");
    db.delete("pts", &top).expect("delete");
    live[idx] = false;
    // Liveness is external: the graph still routes through the node, the
    // executor filters it. Oracle (which never had it) must match.
    assert_eq!(knn_ids(&db, q, 10), brute_top(q, &pts, &live, 10));
    assert!(!knn_ids(&db, q, 10).contains(&top));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn update_moves_ranking() {
    let dir = tmp("upd");
    let db = open_db(&dir);
    let mut pts = seed(&db, 100, 0xE5);
    wait_complete(&db, "pts", "emb");
    let q = &queries(0xF6)[0];
    // Teleport d0042 onto the query: update-in-place must take effect.
    let mut doc = HakoDoc::default();
    doc.insert("emb", Value::Binary(encode_f32s(q)));
    doc.insert("grp", Value::String("ev".into()));
    db.put("pts", "d0042", &doc).expect("reput");
    wait_ready(&db);
    pts[42] = q.clone();
    let live = vec![true; pts.len()];
    let got = knn_ids(&db, q, 5);
    assert_eq!(got[0], "d0042");
    assert_eq!(got, brute_top(q, &pts, &live, 5));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_stays_exact_with_complete_graph() {
    use hakodb::query::filter::Operator;
    use hakodb::query::query::Query;
    let dir = tmp("hybrid");
    let db = open_db(&dir);
    let pts = seed(&db, 200, 0x11);
    wait_complete(&db, "pts", "emb");
    // Hybrid brute-forces by design (ANN ranks unfiltered); the oracle
    // filters first. grp=ev keeps even ids.
    for q in queries(0x22).iter().take(3) {
        let got: Vec<String> = db
            .query(
                Query::new("pts")
                    .where_filter("emb", Operator::Near, Value::Binary(encode_f32s(q)))
                    .where_filter("grp", Operator::Eq, Value::String("ev".into()))
                    .limit(10),
            )
            .expect("q")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        let live: Vec<bool> = (0..pts.len()).map(|i| i % 2 == 0).collect();
        assert_eq!(got, brute_top(q, &pts, &live, 10));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rebuild_on_reopen() {
    let dir = tmp("reopen");
    let pts = {
        let db = open_db(&dir);
        let pts = seed(&db, 150, 0x33);
        wait_complete(&db, "pts", "emb");
        pts
    };
    {
        let db = open_db(&dir);
        // Fresh snapshot (clean Drop) skips the rescan — STEP C2 rebuilds;
        // stale snapshot rescans through the hook. Either way the
        // invariant is: complete again, results exact.
        wait_complete(&db, "pts", "emb");
        let live = vec![true; pts.len()];
        for q in &queries(0x44) {
            assert_eq!(knn_ids(&db, q, 10), brute_top(q, &pts, &live, 10));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// #191: compaction reaps tombstones from storage; the HNSW graph (whose
/// nodes are never removed) must recycle through the same backfill, so
/// dead ids leave the graph instead of riding it forever.
#[test]
fn compact_gc_reaps_dead_graph_nodes() {
    let dir = tmp("gc");
    let db = open_db(&dir);
    let pts = seed(&db, 100, 0x77);
    wait_complete(&db, "pts", "emb");
    assert_eq!(db.vector_graph_len("pts", "emb"), Some(100));
    // Delete 30: graph keeps them (liveness filters), storage tombstones.
    let mut live = vec![true; pts.len()];
    for (i, dead) in live.iter_mut().enumerate() {
        if i % 10 < 3 {
            db.delete("pts", &format!("d{i:04}")).expect("delete");
            *dead = false;
        }
    }
    assert_eq!(db.vector_graph_len("pts", "emb"), Some(100), "no GC yet");
    // Compaction + graph recycle: dead ids leave, ANN serves again.
    db.compact_collection("pts").expect("compact");
    wait_complete(&db, "pts", "emb");
    assert_eq!(
        db.vector_graph_len("pts", "emb"),
        Some(70),
        "dead ids must leave the graph"
    );
    let q = &queries(0x88)[0];
    assert_eq!(knn_ids(&db, q, 10), brute_top(q, &pts, &live, 10));
    let _ = std::fs::remove_dir_all(&dir);
}

/// #189: the graph rides the index snapshot — reopen restores it and the
/// repair pass re-upserts live docs (post-snapshot writes included)
/// before ANN serves. Invariant: complete + exact, same node count.
#[test]
fn graph_persists_and_repairs_on_reopen() {
    let dir = tmp("persist");
    let pts = {
        let db = open_db(&dir);
        let pts = seed(&db, 150, 0x99);
        wait_complete(&db, "pts", "emb");
        assert_eq!(db.vector_graph_len("pts", "emb"), Some(150));
        pts
    };
    {
        let db = open_db(&dir);
        wait_complete(&db, "pts", "emb");
        assert_eq!(
            db.vector_graph_len("pts", "emb"),
            Some(150),
            "restored graph must hold every live node after repair"
        );
        let live = vec![true; pts.len()];
        for q in &queries(0xAA) {
            assert_eq!(knn_ids(&db, q, 10), brute_top(q, &pts, &live, 10));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

/// #189 stale side: a snapshot taken before a delete is stale (WAL tails
/// mismatch), so the rescan path clears and refills the graph through the
/// hook — dead id gone, exact results, not the snapshotted count.
#[test]
fn stale_snapshot_rescans_graph() {
    let dir = tmp("stale");
    let pts = {
        let db = open_db(&dir);
        let pts = seed(&db, 100, 0xBB);
        wait_complete(&db, "pts", "emb");
        db.delete("pts", "d0003").expect("delete");
        pts
    };
    {
        let db = open_db(&dir);
        wait_complete(&db, "pts", "emb");
        assert_eq!(db.vector_graph_len("pts", "emb"), Some(99));
        let mut live = vec![true; pts.len()];
        live[3] = false;
        for q in &queries(0xCC) {
            assert_eq!(knn_ids(&db, q, 10), brute_top(q, &pts, &live, 10));
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn empty_graph_is_exact_trivially() {
    let dir = tmp("empty");
    let db = open_db(&dir);
    db.create_vector_index("pts", "emb", DIM as u32, Metric::Cosine).expect("create");
    wait_ready(&db);
    let q = vec![0.5; DIM];
    assert!(db.find_near("pts", "emb", &q, 5).expect("q").is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

/// #190: a binary-stage1 index traverses on popcount Hamming (sign
/// bitsets) with the exact rescore unchanged, and the bitsets persist +
/// restore with the graph (#189 path; params match keeps the graph, not
/// a rebuild). Recall vs the brute oracle holds at the toy epsilon; the
/// duel harness owns the 0.99 gate at embedding dims.
#[test]
fn binary_stage1_serves_exact_and_persists() {
    let dir = tmp("bin1");
    let pts = {
        let db = open_db(&dir);
        let pts = seed_binary(&db, 150, 0xDD);
        wait_complete(&db, "pts", "emb");
        assert_eq!(db.vector_graph_len("pts", "emb"), Some(150));
        let live = vec![true; pts.len()];
        for q in &binary_queries(0xEE) {
            let got = knn_ids(&db, q, 10);
            let want = brute_top(q, &pts, &live, 10);
            let hits = got.iter().filter(|id| want.contains(id)).count();
            assert!(hits >= 9, "binary stage-1 recall {hits}/10");
        }
        pts
    };
    {
        let db = open_db(&dir);
        wait_complete(&db, "pts", "emb");
        assert_eq!(db.vector_graph_len("pts", "emb"), Some(150));
        let live = vec![true; pts.len()];
        for q in &binary_queries(0xFF) {
            let got = knn_ids(&db, q, 10);
            let want = brute_top(q, &pts, &live, 10);
            let hits = got.iter().filter(|id| want.contains(id)).count();
            assert!(hits >= 9, "binary stage-1 recall {hits}/10 after reopen");
        }
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn blob_resident_vectors_index_on_live_writes() {
    // Force embeddings over the blob threshold: at 64B a dim-32 vector
    // (128B) is blob-resident. Index created FIRST on empty, then docs
    // arrive via live puts — this exercises the worker inflate-on-index
    // path, not the create backfill (which always resolved).
    const BDIM: usize = 32;
    let dir = tmp("blob");
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.value_blob_threshold_bytes = 64;
    let db = Hako::open(&dir, cfg).expect("open");
    db.create_vector_index("pts", "emb", BDIM as u32, Metric::Cosine).expect("create");
    let mut rng = Rng(0x5EED);
    let pts: Vec<Vec<f32>> = (0..50).map(|_| (0..BDIM).map(|_| rng.next_f32()).collect()).collect();
    let batch = pts
        .iter()
        .enumerate()
        .map(|(i, p)| {
            let mut doc = HakoDoc::default();
            doc.insert("emb", Value::Binary(encode_f32s(p)));
            BatchMutation::Put {
                collection: "pts".into(),
                doc_id: format!("d{i:04}"),
                doc,
            }
        })
        .collect();
    db.write_batch(batch).expect("seed");
    wait_complete(&db, "pts", "emb");
    // Sanity: the vectors really live in the blob file.
    let blob_len = std::fs::metadata(dir.join("pts").join("blobs.dat")).expect("blobs").len();
    assert!(blob_len > 0, "embeddings must be blob-resident for this test");
    // Oracle over f32 docs; engine must agree exactly (ANN engaged —
    // wait_complete proved the flag — plus exact rescore).
    let mut rng = Rng(0x60ED);
    let live = vec![true; pts.len()];
    for _ in 0..5 {
        let q: Vec<f32> = (0..BDIM).map(|_| rng.next_f32()).collect();
        let got: Vec<String> = db.find_near("pts", "emb", &q, 10).expect("knn").into_iter().map(|(id, _)| id).collect();
        assert_eq!(got, brute_top_dim(&q, &pts, &live, 10));
    }
    let _ = std::fs::remove_dir_all(&dir);
}

fn brute_top_dim(q: &[f32], pts: &[Vec<f32>], live: &[bool], k: usize) -> Vec<String> {
    let mut scored: Vec<(f64, usize)> = pts
        .iter()
        .enumerate()
        .filter(|(i, _)| live[*i])
        .map(|(i, p)| (cosine(q, p), i))
        .collect();
    scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
    scored.into_iter().take(k).map(|(_, i)| format!("d{i:04}")).collect()
}

/// #195 parallel bulk build: 2400 docs over 400-doc chunks (6 chunks)
/// build in worker-local graphs, merge, then a sequential repair pass.
/// Recall holds against the brute oracle, and the graph is identical
/// across worker counts (fixed partitions + fixed merge order ⇒ the
/// worker count changes wall-clock, never the result). Knobs: all other
/// fixtures here stay < 400 docs, so they can't flip their sequential
/// paths while these env values are set.
#[test]
fn parallel_bulk_deterministic_and_exact() {
    let queries: Vec<Vec<f32>> = queries(0x61)
        .into_iter()
        .chain(queries(0x62))
        .collect();

    let build = |dir: &std::path::Path, workers: &str| -> Vec<Vec<String>> {
        std::env::set_var("HKO_HNSW_PAR_CHUNK", "400");
        std::env::set_var("HKO_HNSW_PAR_WORKERS", workers);
        let db = open_db(dir);
        let pts = seed(&db, 2400, 0x51);
        wait_complete(&db, "pts", "emb");
        assert_eq!(db.vector_graph_len("pts", "emb"), Some(2400), "all docs indexed");
        let live = vec![true; pts.len()];
        let got: Vec<Vec<String>> = queries.iter().map(|q| knn_ids(&db, q, 10)).collect();
        for (q, ids) in queries.iter().zip(&got) {
            let hits = ids.iter().filter(|id| brute_top(q, &pts, &live, 10).contains(id)).count();
            assert!(hits >= 9, "recall {hits}/10 for parallel build");
        }
        got
    };

    let dir4 = tmp("par4");
    let got4 = build(&dir4, "4");
    let dir1 = tmp("par1");
    let got1 = build(&dir1, "1");
    std::env::remove_var("HKO_HNSW_PAR_CHUNK");
    std::env::remove_var("HKO_HNSW_PAR_WORKERS");
    assert_eq!(got4, got1, "1 vs 4 workers must produce identical graphs");
    let _ = std::fs::remove_dir_all(&dir4);
    let _ = std::fs::remove_dir_all(&dir1);
}
