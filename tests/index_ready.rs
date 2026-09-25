use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::query::Query;

const N: usize = 500;

fn open_bench(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    while !db.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn wait_quiescent(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        if db.quiescence_status().index_backfills == 0 {
            return;
        }
        assert!(t0.elapsed() < std::time::Duration::from_secs(60), "backfill never drains");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn seed(db: &Hako) {
    let mut batch = Vec::with_capacity(N);
    for i in 0..N {
        let mut doc = HakoDoc::default();
        doc.insert("age", Value::Int((i % 50) as i64));
        batch.push(BatchMutation::Put {
            collection: "rd".into(),
            doc_id: format!("d{i:05}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    wait_ready(db);
}

fn ages(db: &Hako) -> Vec<String> {
    let q = Query::new("rd")
        .where_eq("age", Value::Int(7))
        .order_by("age", false)
        .limit(100);
    db.query(q).expect("q").into_iter().map(|(id, _)| id).collect()
}

/// Queries issued DURING a runtime index build must see complete results
/// (FullCollection fallback), never the partial index. Same rows before,
/// during, and after the build.
#[test]
fn runtime_build_never_serves_partial() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-idxready-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);
    // Baseline before the index exists (full scan): 10 docs with age 7.
    let base = ages(&db);
    assert_eq!(base.len(), 10, "seed sanity");

    // Create the index and query IMMEDIATELY — no waiting. Pre-fix this
    // returned partial/empty rows from the half-built secondary index.
    db.create_index("rd", "age").expect("create");
    for _ in 0..20 {
        assert_eq!(ages(&db), base, "partial index served");
    }
    // After the drain the fast path serves the same rows.
    wait_quiescent(&db);
    assert_eq!(ages(&db), base, "post-build divergence");
    let _ = std::fs::remove_dir_all(&dir);
}
