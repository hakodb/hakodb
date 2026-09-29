//! Proves queries really hit indexes (not just the planner in isolation):
//! same rows via FullCollection and SecondaryIndex, with the ScanType
//! pinned through the public `explain()` path.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::plan::ScanType;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-idxroute-{}-{}",
        label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn seed(db: &Hako, n: usize) {
    let mut batch = Vec::with_capacity(n);
    for i in 0..n {
        let mut doc = HakoDoc::default();
        doc.insert("tenant", Value::String(format!("tenant-{}", i % 32)));
        doc.insert("age", Value::Int(18 + (i % 70) as i64));
        batch.push(BatchMutation::Put {
            collection: "rt".into(),
            doc_id: format!("d{i:05}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
}

fn wait_quiescent(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        if db.quiescence_status().index_backfills == 0 {
            return;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(60),
            "backfill never drains"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn tenant_q() -> Query {
    Query::new("rt")
        .where_eq("tenant", Value::String("tenant-7".into()))
        .limit(100)
}

fn scan_of(db: &Hako, q: Query) -> ScanType {
    db.explain(q).expect("explain").scan
}

#[test]
fn filter_eq_rides_secondary_index_when_ready() {
    let dir = tmp("big");
    let db = open_db(&dir);
    seed(&db, 1000);

    // No index yet: full scan by necessity.
    assert!(
        matches!(scan_of(&db, tenant_q()), ScanType::FullCollection),
        "expected FullCollection without index"
    );
    let base: Vec<String> = db.query(tenant_q()).expect("q").into_iter().map(|(id, _)| id).collect();
    assert!(!base.is_empty(), "seed sanity");

    // Build + drain, then the same shape must ride the secondary index…
    db.create_index("rt", "tenant").expect("create");
    wait_quiescent(&db);
    assert!(
        matches!(scan_of(&db, tenant_q()), ScanType::SecondaryIndex { .. }),
        "expected SecondaryIndex after build"
    );
    // …with identical rows (order may differ: scan follows storage
    // order, the index its own — compare as sets).
    let fast: Vec<String> = db.query(tenant_q()).expect("q2").into_iter().map(|(id, _)| id).collect();
    let mut base_sorted = base.clone();
    let mut fast_sorted = fast.clone();
    base_sorted.sort();
    fast_sorted.sort();
    assert_eq!(base_sorted.len(), fast_sorted.len(), "row count diverges");
    for (b, f) in base_sorted.iter().zip(fast_sorted.iter()) {
        if b != f {
            panic!("first divergence: scan={b} index={f} (lens {} vs {})", base_sorted.len(), fast_sorted.len());
        }
    }

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn small_collection_skips_index_by_heuristic() {
    let dir = tmp("small");
    let db = open_db(&dir);
    seed(&db, 20);
    db.create_index("rt", "tenant").expect("create");
    wait_quiescent(&db);
    // 20 rows / workers < 50-per-thread heuristic: full scan on purpose.
    assert!(
        matches!(scan_of(&db, tenant_q()), ScanType::FullCollection),
        "small collection should scan, not index"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
