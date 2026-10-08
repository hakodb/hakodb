//! Planner picks the smallest Eq posting regardless of filter order
//! (selectivity probe), with row-identical results to a full scan.
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
        "fl-test-sel-{label}-{}",
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
        // 90% hot, 10% spread over rare values.
        let grp = if i % 10 == 0 {
            format!("rare-{}", i % 100)
        } else {
            "hot".to_string()
        };
        doc.insert("grp", Value::String(grp));
        doc.insert("tenant", Value::String(format!("tenant-{}", i % 32)));
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

#[test]
fn multi_eq_rides_smallest_posting_first_order_hot() {
    let dir = tmp("hotfirst");
    let db = open_db(&dir);
    seed(&db, 2000);
    db.create_index("rt", "grp").expect("c1");
    db.create_index("rt", "tenant").expect("c2");
    wait_quiescent(&db);

    // Hot (90%) filter listed FIRST — the probe must still ride tenant.
    let q = Query::new("rt")
        .where_eq("grp", Value::String("hot".into()))
        .where_eq("tenant", Value::String("tenant-7".into()));
    match db.explain(q.clone()).expect("explain").scan {
        ScanType::SecondaryIndex { field, .. } => {
            assert_eq!(field, "tenant", "probe should pick the rare posting");
        }
        other => panic!("expected SecondaryIndex, got {other:?}"),
    }
    let mut got: Vec<String> = db.query(q).expect("q").into_iter().map(|(id, _)| id).collect();
    got.sort();
    // Ground truth: every tenant-7 doc whose grp is hot.
    let mut want = Vec::new();
    for i in 0..2000 {
        if i % 32 == 7 && i % 10 != 0 {
            want.push(format!("d{i:05}"));
        }
    }
    want.sort();
    assert_eq!(got, want, "row set must match the conjunction");

    let _ = std::fs::remove_dir_all(&dir);
}
