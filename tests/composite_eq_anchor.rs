//! Repro for hakodb/hakodb#4: two-eq filters against a composite index
//! must plan an index scan regardless of filter order. Today only the
//! order matching the index prefix does; the other order falls to a full
//! collection scan (measured 47 rps vs 26k live).
//!
//! Shape mirrors the production login query: composite (passw, nim),
//! `nim` alphanumeric, `passw` numeric-shaped, both stored as strings.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::plan::ScanType;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-anchor-{}-{}",
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
        doc.insert("nim", Value::String(format!("NIM{i:08}")));
        doc.insert("passw", Value::String(format!("{}", 28_000_000 + i)));
        batch.push(BatchMutation::Put {
            collection: "students".into(),
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

fn login_q(nim_first: bool) -> Query {
    let nim = Value::String("NIM00000123".into());
    let passw = Value::String("28000123".into());
    let base = Query::new("students").limit(1);
    if nim_first {
        base.where_eq("nim", nim).where_eq("passw", passw)
    } else {
        base.where_eq("passw", passw).where_eq("nim", nim)
    }
}

fn scan_of(db: &Hako, q: Query) -> ScanType {
    db.explain(q).expect("explain").scan
}

#[test]
fn composite_eq_hits_index_in_filter_order() {
    let dir = tmp("order");
    let db = open_db(&dir);
    seed(&db, 25_000);
    db.create_composite_index(
        "students",
        vec![
            ("passw".to_string(), SortDirection::Asc),
            ("nim".to_string(), SortDirection::Asc),
        ],
    )
    .expect("composite");
    // Live also carries simple indexes (nim, twice over): mirror them.
    db.create_index("students", "nim").expect("simple nim");
    db.create_index("students", "nim").expect("simple nim again");
    wait_quiescent(&db);

    // Sanity: the target row exists and matches.
    let rows = db.query(login_q(true)).expect("q");
    assert_eq!(rows.len(), 1, "seed must contain the login row");
    assert_eq!(rows[0].0, "d00123");

    // Prefix order plans an index scan (today's behavior, keep it).
    assert!(
        !matches!(scan_of(&db, login_q(false)), ScanType::FullCollection),
        "prefix order must not full-scan"
    );
    // Losing order must plan an index scan too (hakodb#4: full-scans today).
    assert!(
        !matches!(scan_of(&db, login_q(true)), ScanType::FullCollection),
        "filter order must not decide index use"
    );
    // Both orders return the identical row.
    let a: Vec<String> = db.query(login_q(true)).expect("q").into_iter().map(|(id, _)| id).collect();
    let b: Vec<String> = db.query(login_q(false)).expect("q").into_iter().map(|(id, _)| id).collect();
    assert_eq!(a, b);

    let _ = std::fs::remove_dir_all(&dir);
}
