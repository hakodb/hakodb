//! Regression for hakodb/hakodb#4: composite indexes must survive a
//! restart. Before the fix, `export_state` covered secondary/fts only, so
//! every reopen started with an empty composite (probes missed -> the
//! 0.9.5 empty-sweep fallback scanned everything at ~38 ms on 25k docs).
//!
//! Deterministic assertions (no timing): after drop + reopen WITHOUT
//! re-creating the index, the persisted file must still hold the full
//! tree, the plan must avoid FullCollection, and rows must be correct.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::composite_index::CompositeIndex;
use hakodb::index::composite::definition::SortDirection;
use hakodb::index::inverted_index::InvertedIndex;
use hakodb::index::secondary_index::SecondaryIndex;
use hakodb::query::plan::ScanType;
use hakodb::query::query::Query;
use std::collections::HashMap;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-cpersist-{}-{}",
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

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        if db.is_indexes_ready() && db.quiescence_status().index_backfills == 0 {
            return;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(120),
            "never ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
}

fn login_q() -> Query {
    Query::new("students")
        .where_eq("nim", Value::String("NIM00000123".into()))
        .where_eq("passw", Value::String("28000123".into()))
        .limit(1)
}

/// The persisted snapshot must hold the full composite tree (id 1).
/// Shapes mirror export_state's tuple exactly.
#[allow(clippy::type_complexity)]
fn persisted_composite_len(dir: &std::path::Path) -> usize {
    let bytes = std::fs::read(dir.join("_indices").join("ram_indexes.bin")).expect("snapshot file");
    let (_, _, (trees, _)): (
        HashMap<String, HashMap<String, SecondaryIndex>>,
        HashMap<String, HashMap<String, InvertedIndex>>,
        (HashMap<u32, CompositeIndex>, u32),
    ) = bincode::deserialize(&bytes).expect("snapshot parses");
    trees.get(&1).map(|t| t.tree.len()).unwrap_or(0)
}

#[test]
fn composite_survives_restart() {
    let dir = tmp("restart");
    let n = 2000usize;
    {
        let db = open_db(&dir);
        seed(&db, n);
        db.create_composite_index(
            "students",
            vec![
                ("passw".to_string(), SortDirection::Asc),
                ("nim".to_string(), SortDirection::Asc),
            ],
        )
        .expect("composite");
        wait_ready(&db);
        // Sanity before close: indexed and correct.
        let rows = db.query(login_q()).expect("q");
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].0, "d00123");
    } // Drop exports ram_indexes.bin (incl. composite since the fix).

    // Persisted file holds the whole tree (was: zero composite entries).
    assert_eq!(persisted_composite_len(&dir), n);

    // Reopen WITHOUT re-creating anything: the probe must still hit.
    let db = open_db(&dir);
    wait_ready(&db);
    let plan = db.explain(login_q()).expect("explain").scan;
    assert!(
        !matches!(plan, ScanType::FullCollection),
        "reopened composite must plan an index scan, got {plan:?}"
    );
    let rows = db.query(login_q()).expect("q");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "d00123");

    let _ = std::fs::remove_dir_all(&dir);
}
