//! Narrow-lock tier compaction under concurrent writes (P-tailing).
//!
//! Tiny spill/rotate thresholds force real multi-segment tiers; a writer
//! thread mutates docs for ~12s so at least two 5s maintenance ticks
//! merge WHILE writes land. Assertion is convergence, not exactness:
//! every doc must exist with SOME version in [initial, final] (no lost
//! updates, no ghosts, no corruption) regardless of interleave. Any
//! interleaving converges, so this cannot flake — only a real race
//! (e.g. swapping a moved pointer) fails it.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::storage::engine::tier_compactions;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-compact-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

#[test]
fn tier_compaction_never_loses_concurrent_writes() {
    let dir = tmp("race");
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    // Force constant spilling/rotation: steady state (~120KB live) must
    // exceed both, or no segments (hence no merges) ever form — the
    // tripwire below caught exactly that.
    cfg.auto_compaction_threshold_bytes = 16 * 1024;
    cfg.max_inlined_memory_bytes = 32 * 1024;
    let db = Arc::new(Hako::open(&dir, cfg).expect("open"));

    const N: usize = 60;
    const ROUNDS: usize = 40;
    // ~20s wall: checkpoint spills form L0s on ticks 1-2, the first
    // merge needs TWO same-level immutables (tick 3+). Shorter runs
    // cover zero merges and the tripwire below catches that.
    const ROUND_MS: u64 = 500;
    // Per-doc version counters: writer bumps, verifier bounds-checks.
    let versions: Vec<AtomicI64> = (0..N).map(|_| AtomicI64::new(0)).collect();
    let versions = Arc::new(versions);
    // Tripwire against vacuity: the run must actually merge (≥2 ticks
    // fire inside ~12s of writes). A race test that never compacts
    // proves nothing.
    let merges_before = tier_compactions();

    // Seed v0 (2KB values spill fast).
    let seed: Vec<BatchMutation> = (0..N)
        .map(|i| {
            let mut doc = HakoDoc::default();
            doc.insert("v", Value::Int(0));
            doc.insert("pad", Value::String("x".repeat(2048)));
            BatchMutation::Put {
                collection: "race".into(),
                doc_id: format!("k{i:03}"),
                doc,
            }
        })
        .collect();
    db.write_batch(seed).expect("seed");

    // Writer: 40 rounds x 60 docs with 300ms cadence (≈12s, so ≥2
    // maintenance ticks merge WHILE writes land; bounded iterations
    // guarantee join).
    let wdb = Arc::clone(&db);
    let wver = Arc::clone(&versions);
    let writer = std::thread::spawn(move || {
        for _ in 0..ROUNDS {
            let batch: Vec<BatchMutation> = (0..N)
                .map(|i| {
                    let v = wver[i].fetch_add(1, Ordering::Relaxed) + 1;
                    let mut doc = HakoDoc::default();
                    doc.insert("v", Value::Int(v));
                    doc.insert("pad", Value::String("x".repeat(2048)));
                    BatchMutation::Put {
                        collection: "race".into(),
                        doc_id: format!("k{i:03}"),
                        doc,
                    }
                })
                .collect();
            wdb.write_batch(batch).expect("round");
            std::thread::sleep(std::time::Duration::from_millis(ROUND_MS));
        }
    });

    // Main thread churns deletes+recreates on a side key (tombstone path
    // through merges) while the writer runs.
    for r in 0..ROUNDS {
        let mut doc = HakoDoc::default();
        doc.insert("v", Value::Int(r as i64));
        doc.insert("pad", Value::String("y".repeat(2048)));
        db.write_batch(vec![BatchMutation::Put {
            collection: "race".into(),
            doc_id: "churn".into(),
            doc,
        }])
        .expect("churn put");
        std::thread::sleep(std::time::Duration::from_millis(50));
    }
    writer.join().expect("writer");

    // Converge: every doc present, version within [0, ROUNDS].
    for i in 0..N {
        let got = db.get("race", &format!("k{i:03}")).expect("get").expect("present");
        match got.get("v") {
            Some(Value::Int(v)) => assert!(
                (0..=ROUNDS as i64).contains(v),
                "k{i:03} has out-of-range version {v}"
            ),
            other => panic!("k{i:03} corrupt value {other:?}"),
        }
    }
    assert_eq!(versions.iter().map(|v| v.load(Ordering::Relaxed)).max(), Some(ROUNDS as i64));
    assert!(
        tier_compactions() > merges_before,
        "no tier merge ran during 20s of writes — test is vacuous"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
