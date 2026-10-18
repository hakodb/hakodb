//! Issue #80 repro harness: WAL append freeze after flood ingest.
//!
//! Drives the exact sync-ingest path the mesh/cloud use
//! (`sync_core::apply_replicated_batch` -> `append_batch_fast(is_remote=true)`)
//! with flood batches, forced compaction (small `max_inlined_memory_bytes`
//! triggers `checkpoint_inlined_data` -> `rewrite_wal_snapshot` ->
//! `wal.reset()`), and restarts. Every doc ACCEPTED by the ingest must
//! survive restarts and its bytes must reach the shard's WAL file.

use std::collections::HashMap;
use std::sync::{Arc, Mutex as StdMutex};

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::storage::wal::WalOp;

const COL: &str = "latprobe";

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("repro-wal-freeze-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn open(dir: &std::path::Path, max_inlined: usize) -> Arc<Hako> {
    let cfg = HakoConfig {
        max_inlined_memory_bytes: max_inlined,
        ..Default::default()
    };
    Arc::new(Hako::open(dir.to_string_lossy().as_ref(), cfg).unwrap())
}

/// Build one synthetic remote PutInlined op, the exact shape the mesh
/// bootstrap/delta sender produces (doc bytes with an explicit _time).
fn remote_put(key: &str, ts_micros: i64, payload_len: usize) -> WalOp {
    let mut doc = HakoDoc::default();
    doc._time = ts_micros;
    doc.insert("n", Value::Int(0));
    doc.insert("payload", Value::String("x".repeat(payload_len)));
    WalOp::PutInlined { key: key.to_string(), value: doc.encode() }
}

/// Find the shard's wal.log without depending on the root layout
/// (room-less engines nest shards under the root; rooms add a hash dir).
fn find_wal(dir: &std::path::Path) -> std::path::PathBuf {
    fn walk(dir: &std::path::Path, col: &str, out: &mut Vec<std::path::PathBuf>) {
        let Ok(entries) = std::fs::read_dir(dir) else { return };
        for e in entries.flatten() {
            let p = e.path();
            if p.is_dir() {
                walk(&p, col, out);
            } else if p.file_name().map(|f| f == "wal.log").unwrap_or(false)
                && p.parent().map(|d| d.file_name().map(|f| f == col).unwrap_or(false)).unwrap_or(false)
            {
                out.push(p);
            }
        }
    }
    let mut out = Vec::new();
    walk(dir, COL, &mut out);
    assert_eq!(out.len(), 1, "expected exactly one {COL}/wal.log under {}", dir.display());
    out.remove(0)
}

fn wal_len(dir: &std::path::Path) -> u64 {
    std::fs::metadata(find_wal(dir)).unwrap().len()
}

fn ingest(db: &Arc<Hako>, ops: Vec<WalOp>) {
    let db2 = db.clone();
    let rt = tokio::runtime::Runtime::new().unwrap();
    rt.block_on(async move {
        hakodb::sync_core::apply_replicated_batch(
            db2,
            COL.to_string(),
            ops,
            Arc::new(StdMutex::new(HashMap::new())),
        )
        .await;
    });
}

#[test]
fn sigkill_after_remote_ingest_loses_nothing() {
    // The live wave-300 loss: docs ingested via the remote path
    // (write_all, no fsync, no data_len update) vanished after an
    // ungraceful restart. mem::forget = SIGKILL simulator: no Drop, no
    // flush, no shutdown rewrite — the file must speak for itself.
    let dir = tmp_dir("sigkill");
    let db = open(&dir, 64 * 1024 * 1024);
    let mut ts: i64 = 1_800_000_000_000_000;

    let mut wave_keys: Vec<String> = Vec::new();
    for batch in 0..3 {
        let ops: Vec<WalOp> = (0..100)
            .map(|i| {
                ts += 1;
                let key = format!("wave-doc-{batch:01}{i:03}");
                wave_keys.push(key.clone());
                remote_put(&key, ts, 600)
            })
            .collect();
        ingest(&db, ops);
    }
    let len_before = wal_len(&dir);
    assert!(len_before > 0, "remote ingest must write bytes");

    // SIGKILL: no Drop, no flush, no shutdown rewrite.
    std::mem::forget(db);

    // Recovery must restore every remotely-written doc.
    let db = open(&dir, 64 * 1024 * 1024);
    for key in &wave_keys {
        assert!(
            db.get(COL, key).unwrap().is_some(),
            "BUG #80: remote-ACKed write lost across SIGKILL restart: {key}"
        );
    }

    // Post-restart life: new local + remote writes, then graceful close.
    ts += 1;
    let mut doc = HakoDoc::default();
    doc._time = ts;
    doc.insert("v", Value::Int(1));
    db.put(COL, "local-after-restart", &doc).unwrap();
    ts += 1;
    ingest(&db, vec![remote_put("remote-after-restart", ts, 600)]);

    drop(db);
    let db = open(&dir, 64 * 1024 * 1024);
    assert!(db.get(COL, "local-after-restart").unwrap().is_some(), "post-restart local write lost");
    assert!(db.get(COL, "remote-after-restart").unwrap().is_some(), "post-restart remote write lost");
    for key in wave_keys.iter().take(5) {
        assert!(db.get(COL, key).unwrap().is_some(), "wave doc lost after second restart: {key}");
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn jumped_clock_peer_cannot_resurrect_stale_doc_over_local_write() {
    // The live upsert blackhole: a peer whose clock jumped backward keeps
    // re-sending docs stamped in the "future" (its old now). After a local
    // write wins, the next bootstrap re-ingest must NOT revert it.
    let dir = tmp_dir("lww");
    let db = open(&dir, 64 * 1024 * 1024);

    // 1. Jumped peer's doc: stamped far in the future.
    let future_ts: i64 = 1_900_000_000_000_000; // ~year 2070
    ingest(&db, vec![remote_put("k-1", future_ts, 100)]);

    // 2. Local write wins (engine restamps with real now, overwrites).
    let mut doc = HakoDoc::default();
    doc.insert("v", Value::String("local-wins".into()));
    db.put(COL, "k-1", &doc).unwrap();

    // 3. The jumped peer re-sends its stale-but-future-stamped doc
    //    (bootstrap re-offers do this on every reconnect).
    ingest(&db, vec![remote_put("k-1", future_ts, 100)]);

    // 4. The local write must still be the visible state.
    let got = db.get(COL, "k-1").unwrap().expect("doc must exist");
    let v = got.get("v").map(|val| match val {
        Value::String(s) => s.to_string(),
        _ => "?".to_string(),
    }).unwrap_or_default();
    assert_eq!(v, "local-wins", "BUG #80: stale future-stamped remote doc resurrected over a local write");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn flood_ingest_then_compact_then_append_survives_restarts() {
    let dir = tmp_dir("main");
    // 512KB cap: 5000 x ~640B docs force multiple checkpoint_inlined_data
    // spills (and thus rewrite_wal_snapshot / wal.reset()) mid-flood — the
    // suspected trigger. Live default is 64MB; small here for test speed.
    let db = open(&dir, 512 * 1024);
    let mut ts: i64 = 1_800_000_000_000_000; // fixed base, monotonic counter

    // --- Round 1: flood 5000 docs in 1000-op batches (mesh bootstrap shape)
    for batch in 0..5 {
        let ops: Vec<WalOp> = (0..1000)
            .map(|i| {
                ts += 1;
                remote_put(&format!("stress-doc-{batch:01}{i:04}"), ts, 600)
            })
            .collect();
        ingest(&db, ops);
    }

    let len_after_flood = wal_len(&dir);
    assert!(len_after_flood > 0, "flood must reach the WAL file");

    // --- Force the checkpoint/rewrite path explicitly (maintenance also
    // does this every 5s; compact() is the deterministic entry).
    db.compact().unwrap();

    // --- Post-compaction appends: brand-new keys + an LWW-winning upsert —
    // exactly what the live drill pushed after the flood era.
    let mut probe_keys: Vec<String> = Vec::new();
    for i in 0..50 {
        ts += 1;
        let key = format!("post-doc-{i:04}");
        ingest(&db, vec![remote_put(&key, ts, 600)]);
        probe_keys.push(key);
    }
    ts += 1;
    ingest(&db, vec![remote_put("stress-doc-00000", ts, 600)]);

    // THE LIVE SYMPTOM, asserted: post-compaction appends must reach the file.
    let len_after_append = wal_len(&dir);
    assert!(
        len_after_append > len_after_flood,
        "BUG #80: WAL file did not grow after post-compaction appends ({} -> {})",
        len_after_flood,
        len_after_append
    );

    // --- Restart 1: everything accepted must survive
    drop(db);
    let db = open(&dir, 512 * 1024);
    for key in &probe_keys {
        assert!(
            db.get(COL, key).unwrap().is_some(),
            "BUG #80: ACKed write lost after restart: {key}"
        );
    }
    assert!(db.get(COL, "stress-doc-00000").unwrap().is_some(), "upsert lost after restart");

    // --- Restart 2 (after another compaction): still must survive
    db.compact().unwrap();
    drop(db);
    let db = open(&dir, 512 * 1024);
    for key in &probe_keys {
        assert!(
            db.get(COL, key).unwrap().is_some(),
            "BUG #80: ACKed write lost after second restart + compaction: {key}"
        );
    }

    let _ = std::fs::remove_dir_all(&dir);
}
