//! Write-path microbenchmarks. Mirrors benchmark.cpp's WPS / Bulk / Tx columns
//! across all 6 durability modes plus a small + large doc variant to expose
//! the blob-spillover path.
//!
//! What this measures:
//! - single-doc write throughput, per DurabilityMode               [write_single/*]
//! - bulk write_batch throughput, per DurabilityMode                [write_batch/*]
//! - small doc (inlined value) vs large doc (blob-spillover)        [write_blob/*]
//! - serializable transaction get+put+commit throughput             [tx_get_put_commit]
//! - planner-cache hit on identical queries                         [query_planner_cached]
//! - composite (2-field) vs single-field secondary query           [query_composite_vs_eq]
//!
//! Run with `cargo bench --bench write_path`.
//!
//! Ponytail: 14 benches. No fixture framework. Each is a separate engine
//! instance in its own temp dir, so durability modes don't bleed into each
//! other.

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, Throughput};
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::filter::Operator;
use hakodb::query::order::OrderBy;
use hakodb::query::query::Query;

const SMALL_N: usize = 1_000;
const BIG_N: usize = 1_000;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fl-bench-w-{label}-{nanos}"))
}

fn cleanup(label: &str) {
    let _ = std::fs::remove_dir_all(temp_dir(label));
}

/// Small doc: stays inlined (value_blob_threshold=16k, doc body ~150 bytes).
fn make_small_doc(i: usize) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("tenant", Value::String(format!("tenant-{}", i % 32)));
    doc.insert("age", Value::Int(18 + (i % 70) as i64));
    doc.insert("active", Value::Bool(i % 3 != 0));
    doc.insert("score", Value::Float(((i % 10000) as f64) / 7.0 + 0.5));
    doc.insert("description", Value::String(format!("payload {i}")));
    doc
}

/// Large doc: forces blob spillover (24k body > 16k threshold).
fn make_blob_doc(i: usize) -> HakoDoc {
    let mut doc = make_small_doc(i);
    // 24kB of filler to push the doc past the 16k blob threshold.
    doc.insert(
        "blob",
        Value::String("x".repeat(24 * 1024)),
    );
    doc
}

fn cfg_for(mode: DurabilityMode, threshold: usize) -> HakoConfig {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = mode;
    cfg.page_cache_capacity = 4096;
    cfg.mmap_size = 64 * 1024 * 1024;
    cfg.value_blob_threshold_bytes = threshold;
    cfg
}

fn open_engine(label: &str, mode: DurabilityMode) -> Hako {
    Hako::open(&temp_dir(label), cfg_for(mode, 16 * 1024)).expect("open")
}

// ---------------------------------------------------------------------------
// 1. Single-doc write — `hk_engine_insert` equivalent.
//    Per DurabilityMode. Sweep Always / Interval / Manual / OnCommit.
// ---------------------------------------------------------------------------

fn bench_write_single(c: &mut Criterion) {
    let modes = [
        ("always", DurabilityMode::Always),
        ("interval", DurabilityMode::Interval),
        ("manual", DurabilityMode::Manual),
        ("oncommit", DurabilityMode::OnCommit),
    ];

    for (label, mode) in modes.iter() {
        let db = open_engine(&format!("ws_{label}"), *mode);
        let mut group = c.benchmark_group(format!("write_single/{label}"));
        group.throughput(Throughput::Elements(1));

        let mut counter: usize = 0;
        group.bench_function("put", |b| {
            b.iter(|| {
                let id = format!("s_{}", counter);
                counter += 1;
                db.write_batch(vec![BatchMutation::Put {
                    collection: "bench".into(),
                    doc_id: id,
                    doc: make_small_doc(counter),
                }])
                .expect("put");
            });
        });

        group.finish();
        drop(db);
        cleanup(&format!("ws_{label}"));
    }
}

// ---------------------------------------------------------------------------
// 2. Bulk write — `write_batch(BIG_N)` in one call.
//    Per DurabilityMode. This is what benchmark.cpp's "Bulk Upd/Del" column
//    measures (without the per-op hk_batch_set overhead).
// ---------------------------------------------------------------------------

fn bench_write_batch(c: &mut Criterion) {
    let modes = [
        ("always", DurabilityMode::Always),
        ("interval", DurabilityMode::Interval),
        ("manual", DurabilityMode::Manual),
        ("oncommit", DurabilityMode::OnCommit),
    ];

    for (label, mode) in modes.iter() {
        let db = open_engine(&format!("wb_{label}"), *mode);
        let mut group = c.benchmark_group(format!("write_batch/{label}"));
        // Throughput per element: bench batch has ~BIG_N writes.
        group.throughput(Throughput::Elements(BIG_N as u64));

        let mut counter: usize = 0;
        group.bench_function("write_batch", |b| {
            b.iter(|| {
                let mut mutations = Vec::with_capacity(BIG_N);
                for i in 0..BIG_N {
                    mutations.push(BatchMutation::Put {
                        collection: "bench".into(),
                        doc_id: format!("b_{}_{}", counter, i),
                        doc: make_small_doc(counter + i),
                    });
                }
                counter += BIG_N;
                db.write_batch(mutations).expect("batch");
            });
        });

        group.finish();
        drop(db);
        cleanup(&format!("wb_{label}"));
    }
}

// ---------------------------------------------------------------------------
// 3. Blob-spillover write — single doc with 24kB body, default threshold 16k.
//    Only Manual durability — we want to isolate the blob write cost.
//    If small-doc Manual write takes X and blob-doc takes 2X, that's the
//    blob-manager overhead.
// ---------------------------------------------------------------------------

fn bench_write_blob(c: &mut Criterion) {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.page_cache_capacity = 4096;
    cfg.mmap_size = 64 * 1024 * 1024;
    cfg.value_blob_threshold_bytes = 16 * 1024;
    let db = Hako::open(&temp_dir("blob"), cfg).expect("open");

    let mut group = c.benchmark_group("write_blob");
    group.throughput(Throughput::Elements(1));

    let mut counter: usize = 0;
    group.bench_function("blob_doc", |b| {
        b.iter(|| {
            let id = format!("blob_{counter}");
            counter += 1;
            db.write_batch(vec![BatchMutation::Put {
                collection: "bench".into(),
                doc_id: id,
                doc: make_blob_doc(counter),
            }])
            .expect("put");
        });
    });

    group.finish();
    drop(db);
    cleanup("blob");
}

// ---------------------------------------------------------------------------
// 4. Serializable transaction — get a doc, modify it, commit.
//    Mirrors benchmark.cpp's "Tx WPS" column.
// ---------------------------------------------------------------------------

fn bench_tx(c: &mut Criterion) {
    // Seed one doc to mutate.
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.page_cache_capacity = 4096;
    cfg.mmap_size = 64 * 1024 * 1024;
    cfg.value_blob_threshold_bytes = 16 * 1024;
    let db = Hako::open(&temp_dir("tx"), cfg).expect("open");
    db.write_batch(vec![BatchMutation::Put {
        collection: "bench".into(),
        doc_id: "tx_target".into(),
        doc: make_small_doc(0),
    }])
    .expect("seed");

    let mut group = c.benchmark_group("tx");
    group.throughput(Throughput::Elements(1));

    group.bench_function("get_put_commit", |b| {
        b.iter(|| {
            let mut tx = db.begin_serializable_transaction();
            let _ = tx.get(&db, "bench", "tx_target").expect("get");
            tx.put(
                "bench",
                "tx_target",
                make_small_doc(0),
            );
            tx.commit(&db).expect("commit");
        });
    });

    group.finish();
    drop(db);
    cleanup("tx");
}

// ---------------------------------------------------------------------------
// 5. Query planner cache — same query fired N times should hit the cache
//    and be ~constant. This is the FFI hot-path that matters most for
//    benchmark.cpp's Qry/Cmp stress loops.
// ---------------------------------------------------------------------------

fn bench_query_planner_cached(c: &mut Criterion) {
    let db = open_engine("qpc", DurabilityMode::Manual);
    db.write_batch({
        let mut m = Vec::with_capacity(SMALL_N);
        for i in 0..SMALL_N {
            m.push(BatchMutation::Put {
                collection: "bench".into(),
                doc_id: format!("b_{i}"),
                doc: make_small_doc(i),
            });
        }
        m
    })
    .expect("seed");
    db.create_index("bench", "active").expect("idx active");
    std::thread::sleep(Duration::from_millis(500));

    let mut group = c.benchmark_group("query_planner_cached");
    group.throughput(Throughput::Elements(1));

    group.bench_function("active_eq_limit_50", |b| {
        b.iter(|| {
            let mut q = Query::new("bench");
            q.filters.push(hakodb::query::filter::Filter {
                field: "active".into(),
                op: Operator::Eq,
                value: Value::Bool(true),
            });
            q.limit = Some(50);
            let _ = db.query(q).expect("q");
        });
    });

    group.finish();
    drop(db);
    cleanup("qpc");
}

// ---------------------------------------------------------------------------
// 6. Single-field secondary vs 2-field composite — the apples-to-apples
//    comparison benchmark.cpp's "Qry vs Cmp" columns are missing.
//    Both queries return ~50 docs on a 1k corpus so the doc-decode cost
//    is identical; only the planner + index lookup differ.
// ---------------------------------------------------------------------------

fn bench_query_composite_vs_eq(c: &mut Criterion) {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.page_cache_capacity = 4096;
    cfg.mmap_size = 64 * 1024 * 1024;
    cfg.value_blob_threshold_bytes = 16 * 1024;
    let db = Hako::open(&temp_dir("cmp"), cfg).expect("open");
    db.write_batch({
        let mut m = Vec::with_capacity(SMALL_N);
        for i in 0..SMALL_N {
            m.push(BatchMutation::Put {
                collection: "bench".into(),
                doc_id: format!("b_{i}"),
                doc: make_small_doc(i),
            });
        }
        m
    })
    .expect("seed");
    db.create_index("bench", "active").expect("idx active");
    db.create_composite_index(
        "bench",
        vec![
            ("tenant".into(), SortDirection::Asc),
            ("score".into(), SortDirection::Desc),
        ],
    )
    .expect("idx composite");
    std::thread::sleep(Duration::from_millis(500));

    let mut group = c.benchmark_group("query_composite_vs_eq");
    group.throughput(Throughput::Elements(1));

    group.bench_function("secondary_active_eq_50", |b| {
        b.iter(|| {
            let mut q = Query::new("bench");
            q.filters.push(hakodb::query::filter::Filter {
                field: "active".into(),
                op: Operator::Eq,
                value: Value::Bool(true),
            });
            q.limit = Some(50);
            let _ = db.query(q).expect("q");
        });
    });

    group.bench_function("composite_tenant_score_20", |b| {
        b.iter(|| {
            let mut q = Query::new("bench");
            q.filters.push(hakodb::query::filter::Filter {
                field: "tenant".into(),
                op: Operator::Eq,
                value: Value::String("tenant-2".into()),
            });
            q.order_by.push(OrderBy {
                field: "score".into(),
                ascending: false,
            });
            q.limit = Some(20);
            let _ = db.query(q).expect("q");
        });
    });

    group.finish();
    drop(db);
    cleanup("cmp");
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(100)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets =
        bench_write_single,
        bench_write_batch,
        bench_write_blob,
        bench_tx,
        bench_query_planner_cached,
        bench_query_composite_vs_eq,
);

criterion_main!(benches);
