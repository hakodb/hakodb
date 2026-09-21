//! Read-path microbenchmarks. Mirrors the stress loop in benchmark.cpp so the
//! criterion numbers are comparable where they overlap, but deterministic and
//! statistical — eliminates the per-run warmup noise that makes benchmark.cpp
//! swings so wide on a 4 GB box.
//!
//! What this measures:
//! - planner overhead (cached vs uncached path)            [planner_*]
//! - single-doc GET                                       [get_*]
//! - equality query via secondary index                   [query_eq_*]
//! - composite range query + cursor pagination            [query_composite_*]
//! - offset pagination (the 5k qps floor from bench)      [query_offset_*]
//!
//! Run with `cargo bench --bench read_path`.
//!
//! Ponytail: 6 benches. No fixture framework, no async, no thread-pool juggling.
//! Each bench is sub-second on a single core.

use std::time::Duration;

use criterion::{criterion_group, criterion_main, Criterion, BenchmarkId, Throughput};
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::filter::Operator;
use hakodb::query::order::OrderBy;
use hakodb::query::query::Query;

const N: usize = 1000;

fn temp_dir(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("fl-bench-{label}-{nanos}"))
}

fn make_doc(i: usize) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("tenant", Value::String(format!("tenant-{}", i % 32)));
    doc.insert("age", Value::Int(18 + (i % 70) as i64));
    doc.insert("active", Value::Bool(i % 3 != 0));
    doc.insert("score", Value::Float(((i % 10000) as f64) / 7.0 + 0.5));
    doc.insert(
        "description",
        Value::String(format!("hakodb v0.7.1 bench payload {i}")),
    );
    doc.insert(
        "tags",
        Value::Array(vec![
            Value::String(format!("tag-{}", i % 10)),
            Value::String("bench".into()),
        ]),
    );
    doc
}

/// Seed N docs into a fresh engine. Returns the open handle.
fn build_engine(label: &str) -> Hako {
    let dir = temp_dir(label);
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual; // group commit off → all writes hit disk once
    cfg.page_cache_capacity = 4096;
    cfg.mmap_size = 64 * 1024 * 1024;
    cfg.value_blob_threshold_bytes = 16 * 1024;
    let db = Hako::open(&dir, cfg).expect("open");

    let mut mutations = Vec::with_capacity(N);
    for i in 0..N {
        mutations.push(BatchMutation::Put {
            collection: "bench".into(),
            doc_id: format!("b_{i}"),
            doc: make_doc(i),
        });
    }
    db.write_batch(mutations).expect("seed");

    // Give the async index backfill a moment to land.
    std::thread::sleep(Duration::from_millis(500));

    // Create the indexes the planner will route against.
    db.create_index("bench", "active").expect("idx active");
    db.create_index("bench", "tenant").expect("idx tenant");
    db.create_index("bench", "id").expect("idx id");
    db.create_composite_index(
        "bench",
        vec![
            ("tenant".into(), SortDirection::Asc),
            ("score".into(), SortDirection::Desc),
        ],
    )
    .expect("idx composite");
    std::thread::sleep(Duration::from_millis(500));

    db
}

// ---------------------------------------------------------------------------
// 1. Single-doc GET — the "you beat SQLite" column.
// ---------------------------------------------------------------------------

fn bench_get(c: &mut Criterion) {
    let db = build_engine("get");
    let mut group = c.benchmark_group("get");
    group.throughput(Throughput::Elements(1));

    group.bench_function("single_doc", |b| {
        b.iter(|| {
            let _ = db.get("bench", "b_500").expect("get");
        });
    });

    group.finish();
    drop(db);
    cleanup("get");
}

fn cleanup(label: &str) {
    // Best-effort. Criterion benches are hermetic per-temp-dir so leftover dirs are fine.
    let _ = std::fs::remove_dir_all(temp_dir(label));
}

// ---------------------------------------------------------------------------
// 2. Stress query — equality filter, no projection. This is what the bench.cpp
//    "Stress Query QPS" column measures. We want to see the planner-cache hit.
// ---------------------------------------------------------------------------

fn bench_query_eq(c: &mut Criterion) {
    let db = build_engine("qeq");
    let mut group = c.benchmark_group("query_eq");
    group.throughput(Throughput::Elements(1));

    group.bench_function("active_eq", |b| {
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
    cleanup("qeq");
}

// ---------------------------------------------------------------------------
// 3. Composite range query — tenant + score, descending score.
//    This is the "Composite Query QPS" column in bench.cpp.
// ---------------------------------------------------------------------------

fn bench_query_composite(c: &mut Criterion) {
    let db = build_engine("qcomp");
    let mut group = c.benchmark_group("query_composite");
    group.throughput(Throughput::Elements(1));

    group.bench_function("tenant_eq_score_desc", |b| {
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
    cleanup("qcomp");
}

// ---------------------------------------------------------------------------
// 4. Offset pagination — the 5k qps floor. We sweep offsets to expose any
//    linear-in-offset behaviour (the current scan-and-discard pattern).
// ---------------------------------------------------------------------------

fn bench_query_offset(c: &mut Criterion) {
    let db = build_engine("qoff");
    let mut group = c.benchmark_group("query_offset");
    group.throughput(Throughput::Elements(1));

    for &offset in &[0usize, 100, 250, 500, 750] {
        group.bench_with_input(BenchmarkId::from_parameter(offset), &offset, |b, &off| {
            b.iter(|| {
                let mut q = Query::new("bench");
                q.order_by.push(OrderBy {
                    field: "id".into(),
                    ascending: true,
                });
                q.offset = Some(off);
                q.limit = Some(5);
                let _ = db.query(q).expect("q");
            });
        });
    }

    group.finish();
    drop(db);
    cleanup("qoff");
}

// ---------------------------------------------------------------------------
// 5. Cursor pagination — same shape as offset but using start_at. Expected to
//    be much faster, validates that the BTree-seek path is doing its job.
// ---------------------------------------------------------------------------

fn bench_query_cursor(c: &mut Criterion) {
    let db = build_engine("qcur");
    let mut group = c.benchmark_group("query_cursor");
    group.throughput(Throughput::Elements(1));

    for &start in &[100usize, 250, 500, 750] {
        group.bench_with_input(BenchmarkId::from_parameter(start), &start, |b, &s| {
            b.iter(|| {
                let mut q = Query::new("bench");
                q.order_by.push(OrderBy {
                    field: "id".into(),
                    ascending: true,
                });
                q.start_at = Some(vec![Value::String(format!("b_{s}"))]);
                q.limit = Some(5);
                let _ = db.query(q).expect("q");
            });
        });
    }

    group.finish();
    drop(db);
    cleanup("qcur");
}

// ---------------------------------------------------------------------------
// 6. Large offset on 10k docs — the realistic "skip-ahead" case. N=10k means
//    the storage index exceeds page cache and forces the linear-scan path to
//    actually do work per offset doc.
// ---------------------------------------------------------------------------

const BIG_N: usize = 10_000;

fn build_engine_big(label: &str) -> Hako {
    let dir = temp_dir(label);
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.page_cache_capacity = 1024;
    cfg.mmap_size = 64 * 1024 * 1024;
    cfg.value_blob_threshold_bytes = 16 * 1024;
    let db = Hako::open(&dir, cfg).expect("open");

    let mut mutations = Vec::with_capacity(BIG_N);
    for i in 0..BIG_N {
        mutations.push(BatchMutation::Put {
            collection: "bench".into(),
            doc_id: format!("b_{i}"),
            doc: make_doc(i),
        });
    }
    db.write_batch(mutations).expect("seed");
    std::thread::sleep(Duration::from_millis(500));

    db.create_index("bench", "id").expect("idx id");
    db.create_index("bench", "active").expect("idx active");
    std::thread::sleep(Duration::from_millis(500));
    db
}

fn bench_query_offset_big(c: &mut Criterion) {
    let db = build_engine_big("qoff_big");
    let mut group = c.benchmark_group("query_offset_10k");
    group.throughput(Throughput::Elements(1));

    for &offset in &[0usize, 1000, 5000, 9000] {
        group.bench_with_input(BenchmarkId::from_parameter(offset), &offset, |b, &off| {
            b.iter(|| {
                let mut q = Query::new("bench");
                q.order_by.push(OrderBy {
                    field: "id".into(),
                    ascending: true,
                });
                q.offset = Some(off);
                q.limit = Some(5);
                let _ = db.query(q).expect("q");
            });
        });
    }

    group.finish();
    drop(db);
    cleanup("qoff_big");
}

criterion_group!(
    name = benches;
    config = Criterion::default()
        .sample_size(200)
        .measurement_time(Duration::from_secs(3))
        .warm_up_time(Duration::from_secs(1));
    targets =
        bench_get,
        bench_query_eq,
        bench_query_composite,
        bench_query_offset,
        bench_query_cursor,
        bench_query_offset_big,
);
criterion_main!(benches);
