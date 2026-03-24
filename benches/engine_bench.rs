use criterion::{black_box, criterion_group, criterion_main, BenchmarkId, Criterion, Throughput};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use firelite::index::composite::definition::SortDirection;
use firelite::query::filter::Operator;
use firelite::query::query::Query;
use rand::{rngs::StdRng, Rng, SeedableRng};
use std::collections::HashMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant};

const COLLECTION: &str = "bench";
const BENCH_SEED: u64 = 0xF1A5E17E;
const MIXED_OPS: usize = 1_500;

#[derive(Debug, Clone, Copy)]
struct DatasetScale {
    name: &'static str,
    docs: usize,
}

const DATASET_SCALES: [DatasetScale; 4] = [
    DatasetScale {
        name: "small",
        docs: 1_000,
    },
    DatasetScale {
        name: "medium",
        docs: 10_000,
    },
    DatasetScale {
        name: "large",
        docs: 50_000,
    },
    DatasetScale {
        name: "extreme",
        docs: 100_000,
    },
];

#[derive(Debug, Clone, Copy)]
struct ScenarioConfig {
    compression: bool,
    encryption: bool,
    durability: DurabilityMode,
}

impl ScenarioConfig {
    fn label(self) -> String {
        format!(
            "dur={:?},cmp={},enc={}",
            self.durability, self.compression, self.encryption
        )
    }
}

fn config_matrix() -> Vec<ScenarioConfig> {
    let durabilities = [
        DurabilityMode::Always,
        DurabilityMode::Interval,
        DurabilityMode::Manual,
        DurabilityMode::OnCommit,
    ];

    let mut out = Vec::new();
    for durability in durabilities {
        for compression in [false, true] {
            for encryption in [false, true] {
                out.push(ScenarioConfig {
                    compression,
                    encryption,
                    durability,
                });
            }
        }
    }
    out
}

#[derive(Debug, Clone)]
struct LatencySummary {
    p50: Duration,
    p95: Duration,
    p99: Duration,
    min: Duration,
    max: Duration,
    mean: Duration,
}

#[derive(Debug, Clone)]
struct ScenarioMetrics {
    throughput_ops_sec: f64,
    latency: LatencySummary,
    rss_bytes: usize,
    disk_bytes: u64,
    wal_bytes: u64,
    segment_bytes: u64,
    index_build_ms: f64,
    index_lookup_us: f64,
}

#[derive(Debug, Clone)]
struct ComparisonRow {
    scale: &'static str,
    config: String,
    workload: &'static str,
    warm_state: &'static str,
    metrics: ScenarioMetrics,
}

fn make_doc(i: usize, rng: &mut StdRng) -> FireLiteDoc {
    let mut doc = FireLiteDoc::default();
    doc.insert("id", Value::Int(i as i64));
    doc.insert("tenant", Value::String(format!("tenant-{}", i % 32)));
    doc.insert("age", Value::Int((18 + (i % 70)) as i64));
    doc.insert("active", Value::Bool(i % 3 != 0));
    doc.insert(
        "score",
        Value::Float(((i % 10_000) as f64 / 7.0) + rng.gen_range(0.0..1.0)),
    );
    doc.insert(
        "description",
        Value::String(format!(
            "firelite benchmark payload {} {}",
            i,
            if i % 2 == 0 { "alpha" } else { "beta" }
        )),
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

fn build_config(s: ScenarioConfig) -> FireLiteConfig {
    let mut cfg = FireLiteConfig {
        durability_mode: s.durability,
        use_compression: s.compression,
        compression_level: if s.compression { 3 } else { 0 },
        query_workers: 4,
        enable_audit_log: false,
        ..FireLiteConfig::default()
    };

    if s.encryption {
        cfg.encryption_key = Some("bench-encryption-key-v1".to_string());
    }

    cfg
}

fn bench_path(tag: &str) -> PathBuf {
    std::env::temp_dir().join(format!("firelite-bench-{}-{}", tag, rand::random::<u32>()))
}

fn dir_size_bytes(root: &Path) -> u64 {
    let mut total = 0_u64;
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let read = match fs::read_dir(&path) {
            Ok(v) => v,
            Err(_) => continue,
        };

        for entry in read.flatten() {
            let p = entry.path();
            if let Ok(meta) = entry.metadata() {
                if meta.is_dir() {
                    stack.push(p);
                } else {
                    total = total.saturating_add(meta.len());
                }
            }
        }
    }

    total
}

fn file_pattern_size(root: &Path, needle: &str) -> u64 {
    let mut total = 0_u64;
    let mut stack = vec![root.to_path_buf()];

    while let Some(path) = stack.pop() {
        let read = match fs::read_dir(&path) {
            Ok(v) => v,
            Err(_) => continue,
        };

        for entry in read.flatten() {
            let p = entry.path();
            let Ok(meta) = entry.metadata() else { continue };
            if meta.is_dir() {
                stack.push(p);
                continue;
            }
            let name = p.file_name().and_then(|v| v.to_str()).unwrap_or_default();
            if name.contains(needle) {
                total = total.saturating_add(meta.len());
            }
        }
    }

    total
}

fn process_rss_bytes() -> usize {
    #[cfg(target_os = "linux")]
    {
        if let Ok(statm) = fs::read_to_string("/proc/self/statm") {
            if let Some(pages) = statm.split_whitespace().nth(1) {
                if let Ok(pages) = pages.parse::<usize>() {
                    return pages.saturating_mul(4096);
                }
            }
        }
    }
    0
}

fn summarize_latencies(samples: &mut [Duration]) -> LatencySummary {
    samples.sort_unstable();
    let len = samples.len();
    let pick = |pct: f64| -> Duration {
        let idx = (((len.saturating_sub(1)) as f64) * pct).round() as usize;
        samples[idx]
    };

    let min = samples.first().copied().unwrap_or_default();
    let max = samples.last().copied().unwrap_or_default();
    let total_ns: u128 = samples.iter().map(|d| d.as_nanos()).sum();
    let mean = if len == 0 {
        Duration::ZERO
    } else {
        Duration::from_nanos((total_ns / len as u128) as u64)
    };

    LatencySummary {
        p50: pick(0.50),
        p95: pick(0.95),
        p99: pick(0.99),
        min,
        max,
        mean,
    }
}

fn prep_db(path: &Path, scale: DatasetScale, config: ScenarioConfig) -> FireLite {
    let db = FireLite::open(path, build_config(config)).expect("open firelite db");
    let mut rng = StdRng::seed_from_u64(BENCH_SEED);
    let mut ops = Vec::with_capacity(1_000);

    for i in 0..scale.docs {
        let doc = make_doc(i, &mut rng);
        ops.push(BatchMutation::Put {
            collection: COLLECTION.into(),
            doc_id: i.to_string(),
            doc,
        });

        if ops.len() == 1_000 {
            db.write_batch(std::mem::take(&mut ops))
                .expect("seed batch");
        }
    }
    if !ops.is_empty() {
        db.write_batch(ops).expect("seed final batch");
    }
    db.flush().expect("seed flush");

    db
}

fn warm_cache(db: &FireLite, sample: usize) {
    for i in 0..sample {
        let _ = db.get(COLLECTION, &(i % sample.max(1)).to_string());
    }
}

fn bench_single_writes(db: &FireLite, start_id: usize, n: usize) -> ScenarioMetrics {
    let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x11);
    let mut lats = Vec::with_capacity(n);

    let begin = Instant::now();
    for i in 0..n {
        let id = start_id + i;
        let doc = make_doc(id, &mut rng);
        let t0 = Instant::now();
        db.put(COLLECTION, &id.to_string(), &doc)
            .expect("single put");
        lats.push(t0.elapsed());
    }
    db.flush().expect("single flush");

    metrics_from_samples(begin.elapsed(), n, lats, db)
}

fn bench_batch_writes(
    db: &FireLite,
    start_id: usize,
    n: usize,
    batch_size: usize,
) -> ScenarioMetrics {
    let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x22);
    let mut lats = Vec::new();

    let begin = Instant::now();
    let mut cursor = start_id;
    while cursor < start_id + n {
        let mut batch = Vec::with_capacity(batch_size);
        for _ in 0..batch_size {
            if cursor >= start_id + n {
                break;
            }
            batch.push(BatchMutation::Put {
                collection: COLLECTION.into(),
                doc_id: cursor.to_string(),
                doc: make_doc(cursor, &mut rng),
            });
            cursor += 1;
        }
        let t0 = Instant::now();
        db.write_batch(batch).expect("batch write");
        lats.push(t0.elapsed());
    }
    db.flush().expect("batch flush");

    metrics_from_samples(begin.elapsed(), n, lats, db)
}

fn bench_reads(db: &FireLite, scale: DatasetScale, random: bool, n: usize) -> ScenarioMetrics {
    let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x33);
    let mut lats = Vec::with_capacity(n);

    let begin = Instant::now();
    for i in 0..n {
        let id = if random {
            rng.gen_range(0..scale.docs)
        } else {
            i % scale.docs
        };
        let t0 = Instant::now();
        let out = db.get(COLLECTION, &id.to_string()).expect("read");
        black_box(out);
        lats.push(t0.elapsed());
    }

    metrics_from_samples(begin.elapsed(), n, lats, db)
}

fn bench_query_execution(db: &FireLite, scale: DatasetScale, n: usize) -> ScenarioMetrics {
    let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x44);
    let mut lats = Vec::with_capacity(n);

    let begin = Instant::now();
    for _ in 0..n {
        let age = rng.gen_range(18..88) as i64;
        let q = Query::new(COLLECTION)
            .where_filter("age", Operator::Gte, Value::Int(age))
            .where_filter("active", Operator::Eq, Value::Bool(true))
            .order_by("score", false)
            .limit((scale.docs / 100).max(10));
        let t0 = Instant::now();
        let rows = db.query(q).expect("query execution");
        black_box(rows.len());
        lats.push(t0.elapsed());
    }

    metrics_from_samples(begin.elapsed(), n, lats, db)
}

fn bench_pagination_offset(db: &FireLite, page_size: usize, pages: usize) -> ScenarioMetrics {
    let mut lats = Vec::with_capacity(pages);

    let begin = Instant::now();
    for page in 0..pages {
        let q = Query::new(COLLECTION)
            .order_by("id", true)
            .offset(page * page_size)
            .limit(page_size);
        let t0 = Instant::now();
        let rows = db.query(q).expect("offset pagination");
        black_box(rows.len());
        lats.push(t0.elapsed());
    }

    metrics_from_samples(begin.elapsed(), pages * page_size, lats, db)
}

fn bench_pagination_cursor(db: &FireLite, page_size: usize, pages: usize) -> ScenarioMetrics {
    let mut lats = Vec::with_capacity(pages);
    let mut cursor: Option<i64> = None;

    let begin = Instant::now();
    for _ in 0..pages {
        let mut q = Query::new(COLLECTION).order_by("id", true).limit(page_size);
        if let Some(after) = cursor {
            q = q.start_after(vec![Value::Int(after)]);
        }

        let t0 = Instant::now();
        let rows = db.query(q).expect("cursor pagination");
        cursor = rows.last().and_then(|(_, d)| d.get("id")).and_then(|v| {
            if let Value::Int(i) = v {
                Some(*i)
            } else {
                None
            }
        });
        black_box(rows.len());
        lats.push(t0.elapsed());

        if cursor.is_none() {
            break;
        }
    }

    metrics_from_samples(begin.elapsed(), pages * page_size, lats, db)
}

fn bench_index_costs(db: &FireLite) -> (f64, f64) {
    let build_t0 = Instant::now();
    db.create_index(COLLECTION, "age")
        .expect("create simple index");
    let _ = db.create_composite_index(
        COLLECTION,
        vec![
            ("tenant".to_string(), SortDirection::Asc),
            ("score".to_string(), SortDirection::Desc),
        ],
    );
    let build_ms = build_t0.elapsed().as_secs_f64() * 1_000.0;

    let q = Query::new(COLLECTION)
        .where_filter("tenant", Operator::Eq, Value::String("tenant-1".into()))
        .order_by("score", false)
        .limit(50);
    let lookup_t0 = Instant::now();
    let rows = db.query(q).expect("indexed lookup");
    black_box(rows.len());
    let lookup_us = lookup_t0.elapsed().as_secs_f64() * 1_000_000.0;

    (build_ms, lookup_us)
}

fn mixed_workload(db: &FireLite, scale: DatasetScale) -> ScenarioMetrics {
    let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x55);
    let mut lats = Vec::with_capacity(MIXED_OPS);

    let begin = Instant::now();
    for i in 0..MIXED_OPS {
        let op = i % 10;
        let t0 = Instant::now();
        match op {
            0..=3 => {
                let id = rng.gen_range(0..scale.docs);
                let out = db.get(COLLECTION, &id.to_string()).expect("mixed read");
                black_box(out);
            }
            4..=6 => {
                let q = Query::new(COLLECTION)
                    .where_filter("age", Operator::Gte, Value::Int(30))
                    .limit(20);
                let out = db.query(q).expect("mixed query");
                black_box(out.len());
            }
            7..=8 => {
                let id = scale.docs + i;
                let doc = make_doc(id, &mut rng);
                db.put(COLLECTION, &id.to_string(), &doc)
                    .expect("mixed put");
            }
            _ => {
                let id = rng.gen_range(0..scale.docs);
                db.delete(COLLECTION, &id.to_string())
                    .expect("mixed delete");
            }
        }
        lats.push(t0.elapsed());
    }

    db.flush().expect("mixed flush");
    metrics_from_samples(begin.elapsed(), MIXED_OPS, lats, db)
}

fn metrics_from_samples(
    elapsed: Duration,
    op_count: usize,
    mut latencies: Vec<Duration>,
    db: &FireLite,
) -> ScenarioMetrics {
    let throughput_ops_sec = op_count as f64 / elapsed.as_secs_f64().max(1e-9);
    let latency = summarize_latencies(&mut latencies);
    let stats = db.get_stats();
    let rss_bytes = process_rss_bytes();
    let disk_bytes = *stats.get("total_bytes").unwrap_or(&0) as u64;
    let wal_bytes = *stats.get("wal_bytes").unwrap_or(&0) as u64;
    let segment_bytes = *stats.get("segment_bytes").unwrap_or(&0) as u64;

    ScenarioMetrics {
        throughput_ops_sec,
        latency,
        rss_bytes,
        disk_bytes,
        wal_bytes,
        segment_bytes,
        index_build_ms: 0.0,
        index_lookup_us: 0.0,
    }
}

fn run_systematic_matrix() -> Vec<ComparisonRow> {
    let mut rows = Vec::new();

    for scale in DATASET_SCALES {
        for cfg in config_matrix() {
            let path = bench_path(&format!("{}-{}", scale.name, cfg.label()));
            let db = prep_db(&path, scale, cfg);

            // Cold: read/query before any manual warm-up.
            let cold_seq = bench_reads(&db, scale, false, (scale.docs / 10).max(500));
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "sequential_read",
                warm_state: "cold",
                metrics: cold_seq,
            });

            // Warm cache.
            warm_cache(&db, (scale.docs / 5).max(500));
            let mut warm_rand = bench_reads(&db, scale, true, (scale.docs / 10).max(500));
            let (build_ms, lookup_us) = bench_index_costs(&db);
            warm_rand.index_build_ms = build_ms;
            warm_rand.index_lookup_us = lookup_us;
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "random_read",
                warm_state: "warm",
                metrics: warm_rand,
            });

            let query = bench_query_execution(&db, scale, 150);
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "query_execution",
                warm_state: "warm",
                metrics: query,
            });

            let single_writes = bench_single_writes(&db, scale.docs * 2, 500);
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "single_write",
                warm_state: "warm",
                metrics: single_writes,
            });

            let batch_writes = bench_batch_writes(&db, scale.docs * 3, 1_000, 100);
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "batch_write_100",
                warm_state: "warm",
                metrics: batch_writes,
            });

            let offset_page = bench_pagination_offset(&db, 100, 20);
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "pagination_offset",
                warm_state: "warm",
                metrics: offset_page,
            });

            let cursor_page = bench_pagination_cursor(&db, 100, 20);
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "pagination_cursor",
                warm_state: "warm",
                metrics: cursor_page,
            });

            let mixed = mixed_workload(&db, scale);
            rows.push(ComparisonRow {
                scale: scale.name,
                config: cfg.label(),
                workload: "mixed_workload",
                warm_state: "warm",
                metrics: mixed,
            });

            let disk_after = dir_size_bytes(&path);
            let wal_after = file_pattern_size(&path, "wal");
            let seg_after = file_pattern_size(&path, "segment");

            eprintln!(
                "[matrix] scale={} cfg={} disk={} wal={} seg={}",
                scale.name,
                cfg.label(),
                disk_after,
                wal_after,
                seg_after
            );

            drop(db);
            let _ = fs::remove_dir_all(path);
        }
    }

    rows
}

fn print_analysis(rows: &[ComparisonRow]) {
    let mut grouped: HashMap<(&str, &str), Vec<&ComparisonRow>> = HashMap::new();
    for row in rows {
        grouped
            .entry((row.scale, row.workload))
            .or_default()
            .push(row);
    }

    eprintln!("\n==== FireLite Systematic Benchmark Analysis ====\n");

    for ((scale, workload), vals) in grouped {
        let mut sorted = vals;
        sorted.sort_by(|a, b| {
            b.metrics
                .throughput_ops_sec
                .partial_cmp(&a.metrics.throughput_ops_sec)
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        if let Some(best) = sorted.first() {
            let worst = sorted.last().unwrap_or(best);
            eprintln!(
                "[{scale}/{workload}] best={} ({}) {:.1} ops/s p50={:?} p95={:?} p99={:?} mean={:?}; \
worst={} ({}) {:.1} ops/s p50={:?} p95={:?} p99={:?} mean={:?}; rss={}KB disk={}B wal={}B seg={}B",
                best.config,
                best.warm_state,
                best.metrics.throughput_ops_sec,
                best.metrics.latency.p50,
                best.metrics.latency.p95,
                best.metrics.latency.p99,
                best.metrics.latency.mean,
                worst.config,
                worst.warm_state,
                worst.metrics.throughput_ops_sec,
                worst.metrics.latency.p50,
                worst.metrics.latency.p95,
                worst.metrics.latency.p99,
                worst.metrics.latency.mean,
                best.metrics.rss_bytes / 1024,
                best.metrics.disk_bytes,
                best.metrics.wal_bytes,
                best.metrics.segment_bytes,
            );
            eprintln!(
                "    spread: latency_min={:?} latency_max={:?} index_build_ms={:.3} index_lookup_us={:.3}",
                best.metrics.latency.min,
                best.metrics.latency.max,
                best.metrics.index_build_ms,
                best.metrics.index_lookup_us,
            );
        }
    }
}

fn systematic_matrix_benchmark(c: &mut Criterion) {
    c.bench_function("firelite_systematic_matrix_full_surface", |b| {
        b.iter(|| {
            let rows = run_systematic_matrix();
            print_analysis(&rows);
            black_box(rows.len());
        })
    });
}

fn criterion_microbenchmarks(c: &mut Criterion) {
    let scale = DATASET_SCALES[1]; // medium
    let cfg = ScenarioConfig {
        compression: false,
        encryption: false,
        durability: DurabilityMode::Manual,
    };
    let path = bench_path("criterion-micro");
    let db = prep_db(&path, scale, cfg);

    let mut write_group = c.benchmark_group("write_modes");
    write_group.throughput(Throughput::Elements(500));
    write_group.bench_with_input(
        BenchmarkId::new("single_put", scale.name),
        &500usize,
        |b, n| {
            let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x66);
            let mut id = scale.docs + 10;
            b.iter(|| {
                for _ in 0..*n {
                    let doc = make_doc(id, &mut rng);
                    db.put(COLLECTION, &id.to_string(), &doc).expect("put");
                    id += 1;
                }
            })
        },
    );
    write_group.bench_with_input(
        BenchmarkId::new("batch_put_100", scale.name),
        &500usize,
        |b, n| {
            let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x77);
            let mut id = scale.docs + 1000;
            b.iter(|| {
                let mut remaining = *n;
                while remaining > 0 {
                    let chunk = remaining.min(100);
                    let mut ops = Vec::with_capacity(chunk);
                    for _ in 0..chunk {
                        ops.push(BatchMutation::Put {
                            collection: COLLECTION.into(),
                            doc_id: id.to_string(),
                            doc: make_doc(id, &mut rng),
                        });
                        id += 1;
                    }
                    db.write_batch(ops).expect("batch put");
                    remaining -= chunk;
                }
            })
        },
    );
    write_group.finish();

    let mut read_group = c.benchmark_group("read_patterns");
    read_group.throughput(Throughput::Elements(2_000));
    read_group.bench_function("sequential", |b| {
        let mut cursor = 0usize;
        b.iter(|| {
            for _ in 0..2_000 {
                let out = db.get(COLLECTION, &cursor.to_string()).expect("seq read");
                black_box(out);
                cursor = (cursor + 1) % scale.docs;
            }
        })
    });
    read_group.bench_function("random", |b| {
        let mut rng = StdRng::seed_from_u64(BENCH_SEED ^ 0x88);
        b.iter(|| {
            for _ in 0..2_000 {
                let id = rng.gen_range(0..scale.docs);
                let out = db.get(COLLECTION, &id.to_string()).expect("rand read");
                black_box(out);
            }
        })
    });
    read_group.finish();

    let mut query_group = c.benchmark_group("queries_and_pagination");
    query_group.bench_function("query_indexed", |b| {
        db.create_index(COLLECTION, "age").expect("index age");
        b.iter(|| {
            let q = Query::new(COLLECTION)
                .where_filter("age", Operator::Gte, Value::Int(32))
                .order_by("score", false)
                .limit(100);
            let out = db.query(q).expect("query");
            black_box(out.len());
        })
    });
    query_group.bench_function("pagination_offset", |b| {
        b.iter(|| {
            let q = Query::new(COLLECTION)
                .order_by("id", true)
                .offset(1_000)
                .limit(100);
            let out = db.query(q).expect("offset");
            black_box(out.len());
        })
    });
    query_group.bench_function("pagination_cursor", |b| {
        b.iter(|| {
            let q = Query::new(COLLECTION)
                .order_by("id", true)
                .start_after(vec![Value::Int(1_000)])
                .limit(100);
            let out = db.query(q).expect("cursor");
            black_box(out.len());
        })
    });
    query_group.finish();

    drop(db);
    let _ = fs::remove_dir_all(path);
}

criterion_group!(
    benches,
    criterion_microbenchmarks,
    systematic_matrix_benchmark
);
criterion_main!(benches);
