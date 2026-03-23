use criterion::{black_box, criterion_group, criterion_main, Criterion};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use rand::{thread_rng, Rng};
use std::sync::Arc;

const DATASET: usize = 10_000;
const BATCH_SIZE: usize = 100;
const READ_THREADS: usize = 8;

//
// Minimal Hot-Key Generator (Zipf-like)
//
pub struct HotKeyGen {
    size: usize,
}

impl HotKeyGen {
    pub fn new(size: usize) -> Self {
        Self { size }
    }

    pub fn sample<R: Rng>(&self, rng: &mut R) -> usize {
        if rng.gen::<f64>() < 0.8 {
            rng.gen_range(0..(self.size / 5).max(1))
        } else {
            rng.gen_range(0..self.size)
        }
    }
}

//
// Database Setup
//
fn prepare_db() -> (FireLite, std::path::PathBuf) {
    let path = std::env::temp_dir().join(format!("firelite-bench-{}", thread_rng().gen::<u32>()));
    let db = FireLite::open(
        &path,
        FireLiteConfig {
            durability_mode: DurabilityMode::Manual,
            ..FireLiteConfig::default()
        },
    )
    .expect("open");

    // Populate initial data
    let mut mutations = Vec::with_capacity(1000);
    for i in 0..DATASET {
        let mut doc = FireLiteDoc::default();
        doc.insert("id", Value::Int(i as i64));
        mutations.push(BatchMutation::Put {
            collection: "bench".into(),
            doc_id: i.to_string(),
            doc,
        });

        if mutations.len() >= 1000 {
            db.write_batch(mutations.drain(..).collect()).unwrap();
        }
    }
    if !mutations.is_empty() {
        db.write_batch(mutations).unwrap();
    }

    db.flush().unwrap();
    (db, path)
}

//
// Single Write with Watcher (onSnapshot)
//
fn watch_latency_benchmark(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let rx = db.watch_collection("bench");
    let mut counter = DATASET;

    c.bench_function("firelite_watch_single_put", |b| {
        b.iter(|| {
            let mut doc = FireLiteDoc::default();
            doc.insert("id", Value::Int(counter as i64));
            
            db.put("bench", &counter.to_string(), &doc).unwrap();
            
            // Simulation of a real app: the watcher drains the event
            let event = rx.try_recv().unwrap();
            black_box(event);

            counter += 1;
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// Reading Benchmarks
//
fn read_benchmarks(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let db = Arc::new(db);
    let mut rng = thread_rng();
    let hot = HotKeyGen::new(DATASET);

    // 1. Sequential Read
    c.bench_function("firelite_read_sequential", |b| {
        let mut idx = 0;
        b.iter(|| {
            let res = db.get("bench", &idx.to_string()).unwrap();
            black_box(res);
            idx = (idx + 1) % DATASET;
        })
    });

    // 2. Hot-Key Read (Zipf)
    c.bench_function("firelite_read_hotkey", |b| {
        b.iter(|| {
            let key = hot.sample(&mut rng);
            let res = db.get("bench", &key.to_string()).unwrap();
            black_box(res);
        })
    });

    // 3. Parallel Read (Multi-threaded)
    // We measure the total time for READ_THREADS to each do 100 reads
    c.bench_function("firelite_read_parallel_8", |b| {
        b.iter(|| {
            std::thread::scope(|s| {
                for t in 0..READ_THREADS {
                    let db = Arc::clone(&db);
                    s.spawn(move || {
                        for i in 0..100 {
                            let key = (i + t * 100) % DATASET;
                            let res = db.get("bench", &key.to_string()).unwrap();
                            black_box(res);
                        }
                    });
                }
            });
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// Write Benchmarks (from previous)
//
fn write_benchmarks(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let mut counter = DATASET;

    c.bench_function("firelite_put_single", |b| {
        b.iter(|| {
            let mut doc = FireLiteDoc::default();
            doc.insert("id", Value::Int(counter as i64));
            db.put("bench", &counter.to_string(), &doc).unwrap();
            counter += 1;
        })
    });

    c.bench_function("firelite_put_batch_100", |b| {
        b.iter(|| {
            let mut mutations = Vec::with_capacity(BATCH_SIZE);
            for _ in 0..BATCH_SIZE {
                let mut doc = FireLiteDoc::default();
                doc.insert("id", Value::Int(counter as i64));
                mutations.push(BatchMutation::Put {
                    collection: "bench".to_string(),
                    doc_id: counter.to_string(),
                    doc,
                });
                counter += 1;
            }
            db.write_batch(mutations).unwrap();
        });
    });

    std::fs::remove_dir_all(path).ok();
}

criterion_group!(
    benches,
    write_benchmarks,
    watch_latency_benchmark,
    read_benchmarks,
);
criterion_main!(benches);