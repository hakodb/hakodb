use criterion::{black_box, criterion_group, criterion_main, Criterion};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

// use rand::Rng;
use rand_distr::{Distribution, Zipf};
use rand::thread_rng;

use std::sync::{Arc};
use std::thread;

const DATASET: usize = 10_000;
const BATCH_SIZE: usize = 100;
const READ_THREADS: usize = 8;

fn prepare_db() -> (FireLite, std::path::PathBuf) {
    let path = std::env::temp_dir().join("firelite-bench-db");

    let db = FireLite::open(
        &path,
        FireLiteConfig {
            durability_mode: DurabilityMode::Manual,
            ..FireLiteConfig::default()
        },
    ).expect("open");

    for i in 0..DATASET {
        let mut doc = FireLiteDoc::default();
        doc.insert("id", Value::Int(i as i64));
        db.put("bench", &i.to_string(), &doc).unwrap();
    }

    db.flush().unwrap();

    (db, path)
}

//
// SINGLE WRITE
//

fn write_single(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let mut counter = DATASET;

    c.bench_function("firelite_put_single", |b| {
        b.iter(|| {

            let mut doc = FireLiteDoc::default();
            doc.insert("id", Value::Int(counter as i64));

            db.put("bench", &counter.to_string(), &doc).unwrap();

            counter += 1;

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// BATCH WRITE
//

fn write_batch(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let mut counter = DATASET;

    c.bench_function("firelite_put_batch_100", |b| {
        b.iter(|| {

            for _ in 0..BATCH_SIZE {
                let mut doc = FireLiteDoc::default();
                doc.insert("id", Value::Int(counter as i64));

                db.put("bench", &counter.to_string(), &doc).unwrap();

                counter += 1;
            }

            db.flush().unwrap();

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// SEQUENTIAL READ
//

fn read_sequential(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let mut idx = 0;

    c.bench_function("firelite_get_sequential", |b| {
        b.iter(|| {

            let key = idx.to_string();
            let _ = db.get("bench", &key).unwrap();

            idx = (idx + 1) % DATASET;

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// ZIPFIAN READ (REALISTIC CACHE HOTSPOT)
//

fn read_zipf(c: &mut Criterion) {
    let (db, path) = prepare_db();

    let mut rng = thread_rng();
    let zipf = Zipf::new(DATASET as f64, 1.03).unwrap();

    c.bench_function("firelite_get_zipf", |b| {
        b.iter(|| {

            let key = zipf.sample(&mut rng) as usize % DATASET;

            let _ = db.get("bench", &key.to_string()).unwrap();

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// MULTITHREAD READ
//

fn read_parallel(c: &mut Criterion) {
    let (db, path) = prepare_db();
    let db = Arc::new(db);

    c.bench_function("firelite_get_parallel_8", |b| {
        b.iter(|| {

            let mut handles = Vec::new();

            for _ in 0..READ_THREADS {

                let db = db.clone();

                handles.push(thread::spawn(move || {

                    for _ in 0..2000 {

                        let key =
                            (rand::random::<usize>() % DATASET).to_string();

                        let _ = db.get("bench", &key).unwrap();

                    }

                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

//
// MIXED WORKLOAD
//

fn mixed_workload(c: &mut Criterion) {
    let (db, path) = prepare_db();

    let db = Arc::new(db);

    c.bench_function("firelite_mixed_70r_30w", |b| {
        b.iter(|| {

            let mut handles = Vec::new();

            for _ in 0..READ_THREADS {

                let db = db.clone();

                handles.push(thread::spawn(move || {

                    let mut rng = rand::thread_rng();

                    for i in 0..1000 {

                        if rng.gen_bool(0.7) {

                            let key =
                                (rand::random::<usize>() % DATASET).to_string();

                            let _ = db.get("bench", &key).unwrap();

                        } else {

                            let mut doc = FireLiteDoc::default();
                            doc.insert("id", Value::Int(i as i64));

                            let key = format!("w{}", i);

                            let _ = db.put("bench", &key, &doc);

                        }

                    }

                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

criterion_group!(
    benches,
    write_single,
    write_batch,
    read_sequential,
    read_zipf,
    read_parallel,
    mixed_workload
);

criterion_main!(benches);