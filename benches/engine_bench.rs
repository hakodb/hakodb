// use criterion::{black_box, criterion_group, criterion_main, Criterion};
// use firelite::config::{DurabilityMode, FireLiteConfig};
// use firelite::document::firelite_doc::FireLiteDoc;
// use firelite::document::value::Value;
// use firelite::engine::FireLite;

// fn write_benchmark(c: &mut Criterion) {
//     c.bench_function("firelite_put_1000", |b| {
//         b.iter(|| {
//             let path = std::env::temp_dir().join(format!(
//                 "firelite-bench-{}",
//                 std::time::SystemTime::now()
//                     .duration_since(std::time::UNIX_EPOCH)
//                     .expect("clock")
//                     .as_nanos()
//             ));
//             let db = FireLite::open(
//                 &path,
//                 FireLiteConfig {
//                     durability_mode: DurabilityMode::Manual,
//                     ..FireLiteConfig::default()
//                 },
//             )
//             .expect("open");

//             for i in 0..1000 {
//                 let mut doc = FireLiteDoc::default();
//                 doc.insert("id", Value::Int(i));
//                 db.put("bench", &i.to_string(), &doc).expect("put");
//             }

//             db.flush().expect("flush");
//             std::fs::remove_dir_all(path).expect("cleanup");
//             black_box(())
//         })
//     });
// }

// criterion_group!(benches, write_benchmark);
// criterion_main!(benches);

use criterion::{black_box, criterion_group, criterion_main, Criterion};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

const DATASET: i64 = 10_000;

fn prepare_db() -> (FireLite, std::path::PathBuf) {
    let path = std::env::temp_dir().join("firelite-bench-db");

    let db = FireLite::open(
        &path,
        FireLiteConfig {
            durability_mode: DurabilityMode::Manual,
            ..FireLiteConfig::default()
        },
    )
    .expect("open");

    for i in 0..DATASET {
        let mut doc = FireLiteDoc::default();
        doc.insert("id", Value::Int(i));
        db.put("bench", &i.to_string(), &doc).expect("put");
    }

    db.flush().expect("flush");

    (db, path)
}

fn write_benchmark(c: &mut Criterion) {
    let (db, path) = prepare_db();

    let mut counter: i64 = DATASET;

    c.bench_function("firelite_put", |b| {
        b.iter(|| {
            let mut doc = FireLiteDoc::default();
            doc.insert("id", Value::Int(counter));

            db.put("bench", &counter.to_string(), &doc)
                .expect("put");

            counter += 1;

            black_box(())
        })
    });

    std::fs::remove_dir_all(path).ok();
}

// fn read_random_benchmark(c: &mut Criterion) {
//     let (db, path) = prepare_db();

//     c.bench_function("firelite_get_random", |b| {
//         b.iter(|| {
//             let key = (black_box(rand::random::<u64>()) % DATASET as u64).to_string();

//             let _ = db.get("bench", &key).expect("get");

//             black_box(())
//         })
//     });

//     std::fs::remove_dir_all(path).ok();
// }

// fn read_sequential_benchmark(c: &mut Criterion) {
//     let (db, path) = prepare_db();

//     let mut idx: i64 = 0;

//     c.bench_function("firelite_get_sequential", |b| {
//         b.iter(|| {
//             let key = idx.to_string();

//             let _ = db.get("bench", &key).expect("get");

//             idx = (idx + 1) % DATASET;

//             black_box(())
//         })
//     });

//     std::fs::remove_dir_all(path).ok();
// }

criterion_group!(
    benches,
    write_benchmark,
    // read_random_benchmark,
    // read_sequential_benchmark
);
criterion_main!(benches);