use criterion::{black_box, criterion_group, criterion_main, Criterion};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

fn write_benchmark(c: &mut Criterion) {
    c.bench_function("firelite_put_1000", |b| {
        b.iter(|| {
            let path = std::env::temp_dir().join(format!(
                "firelite-bench-{}",
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .expect("clock")
                    .as_nanos()
            ));
            let db = FireLite::open(
                &path,
                FireLiteConfig {
                    durability_mode: DurabilityMode::Manual,
                    ..FireLiteConfig::default()
                },
            )
            .expect("open");

            for i in 0..1000 {
                let mut doc = FireLiteDoc::default();
                doc.insert("id", Value::Int(i));
                db.put("bench", &i.to_string(), &doc).expect("put");
            }

            db.flush().expect("flush");
            std::fs::remove_dir_all(path).expect("cleanup");
            black_box(())
        })
    });
}

criterion_group!(benches, write_benchmark);
criterion_main!(benches);
