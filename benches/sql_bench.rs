use criterion::{black_box, criterion_group, criterion_main, Criterion};
use tokio::runtime::Runtime;

use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqliteSynchronous};
use sqlx::{Connection, SqliteConnection};
use std::str::FromStr;

use std::env;
use std::sync::{Arc, Mutex};
use std::thread;
use std::time::SystemTime;

const DATASET: i64 = 10_000;
const BATCH_SIZE: i64 = 100;
const READ_THREADS: usize = 8;

let path = std::env::temp_dir().join("sqlite-bench.db");

let options = SqliteConnectOptions::from_str(
    path.to_str().unwrap()
)?
.create_if_missing(true)
.journal_mode(SqliteJournalMode::Wal)
.synchronous(SqliteSynchronous::Off);

let mut conn = SqliteConnection::connect_with(&options).await?;

fn tmp_db_path() -> String {
    let path = env::temp_dir().join(format!(
        "sqlite-bench-{}",
        SystemTime::now()
            .duration_since(SystemTime::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));

    format!("sqlite://{}", path.to_string_lossy().replace("\\", "/"))
}

fn prepare_db(rt: &Runtime, url: &str) {
    rt.block_on(async {
        // let mut conn = SqliteConnection::connect(url).await.expect("connect");

        // SQLite tuning (similar durability to FireLite manual mode)
        conn.execute("PRAGMA journal_mode=WAL;").await.unwrap();
        conn.execute("PRAGMA synchronous=OFF;").await.unwrap();
        conn.execute("PRAGMA temp_store=MEMORY;").await.unwrap();

        conn.execute(
            r#"
            CREATE TABLE bench (
                id INTEGER PRIMARY KEY,
                value INTEGER
            )
        "#,
        )
        .await
        .unwrap();

        for i in 0..DATASET {
            sqlx::query("INSERT INTO bench (id,value) VALUES (?,?)")
                .bind(i)
                .bind(i)
                .execute(&mut conn)
                .await
                .unwrap();
        }
    });
}

//
// SINGLE WRITE
//

fn write_single_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let url = tmp_db_path();

    prepare_db(&rt, &url);

    let mut conn = rt.block_on(SqliteConnection::connect(&url)).unwrap();
    let mut counter = DATASET;

    c.bench_function("sqlite_put_single", |b| {
        b.iter(|| {
            rt.block_on(async {
                sqlx::query("INSERT INTO bench (id,value) VALUES (?,?)")
                    .bind(counter)
                    .bind(counter)
                    .execute(&mut conn)
                    .await
                    .unwrap();
            });

            counter += 1;

            black_box(())
        })
    });
}

//
// BATCH WRITE
//

fn write_batch_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let url = tmp_db_path();

    prepare_db(&rt, &url);

    let mut conn = rt.block_on(SqliteConnection::connect(&url)).unwrap();
    let mut counter = DATASET;

    c.bench_function("sqlite_put_batch_100", |b| {
        b.iter(|| {
            rt.block_on(async {
                conn.execute("BEGIN").await.unwrap();

                for _ in 0..BATCH_SIZE {
                    sqlx::query("INSERT INTO bench (id,value) VALUES (?,?)")
                        .bind(counter)
                        .bind(counter)
                        .execute(&mut conn)
                        .await
                        .unwrap();

                    counter += 1;
                }

                conn.execute("COMMIT").await.unwrap();
            });

            black_box(())
        })
    });
}

//
// PARALLEL READ
//

fn read_parallel_benchmark(c: &mut Criterion) {
    let rt = Runtime::new().unwrap();
    let url = tmp_db_path();

    prepare_db(&rt, &url);

    let conn = rt.block_on(SqliteConnection::connect(&url)).unwrap();
    let conn = Arc::new(Mutex::new(conn));

    c.bench_function("sqlite_get_parallel_8", |b| {
        b.iter(|| {
            let mut handles = Vec::new();

            for _ in 0..READ_THREADS {
                let conn = conn.clone();
                let rt = Runtime::new().unwrap();

                handles.push(thread::spawn(move || {
                    for _ in 0..1000 {
                        let key = rand::random::<u64>() % DATASET as u64;

                        let mut conn = conn.lock().unwrap();

                        rt.block_on(async {
                            let _ =
                                sqlx::query("SELECT value FROM bench WHERE id=?")
                                    .bind(key as i64)
                                    .fetch_optional(&mut *conn)
                                    .await
                                    .unwrap();
                        });
                    }
                }));
            }

            for h in handles {
                h.join().unwrap();
            }

            black_box(())
        })
    });
}

criterion_group!(
    benches,
    write_single_benchmark,
    write_batch_benchmark,
    read_parallel_benchmark
);

criterion_main!(benches);