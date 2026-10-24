//! P4: `min` / `max` in simple aggregation (index fast path, scan slow path,
//! parallel merge, and rebuilt state after reopen).

use std::path::PathBuf;
use std::time::{Duration, Instant};

use hashbrown::HashMap;

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::query::{AggregateOp, Query};

fn cfg() -> HakoConfig {
    HakoConfig {
        durability_mode: DurabilityMode::Manual,
        ..Default::default()
    }
}

fn open(label: &str, config: HakoConfig) -> (PathBuf, Hako) {
    let dir = std::env::temp_dir().join(format!(
        "hako-minmax-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = Hako::open(&dir, config).expect("open");
    (dir, db)
}

fn doc(n: i64, grp: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("n", Value::Int(n));
    d.insert("grp", Value::String(grp.into()));
    d
}

fn seed(db: &Hako, collection: &str, count: i64) {
    let batch: Vec<_> = (0..count)
        .map(|i| BatchMutation::Put {
            collection: collection.into(),
            doc_id: format!("d{i:05}"),
            doc: doc(i, if i % 2 == 0 { "even" } else { "odd" }),
        })
        .collect();
    db.write_batch(batch).expect("seed");
}

fn wait_indexes(db: &Hako) {
    let t0 = Instant::now();
    while db.quiescence_status().index_backfills > 0 {
        assert!(
            t0.elapsed() < Duration::from_secs(60),
            "index backfill never drains"
        );
        std::thread::sleep(Duration::from_millis(5));
    }
}

fn agg(db: &Hako, q: Query) -> HashMap<String, f64> {
    db.execute_aggregation(q).expect("aggregate")
}

#[test]
fn indexed_fast_path_min_max_with_equality_filter() {
    let (dir, db) = open("fast", cfg());
    seed(&db, "c", 50);
    db.create_index("c", "n").unwrap();
    db.create_index("c", "grp").unwrap();
    wait_indexes(&db);

    assert_eq!(agg(&db, Query::new("c").aggregate(AggregateOp::Min("n".into())))["min_n"], 0.0);
    assert_eq!(agg(&db, Query::new("c").aggregate(AggregateOp::Max("n".into())))["max_n"], 49.0);

    let odd = Query::new("c")
        .where_eq("grp", Value::String("odd".into()))
        .aggregate(AggregateOp::Min("n".into()));
    assert_eq!(agg(&db, odd)["min_n"], 1.0);
    let odd = Query::new("c")
        .where_eq("grp", Value::String("odd".into()))
        .aggregate(AggregateOp::Max("n".into()));
    assert_eq!(agg(&db, odd)["max_n"], 49.0);

    db.flush().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn scan_slow_path_skips_non_numeric_and_missing() {
    let (dir, db) = open("slow", cfg());
    seed(&db, "c", 50);

    let mut no_number = HakoDoc::default();
    no_number.insert("grp", Value::String("odd".into()));
    db.put("c", "missing", &no_number).unwrap();
    let mut text = HakoDoc::default();
    text.insert("n", Value::String("nope".into()));
    db.put("c", "text", &text).unwrap();

    assert_eq!(agg(&db, Query::new("c").aggregate(AggregateOp::Min("n".into())))["min_n"], 0.0);
    assert_eq!(agg(&db, Query::new("c").aggregate(AggregateOp::Max("n".into())))["max_n"], 49.0);

    db.flush().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn empty_or_non_numeric_set_omits_the_key() {
    let (dir, db) = open("empty", cfg());

    assert!(agg(&db, Query::new("c").aggregate(AggregateOp::Min("n".into()))).is_empty());
    assert!(agg(&db, Query::new("c").aggregate(AggregateOp::Max("n".into()))).is_empty());

    let mut text = HakoDoc::default();
    text.insert("n", Value::String("nope".into()));
    db.put("c", "text", &text).unwrap();
    db.create_index("c", "n").unwrap();
    wait_indexes(&db);
    assert!(agg(&db, Query::new("c").aggregate(AggregateOp::Min("n".into()))).is_empty());
    assert!(agg(&db, Query::new("c").aggregate(AggregateOp::Max("n".into()))).is_empty());

    assert_eq!(db.collection("c").all().min("n").unwrap(), None);
    assert_eq!(db.collection("c").all().max("n").unwrap(), None);

    db.flush().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn multiple_ops_share_one_scan() {
    let (dir, db) = open("multi", cfg());
    seed(&db, "c", 50);

    let q = Query::new("c")
        .aggregate(AggregateOp::Count)
        .aggregate(AggregateOp::Sum("n".into()))
        .aggregate(AggregateOp::Min("n".into()))
        .aggregate(AggregateOp::Max("n".into()));
    let res = agg(&db, q);
    assert_eq!(res["count"], 50.0);
    assert_eq!(res["sum_n"], (0..50).sum::<i64>() as f64);
    assert_eq!(res["min_n"], 0.0);
    assert_eq!(res["max_n"], 49.0);

    db.flush().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn parallel_merge_and_reopen_parity() {
    let config = cfg();
    let (dir, db) = open("parallel", config.clone());
    seed(&db, "c", 1200);
    db.flush().unwrap();

    // doc_count >= 1000 dispatches the parallel fold; min/max must not sum.
    let res = agg(
        &db,
        Query::new("c")
            .aggregate(AggregateOp::Min("n".into()))
            .aggregate(AggregateOp::Max("n".into())),
    );
    assert_eq!(res["min_n"], 0.0);
    assert_eq!(res["max_n"], 1199.0);
    drop(db);

    let db = Hako::open(&dir, config).expect("reopen");
    wait_indexes(&db);
    let res = agg(
        &db,
        Query::new("c")
            .aggregate(AggregateOp::Min("n".into()))
            .aggregate(AggregateOp::Max("n".into())),
    );
    assert_eq!(res["min_n"], 0.0);
    assert_eq!(res["max_n"], 1199.0);

    db.flush().unwrap();
    std::fs::remove_dir_all(&dir).ok();
}
