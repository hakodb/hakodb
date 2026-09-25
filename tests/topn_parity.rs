use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::executor::executor::topn_runs;
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

// ponytail: 2k docs keeps `cargo test` fast; the TopN-vs-legacy comparison
// is shape-exact at any volume (perf separation is measured, not asserted).
const N: usize = 2_000;

fn open_bench(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    while !db.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn seed(db: &Hako) {
    for chunk in (0..N).collect::<Vec<_>>().chunks(500) {
        let mut batch = Vec::with_capacity(chunk.len());
        for &i in chunk {
            let mut doc = HakoDoc::default();
            // Heavy ties (age % 50) exercise the seq tiebreak; every 11th
            // doc lacks age entirely (None-ordering parity).
            if i % 11 != 0 {
                doc.insert("age", Value::Int((i % 50) as i64));
            }
            doc.insert("name", Value::String(format!("t{}", i % 37)));
            batch.push(BatchMutation::Put {
                collection: "topn".into(),
                doc_id: format!("d{i:05}"),
                doc,
            });
        }
        db.write_batch(batch).expect("seed batch");
    }
    wait_ready(db);
}

/// Legacy oracle: same shape WITHOUT limit/offset (hook requires limit, so
/// this always takes the full-scan path), truncated in-test to the page.
fn legacy_page(db: &Hako, mut q: Query, limit: usize, offset: usize) -> Vec<String> {
    q.limit = None;
    q.offset = None;
    let rows = db.query(q).expect("legacy query");
    rows.into_iter().skip(offset).take(limit).map(|(id, _)| id).collect()
}

fn topn_page(db: &Hako, mut q: Query, limit: usize, offset: usize) -> Vec<String> {
    q.limit = Some(limit);
    q.offset = Some(offset);
    db.query(q).expect("topn query").into_iter().map(|(id, _)| id).collect()
}

fn check(db: &Hako, build: impl Fn() -> Query, limit: usize, offset: usize) {
    let t0 = std::time::Instant::now();
    let a = topn_page(db, build(), limit, offset);
    let topn_us = t0.elapsed().as_micros();
    let t1 = std::time::Instant::now();
    let b = legacy_page(db, build(), limit, offset);
    let legacy_us = t1.elapsed().as_micros();
    assert_eq!(a, b, "topn != legacy (limit={limit} offset={offset})");
    eprintln!("shape limit={limit:>5} offset={offset:>5}: topn={topn_us:>7}us legacy={legacy_us:>7}us");
}

fn age_ord(desc: bool) -> Query {
    Query::new("topn").order_by("age", !desc)
}

#[test]
fn topn_parity_matrix() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-topn-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let db = open_bench(&dir);
    seed(&db);
    let before = topn_runs();

    // Single-field order, both directions, offsets, over/under-fetch.
    check(&db, || age_ord(false), 100, 0);
    check(&db, || age_ord(true), 100, 0);
    check(&db, || age_ord(false), 100, 150);
    check(&db, || age_ord(true), 50, 1990);
    // Over-fetch (limit >= rows): legacy lane by design (no eviction
    // possible) — equality holds, counter must NOT move.
    let g0 = topn_runs();
    check(&db, || age_ord(false), 5000, 0);
    assert_eq!(topn_runs(), g0, "over-fetch must stay legacy");
    check(&db, || age_ord(false), 0, 0);
    // String order + multi-order (ties across both keys).
    check(&db, || Query::new("topn").order_by("name", true), 50, 0);
    check(&db, || {
        Query::new("topn").order_by("age", true).order_by("name", false)
    }, 100, 0);
    // Logical-time order.
    check(&db, || Query::new("topn").order_by("_time", false), 100, 0);
    // Filtered: eq point + range sweep, with and without offset.
    check(&db, || {
        Query::new("topn")
            .where_filter("age", Operator::Eq, Value::Int(7))
            .order_by("age", false)
    }, 50, 0);
    check(&db, || {
        Query::new("topn")
            .where_filter("age", Operator::Gt, Value::Int(40))
            .order_by("age", true)
    }, 100, 10);
    check(&db, || {
        Query::new("topn")
            .where_filter("name", Operator::Eq, Value::String("t3".into()))
            .order_by("name", true)
            .order_by("age", false)
    }, 25, 5);
    // Control: no order → legacy lane in both (sanity, not TopN).
    check(&db, || Query::new("topn"), 100, 0);

    // Engagement: every ordered shape above took the TopN lane (the
    // over-fetch guard is asserted separately, the no-order control never).
    assert_eq!(topn_runs() - before, 11, "topn lane did not fire");
    let _ = std::fs::remove_dir_all(&dir);
}
