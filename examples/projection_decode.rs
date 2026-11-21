//! Deterministic projection-vs-full query microbench (issue #234 baseline).
//!
//! Run from the hakodb root:
//!   cargo run --release --example projection_decode
//!
//! 1000 wide documents (8 x 512 B fields), 200 loops, identical filter and
//! limit. The projected lane selects three small fields, so any regression
//! in selective decoding shows up as a QPS drop relative to the full lane.
use std::time::Instant;

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

const DOCS: usize = 1000;
const LOOPS: usize = 200;

fn main() {
    let dir = std::env::temp_dir().join(format!("hako-projection-decode-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&dir, cfg).expect("open");

    for chunk in (0..DOCS).collect::<Vec<_>>().chunks(200) {
        let mut batch = Vec::with_capacity(chunk.len());
        for &i in chunk {
            let mut doc = HakoDoc::default();
            doc.insert("tenant", Value::String(format!("tenant-{}", i % 40)));
            doc.insert("active", Value::Bool(i % 2 == 0));
            doc.insert("extra", Value::Int(i as i64));
            for f in 0..8 {
                doc.insert(
                    format!("pad{f}"),
                    Value::String(format!("{:0512}", i)),
                );
            }
            batch.push(BatchMutation::Put {
                collection: "bench".into(),
                doc_id: format!("b_{i:05}"),
                doc,
            });
        }
        db.write_batch(batch).expect("seed");
    }
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let measure = |label: &str, projected: bool| {
        let started = Instant::now();
        for _ in 0..LOOPS {
            let mut query = Query::new("bench")
                .where_filter("tenant", Operator::Eq, Value::String("tenant-2".into()))
                .limit(20);
            if projected {
                query = query.select_fields(vec![
                    "tenant".into(),
                    "active".into(),
                    "extra".into(),
                ]);
            }
            let rows = db.query(query).expect("query");
            assert_eq!(rows.len(), 20, "{label}: wrong row count");
            if projected {
                assert!(
                    !rows[0].1.fields.iter().any(|(k, _)| k.as_ref() == "pad0"),
                    "projection must drop unselected wide fields"
                );
            }
        }
        let secs = started.elapsed().as_secs_f64();
        println!(
            "{label}: {} qps ({} loops in {:.3}s)",
            (LOOPS as f64 / secs) as u64,
            LOOPS,
            secs
        );
    };

    // Warm the planner and page cache before timing.
    measure("warm full", false);
    measure("warm projected", true);
    measure("full", false);
    measure("projected", true);

    // TOPN shape: unsorted ORDER BY + small limit over all candidates.
    let topn_started = Instant::now();
    for _ in 0..LOOPS {
        let query = Query::new("bench")
            .order_by("extra", false)
            .select_fields(vec!["tenant".into(), "active".into()])
            .limit(20);
        let rows = db
            .query_projected_zero_copy(query, &["tenant".into(), "active".into()])
            .expect("topn query");
        assert_eq!(rows.len(), 20, "topn: wrong row count");
    }
    let secs = topn_started.elapsed().as_secs_f64();
    println!("projected TOPN: {} qps ({} loops in {:.3}s)",
        (LOOPS as f64 / secs) as u64, LOOPS, secs);
}
