//! Equivalence for the composite covering aggregation lane.
//!
//! A predicate prefix plus the aggregate field in one composite index must
//! produce exactly the same values as scanning and aggregating the documents.
//! Shapes outside the gate keep the historical paths.

use std::collections::HashMap;

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::{AggregateOp, Query};

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-agg-cover-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn open(path: &std::path::Path, composite: bool) -> Hako {
    let cfg = HakoConfig {
        durability_mode: DurabilityMode::Manual,
        ..Default::default()
    };
    let db = Hako::open(path, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..600usize {
        let mut doc = HakoDoc::default();
        doc.insert("category", Value::String(format!("cat-{}", i % 5)));
        doc.insert("tenant", Value::String(format!("t{:02}", i % 12)));
        doc.insert("score", Value::Int((i % 97) as i64));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:04}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    if composite {
        db.create_composite_index(
            "records",
            vec![
                ("category".into(), hakodb::index::composite::definition::SortDirection::Asc),
                ("tenant".into(), hakodb::index::composite::definition::SortDirection::Asc),
                ("score".into(), hakodb::index::composite::definition::SortDirection::Asc),
            ],
        )
        .expect("composite");
    }
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
    db
}

fn base_query() -> Query {
    Query::new("records")
        .where_filter("category", Operator::Eq, Value::String("cat-1".into()))
        .where_filter("tenant", Operator::Eq, Value::String("t02".into()))
        .limit(1000)
}

fn reference_numbers(db: &Hako, query: &Query) -> Vec<f64> {
    db.query(query.clone())
        .expect("query")
        .into_iter()
        .filter_map(|(_, doc)| match doc.get("score") {
            Some(Value::Int(v)) => Some(*v as f64),
            Some(Value::Float(v)) => Some(*v),
            _ => None,
        })
        .collect()
}

fn expected(op: &AggregateOp, nums: &[f64]) -> Option<f64> {
    match op {
        AggregateOp::Count => Some(nums.len() as f64),
        AggregateOp::Sum(_) => Some(nums.iter().sum()),
        AggregateOp::Avg(_) => {
            if nums.is_empty() { None } else { Some(nums.iter().sum::<f64>() / nums.len() as f64) }
        }
        AggregateOp::Min(_) => nums.iter().copied().min_by(|a, b| a.partial_cmp(b).unwrap()),
        AggregateOp::Max(_) => nums.iter().copied().max_by(|a, b| a.partial_cmp(b).unwrap()),
    }
}

fn run(db: &Hako, op: AggregateOp) -> HashMap<String, f64> {
    let mut query = base_query();
    query = query.aggregate(op);
    db.execute_aggregation(query)
        .expect("aggregate")
        .into_iter()
        .collect()
}

fn run_ops(db: &Hako, ops: Vec<AggregateOp>) -> HashMap<String, f64> {
    let mut query = base_query();
    for op in ops {
        query = query.aggregate(op);
    }
    db.execute_aggregation(query)
        .expect("aggregate")
        .into_iter()
        .collect()
}

#[test]
fn covering_aggregation_matches_scan() {
    let d = dir("cover");
    let db = open(&d, true);
    let nums = reference_numbers(&db, &base_query());
    assert!(!nums.is_empty());

    for op in [
        AggregateOp::Count,
        AggregateOp::Sum("score".into()),
        AggregateOp::Avg("score".into()),
        AggregateOp::Min("score".into()),
        AggregateOp::Max("score".into()),
    ] {
        let got = run(&db, op.clone());
        let key = match &op {
            AggregateOp::Count => "count".to_string(),
            AggregateOp::Sum(f) => format!("sum_{f}"),
            AggregateOp::Avg(f) => format!("avg_{f}"),
            AggregateOp::Min(f) => format!("min_{f}"),
            AggregateOp::Max(f) => format!("max_{f}"),
        };
        match expected(&op, &nums) {
            Some(want) => assert!((got.get(&key).copied().unwrap_or(f64::NAN) - want).abs() < 1e-9,
                "op={op:?} got={got:?} want={want}"),
            None => assert!(!got.contains_key(&key), "op={op:?} got={got:?}"),
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn multi_op_covering_matches_reference() {
    let d = dir("multi");
    let db = open(&d, true);
    let nums = reference_numbers(&db, &base_query());
    assert!(!nums.is_empty());

    let combos: Vec<Vec<AggregateOp>> = vec![
        vec![AggregateOp::Count, AggregateOp::Sum("score".into())],
        vec![
            AggregateOp::Count,
            AggregateOp::Sum("score".into()),
            AggregateOp::Avg("score".into()),
            AggregateOp::Min("score".into()),
            AggregateOp::Max("score".into()),
        ],
    ];
    for ops in combos {
        let got = run_ops(&db, ops.clone());
        for op in &ops {
            let key = match op {
                AggregateOp::Count => "count".to_string(),
                AggregateOp::Sum(f) => format!("sum_{f}"),
                AggregateOp::Avg(f) => format!("avg_{f}"),
                AggregateOp::Min(f) => format!("min_{f}"),
                AggregateOp::Max(f) => format!("max_{f}"),
            };
            let want = expected(op, &nums).expect("multi-op fixture has values");
            assert!((got.get(&key).copied().unwrap_or(f64::NAN) - want).abs() < 1e-9,
                "ops={ops:?} key={key} got={got:?} want={want}");
        }
    }
    let _ = std::fs::remove_dir_all(&d);
}

#[test]
fn non_covering_shape_is_unchanged() {
    let d = dir("declined");
    let db = open(&d, false);
    let nums = reference_numbers(&db, &base_query());
    let got = run(&db, AggregateOp::Sum("score".into()));
    let want: f64 = nums.iter().sum();
    assert!((got.get("sum_score").copied().unwrap_or(f64::NAN) - want).abs() < 1e-9);
    let _ = std::fs::remove_dir_all(&d);
}
