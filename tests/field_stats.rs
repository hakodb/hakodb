//! #243: planner field statistics and EXPLAIN selectivity reporting.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn path(label: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let p = std::env::temp_dir().join(format!(
        "hako-field-stats-{}-{}-{}",
        label,
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn cfg() -> HakoConfig {
    let mut c = HakoConfig::default();
    c.durability_mode = DurabilityMode::Manual;
    c
}

#[test]
fn explain_reports_selectivity_and_plan_reason() {
    let dir = path("explain");
    let db = Hako::open(&dir, cfg()).expect("open");
    let mut batch = Vec::new();
    for i in 0..20 {
        let mut doc = HakoDoc::default();
        doc.insert("tier", Value::String(if i < 18 { "common" } else { "rare" }.into()));
        doc.insert("id_tag", Value::String(format!("t{i:03}")));
        batch.push(BatchMutation::Put {
            collection: "users".into(),
            doc_id: format!("u{i:03}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }

    let report = db
        .explain_report(Query::new("users").where_filter("tier", Operator::Eq, Value::String("common".into())))
        .expect("explain");
    assert!(!report.plan_reason.is_empty(), "plan_reason must be populated");
    let est = report
        .estimated_selectivity
        .iter()
        .find(|(field, _)| field == "tier")
        .map(|(_, s)| *s)
        .expect("tier estimate");
    // 18/20 docs share "common" -> NDV 2 -> estimate ~0.5
    assert!(est > 0.4 && est < 0.6, "unexpected selectivity {est}");
}

#[test]
fn stats_rebuild_deterministically_after_restart() {
    let dir = path("restart");
    {
        let db = Hako::open(&dir, cfg()).expect("open");
        let mut batch = Vec::new();
        for i in 0..40 {
            let mut doc = HakoDoc::default();
            doc.insert("bucket", Value::Int(i % 4));
            batch.push(BatchMutation::Put {
                collection: "users".into(),
                doc_id: format!("u{i:03}"),
                doc,
            });
        }
        db.write_batch(batch).expect("seed");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    }

    let db = Hako::open(&dir, cfg()).expect("reopen");
    let report = db
        .explain_report(Query::new("users").where_filter("bucket", Operator::Eq, Value::Int(1)))
        .expect("explain");
    let est = report
        .estimated_selectivity
        .iter()
        .find(|(field, _)| field == "bucket")
        .map(|(_, s)| *s)
        .expect("bucket estimate");
    assert!((est - 0.25).abs() < 0.01, "rebuilt estimate {est}");
}
