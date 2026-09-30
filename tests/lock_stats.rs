//! Lock-wait instrumentation: time blocked acquiring shard locks
//! (write apply, point get, query scan). Wait ~= contention signal;
//! hold times on write are already covered by WRITE_STATS phases.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::engine::engine::lock_stats_report;
use hakodb::query::query::Query;

fn tmp(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hako-lock-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).unwrap()
}

#[test]
fn lock_waits_counted_and_reported() {
    let dir = tmp("waits");
    let db = open_db(&dir);
    // Drain any counts from other tests in this process (global stats).
    let _ = lock_stats_report();

    for i in 0..5 {
        let mut d = HakoDoc::default();
        d.insert("v", Value::String("x".into()));
        db.put_owned("c", &format!("k{i}"), d).unwrap();
    }
    for i in 0..5 {
        assert!(db.get("c", &format!("k{i}")).unwrap().is_some());
    }
    db.query(Query::new("c").limit(5)).unwrap();

    let rep = lock_stats_report();
    assert!(rep.contains("lock profile"), "got: {rep}");
    // 5 write acquisitions (one per single-put batch).
    let wline = rep.lines().find(|l| l.trim_start().starts_with("write")).unwrap();
    let wacq: u64 = wline.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(wacq >= 5, "got: {rep}");
    // 5 gets + 1 query scan at minimum.
    let rline = rep.lines().find(|l| l.trim_start().starts_with("read ")).unwrap();
    let racq: u64 = rline.split_whitespace().nth(1).unwrap().parse().unwrap();
    assert!(racq >= 6, "got: {rep}");

    // Reset semantics: quiet period reports zeros.
    let rep2 = lock_stats_report();
    let wline2 = rep2.lines().find(|l| l.trim_start().starts_with("write")).unwrap();
    assert!(wline2.split_whitespace().nth(1).unwrap() == "0", "got: {rep2}");
}
