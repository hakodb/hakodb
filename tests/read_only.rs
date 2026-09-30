//! Read-only enforcement + group-commit interval getter (hakocluster
//! phase 3 prerequisites). Guard placement: write_batch is the single
//! admission gate for ALL local writes; replicated ingest bypasses it by
//! design (replicas must keep converging while read-only).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn tmp(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!(
        "hako-ro-{label}-{nanos}-{}",
        std::process::id()
    ))
}

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).unwrap()
}

fn kv(v: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    d
}

#[test]
fn read_only_blocks_local_writes_not_reads() {
    let dir = tmp("flag");
    let db = open_db(&dir);
    assert!(!db.is_read_only());

    db.put_owned("c", "k0", kv("before")).unwrap();
    db.set_read_only(true);
    assert!(db.is_read_only());

    // Every local write kind refuses.
    assert!(db.put_owned("c", "k1", kv("x")).is_err());
    assert!(db.put("c", "k2", &kv("x")).is_err());
    assert!(db.delete("c", "k0").is_err());
    assert!(db
        .patch("c", "k0", vec![("v".to_string(), Value::String("y".into()))])
        .is_err());

    // Reads unaffected; pre-existing data intact.
    assert_eq!(
        db.get("c", "k0").unwrap().unwrap().get("v"),
        Some(&Value::String("before".into()))
    );

    // Reversible.
    db.set_read_only(false);
    db.put_owned("c", "k1", kv("after")).unwrap();
    assert!(db.get("c", "k1").unwrap().is_some());
}

#[test]
fn interval_getter_reports_configured_window() {
    let dir = tmp("iv");
    let mut cfg = HakoConfig::default();
    cfg.group_commit_interval_ms = 42;
    let db = Hako::open(&dir, cfg).unwrap();
    assert_eq!(db.group_commit_interval_ms(), 42);

    let dir2 = tmp("iv-def");
    let db2 = open_db(&dir2);
    assert_eq!(
        db2.group_commit_interval_ms(),
        HakoConfig::default().group_commit_interval_ms
    );
}
