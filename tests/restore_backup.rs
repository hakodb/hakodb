//! Public offline restore from a `Hako::backup` (issue #133).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn dir(label: &str) -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let p = std::env::temp_dir().join(format!(
        "hako-restore-{}-{}-{}",
        label,
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

fn config() -> HakoConfig {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg
}

fn seed(path: &std::path::Path) {
    let db = Hako::open(path, config()).expect("open");
    for (id, name) in [("u1", "alice"), ("u2", "bob")] {
        let mut doc = HakoDoc::default();
        doc.insert("name", Value::String(name.into()));
        db.put("users", id, &doc).expect("put");
    }
    db.flush().expect("flush");
}

#[test]
fn backup_then_restore_restores_pre_backup_contents() {
    let src = dir("src");
    let backup = dir("bak");
    let target = dir("target");
    seed(&src);

    let db = Hako::open(&src, config()).expect("reopen");
    db.backup(&backup).expect("backup");
    db.delete("users", "u2").expect("mutate after backup");
    drop(db);

    Hako::restore_backup(&backup, &target).expect("restore");
    let restored = Hako::open(&target, config()).expect("open restored");
    let mut names: Vec<String> = restored
        .query(hakodb::query::query::Query::new("users").limit(10))
        .expect("query")
        .into_iter()
        .map(|(_, doc)| match doc.get("name") {
                Some(Value::String(s)) => s.to_string(),
                other => panic!("unexpected name: {other:?}"),
            })
        .collect();
    names.sort();
    assert_eq!(names, vec!["alice", "bob"], "restore must undo later mutation");

    let _ = std::fs::remove_dir_all(&src);
    let _ = std::fs::remove_dir_all(&backup);
    let _ = std::fs::remove_dir_all(&target);
}

#[test]
fn restore_refuses_truncated_wal_without_touching_target() {
    let src = dir("src2");
    let backup = dir("bak2");
    let target = dir("target2");
    seed(&src);
    let db = Hako::open(&src, config()).expect("reopen");
    db.flush().expect("flush before backup");
    db.backup(&backup).expect("backup");
    drop(db);

    let wal = backup.join("users").join("wal.log");
    let meta = std::fs::metadata(&wal).expect("wal exists");
    if meta.len() > 4 {
        let f = std::fs::OpenOptions::new().write(true).open(&wal).unwrap();
        f.set_len(meta.len() - 1).unwrap();
    }
    let err = Hako::restore_backup(&backup, &target).expect_err("truncated backup must be refused");
    assert!(!target.exists(), "target must stay untouched: {err}");
}

#[test]
fn restore_refuses_non_empty_target_and_foreign_layout() {
    let src = dir("src3");
    let backup = dir("bak3");
    let target = dir("target3");
    seed(&src);
    let db = Hako::open(&src, config()).expect("reopen");
    db.backup(&backup).expect("backup");
    drop(db);

    std::fs::create_dir_all(&target).unwrap();
    std::fs::write(target.join("keep.txt"), b"keep").unwrap();
    let err = Hako::restore_backup(&backup, &target).expect_err("non-empty target");
    assert!(std::fs::read_to_string(target.join("keep.txt")).unwrap() == "keep");
    assert!(err.to_string().contains("not empty"), "{err}");

    let foreign = dir("foreign");
    std::fs::create_dir_all(&foreign).unwrap();
    std::fs::write(foreign.join("stray.txt"), b"x").unwrap();
    let foreign_target = dir("foreign-target");
    let err = Hako::restore_backup(&foreign, &foreign_target).expect_err("foreign layout");
    assert!(err.to_string().contains("unexpected"), "{err}");
}
