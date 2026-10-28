// #177 soft delete / sweeper / restore: public-API behavior.
// Sweep-timing internals (due-purge, cap eviction, cancel-rule forget)
// are covered by unit tests in src/engine/softdel.rs; here we pin the
// user-visible contract: visibility parity, restore roundtrip, force
// parity, retention=0 degradation, restart persistence.
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn temp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!(
        "hako-test-softdel-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn doc(a: i64) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("a", Value::Int(a));
    d
}

fn assert_a(db: &Hako, col: &str, id: &str, want: i64) {
    let got = db.get(col, id).unwrap().expect("doc should exist");
    match got.get("a") {
        Some(Value::Int(v)) => assert_eq!(*v, want),
        other => panic!("unexpected value {other:?}"),
    }
}

#[test]
fn soft_delete_restore_roundtrip_and_force() {
    let dir = temp_dir("roundtrip");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    db.put("t", "1", &doc(42)).unwrap();
    db.delete("t", "1").unwrap();

    // Visibility parity: tombstone hides the doc everywhere.
    assert!(db.get("t", "1").unwrap().is_none());

    // Retention record exists; nothing is due yet (24h window).
    let avail = db.restore_available(None).unwrap();
    assert_eq!(avail.len(), 1);
    assert_eq!(avail[0].col, "t");
    assert_eq!(avail[0].id, "1");
    assert!(avail[0].scheduled_at > avail[0].deleted_at);
    assert!(db.delete_queue(None).unwrap().is_empty());

    // Restore roundtrip: payload comes back, record is consumed.
    db.restore("t", "1").unwrap();
    assert_a(&db, "t", "1", 42);
    assert!(db.restore_available(None).unwrap().is_empty());
    assert!(db.restore("t", "1").is_err());

    // Force parity: legacy shape writes no record.
    db.put("t", "2", &doc(9)).unwrap();
    db.delete_force("t", "2").unwrap();
    assert!(db.get("t", "2").unwrap().is_none());
    assert!(db.restore_available(None).unwrap().is_empty());

    // Filter by collection.
    db.put("u", "3", &doc(5)).unwrap();
    db.delete("u", "3").unwrap();
    assert_eq!(db.restore_available(Some("t")).unwrap().len(), 0);
    assert_eq!(db.restore_available(Some("u")).unwrap().len(), 1);

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn retention_zero_degrades_to_force() {
    let dir = temp_dir("zero");
    let cfg = HakoConfig { soft_delete_retention_secs: 0, ..Default::default() };
    let db = Hako::open(&dir, cfg).unwrap();

    db.put("t", "1", &doc(1)).unwrap();
    db.delete("t", "1").unwrap();
    assert!(db.get("t", "1").unwrap().is_none());
    assert!(db.restore_available(None).unwrap().is_empty());
    assert!(db.restore("t", "1").is_err());

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn restart_preserves_restore_window() {
    let dir = temp_dir("restart");
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        db.put("t", "1", &doc(42)).unwrap();
        db.delete("t", "1").unwrap();
        assert!(db.get("t", "1").unwrap().is_none());
    }
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    assert_eq!(db.restore_available(None).unwrap().len(), 1);
    db.restore("t", "1").unwrap();
    assert_a(&db, "t", "1", 42);

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn cancel_rule_re_put_owns_id() {
    let dir = temp_dir("cancel");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    db.put("t", "1", &doc(1)).unwrap();
    db.delete("t", "1").unwrap();
    // Fresh version over the tombstone: restore must refuse.
    db.put("t", "1", &doc(2)).unwrap();
    assert!(db.restore("t", "1").is_err());
    assert_a(&db, "t", "1", 2);

    // Re-delete then restore brings back the newest payload.
    db.delete("t", "1").unwrap();
    db.restore("t", "1").unwrap();
    assert_a(&db, "t", "1", 2);

    drop(db);
    let _ = std::fs::remove_dir_all(&dir);
}
