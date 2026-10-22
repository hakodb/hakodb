use hakodb::cancel::CancelToken;
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::error::ErrorKind;

fn temp_db(tag: &str) -> (Hako, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-cancel-maint-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.background_maintenance = false;
    cfg.max_inlined_memory_bytes = 8 * 1024;
    cfg.auto_compaction_threshold_bytes = 8 * 1024;
    let db = Hako::open(&dir, cfg).expect("open");
    (db, dir)
}

fn put_pad(db: &Hako, col: &str, id: &str, bytes: usize) {
    let mut doc = HakoDoc::default();
    doc.insert("pad", Value::String("x".repeat(bytes)));
    db.write_batch(vec![BatchMutation::Put {
        collection: col.into(),
        doc_id: id.into(),
        doc,
    }])
    .expect("put");
}

fn assert_docs(db: &Hako, col: &str, n: usize) {
    for i in 0..n {
        let doc = db
            .get(col, &format!("k{i}"))
            .expect("get")
            .unwrap_or_else(|| panic!("k{i} missing"));
        assert_eq!(
            doc.get("pad"),
            Some(&Value::String("x".repeat(2048))),
            "k{i} payload changed"
        );
    }
}

#[test]
fn cancelled_compact_is_a_no_op() {
    let (db, dir) = temp_db("compact");
    for i in 0..12 {
        put_pad(&db, "c", &format!("k{i}"), 2048);
    }

    let token = CancelToken::new();
    token.cancel();
    let err = db.compact_with_cancel(&token).expect_err("cancelled");
    assert_eq!(err.kind(), ErrorKind::Cancelled);
    assert_docs(&db, "c", 12);

    db.compact().expect("compact");
    assert_docs(&db, "c", 12);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn cancelled_vacuum_keeps_blobs_and_leaves_no_temp() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-cancel-maint-vacuum-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.background_maintenance = false;
    cfg.value_blob_threshold_bytes = 1024;
    let db = Hako::open(&dir, cfg).expect("open");

    for id in ["a", "b"] {
        let mut doc = HakoDoc::default();
        doc.insert("blob", Value::String("x".repeat(4096)));
        db.write_batch(vec![BatchMutation::Put {
            collection: "c".into(),
            doc_id: id.into(),
            doc,
        }])
        .expect("put");
    }
    db.delete("c", "a").expect("delete");
    std::thread::sleep(std::time::Duration::from_millis(500));

    let token = CancelToken::new();
    token.cancel();
    let err = db.vacuum_with_cancel("c", &token).expect_err("cancelled");
    assert_eq!(err.kind(), ErrorKind::Cancelled);
    assert!(dir.join("c").join("blobs.dat").exists());
    assert!(!dir.join("c").join("blobs.tmp").exists());
    let got = db.get("c", "b").expect("get").expect("present");
    assert_eq!(got.get("blob"), Some(&Value::String("x".repeat(4096))));

    db.vacuum("c").expect("vacuum");
    assert!(!dir.join("c").join("blobs.tmp").exists());
    let got = db.get("c", "b").expect("get").expect("present");
    assert_eq!(got.get("blob"), Some(&Value::String("x".repeat(4096))));

    std::fs::remove_dir_all(&dir).ok();
}
