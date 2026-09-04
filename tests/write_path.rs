use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};

#[test]
fn write_path_nofsync_modes() {
    // Skip DurabilityMode::Always here — it does a full fsync per write
    // batch which makes the test take many seconds on slow disks. The
    // Always path is exercised by the criterion bench suite instead.
    for mode in [
        DurabilityMode::Interval,
        DurabilityMode::Manual,
        DurabilityMode::OnCommit,
    ] {
        let dir = std::env::temp_dir().join(format!(
            "fl-test-wpm-{:?}-{}",
            mode,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let mut cfg = FireLiteConfig::default();
        cfg.durability_mode = mode;
        let db = FireLite::open(&dir, cfg).expect("open");

        let mut mutations = Vec::with_capacity(100);
        for i in 0..100 {
            let mut doc = FireLiteDoc::default();
            doc.insert("v", Value::Int(i as i64));
            mutations.push(BatchMutation::Put {
                collection: "bench".into(),
                doc_id: format!("k_{}", i),
                doc,
            });
        }
        db.write_batch(mutations).expect("write");
        for i in 0..100 {
            let got = db.get("bench", &format!("k_{}", i)).expect("get");
            assert!(got.is_some(), "doc {i} missing after write in {mode:?}");
        }
        std::fs::remove_dir_all(&dir).ok();
    }
}

#[test]
fn blob_write_path() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-blob-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.value_blob_threshold_bytes = 1024;
    let db = FireLite::open(&dir, cfg).expect("open");

    let mut small = FireLiteDoc::default();
    small.insert("k", Value::Int(1));
    db.write_batch(vec![BatchMutation::Put {
        collection: "bench".into(),
        doc_id: "small_doc".into(),
        doc: small,
    }])
    .expect("small write");
    assert!(db.get("bench", "small_doc").expect("get").is_some(), "small_doc visible");

    let mut doc = FireLiteDoc::default();
    doc.insert("blob", Value::String("x".repeat(4096)));
    db.write_batch(vec![BatchMutation::Put {
        collection: "bench".into(),
        doc_id: "blob_doc".into(),
        doc: doc.clone(),
    }])
    .expect("blob write");

    // Let the async blob worker land the blob and the recovery thread finish.
    // Both run in the background; without this sleep, a race between
    // recovery's unconditional shard replace and the blob worker's pointer
    // transition used to lose the previous writes (see comment on
    // shards.entry().or_insert_with in engine.rs).
    std::thread::sleep(std::time::Duration::from_millis(500));

    let small = db.get("bench", "small_doc").expect("get").expect("small_doc still present after sleep");
    assert_eq!(small.get("k"), Some(&Value::Int(1)), "small_doc value preserved");

    let blob = db.get("bench", "blob_doc").expect("get").expect("blob_doc present");
    // db.get internally calls resolve_doc which replaces BlobLink with the
    // full String from the blob manager — so we get the resolved value back,
    // not the skeleton BlobLink.
    match blob.get("blob") {
        Some(Value::String(s)) => assert_eq!(s.len(), 4096, "blob string round-trip length"),
        other => panic!("expected resolved String after worker flush, got {:?}", other),
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn tx_get_put_commit() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-tx-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = FireLite::open(&dir, cfg).expect("open");

    let mut initial = FireLiteDoc::default();
    initial.insert("counter", Value::Int(0));
    db.write_batch(vec![BatchMutation::Put {
        collection: "bench".into(),
        doc_id: "target".into(),
        doc: initial,
    }])
    .expect("seed");

    let mut tx = db.begin_serializable_transaction();
    let doc = tx.get(&db, "bench", "target").expect("tx get").expect("present");
    let mut next = doc.clone();
    next.insert("counter", Value::Int(42));
    tx.put("bench", "target", next);
    tx.commit(&db).expect("tx commit");

    let after = db.get("bench", "target").expect("get").expect("present");
    assert_eq!(after.get("counter"), Some(&Value::Int(42)));
    std::fs::remove_dir_all(&dir).ok();
}
