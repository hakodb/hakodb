//! Transaction patch must keep its read-set for conflict detection without
//! decoding the whole document up front.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use std::time::Duration;

fn dir() -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    let p = std::env::temp_dir().join(format!("hako-txn-patch-{}-{nanos}", std::process::id()));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn transaction_patch_still_detects_concurrent_write() {
    let d = dir();
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(&d, cfg).expect("open");
    let mut doc = HakoDoc::default();
    doc.insert("active", Value::Bool(true));
    doc.insert("score", Value::Int(1));
    db.write_batch(vec![BatchMutation::Put {
        collection: "records".into(),
        doc_id: "k1".into(),
        doc,
    }]).expect("seed");
    assert!(db.await_quiescent(Duration::from_secs(30)));

    let mut tx = db.begin_serializable_transaction();
    tx.patch(&db, "records", "k1", vec![("active".into(), Value::Bool(false))])
        .expect("queue patch");

    // External writer bumps the version after the read-set was registered.
    let mut concurrent = HakoDoc::default();
    concurrent.insert("active", Value::Bool(true));
    concurrent.insert("score", Value::Int(2));
    db.write_batch(vec![BatchMutation::Put {
        collection: "records".into(),
        doc_id: "k1".into(),
        doc: concurrent,
    }]).expect("concurrent write");

    assert!(tx.commit(&db).is_err(), "stale transaction must conflict");
    let _ = std::fs::remove_dir_all(&d);
}

