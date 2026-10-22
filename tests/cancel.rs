use hakodb::cancel::CancelToken;
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::error::ErrorKind;
use hakodb::query::query::Query;

fn temp_db(tag: &str) -> (Hako, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-cancel-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    cfg.query_workers = 1;
    let db = Hako::open(&dir, cfg).expect("open");
    (db, dir)
}

fn insert(db: &Hako, col: &str, n: usize) {
    let mut batch = Vec::with_capacity(n);
    for i in 0..n {
        let mut doc = HakoDoc::default();
        doc.insert("v", Value::Int(i as i64));
        batch.push(BatchMutation::Put {
            collection: col.into(),
            doc_id: format!("k{i}"),
            doc,
        });
    }
    db.write_batch(batch).expect("write_batch");
}

fn count(db: &Hako, col: &str) -> usize {
    db.query(Query::new(col)).expect("query").len()
}

#[test]
fn cancelled_delete_writes_nothing() {
    let (db, dir) = temp_db("delete");
    insert(&db, "c", 3);

    let token = CancelToken::new();
    token.cancel();
    let err = db
        .delete_where_with_cancel(Query::new("c"), &token)
        .expect_err("cancelled");
    assert_eq!(err.kind(), ErrorKind::Cancelled);
    assert_eq!(count(&db, "c"), 3);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn cancelled_patch_writes_nothing() {
    let (db, dir) = temp_db("patch");
    insert(&db, "c", 3);

    let token = CancelToken::new();
    token.cancel();
    let err = db
        .patch_where_with_cancel(Query::new("c"), vec![("v".into(), Value::Int(9))], &token)
        .expect_err("cancelled");
    assert_eq!(err.kind(), ErrorKind::Cancelled);
    let docs = db.query(Query::new("c")).expect("query");
    assert_eq!(docs.len(), 3);
    assert!(docs
        .iter()
        .all(|(_, doc)| doc.get("v") != Some(&Value::Int(9))));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn cancel_during_delete_never_partially_deletes() {
    let (db, dir) = temp_db("race");
    insert(&db, "c", 5000);

    let token = CancelToken::new();
    let signal = token.clone();
    let canceller = std::thread::spawn(move || {
        std::thread::sleep(std::time::Duration::from_millis(1));
        signal.cancel();
    });
    let result = db.delete_where_with_cancel(Query::new("c"), &token);
    canceller.join().unwrap();

    match result {
        Ok(n) => {
            // Cancelled after commit: the whole batch landed.
            assert_eq!(n, 5000);
            assert_eq!(count(&db, "c"), 0);
        }
        Err(err) => {
            // Cancelled before commit: none of the batch landed.
            assert_eq!(err.kind(), ErrorKind::Cancelled);
            assert_eq!(count(&db, "c"), 5000);
        }
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn uncancelled_variants_match_existing_methods() {
    let (db, dir) = temp_db("ok");
    insert(&db, "c", 3);

    let token = CancelToken::new();
    let patched = db
        .patch_where_with_cancel(Query::new("c"), vec![("v".into(), Value::Int(7))], &token)
        .expect("patch");
    assert_eq!(patched, 3);
    assert!(db
        .query(Query::new("c"))
        .expect("query")
        .iter()
        .all(|(_, doc)| doc.get("v") == Some(&Value::Int(7))));

    let deleted = db.delete_where_with_cancel(Query::new("c"), &token).expect("delete");
    assert_eq!(deleted, 3);
    assert_eq!(count(&db, "c"), 0);

    std::fs::remove_dir_all(&dir).ok();
}
