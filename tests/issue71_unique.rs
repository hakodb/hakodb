//! hakodb#71 Release A: unique/sparse/filtered composite indexes.
//!
//! Gate semantics (engine-owned claims ledger): synchronous admission on
//! the write path, historical-duplicate rejection at create time, batch
//! atomicity, snapshot-persisted enforcement across restarts.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::error::ErrorKind;
use hakodb::query::query::Query;
use hakodb::query::filter::{Filter, FilterGroup, FilterLogic, Operator};

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn doc(platform: &str, region: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("platform", Value::String(platform.into()));
    d.insert("region", Value::String(region.into()));
    d
}

#[test]
fn unique_blocks_duplicates_and_allows_rewrites() {
    let dir = tmp_dir("dup");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    let id = db
        .create_composite_index_ex(
            "accounts",
            vec![("platform".into(), hakodb::index::composite::definition::SortDirection::Asc)],
            true, false, None,
        )
        .unwrap();
    assert!(id > 0);

    db.put("accounts", "a1", &doc("ios", "eu")).unwrap();
    // Second doc with the same tuple: typed unique violation.
    let err = db.put("accounts", "a2", &doc("ios", "ca")).unwrap_err();
    assert!(matches!(err.kind(), ErrorKind::UniqueViolation), "got {err:?}");
    // The violating doc must not exist (atomic admission).
    assert!(db.get("accounts", "a2").unwrap().is_none());

    // Same doc id re-put with the same tuple: rewrite, fine.
    db.put("accounts", "a1", &doc("ios", "eu")).unwrap();

    // Different tuple: fine.
    db.put("accounts", "a2", &doc("android", "eu")).unwrap();

    // Delete releases the claim; a new doc may take the tuple back.
    db.delete("accounts", "a1").unwrap();
    db.put("accounts", "a3", &doc("ios", "eu")).unwrap();
    err_is_unique(db.put("accounts", "a4", &doc("ios", "ca")).unwrap_err());
}

fn err_is_unique(err: hakodb::error::HakoError) {
    assert!(matches!(err.kind(), ErrorKind::UniqueViolation), "got {err:?}");
}

#[test]
fn batch_with_duplicate_is_atomic() {
    let dir = tmp_dir("batch");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    db.create_composite_index_ex(
        "items",
        vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        true, false, None,
    )
    .unwrap();

    // Batch of three puts where the last collides with the first.
    let mut tx = Vec::new();
    tx.push(hakodb::engine::BatchMutation::Put {
        collection: "items".into(),
        doc_id: "b1".into(),
        doc: doc("p", "r1"),
    });
    tx.push(hakodb::engine::BatchMutation::Put {
        collection: "items".into(),
        doc_id: "b2".into(),
        doc: doc("p", "r2"),
    });
    tx.push(hakodb::engine::BatchMutation::Put {
        collection: "items".into(),
        doc_id: "b3".into(),
        doc: doc("p", "r1"),
    });
    let err = db.write_batch(tx).unwrap_err();
    err_is_unique(err);
    // None of the batch landed (claims rolled back with the WAL ops).
    assert!(db.get("items", "b1").unwrap().is_none());
    assert!(db.get("items", "b2").unwrap().is_none());
    assert!(db.get("items", "b3").unwrap().is_none());

    // A clean re-run (fix the duplicate) succeeds and claims correctly.
    let mut ok = Vec::new();
    ok.push(hakodb::engine::BatchMutation::Put {
        collection: "items".into(),
        doc_id: "b1".into(),
        doc: doc("p", "r1"),
    });
    ok.push(hakodb::engine::BatchMutation::Put {
        collection: "items".into(),
        doc_id: "b3".into(),
        doc: doc("p", "r3"),
    });
    db.write_batch(ok).unwrap();
    assert!(db.get("items", "b3").unwrap().is_some());
}

#[test]
fn concurrent_writers_produce_one_winner_per_tuple() {
    let dir = tmp_dir("race");
    let db = std::sync::Arc::new(
        Hako::open(&dir, HakoConfig::default()).unwrap(),
    );
    db.create_composite_index_ex(
        "race",
        vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        true, false, None,
    )
    .unwrap();

    const N: usize = 16;
    let mut handles = Vec::new();
    for i in 0..N {
        let db = std::sync::Arc::clone(&db);
        handles.push(std::thread::spawn(move || {
            let mut d = HakoDoc::default();
            d.insert("region", Value::String("only".into()));
            db.put("race", &format!("t{i:02}"), &d)
        }));
    }
    let mut winners = 0usize;
    let mut losers = 0usize;
    for h in handles {
        match h.join().unwrap() {
            Ok(_) => winners += 1,
            Err(e) => {
                err_is_unique(e);
                losers += 1;
            }
        }
    }
    assert_eq!(winners, 1, "exactly one writer owns the tuple");
    assert_eq!(losers, N - 1);
    assert_eq!(db.query(Query::new("race").where_eq("region", Value::String("only".into()))).unwrap().len(), 1);
}

#[test]
fn sparse_and_dense() {
    let dir = tmp_dir("sparse");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    // Sparse unique (missing field → doc unclaimed).
    db.create_composite_index_ex(
        "sparse",
        vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        true, true, None,
    )
    .unwrap();
    let mut d = HakoDoc::default();
    d.insert("platform", Value::String("x".into()));
    db.put("sparse", "s1", &d).unwrap();
    db.put("sparse", "s2", &d).unwrap(); // both missing region — no claim, no clash

    // Dense unique (missing field → NULL tuple, uniqueness enforced).
    db.create_composite_index_ex(
        "dense",
        vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        true, false, None,
    )
    .unwrap();
    db.put("dense", "d1", &d).unwrap();
    let err = db.put("dense", "d2", &d).unwrap_err();
    err_is_unique(err); // both NULL → collision
}

#[test]
fn filtered_index_only_claims_matching_docs() {
    let dir = tmp_dir("filter");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    // Partial index: only docs with platform == "ios" are claimed.
    let f = FilterGroup {
        logic: FilterLogic::And,
        filters: vec![Filter {
            field: "platform".into(),
            op: Operator::Eq,
            value: Value::String("ios".into()),
        }],
    };
    db.create_composite_index_ex(
        "partial",
        vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        true, true, Some(f),
    )
    .unwrap();

    // Both non-ios docs share the region tuple but aren't claimed.
    db.put("partial", "a1", &doc("android", "same")).unwrap();
    db.put("partial", "a2", &doc("win", "same")).unwrap();
    // ios doc claims the tuple; a second ios doc with it violates.
    db.put("partial", "a3", &doc("ios", "same")).unwrap();
    err_is_unique(db.put("partial", "a4", &doc("ios", "same")).unwrap_err());

    // The ios doc's tuple is served from the partial tree; the
    // table-level query remains complete for all matching docs.
    let hits = db.query(Query::new("partial").where_eq("region", Value::String("same".into()))).unwrap();
    assert!(hits.iter().any(|(id, _)| id == "a3"), "a3 must be served");
}

#[test]
fn unique_survives_restart_and_list_indexes_reports_flags() {
    let dir = tmp_dir("persist");
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        db.create_composite_index_ex(
            "keep",
            vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
            true, false, None,
        )
        .unwrap();
        db.put("keep", "k1", &doc("p", "r1")).unwrap();
        // The async index worker owns tree fills; a snapshot taken
        // mid-flight would drop k1's tree row and weaken the restart
        // fold. Wait for the worker before snapshotting.
        assert!(db.await_quiescent(std::time::Duration::from_secs(10)), "worker settle");
        db.export_index_snapshot().unwrap();
    }
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        let list = db.list_indexes(Some("keep"));
        assert!(list.composite.iter().any(|c| c.unique && !c.sparse), "flags must persist");
        // Gate active after the restart fold — duplicate rejected. The
        // recovery fold is asynchronous, so poll until it engages.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match db.put("keep", "k2", &doc("p", "r1")) {
                Err(e) if matches!(e.kind(), ErrorKind::UniqueViolation) => break,
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                other => panic!("expected unique violation after restart, got {other:?}"),
            }
        }
        // Delete frees the claim even post-restart.
        db.delete("keep", "k1").unwrap();
        db.put("keep", "k2", &doc("p", "r1")).unwrap();
    }
}

#[test]
fn creating_unique_over_historical_duplicates_fails_clean() {
    let dir = tmp_dir("history");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    db.put("dupe", "h1", &doc("p", "same")).unwrap();
    db.put("dupe", "h2", &doc("p", "same")).unwrap();

    let err = db
        .create_composite_index_ex(
            "dupe",
            vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
            true, false, None,
        )
        .unwrap_err();
    err_is_unique(err);

    // Rolled back: not listed, not ready → the plain non-unique sibling
    // may still exist and work.
    let list = db.list_indexes(Some("dupe"));
    assert!(list.composite.is_empty());
    // Writes still succeed (no zombie gate).
    db.put("dupe", "h3", &doc("p", "other")).unwrap();
}

#[test]
fn non_unique_composite_paths_unchanged() {
    let dir = tmp_dir("legacy");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    let id = db
        .create_composite_index(
            "legacyq",
            vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        )
        .unwrap();
    // Duplicate tuples allowed (no uniqueness).
    db.put("legacyq", "l1", &doc("p", "r")).unwrap();
    db.put("legacyq", "l2", &doc("p", "r")).unwrap();
    // Classic tree query still answers via the non-unique tree.
    let hits = db.query(Query::new("legacyq").where_eq("region", Value::String("r".into()))).unwrap();
    assert_eq!(hits.len(), 2);
    // Dedupe returns the same id with identical flags on a later call.
    assert_eq!(
        db.create_composite_index(
            "legacyq",
            vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
        )
        .unwrap(),
        id
    );
}

// --- fkip-234 report regressions (hakodb#71 Release B branch) ---

/// BUG-1: after a restart, delete() then re-inserting the freed tuple
/// must succeed — a late STEP-C fold (mixed touched-collection tree)
/// must not revive the released claim. Poll-based: recovery is async.
#[test]
fn delete_after_restart_frees_claim_even_with_late_fold() {
    let dir = tmp_dir("ghost");
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        db.create_composite_index_ex(
            "keep",
            vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
            true, false, None,
        ).unwrap();
        db.put("keep", "k1", &doc("p", "r1")).unwrap();
        // Worker settles first so the snapshot carries the tree row
        // (STEP A fold path).
        assert!(db.await_quiescent(std::time::Duration::from_secs(10)), "settle");
        db.export_index_snapshot().unwrap();
    }
    {
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        // Delete FIRST (pre- or post-fold order must both end sound),
        // then the claim must be free. The delete marks the collection
        // touched → STEP C folds a stale tree merged with fresh state —
        // the tombstone keeps that fold from resurrecting k1's claim.
        db.delete("keep", "k1").unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        loop {
            match db.put("keep", "k2", &doc("p", "r1")) {
                Ok(_) => break,
                Err(e) if matches!(e.kind(), ErrorKind::UniqueViolation) => {
                    panic!("ghost claim survived delete: {e:?}")
                }
                Err(_) if std::time::Instant::now() < deadline => {
                    std::thread::sleep(std::time::Duration::from_millis(20));
                }
                other => panic!("expected claim reuse, got {other:?}"),
            }
        }
    }
}

/// BUG-2: read-your-writes without await_quiescent — a query right
/// after put() must see the doc even while the index worker still owes
/// ops (index_query_ready gates on index_inflight == 0 now).
#[test]
fn query_right_after_put_sees_the_put() {
    let dir = tmp_dir("ryw");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    db.create_composite_index(
        "ryw",
        vec![("region".into(), hakodb::index::composite::definition::SortDirection::Asc)],
    ).unwrap();
    for i in 0..50 {
        let r = format!("r{i}");
        db.put("ryw", &format!("d{i}"), &doc("p", &r)).unwrap();
        // Immediately: planner must NOT ride the not-yet-updated tree.
        let hits = db.query(Query::new("ryw").where_eq("region", Value::String(r))).unwrap();
        assert_eq!(hits.len(), 1, "iteration {i}: read-your-writes window");
    }
}
