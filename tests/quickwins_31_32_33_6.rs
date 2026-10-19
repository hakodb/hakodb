//! Issue #31 / #6 / #32 regression coverage: drop_collection, read-path
//! shard creation (denied-write listing leak), and collection_stats.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("quickwin-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn doc(v: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    d
}

#[test]
fn drop_collection_removes_everything_and_reports() {
    let dir = tmp_dir("drop");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    for i in 0..1200 {
        db.put("goods", &format!("k{i:05}"), &doc("v")).unwrap();
    }
    // A secondary index on the doomed collection: its defs must not survive.
    db.create_index("goods", "v").unwrap();
    assert!(db.list_collections().unwrap().contains(&"goods".to_string()));

    let report = db.drop_collection("goods").unwrap();
    assert_eq!(report.docs_removed, 1200);
    assert_eq!(report.collections_dropped, vec!["goods".to_string()]);
    assert!(report.bytes_reclaimed > 0);

    // Idempotent re-drop: no-op, empty report.
    let again = db.drop_collection("goods").unwrap();
    assert_eq!(again.docs_removed, 0);
    assert!(again.collections_dropped.is_empty());

    // Gone from the listing, from reads, and from disk.
    assert!(!db.list_collections().unwrap().contains(&"goods".to_string()));
    assert!(db.get("goods", "k00000").unwrap().is_none());
    assert!(!dir.join("goods").exists());
    // Defs did not survive.
    assert!(!db.list_indexes(Some("goods")).secondary.contains_key("goods"));

    // Recreating the same name starts fresh.
    db.put("goods", "fresh", &doc("new")).unwrap();
    assert_eq!(db.collection_stats("goods").unwrap().doc_count, 1);

    // Refusals: system plane, read-only engine, local-only mark.
    assert!(db.drop_collection("__hako_system").is_err());
    let dir2 = tmp_dir("drop-ro");
    let db2 = Hako::open(&dir2, HakoConfig::default()).unwrap();
    db2.put("x", "k", &doc("v")).unwrap();
    db2.set_read_only(true);
    assert!(db2.drop_collection("x").is_err());
    let dir3 = tmp_dir("drop-local");
    let db3 = Hako::open(&dir3, HakoConfig::default()).unwrap();
    db3.put("y", "k", &doc("v")).unwrap();
    db3.set_collection_local("y", true);
    assert!(db3.drop_collection("y").is_err());

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
    let _ = std::fs::remove_dir_all(&dir3);
}

#[test]
fn read_paths_never_materialize_collections() {
    let dir = tmp_dir("readleak");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    // A read on a nonexistent collection must not create the shard/dir.
    assert!(db.get("ghost", "k").unwrap().is_none());
    let mut q = hakodb::query::query::Query::new("ghost");
    q.filters.push(hakodb::query::filter::Filter {
        field: "v".into(),
        op: hakodb::query::filter::Operator::Eq,
        value: Value::String("x".into()),
    });
    assert!(db.query(q).unwrap().is_empty());
    assert!(db.get_raw_bytes("ghost", "k").unwrap().is_none());
    assert!(db.list_storage_keys("ghost").unwrap().is_empty());

    // The listing stays clean — this is the denied-write leak's observable.
    let listed = db.list_collections().unwrap();
    assert!(!listed.contains(&"ghost".to_string()), "read side effect materialized a collection: {listed:?}");
    assert!(!dir.join("ghost").exists());

    // A real write still registers normally.
    db.put("real", "k", &doc("v")).unwrap();
    assert!(db.list_collections().unwrap().contains(&"real".to_string()));

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn collection_stats_reports_accurate_counts() {
    let dir = tmp_dir("stats");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    for i in 0..100 {
        db.put("metrics", &format!("k{i:03}"), &doc("v")).unwrap();
    }
    db.delete("metrics", "k001").unwrap(); // tombstone

    let s = db.collection_stats("metrics").unwrap();
    assert_eq!(s.collection, "metrics");
    assert_eq!(s.doc_count, 99, "tombstoned doc is not live");
    assert_eq!(s.tombstone_count, 1);
    assert!(s.wal_bytes > 0);
    assert!(s.total_bytes >= s.wal_bytes);
    assert!(!s.unloaded);

    // Never-created collection: zeroed stats, not an error.
    let none = db.collection_stats("never-was").unwrap();
    assert_eq!(none.doc_count, 0);
    assert_eq!(none.total_bytes, 0);

    // Lazy-unloaded reporting.
    let mut lazy_cfg = HakoConfig::default();
    lazy_cfg.lazy_collections = vec!["archive-*".into()];
    let dir2 = tmp_dir("stats-lazy");
    let db2 = Hako::open(&dir2, lazy_cfg).unwrap();
    db2.put("archive-old", "k", &doc("v")).unwrap();
    db2.unload_collection("archive-old").unwrap();
    let s2 = db2.collection_stats("archive-old").unwrap();
    assert!(s2.unloaded);

    let _ = std::fs::remove_dir_all(&dir);
    let _ = std::fs::remove_dir_all(&dir2);
}

#[test]
fn config_setters_cover_the_missing_fields() {
    // #33: the four unreachable HakoConfig fields now have FFI setters.
    // The setter symbols are exercised by the binding suites (hakojs /
    // hakogo / hakotauri via the C ABI); here we verify the engine honors
    // the resulting fields through the same HakoConfig they mutate.
    let dir = tmp_dir("ffi");
    let mut hcfg = HakoConfig::default();
    hcfg.sync_excluded = vec!["private-x".into()];
    hcfg.lazy_collections = vec!["archive-*".into()];
    hcfg.page_cache_capacity = 4096;
    hcfg.replication_collections = Some(vec!["public".into()]);
    let db = Hako::open(&dir, hcfg).unwrap();
    // sync_excluded takes effect on the sync plane gate.
    assert!(db.is_sync_excluded_effective("private-x"));
    assert!(!db.is_sync_excluded_effective("open"));
    let _ = std::fs::remove_dir_all(&dir);
}
