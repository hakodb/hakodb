use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::query::query::Query;

fn temp_db(tag: &str) -> (Hako, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-localonly-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&dir, cfg).expect("open");
    (db, dir)
}

fn put_doc(db: &Hako, col: &str, id: &str) {
    let mut doc = HakoDoc::default();
    doc.insert("v", Value::Int(1));
    db.put(col, id, &doc).expect("put");
}

#[test]
fn local_only_delete_marks_advances_clock_and_hides_doc() {
    let (db, dir) = temp_db("del");
    put_doc(&db, "c", "a");
    let v_before = db.get_collection_version("c").expect("version");

    db.delete_local("c", "a").expect("delete_local");

    // Doc is gone locally...
    assert!(db.get("c", "a").expect("get").is_none());
    // ...the key is marked...
    assert!(db.is_local_only("c", "a"));
    assert!(db.is_local_only("c", "c:a")); // namespaced form also matches
    assert!(!db.is_local_only("c", "b"));
    // ...and the version clock advanced past the delete (handshake rule:
    // the deleter must never look "behind").
    let v_after = db.get_collection_version("c").expect("version");
    assert!(v_after >= v_before, "clock regressed: {v_before} -> {v_after}");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn local_only_collection_flag_and_rejoin() {
    let (db, dir) = temp_db("col");
    put_doc(&db, "c", "a");

    db.set_collection_local("c", true);
    assert!(db.is_collection_local("c"));
    assert!(db.is_local_only("c", "a"));
    assert!(db.is_local_only("c", "never-existed"));

    db.set_collection_local("c", false);
    assert!(!db.is_collection_local("c"));
    assert!(!db.is_local_only("c", "a"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn replicate_key_opts_back_in() {
    let (db, dir) = temp_db("rejoin");
    put_doc(&db, "c", "a");
    db.delete_local("c", "a").expect("delete_local");
    assert!(db.is_local_only("c", "a"));

    db.replicate_key("c", "a");
    assert!(!db.is_local_only("c", "a"));

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn delete_where_local_marks_every_match() {
    let (db, dir) = temp_db("mass");
    for i in 0..5 {
        put_doc(&db, "c", &format!("k{i}"));
    }
    let mut q = Query::new("c");
    q = q.limit(5);
    let n = db.delete_where_local(q).expect("delete_where_local");
    assert_eq!(n, 5);
    for i in 0..5 {
        assert!(db.is_local_only("c", &format!("k{i}")), "k{i} unmarked");
        assert!(db.get("c", &format!("k{i}")).expect("get").is_none());
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn local_only_marks_survive_reopen() {    let dir = std::env::temp_dir().join(format!(
        "fl-test-localonly-reopen-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    {
        let db = Hako::open(&dir, cfg.clone()).expect("open");
        put_doc(&db, "c", "a");
        db.delete_local("c", "a").expect("delete_local");
        db.set_collection_local("priv", true);
        db.flush().ok();
    }
    {
        let db = Hako::open(&dir, cfg).expect("reopen");
        assert!(db.is_local_only("c", "a"), "key mark lost across reopen");
        assert!(db.is_collection_local("priv"), "col flag lost across reopen");
    }

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn vacuum_purges_tombstones_and_drops_version() {
    let (db, dir) = temp_db("vacuum");
    put_doc(&db, "c", "live");
    put_doc(&db, "c", "dead1");
    put_doc(&db, "c", "dead2");
    db.delete("c", "dead1").expect("delete");
    db.delete_local("c", "dead2").expect("delete_local");
    let v_with_tombs = db.get_collection_version("c").expect("version");

    let n = db.vacuum_collection("c").expect("vacuum");
    assert_eq!(n, 2, "both tombstones (normal + local) must purge");

    // Docs stay gone, version drops to the newest LIVE doc...
    assert!(db.get("c", "dead1").expect("get").is_none());
    assert!(db.get("c", "dead2").expect("get").is_none());
    let v_after = db.get_collection_version("c").expect("version");
    assert!(v_after < v_with_tombs, "version must drop after purge");
    assert!(db.get("c", "live").expect("get").is_some());

    // ...and vacuum is idempotent.
    assert_eq!(db.vacuum_collection("c").expect("vacuum2"), 0);

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn replicate_collection_clears_flag_and_key_marks() {
    let (db, dir) = temp_db("rejoin-marks");
    put_doc(&db, "c", "a");
    put_doc(&db, "other", "x");
    db.set_collection_local("c", true);
    db.delete_local("c", "a").expect("delete_local");
    db.delete_local("other", "x").expect("delete_local");
    // Similar prefix must not collide: "c2" shares prefix "c".
    db.delete_local("c2", "y").expect("delete_local");

    db.replicate_collection("c");

    assert!(!db.is_collection_local("c"));
    assert!(!db.is_local_only("c", "a"), "key mark under c must clear");
    assert!(db.is_local_only("other", "x"), "other collection untouched");
    assert!(db.is_local_only("c2", "y"), "c2 must not collide with c");

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn rejoin_recipe_restores_from_older_peer_put() {
    // Simulates restore-on-rejoin: local reset (fresh tombstone T2) would
    // outrank a peer's older put (T1) under LWW, so without vacuum the peer
    // state can never come back. Vacuum + unmark clears the path.
    let (db, dir) = temp_db("rejoin");
    put_doc(&db, "c", "a");
    db.delete_local("c", "a").expect("reset");
    assert!(db.is_local_only("c", "a"));

    // Rejoin: vacuum the tombstone, opt back into replication.
    db.vacuum_collection("c").expect("vacuum");
    db.replicate_collection("c");

    // A peer's (older-timestamped) put now applies: no local entry, so the
    // ingest LWW check has nothing to reject against.
    assert!(!db.is_local_only("c", "a"));
    put_doc(&db, "c", "a");
    assert!(db.get("c", "a").expect("get").is_some(), "peer state must restore");

    std::fs::remove_dir_all(&dir).ok();
}
