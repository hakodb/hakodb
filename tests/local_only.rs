use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;
use firelite::query::query::Query;

fn temp_db(tag: &str) -> (FireLite, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-localonly-{tag}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = FireLite::open(&dir, cfg).expect("open");
    (db, dir)
}

fn put_doc(db: &FireLite, col: &str, id: &str) {
    let mut doc = FireLiteDoc::default();
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
fn local_only_marks_survive_reopen() {
    let dir = std::env::temp_dir().join(format!(
        "fl-test-localonly-reopen-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    {
        let db = FireLite::open(&dir, cfg.clone()).expect("open");
        put_doc(&db, "c", "a");
        db.delete_local("c", "a").expect("delete_local");
        db.set_collection_local("priv", true);
        db.flush().ok();
    }
    {
        let db = FireLite::open(&dir, cfg).expect("reopen");
        assert!(db.is_local_only("c", "a"), "key mark lost across reopen");
        assert!(db.is_collection_local("priv"), "col flag lost across reopen");
    }

    std::fs::remove_dir_all(&dir).ok();
}
