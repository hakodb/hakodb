//! P3 ergonomics: serializable reports, cheap existence, per-collection compact.

use hakodb::cancel::CancelToken;
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::engine::{CollectionStats, DropReport, RelocateReport};
use hakodb::engine::Hako;

fn open(label: &str) -> (std::path::PathBuf, Hako) {
    let dir = std::env::temp_dir().join(format!(
        "hako-p3-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let cfg = HakoConfig {
        durability_mode: DurabilityMode::Manual,
        ..Default::default()
    };
    let db = Hako::open(&dir, cfg).expect("open");
    (dir, db)
}

fn doc(n: i64) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("n", Value::Int(n));
    d
}

fn assert_n(db: &Hako, collection: &str, id: &str, expected: i64) {
    match db.get(collection, id).expect("get").expect("doc").get("n") {
        Some(Value::Int(v)) => assert_eq!(*v, expected, "{collection}/{id}"),
        other => panic!("{collection}/{id}: unexpected field {other:?}"),
    }
}

#[test]
fn reports_serialize_with_documented_keys() {
    let stats = CollectionStats {
        collection: "c".into(),
        doc_count: 3,
        live_bytes: 10,
        total_bytes: 20,
        segment_count: 1,
        wal_bytes: 5,
        blob_bytes: 0,
        tombstone_count: 1,
        unloaded: false,
    };
    assert_eq!(
        serde_json::to_value(&stats).unwrap(),
        serde_json::json!({
            "collection": "c",
            "doc_count": 3,
            "live_bytes": 10,
            "total_bytes": 20,
            "segment_count": 1,
            "wal_bytes": 5,
            "blob_bytes": 0,
            "tombstone_count": 1,
            "unloaded": false
        })
    );

    let drop = DropReport {
        docs_removed: 2,
        bytes_reclaimed: 99,
        collections_dropped: vec!["c".into()],
    };
    assert_eq!(
        serde_json::to_value(&drop).unwrap(),
        serde_json::json!({
            "docs_removed": 2,
            "bytes_reclaimed": 99,
            "collections_dropped": ["c"]
        })
    );

    let relocate = RelocateReport {
        moved: vec!["a".into()],
        missing: vec!["b".into()],
    };
    assert_eq!(
        serde_json::to_value(&relocate).unwrap(),
        serde_json::json!({"moved": ["a"], "missing": ["b"]})
    );
}

#[test]
fn exists_is_cheap_and_never_creates_a_collection() {
    let (dir, db) = open("exists");
    assert!(!db.exists("c", "missing").unwrap());
    assert!(db.list_collections().unwrap().is_empty());
    assert!(!dir.join("c").exists(), "existence check must not materialize the collection");

    db.put("c", "a", &doc(1)).unwrap();
    assert!(db.exists("c", "a").unwrap());
    assert!(!db.exists("c", "b").unwrap());

    db.delete("c", "a").unwrap();
    assert!(!db.exists("c", "a").unwrap());

    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn compact_collection_keeps_data_and_is_idempotent() {
    let (dir, db) = open("compact");
    for i in 0..20 {
        db.put("one", &format!("d{i}"), &doc(i)).unwrap();
        db.put("two", &format!("d{i}"), &doc(i * 2)).unwrap();
    }
    db.flush().unwrap();

    db.compact_collection("one").unwrap();
    db.compact_collection("one").unwrap();
    db.compact_collection("absent").unwrap();

    for i in 0..20 {
        assert_n(&db, "one", &format!("d{i}"), i);
        assert_n(&db, "two", &format!("d{i}"), i * 2);
    }

    let token = CancelToken::new();
    token.cancel();
    assert!(db.compact_collection_with_cancel("two", &token).is_err());

    std::fs::remove_dir_all(&dir).ok();
}
