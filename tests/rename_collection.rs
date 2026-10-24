//! Issue #132: `rename_collection` moves docs, index defs, schema, and
//! local-only marks, and survives a reopen.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::composite::definition::SortDirection;
use hakodb::schema::{CollectionSchema, FieldKind, FieldRule};

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("rename-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn doc(v: &str, n: i64) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    d.insert("n", Value::Int(n));
    d
}

#[test]
fn rename_moves_docs_indexes_schema_and_persists() {
    let dir = tmp_dir("full");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    for i in 0..1200 {
        db.put("old", &format!("k{i:05}"), &doc("v", i)).unwrap();
    }
    db.create_index("old", "v").unwrap();
    db.create_fts_index("old", "v").unwrap();
    db.create_composite_index("old", vec![("n".to_string(), SortDirection::Asc)])
        .unwrap();
    db.set_schema(
        "old",
        CollectionSchema::new(vec![FieldRule::new("v", FieldKind::String).required()]),
    )
    .unwrap();

    // Refusals leave both names untouched.
    assert!(db.rename_collection("old", "old").is_err());
    assert!(db.rename_collection("missing", "other").is_err());
    assert!(db.rename_collection("old", "__hako_system").is_err());
    assert!(db.rename_collection("old", "../escape").is_err());
    db.put("taken", "x", &doc("x", 0)).unwrap();
    assert!(db.rename_collection("old", "taken").is_err());
    assert_eq!(db.get("old", "k00000").unwrap().unwrap().get("v"), Some(&Value::String("v".into())));

    assert_eq!(db.rename_collection("old", "new").unwrap(), 1200);

    // Docs moved, old name gone from listing and disk.
    assert_eq!(db.get("new", "k00000").unwrap().unwrap().get("n"), Some(&Value::Int(0)));
    assert_eq!(db.get("new", "k01199").unwrap().unwrap().get("n"), Some(&Value::Int(1199)));
    assert!(db.get("old", "k00000").unwrap().is_none());
    assert!(!db.list_collections().unwrap().contains(&"old".to_string()));
    assert!(db.list_collections().unwrap().contains(&"new".to_string()));
    assert!(!dir.join("old").exists());

    // Index defs and schema followed the data.
    let indexes = db.list_indexes(Some("new"));
    assert_eq!(indexes.secondary.get("new"), Some(&vec!["v".to_string()]));
    assert_eq!(indexes.fts.get("new"), Some(&vec!["v".to_string()]));
    assert_eq!(indexes.composite.len(), 1);
    assert_eq!(indexes.composite[0].collection, "new");
    assert!(db.list_indexes(Some("old")).secondary.is_empty());
    assert!(db.schema("new").is_some());
    assert!(db.schema("old").is_none());

    // Old name is reusable afterwards.
    db.put("old", "fresh", &doc("again", 1)).unwrap();
    assert!(db.get("old", "fresh").unwrap().is_some());

    // Persistence: reopen and check the move survived.
    drop(db);
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    assert!(db.list_collections().unwrap().contains(&"new".to_string()));
    assert_eq!(db.get("new", "k00450").unwrap().unwrap().get("n"), Some(&Value::Int(450)));
    assert!(db.list_indexes(Some("new")).secondary.contains_key("new"));
    assert!(db.schema("new").is_some());

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn rename_moves_local_only_marks() {
    let dir = tmp_dir("local");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    db.set_collection_local("a", true);
    db.put("a", "k", &doc("v", 0)).unwrap();
    // Per-key mark on a replicated collection: delete_local marks the key,
    // the re-put keeps the mark.
    db.put("b", "marked", &doc("v", 1)).unwrap();
    db.delete_local("b", "marked").unwrap();
    db.put("b", "marked", &doc("v", 2)).unwrap();
    assert!(db.is_local_only("b", "marked"));

    assert_eq!(db.rename_collection("a", "a2").unwrap(), 1);
    assert_eq!(db.rename_collection("b", "b2").unwrap(), 1);

    assert!(db.is_collection_local("a2"));
    assert!(!db.is_collection_local("a"));
    assert!(db.is_local_only("b2", "marked"));
    assert!(!db.is_local_only("b2", "other"));
    assert_eq!(db.get("b2", "marked").unwrap().unwrap().get("n"), Some(&Value::Int(2)));

    // Read-only engines refuse before touching anything.
    db.set_read_only(true);
    assert!(db.rename_collection("a2", "a3").is_err());
    assert!(dir.join("a2").exists());

    let _ = std::fs::remove_dir_all(&dir);
}
