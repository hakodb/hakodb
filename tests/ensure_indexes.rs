//! #217 P2: idempotent index declaration. `create_fts_index` /
//! `create_vector_index` must be no-ops on re-declaration (a boot flow
//! re-declares on every open — the old behavior wiped a populated FTS
//! index with an empty one and re-backfilled), a conflicting vector def
//! is loud, and `ensure_indexes` batches the whole boot schema.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{Hako, IndexSchema};
use hakodb::index::vector::{encode_f32s, Metric};

fn cfg() -> HakoConfig {
    let mut c = HakoConfig::default();
    c.durability_mode = DurabilityMode::Manual;
    c
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-ensidx-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn schema() -> IndexSchema {
    IndexSchema {
        fts: vec![("items".into(), "name".into())],
        vector: vec![("items".into(), "emb".into(), 2, Metric::Cosine)],
        tracked: vec!["items".into()],
        ..Default::default()
    }
}

fn item_doc(name: &str, emb: [f32; 2]) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("name", Value::String(name.into()));
    doc.insert("emb", Value::Binary(encode_f32s(&emb)));
    doc
}

fn hybrid(db: &Hako) -> Vec<String> {
    db.hybrid_search("items", "name", "alpha beta", "emb", &[1.0, 0.0], 3, 60)
        .unwrap()
        .into_iter()
        .map(|(id, _, _)| id)
        .collect()
}

#[test]
fn re_declaration_keeps_populated_indexes_immediately() {
    let dir = tmp("redeclare");
    let db = Hako::open_exclusive(&dir, cfg()).unwrap();
    db.ensure_indexes(&schema()).unwrap();
    db.put("items", "d1", &item_doc("alpha beta", [1.0, 0.0]))
        .unwrap();
    db.put("items", "d2", &item_doc("alpha gamma", [0.0, 1.0]))
        .unwrap();
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
    assert_eq!(hybrid(&db), vec!["d1", "d2"]);

    // Boot #2: re-declare the same schema — must be a cheap no-op. The
    // pre-P2 behavior wiped the FTS index (empty registry entry) and the
    // vector graph rebuild, so a query issued immediately would miss.
    db.ensure_indexes(&schema()).unwrap();
    assert_eq!(hybrid(&db), vec!["d1", "d2"], "re-declare wiped the FTS index");
    assert!(
        db.is_indexes_ready(),
        "re-declare must not enqueue backfills"
    );

    // And it survives reopen with the same declare-on-boot flow. Reopen
    // runs its own async restore/backfill — wait for readiness first.
    drop(db);
    let db = Hako::open_exclusive(&dir, cfg()).unwrap();
    db.ensure_indexes(&schema()).unwrap();
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
    assert_eq!(hybrid(&db), vec!["d1", "d2"], "reopen + re-declare lost data");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn conflicting_vector_def_is_loud() {
    let dir = tmp("conflict");
    let db = Hako::open(&dir, cfg()).unwrap();
    db.create_vector_index("items", "emb", 2, Metric::Cosine)
        .unwrap();
    let err = db
        .create_vector_index("items", "emb", 3, Metric::Cosine)
        .unwrap_err();
    assert!(
        err.to_string().contains("already exists"),
        "must name the conflict: {err}"
    );
    // The good def survives the failed re-declaration.
    db.put("items", "d1", &item_doc("alpha beta", [1.0, 0.0]))
        .unwrap();
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
    assert_eq!(hybrid(&db), vec!["d1"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn traverse_declaration_is_idempotent() {
    let dir = tmp("traverse");
    let db = Hako::open(&dir, cfg()).unwrap();
    // Tracked list exercises the third ensure branch; a double declare
    // must not error (create_traverse_index early-returns when tracked).
    db.ensure_indexes(&schema()).unwrap();
    db.ensure_indexes(&schema()).unwrap();
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn composite_declaration_is_idempotent() {
    let dir = tmp("composite");
    let db = Hako::open(&dir, cfg()).unwrap();
    let schema = IndexSchema {
        secondary: vec![("items".into(), "category".into())],
        composite: vec![(
            "items".into(),
            vec!["category".into(), "key".into()],
        )],
        ..Default::default()
    };
    db.ensure_indexes(&schema).unwrap();
    db.ensure_indexes(&schema).unwrap();
    let list = db.list_indexes(Some("items"));
    assert_eq!(list.composite.len(), 1, "duplicate composite created");
    assert_eq!(list.secondary["items"].len(), 1, "duplicate secondary created");
    let _ = std::fs::remove_dir_all(&dir);
}
