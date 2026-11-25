//! One-pass declaration of every index family.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::engine::{BatchMutation, Hako, IndexSchema};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::index::vector::Metric;

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-index-schema-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
fn ensure_indexes_covers_all_families() {
    let d = dir("all");
    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(&d, cfg).expect("open");
    let mut batch = Vec::new();
    for i in 0..100usize {
        let mut doc = HakoDoc::default();
        doc.insert("category", Value::String(format!("cat-{}", i % 5)));
        doc.insert("score", Value::Int(i as i64));
        doc.insert("description", Value::String("alpha beta".into()));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:03}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");

    let schema = IndexSchema {
        secondary: vec![("records".into(), "category".into())],
        composite: vec![("records".into(), vec!["category".into(), "score".into()])],
        fts: vec![("records".into(), "description".into())],
        vector: vec![("records".into(), "emb".into(), 3, Metric::Cosine)],
        tracked: vec!["records".into()],
    };
    db.ensure_indexes(&schema).expect("ensure");
    while !db.is_indexes_ready() {
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
    assert!(db.await_quiescent(std::time::Duration::from_secs(30)));

    let indexes = db.list_indexes(Some("records"));
    let secondary = indexes.secondary.get("records").cloned().unwrap_or_default();
    assert!(secondary.iter().any(|f| f == "category"), "secondary missing: {secondary:?}");
    assert!(indexes.composite.iter().any(|i| i.fields.iter().any(|f| f.field == "score")),
        "composite missing: {:?}", indexes.composite);
    let fts = indexes.fts.get("records").cloned().unwrap_or_default();
    assert!(fts.iter().any(|f| f == "description"), "fts missing: {fts:?}");
    let _ = std::fs::remove_dir_all(&d);
}
