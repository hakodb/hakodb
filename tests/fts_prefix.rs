//! End-to-end test: MatchPrefix full-text autocomplete.
//!
//! Mirrors the client scenario: product names are indexed as full text and a
//! partial word ("indom") must resolve to the full word ("indomie"), not just
//! exact multi-word matches. Verifies the FTS index is used (results returned
//! via the prefix scan), that plain `Match` semantics are unchanged, and that
//! multi-word prefix queries keep AND semantics.
//!
//! Run with: cargo test --test fts_prefix

use std::path::PathBuf;
use std::sync::Arc;

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn temp_db(tag: &str) -> (Arc<Hako>, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "hakodb-ftsprefix-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::OnCommit;
    let db = Arc::new(Hako::open(&dir, cfg).unwrap());
    (db, dir)
}

fn food_doc(name: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("name", Value::String(name.to_string()));
    doc
}

fn names_of(results: &[(String, HakoDoc)]) -> Vec<String> {
    let mut names: Vec<String> = results
        .iter()
        .map(|(_, doc)| match doc.get("name") {
            Some(Value::String(s)) => s.clone(),
            _ => String::new(),
        })
        .collect();
    names.sort();
    names
}

#[test]
fn match_prefix_resolves_partial_words_via_fts() {
    let (db, dir) = temp_db("food");
    db.create_fts_index("foods", "name").unwrap();
    db.put("foods", "d1", &food_doc("Indomie Goreng"))
        .unwrap();
    db.put("foods", "d2", &food_doc("Indomie Rendang"))
        .unwrap();
    db.put("foods", "d3", &food_doc("Mie Sedap Ayam"))
        .unwrap();

    // "indom" is a prefix of "indomie": must match d1 and d2.
    let prefix_results = db
        .query(Query::new("foods").where_filter("name", Operator::MatchPrefix, Value::String("indom".into())))
        .unwrap();
    assert_eq!(names_of(&prefix_results), vec!["Indomie Goreng", "Indomie Rendang"]);

    // Multi-word prefix query keeps AND semantics and narrows.
    let narrow = db
        .query(
            Query::new("foods")
                .where_filter("name", Operator::MatchPrefix, Value::String("indomie goreng".into())),
        )
        .unwrap();
    assert_eq!(names_of(&narrow), vec!["Indomie Goreng"]);

    // Unmatched prefix yields no results.
    let none = db
        .query(
            Query::new("foods")
                .where_filter("name", Operator::MatchPrefix, Value::String("nope".into())),
        )
        .unwrap();
    assert!(none.is_empty());

    // Plain Match is unchanged: exact word is required, so "indom" matches nothing.
    let exact = db
        .query(Query::new("foods").where_filter("name", Operator::Match, Value::String("indom".into())))
        .unwrap();
    assert!(exact.is_empty());

    let _ = std::fs::remove_dir_all(&dir);
}