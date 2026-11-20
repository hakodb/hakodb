//! `Hako::query()` must honour select_fields (issue #231).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::query::Query;

fn temp_dir() -> std::path::PathBuf {
    static N: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let path = std::env::temp_dir().join(format!(
        "hako-proj-{}-{}",
        std::process::id(),
        N.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
    ));
    let _ = std::fs::remove_dir_all(&path);
    path
}

fn seed(db: &Hako) {
    let mut batch = Vec::new();
    for (id, name, age) in [("u1", "alice", 30), ("u2", "bob", 40)] {
        let mut doc = HakoDoc::default();
        doc.insert("name", Value::String(name.into()));
        doc.insert("age", Value::Int(age));
        doc.insert("secret", Value::String("hidden".into()));
        doc.insert(
            "profile",
            Value::Map(vec![
                ("city".into(), Value::String("BKK".into())),
                ("zip".into(), Value::String("10110".into())),
            ]),
        );
        batch.push(BatchMutation::Put {
            collection: "users".into(),
            doc_id: id.into(),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
}

fn field<'a>(doc: &'a HakoDoc, name: &str) -> Option<&'a Value> {
    doc.fields
        .iter()
        .find(|(key, _)| key.as_ref() == name)
        .map(|(_, value)| value)
}

#[test]
fn query_select_fields_prunes_documents() {
    let dir = temp_dir();
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&dir, cfg).expect("open");
    seed(&db);

    // No projection: full document (control).
    let full = db.query(Query::new("users").limit(10)).expect("full");
    assert_eq!(full.len(), 2);
    assert!(field(&full[0].1, "secret").is_some());

    // Flat projection: only selected fields remain; row id is still returned.
    let rows = db
        .query(Query::new("users").select_fields(vec!["name".into()]).limit(10))
        .expect("projected");
    assert_eq!(rows.len(), 2);
    assert!(!rows[0].0.is_empty(), "row id must survive projection");
    assert!(field(&rows[0].1, "name").is_some());
    assert!(field(&rows[0].1, "secret").is_none(), "secret must be pruned");
    assert!(field(&rows[0].1, "age").is_none(), "age must be pruned");

    // Dotted projection keeps only the requested subtree leaf.
    let rows = db
        .query(
            Query::new("users")
                .select_fields(vec!["profile.city".into()])
                .limit(10),
        )
        .expect("dotted");
    assert_eq!(rows.len(), 2);
    let profile = field(&rows[0].1, "profile").expect("profile field");
    match profile {
        Value::Map(items) => {
            assert_eq!(items.len(), 1, "only profile.city should survive: {profile:?}");
            assert_eq!(items[0].0.as_ref(), "city");
        }
        other => panic!("expected Map, got {other:?}"),
    }
    assert!(field(&rows[0].1, "name").is_none());

    // Projection must not change ranking/limit semantics.
    let rows = db
        .query(
            Query::new("users")
                .where_filter("age", hakodb::query::filter::Operator::Gte, Value::Int(35))
                .order_by("name", true)
                .limit(1)
                .select_fields(vec!["name".into()]),
        )
        .expect("filtered");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "u2");
    assert!(field(&rows[0].1, "name").is_some());
    assert!(field(&rows[0].1, "age").is_none());

    // ORDER BY on a field that is NOT selected must still rank correctly;
    // the extra field is decoded for sorting then pruned from the result.
    let ranked = db
        .query(
            Query::new("users")
                .order_by("age", false)
                .select_fields(vec!["name".into()])
                .limit(10),
        )
        .expect("ranked");
    assert_eq!(ranked[0].0, "u2", "older user must rank first");
    assert!(field(&ranked[0].1, "age").is_none(), "order field must be pruned");
    assert!(field(&ranked[0].1, "name").is_some());
}
