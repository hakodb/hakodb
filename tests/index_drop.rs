//! hakodb#115 P0: drop every index family, idempotently, without touching
//! document data and without resurrecting the definition on reopen.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::composite::definition::SortDirection;
use hakodb::index::traverse::{TraverseDir, TraverseSpec};
use hakodb::index::vector::{encode_f32s, Metric};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "hakodb-drop-index-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    loop {
        let q = db.quiescence_status();
        if db.is_indexes_ready() && q.index_backfills == 0 && q.pending_index_ops == 0 {
            return;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(60),
            "indexes never ready"
        );
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

fn doc(status: &str, body: &str, emb: [f32; 2], link: Option<&str>) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("status", Value::String(status.into()));
    d.insert("body", Value::String(body.into()));
    d.insert("emb", Value::Binary(encode_f32s(&emb)));
    if let Some(target) = link {
        d.insert(
            "link",
            Value::Reference {
                collection: "docs".into(),
                doc_id: target.into(),
            },
        );
    }
    d
}

fn edge(from: &str, to: &str) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert(
        "_from",
        Value::Reference {
            collection: "docs".into(),
            doc_id: from.into(),
        },
    );
    d.insert(
        "_to",
        Value::Reference {
            collection: "docs".into(),
            doc_id: to.into(),
        },
    );
    d
}

fn open_rows(db: &Hako) -> usize {
    db.query(
        Query::new("docs")
            .where_filter("status", Operator::Eq, Value::String("open".into()))
            .limit(10),
    )
    .expect("query")
    .len()
}

fn incoming(db: &Hako) -> hakodb::error::Result<Vec<hakodb::engine::TraverseHit>> {
    db.find_paths(
        "docs",
        "b",
        &TraverseSpec {
            direction: TraverseDir::Incoming,
            edge_field: None,
            max_depth: 5,
            limit: 10,
            via_edge: None,
            weight_field: None,
            node_match: None,
        },
    )
}

#[test]
fn drops_all_families_and_survives_reopen() {
    let dir = tmp("all");
    let composite_id;
    {
        let db = open_db(&dir);
        db.put(
            "docs",
            "a",
            &doc("open", "alpha beta", [1.0, 0.0], Some("b")),
        )
        .unwrap();
        db.put(
            "docs",
            "b",
            &doc("closed", "beta gamma", [0.0, 1.0], Some("c")),
        )
        .unwrap();
        db.put("docs", "c", &doc("open", "gamma delta", [1.0, 1.0], None))
            .unwrap();
        db.put("rel", "e1", &edge("a", "b")).unwrap();

        db.create_index("docs", "status").unwrap();
        db.create_fts_index("docs", "body").unwrap();
        composite_id = db
            .create_composite_index("docs", vec![("status".into(), SortDirection::Asc)])
            .unwrap();
        db.create_vector_index("docs", "emb", 2, Metric::Cosine)
            .unwrap();
        db.create_traverse_index("docs").unwrap();
        db.create_edge_index("rel").unwrap();
        wait_ready(&db);

        let list = db.list_indexes(None);
        assert_eq!(list.secondary.get("docs"), Some(&vec!["status".into()]));
        assert_eq!(list.fts.get("docs"), Some(&vec!["body".into()]));
        assert!(list.composite.iter().any(|i| i.id == composite_id));
        assert!(list.vector.contains_key("docs"));
        assert!(list.traverse.contains(&"docs".into()));
        assert!(list.traverse.contains(&"rel".into()));
        assert_eq!(open_rows(&db), 2);
        assert_eq!(
            db.find_near("docs", "emb", &[1.0, 0.5], 10).unwrap().len(),
            3
        );
        // docs/a -> docs/b plus rel/e1's _to -> docs/b.
        assert_eq!(incoming(&db).unwrap().len(), 2);

        // Wrong owning collection is loud, and does not delete anything.
        assert!(db.drop_composite_index("rel", composite_id).is_err());

        db.drop_index("docs", "status").unwrap();
        db.drop_index("docs", "status").unwrap();
        assert!(db
            .list_indexes(Some("docs"))
            .secondary
            .get("docs")
            .is_none());
        assert_eq!(open_rows(&db), 2);

        db.drop_fts_index("docs", "body").unwrap();
        db.drop_fts_index("docs", "body").unwrap();
        assert!(db.list_indexes(Some("docs")).fts.is_empty());

        db.drop_vector_index("docs", "emb").unwrap();
        db.drop_vector_index("docs", "emb").unwrap();
        assert!(db.list_indexes(Some("docs")).vector.is_empty());
        assert!(db
            .find_near("docs", "emb", &[1.0, 0.5], 10)
            .unwrap()
            .is_empty());

        db.drop_composite_index("docs", composite_id).unwrap();
        db.drop_composite_index("docs", composite_id).unwrap();
        assert!(db.list_indexes(Some("docs")).composite.is_empty());
        assert_eq!(open_rows(&db), 2);

        db.drop_traverse_index("docs").unwrap();
        db.drop_traverse_index("docs").unwrap();
        assert!(incoming(&db).is_err());
        assert!(!db.list_indexes(None).traverse.contains(&"docs".into()));

        db.drop_edge_index("rel").unwrap();
        db.drop_edge_index("rel").unwrap();
        let list = db.list_indexes(Some("rel"));
        assert!(!list.traverse.contains(&"rel".into()));
        assert!(list.secondary.is_empty());
    }

    {
        let db = open_db(&dir);
        let list = db.list_indexes(None);
        assert!(list.secondary.is_empty(), "secondary resurrected: {list:?}");
        assert!(list.fts.is_empty(), "fts resurrected: {list:?}");
        assert!(list.composite.is_empty(), "composite resurrected: {list:?}");
        assert!(list.vector.is_empty(), "vector resurrected: {list:?}");
        assert!(list.traverse.is_empty(), "traverse resurrected: {list:?}");
        assert_eq!(open_rows(&db), 2);
        assert!(db
            .find_near("docs", "emb", &[1.0, 0.5], 10)
            .unwrap()
            .is_empty());
        assert!(incoming(&db).is_err());

        // Idempotent after reopen too.
        db.drop_index("docs", "status").unwrap();
        db.drop_fts_index("docs", "body").unwrap();
        db.drop_vector_index("docs", "emb").unwrap();
        db.drop_composite_index("docs", composite_id).unwrap();
        db.drop_traverse_index("docs").unwrap();
        db.drop_edge_index("rel").unwrap();
    }
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn read_only_refuses_index_drops() {
    let dir = tmp("readonly");
    let db = open_db(&dir);
    db.create_index("docs", "status").unwrap();
    db.set_read_only(true);
    assert!(db.drop_index("docs", "status").is_err());
    assert!(db.drop_index("docs", "status").is_err());
    assert!(db.list_indexes(Some("docs")).secondary.contains_key("docs"));
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn dropping_unique_composite_releases_constraint() {
    let dir = tmp("unique");
    let unique_id;
    {
        let db = open_db(&dir);
        let mut d = HakoDoc::default();
        d.insert("email", Value::String("a@example.com".into()));
        db.put("accounts", "a1", &d).unwrap();
        unique_id = db
            .create_composite_index_ex(
                "accounts",
                vec![("email".into(), SortDirection::Asc)],
                true,
                false,
                None,
            )
            .unwrap();

        let mut duplicate = HakoDoc::default();
        duplicate.insert("email", Value::String("a@example.com".into()));
        assert!(db.put("accounts", "a2", &duplicate).is_err());

        db.drop_composite_index("accounts", unique_id).unwrap();
        db.put("accounts", "a2", &duplicate).unwrap();
        assert!(db.list_indexes(Some("accounts")).composite.is_empty());
    }
    {
        let db = open_db(&dir);
        assert!(db.list_indexes(Some("accounts")).composite.is_empty());
        assert!(db.get("accounts", "a2").unwrap().is_some());
    }
    let _ = std::fs::remove_dir_all(&dir);
}
