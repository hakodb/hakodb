//! hakodb#71 Release E: F6 nested (prefix-path) projection + index-only
//! (covering) reads. Correctness is pinned against the decode path; the
//! covering flag is asserted through `explain_report` so a planner change
//! that silently drops the fast path fails a test instead of a benchmark.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::BatchMutation;
use hakodb::engine::Hako;
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::query::Query;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71f-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn nested_doc(name: &str, city: &str, zip: i64) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("name", Value::String(name.into()));
    d.insert(
        "profile",
        Value::Map(vec![
            ("city".into(), Value::String(city.into())),
            ("zip".into(), Value::Int(zip)),
        ]),
    );
    d
}

fn map_get<'a>(fields: &'a [(String, Value)], key: &str) -> Option<&'a Value> {
    fields.iter().find(|(k, _)| k == key).map(|(_, v)| v)
}

#[test]
fn nested_projection_prunes_subtrees() {
    let db = Hako::open(tmp_dir("nested"), HakoConfig::default()).unwrap();
    db.put("users", "u1", &nested_doc("ana", "bandung", 40115)).unwrap();

    let rows = db
        .query_projected_zero_copy(
            Query::new("users"),
            &["profile.city".to_string(), "name".to_string()],
        )
        .unwrap();
    assert_eq!(rows.len(), 1);
    let (id, fields) = &rows[0];
    assert_eq!(id, "u1");
    assert_eq!(map_get(fields, "name"), Some(&Value::String("ana".into())));
    match map_get(fields, "profile") {
        Some(Value::Map(m)) => {
            assert_eq!(m.len(), 1, "only city survives: {m:?}");
            assert_eq!(m[0].0.as_ref(), "city");
        }
        other => panic!("expected pruned profile map, got {other:?}"),
    }

    // A prefix path keeps the whole subtree.
    let rows = db
        .query_projected_zero_copy(Query::new("users"), &["profile".to_string()])
        .unwrap();
    match map_get(&rows[0].1, "profile") {
        Some(Value::Map(m)) => assert_eq!(m.len(), 2, "whole profile kept"),
        other => panic!("expected full profile map, got {other:?}"),
    }

    // Leaf-preserving: descending into a scalar keeps the scalar.
    let rows = db
        .query_projected_zero_copy(Query::new("users"), &["name.first".to_string()])
        .unwrap();
    assert_eq!(map_get(&rows[0].1, "name"), Some(&Value::String("ana".into())));
}

fn covering_setup(tag: &str) -> Hako {
    let db = Hako::open(tmp_dir(tag), HakoConfig::default()).unwrap();
    db.create_composite_index_ex(
        "events",
        vec![
            ("tenant".to_string(), SortDirection::Asc),
            ("status".to_string(), SortDirection::Asc),
        ],
        false,
        false,
        None,
    )
    .unwrap();
    let mut batch = Vec::new();
    for i in 0..120 {
        let mut d = HakoDoc::default();
        d.insert("tenant", Value::String(if i % 2 == 0 { "t1" } else { "t2" }.into()));
        d.insert("status", Value::String("open".into()));
        d.insert("payload", Value::String(format!("p{i}")));
        batch.push(BatchMutation::Put {
            collection: "events".into(),
            doc_id: format!("e{i:03}"),
            doc: d,
        });
    }
    db.write_batch(batch).unwrap();
    db.await_quiescent(std::time::Duration::from_secs(10));
    db
}

fn covering_query() -> Query {
    Query::new("events")
        .where_eq("tenant", Value::String("t1".into()))
        .where_eq("status", Value::String("open".into()))
}

#[test]
fn covering_projection_is_reported_and_equivalent_to_decode() {
    let db = covering_setup("covering");

    let rep = db
        .explain_report(covering_query().select_fields(vec!["tenant".to_string()]))
        .unwrap();
    assert!(rep.covering, "projection fully covered by the composite prefix");
    assert!(rep.to_string().contains("covering: index-only"));

    let rep = db
        .explain_report(covering_query().select_fields(vec!["payload".to_string()]))
        .unwrap();
    assert!(!rep.covering, "payload is not in the index prefix");

    let rep = db
        .explain_report(covering_query().select_fields(vec!["tenant.deep".to_string()]))
        .unwrap();
    assert!(!rep.covering, "nested projection falls back to decode");

    // Covering (index-only) and decode paths must agree on ids + values.
    let fast = db
        .query_projected_zero_copy(covering_query(), &["tenant".to_string()])
        .unwrap();
    let slow = db
        .query_projected_zero_copy(
            covering_query(),
            &["tenant".to_string(), "payload".to_string()],
        )
        .unwrap();
    assert!(!fast.is_empty(), "covering returns rows");
    let mut f: Vec<(String, Value)> = fast
        .iter()
        .map(|(id, fields)| (id.clone(), map_get(fields, "tenant").cloned().unwrap()))
        .collect();
    let mut s: Vec<(String, Value)> = slow
        .iter()
        .map(|(id, fields)| (id.clone(), map_get(fields, "tenant").cloned().unwrap()))
        .collect();
    f.sort_by(|a, b| a.0.cmp(&b.0));
    s.sort_by(|a, b| a.0.cmp(&b.0));
    assert_eq!(f.len(), s.len());
    assert_eq!(f, s, "index-only rows match the decode path");
    assert!(f.iter().all(|(_, v)| v == &Value::String("t1".into())));
}

#[test]
fn covering_respects_limit_offset_and_order() {
    let db = covering_setup("covering-order");

    let q = covering_query()
        .order_by("status", false)
        .limit(5)
        .offset(3)
        .select_fields(vec!["tenant".to_string()]);
    let rows = db.query_projected_zero_copy(q.clone(), &["tenant".to_string()]).unwrap();
    assert_eq!(rows.len(), 5, "limit applied");
    assert!(rows.iter().all(|(_, f)| map_get(f, "tenant") == Some(&Value::String("t1".into()))));

    let full = db.query_projected_zero_copy(q, &["tenant".to_string(), "payload".to_string()]).unwrap();
    assert_eq!(
        rows.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        full.iter().map(|(id, _)| id.clone()).collect::<Vec<_>>(),
        "covering honours the same slice as decode"
    );
}
