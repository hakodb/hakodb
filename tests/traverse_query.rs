//! Query-path traversal (issue #53): routing, hybrid filters, fail-closed
//! shapes, ordering, plan-cache isolation.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::traverse::TraverseDir;
use hakodb::query::builder::traverse_filter;
use hakodb::query::filter::Operator;
use hakodb::query::plan::ScanType;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-travq-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn refdoc(target_col: &str, target_id: &str, grp: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert(
        "link",
        Value::Reference { collection: target_col.into(), doc_id: target_id.into() },
    );
    doc.insert("grp", Value::String(grp.into()));
    doc
}

fn seed(db: &Hako) {
    db.create_traverse_index("docs").unwrap();
    db.put("docs", "a", &refdoc("docs", "b", "x")).unwrap();
    db.put("docs", "b", &refdoc("docs", "c", "x")).unwrap();
    db.put("docs", "c", &refdoc("docs", "a", "y")).unwrap();
    db.put("users", "u1", &refdoc("docs", "a", "x")).unwrap();
}

fn wait(db: &Hako) {
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
}

fn trav(col: &str, anchor_col: &str, anchor: &str, dir: TraverseDir, depth: usize) -> Query {
    let f = traverse_filter(anchor_col, anchor, dir, None, depth);
    Query::new(col).where_filter(&f.field, f.op, f.value)
}

#[test]
fn routes_to_traverse_scan() {
    let dir = tmp("route");
    let db = open_db(&dir);
    seed(&db);
    wait(&db);
    let plan = db.explain(trav("docs", "docs", "a", TraverseDir::Outgoing, 5).limit(10)).expect("explain");
    assert!(matches!(plan.scan, ScanType::Traverse { .. }), "got {:?}", plan.scan);
    // Query-path hits live in the queried collection, BFS order.
    let rows = db.query(trav("docs", "docs", "a", TraverseDir::Outgoing, 5).limit(10)).expect("q");
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, vec!["b", "c"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_extra_filters_verify() {
    let dir = tmp("hybrid");
    let db = open_db(&dir);
    seed(&db);
    wait(&db);
    // Reached docs filtered by grp == x: b(x) passes, c(y) drops.
    let f = traverse_filter("docs", "a", TraverseDir::Outgoing, None, 5);
    let q = Query::new("docs")
        .where_filter(&f.field, f.op, f.value)
        .where_filter("grp", Operator::Eq, Value::String("x".into()))
        .limit(10);
    let rows = db.query(q).expect("q");
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, vec!["b"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn fail_closed_shapes() {
    let dir = tmp("closed");
    let db = open_db(&dir);
    seed(&db);
    wait(&db);
    // Two markers: the arm requires exactly one — falls through to an
    // unsatisfiable plan (compare_values answers false for Traverse).
    let f1 = traverse_filter("docs", "a", TraverseDir::Outgoing, None, 5);
    let f2 = traverse_filter("docs", "b", TraverseDir::Outgoing, None, 5);
    let q = Query::new("docs")
        .where_filter(&f1.field, f1.op, f1.value)
        .where_filter(&f2.field, f2.op, f2.value)
        .limit(10);
    assert!(db.query(q).expect("q").is_empty());
    // OR groups disable the arm.
    let f = traverse_filter("docs", "a", TraverseDir::Outgoing, None, 5);
    let q = Query::new("docs")
        .where_filter(&f.field, f.op, f.value)
        .or_where("grp", Operator::Eq, Value::String("x".into()))
        .limit(10);
    assert!(db.query(q).expect("q").is_empty());
    // Untracked incoming anchor: fail-closed empty (engine API is loud,
    // the query path is empty — Near precedent).
    let rows = db
        .query(trav("docs", "users", "u1", TraverseDir::Incoming, 5).limit(10))
        .expect("q");
    assert!(rows.is_empty());
    // Missing anchor: empty.
    let rows = db
        .query(trav("docs", "docs", "ghost", TraverseDir::Outgoing, 5).limit(10))
        .expect("q");
    assert!(rows.is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn order_limit_offset_and_plan_isolation() {
    let dir = tmp("order");
    let db = open_db(&dir);
    seed(&db);
    wait(&db);
    // order_by id desc re-sorts BFS order.
    let rows = db
        .query(trav("docs", "docs", "a", TraverseDir::Outgoing, 5).order_by("id", false).limit(10))
        .expect("q");
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, vec!["c", "b"]);
    // offset skips, limit truncates.
    let rows = db
        .query(trav("docs", "docs", "a", TraverseDir::Outgoing, 5).limit(1).offset(1))
        .expect("q");
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].0, "c");
    // Different anchors must not share a plan (contents-hash).
    let p1 = db.explain(trav("docs", "docs", "a", TraverseDir::Outgoing, 5).limit(10)).expect("e1");
    let p2 = db.explain(trav("docs", "docs", "b", TraverseDir::Outgoing, 5).limit(10)).expect("e2");
    assert!(matches!(p1.scan, ScanType::Traverse { .. }));
    assert!(matches!(p2.scan, ScanType::Traverse { .. }));
    let r1 = db.query(trav("docs", "docs", "a", TraverseDir::Outgoing, 5).limit(10)).expect("q1");
    let r2 = db.query(trav("docs", "docs", "b", TraverseDir::Outgoing, 5).limit(10)).expect("q2");
    let ids1: Vec<&str> = r1.iter().map(|(id, _)| id.as_str()).collect();
    let ids2: Vec<&str> = r2.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids1, vec!["b", "c"]);
    assert_eq!(ids2, vec!["c", "a"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn weighted_query_routes_and_orders_by_cost() {
    use hakodb::query::builder::traverse_filter_weighted;
    let dir = tmp("wquery");
    let db = open_db(&dir);
    db.create_edge_index("rel").unwrap();
    let edge = |f: &str, t: &str, w: f64| {
        let mut doc = HakoDoc::default();
        doc.insert("_from", Value::Reference { collection: "n".into(), doc_id: f.into() });
        doc.insert("_to", Value::Reference { collection: "n".into(), doc_id: t.into() });
        doc.insert("w", Value::Float(w));
        doc
    };
    db.put("rel", "e1", &edge("a", "b", 1.0)).unwrap();
    db.put("rel", "e2", &edge("b", "c", 1.0)).unwrap();
    db.put("rel", "e3", &edge("a", "c", 10.0)).unwrap();
    for id in ["a", "b", "c"] {
        db.put("n", id, &HakoDoc::default()).unwrap();
    }
    wait(&db);
    // Weighted marker routes to Traverse and orders cheapest-first.
    let mk = || traverse_filter_weighted("n", "a", TraverseDir::Outgoing, None, 5, "rel", Some("w"));
    let f = mk();
    let plan = db.explain(Query::new("n").where_filter(&f.field.clone(), f.op, f.value).limit(10)).expect("e");
    assert!(matches!(plan.scan, ScanType::Traverse { .. }));
    let f = mk();
    let rows = db
        .query(Query::new("n").where_filter(&f.field.clone(), f.op, f.value).limit(10))
        .expect("q");
    let ids: Vec<&str> = rows.iter().map(|(id, _)| id.as_str()).collect();
    assert_eq!(ids, vec!["b", "c"], "Dijkstra: b@1.0 then c@2.0, not direct@10");
    let _ = std::fs::remove_dir_all(&dir);
}
