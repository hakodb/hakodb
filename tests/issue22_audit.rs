//! hakodb#22 audit: composite range planning. The planner may only ride a
//! composite index for one range filter (with equality prefixes); results must
//! stay identical to the unindexed path and filtered indexes must still be
//! gated by the query predicate.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::filter::{Filter, FilterGroup, FilterLogic, Operator};
use hakodb::query::order::OrderBy;
use hakodb::query::query::Query;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue22-audit-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn seed(db: &Hako) {
    let mut batch = Vec::new();
    for i in 0..400 {
        let mut d = HakoDoc::default();
        d.insert("tenant", Value::String(format!("t{}", i % 4)));
        d.insert("score", Value::Int(i as i64));
        d.insert("active", Value::Bool(i % 3 != 0));
        d.insert(
            "region",
            Value::String(if i % 3 == 0 { "eu" } else { "us" }.into()),
        );
        batch.push(BatchMutation::Put {
            collection: "docs".into(),
            doc_id: format!("d{i:02}"),
            doc: d,
        });
    }
    db.write_batch(batch).unwrap();
    db.await_quiescent(std::time::Duration::from_secs(10));
}

fn range_query() -> Query {
    let mut q = Query::new("docs").where_eq("tenant", Value::String("t1".into()));
    q.filters.push(Filter {
        field: "score".into(),
        op: Operator::Gte,
        value: Value::Int(10),
    });
    q.order_by.push(OrderBy {
        field: "score".into(),
        ascending: false,
    });
    q
}

fn ids(db: &Hako, q: Query) -> Vec<String> {
    db.query(q).unwrap().into_iter().map(|(id, _)| id).collect()
}

#[test]
fn composite_range_uses_prefix_and_matches_full_scan() {
    let indexed = Hako::open(tmp_dir("range"), HakoConfig::default()).unwrap();
    indexed
        .create_composite_index(
            "docs",
            vec![
                ("tenant".into(), SortDirection::Asc),
                ("score".into(), SortDirection::Asc),
            ],
        )
        .unwrap();
    seed(&indexed);

    let plain = Hako::open(tmp_dir("range-plain"), HakoConfig::default()).unwrap();
    seed(&plain);

    let q = range_query();
    let rep = indexed.explain_report(q.clone()).unwrap();
    assert!(
        rep.scan.starts_with("composite_index_range"),
        "range should ride the composite prefix: {rep}"
    );
    assert_eq!(ids(&indexed, q.clone()), ids(&plain, q.clone()));
    assert_eq!(ids(&indexed, q), ids(&plain, range_query()));
}

#[test]
fn composite_range_supports_ascending_order_and_extra_filters() {
    let indexed = Hako::open(tmp_dir("range-order"), HakoConfig::default()).unwrap();
    indexed
        .create_composite_index(
            "docs",
            vec![
                ("tenant".into(), SortDirection::Asc),
                ("score".into(), SortDirection::Asc),
            ],
        )
        .unwrap();
    seed(&indexed);

    let plain = Hako::open(tmp_dir("range-order-plain"), HakoConfig::default()).unwrap();
    seed(&plain);

    let mut asc = range_query();
    asc.order_by[0].ascending = true;
    assert_eq!(ids(&indexed, asc.clone()), ids(&plain, asc));

    let mut extra = range_query();
    extra.filters.push(Filter {
        field: "region".into(),
        op: Operator::Eq,
        value: Value::String("eu".into()),
    });
    let rep = indexed.explain_report(extra.clone()).unwrap();
    assert!(
        rep.scan.starts_with("composite_index_range"),
        "unindexed extra filter is re-verified: {rep}"
    );
    assert_eq!(ids(&indexed, extra.clone()), ids(&plain, extra));
}

#[test]
fn composite_range_on_first_field() {
    let indexed = Hako::open(tmp_dir("range-first"), HakoConfig::default()).unwrap();
    indexed
        .create_composite_index("docs", vec![("score".into(), SortDirection::Asc)])
        .unwrap();
    seed(&indexed);

    let plain = Hako::open(tmp_dir("range-first-plain"), HakoConfig::default()).unwrap();
    seed(&plain);

    let mut q = Query::new("docs").where_filter("score", Operator::Gte, Value::Int(25));
    q.order_by.push(OrderBy {
        field: "score".into(),
        ascending: false,
    });
    let rep = indexed.explain_report(q.clone()).unwrap();
    assert!(
        rep.scan.starts_with("composite_index_range"),
        "first-field range should work without an eq prefix: {rep}"
    );
    assert_eq!(ids(&indexed, q.clone()), ids(&plain, q));
}

#[test]
fn filtered_index_is_only_used_when_the_query_implies_its_predicate() {
    let db = Hako::open(tmp_dir("filtered"), HakoConfig::default()).unwrap();
    seed(&db);
    db.create_composite_index_ex(
        "docs",
        vec![("tenant".into(), SortDirection::Asc)],
        false,
        false,
        Some(FilterGroup {
            logic: FilterLogic::And,
            filters: vec![Filter {
                field: "active".into(),
                op: Operator::Eq,
                value: Value::Bool(true),
            }],
        }),
    )
    .unwrap();
    db.await_quiescent(std::time::Duration::from_secs(10));

    // Does not imply `active == true`: the planner must not use the filtered
    // tree, otherwise inactive rows would disappear.
    let plain = Query::new("docs").where_eq("tenant", Value::String("t1".into()));
    let rep = db.explain_report(plain.clone()).unwrap();
    assert!(
        !rep.scan.starts_with("composite_index_range"),
        "filtered index must not answer a broader query: {rep}"
    );
    let rows = db.query(plain).unwrap();
    assert_eq!(rows.len(), 100, "all tenant rows are returned");

    // Exact predicate is implied, so the filtered index may answer it.
    let mut exact = Query::new("docs").where_eq("tenant", Value::String("t0".into()));
    exact.filters.push(Filter {
        field: "active".into(),
        op: Operator::Eq,
        value: Value::Bool(true),
    });
    let rep = db.explain_report(exact.clone()).unwrap();
    assert!(
        rep.scan.starts_with("composite_index_range"),
        "exact filtered predicate should use the tree: {rep}"
    );
    assert_eq!(db.query(exact).unwrap().len(), 66);
}
