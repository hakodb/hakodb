//! hakodb#71 Release B: dotted paths (F2) + filtered-index planner
//! consistency. Fetched (read + order + index encode + filter eval) all
//! ride the dotted fetch hook; semantic decisions from the issue:
//! absent path never matches (including ne), null != absent, type
//! mismatch fail-closed, array mid-path reads absent until F3.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::composite::definition::SortDirection;
use hakodb::query::filter::{Filter, FilterGroup, FilterLogic, Operator};
use hakodb::query::query::Query;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71b-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn nested(city: Option<&str>, zip: Option<&str>) -> HakoDoc {
    let mut d = HakoDoc::default();
    let mut addr = Vec::new();
    let mut profile = Vec::new();
    if let Some(c) = city {
        addr.push(("city".into(), Value::String(c.into())));
    }
    if let Some(z) = zip {
        addr.push(("zip".into(), Value::String(z.into())));
    }
    profile.push(("address".into(), Value::Map(addr)));
    d.insert("profile", Value::Map(profile));
    d
}

fn query_for(col: &str, field: &str, op: Operator, v: Value) -> hakodb::query::query::Query {
    let mut q = Query::new(col);
    q.filters.push(Filter { field: field.into(), op, value: v });
    q
}

#[test]
fn dotted_eq_range_and_ne_absent_semantics() {
    let db = Hako::open(tmp_dir("dotted-eq"), HakoConfig::default()).unwrap();
    db.put("docs", "d1", &nested(Some("Rome"), Some("00100"))).unwrap();
    db.put("docs", "d2", &nested(Some("Oslo"), Some("0150"))).unwrap();
    // null vs absent: d3 carries explicit null city, d4 lacks the
    // whole profile map.
    db.put("docs", "d3", &{
            let mut d = HakoDoc::default();
            d.insert("profile", Value::Map(vec![(
                "address".into(),
                Value::Map(vec![("city".into(), Value::Null)]),
            )]));
            d
        }).unwrap();
    db.put("docs", "d4", &HakoDoc::default()).unwrap();

    let hits_eq = db.query(
        query_for("docs", "profile.address.city", Operator::Eq, Value::String("Rome".into()))
    ).unwrap();
    assert_eq!(hits_eq.iter().map(|(i, _)| i.as_str()).collect::<Vec<_>>(), ["d1"]);

    // Range on nested path.
    let hits_range = db.query(
        query_for("docs", "profile.address.zip", Operator::Gt, Value::String("00999".into()))
    ).unwrap();
    assert_eq!(hits_range.len(), 1, "only Rome's 00100 sorts above 00999");

    // ne on an ABSENT path does NOT auto-match (explicit #71 decision).
    let hits_ne = db.query(
        query_for("docs", "profile.address.city", Operator::Ne, Value::String("Rome".into()))
    ).unwrap();
    assert!(!hits_ne.iter().any(|(i, _)| i == "d4"), "absent must not match ne");

    // eq(null) matches ONLY the explicit null (null != absent).
    let hits_null = db.query(
        query_for("docs", "profile.address.city", Operator::Eq, Value::Null)
    ).unwrap();
    assert_eq!(hits_null.iter().map(|(i, _)| i.as_str()).collect::<Vec<_>>(), ["d3"]);

    // Type mismatch: fail-closed, no match.
    let hits_type = db.query(
        query_for("docs", "profile.address.city", Operator::Eq, Value::Int(42))
    ).unwrap();
    assert!(hits_type.is_empty());

    // Array mid-path reads absent (F3 territory).
    db.put("docs", "d5", &{
            let mut d = HakoDoc::default();
            d.insert("profile", Value::Map(vec![(
                "tags".into(), Value::Array(vec![Value::String("a".into())]),
            )]));
            d
        }).unwrap();
    let hits_arr = db.query(
        query_for("docs", "profile.tags.0", Operator::Eq, Value::String("a".into()))
    ).unwrap();
    assert!(hits_arr.is_empty());
}

#[test]
fn dotted_or_groups_and_order_paths() {
    let dir = tmp_dir("dotted-sort");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    db.put("docs", "a", &nested(Some("Zeta"), None)).unwrap();
    db.put("docs", "b", &nested(Some("Alpha"), None)).unwrap();
    db.put("docs", "c", &nested(Some("Mid"), None)).unwrap();

    // OR group over dotted paths.
    let mut ors: Vec<Vec<Filter>> = Vec::new();
    ors.push(vec![
        Filter { field: "profile.address.city".into(), op: Operator::Eq, value: Value::String("Zeta".into()) },
        Filter { field: "profile.address.city".into(), op: Operator::Eq, value: Value::String("Alpha".into()) },
    ]);
    let mut q = Query::new("docs");
    q.or_groups = ors;
    let hits = db.query(q).unwrap();
    assert_eq!(hits.len(), 2);

    // ORDER BY dotted â€” legacy sort lane.
    let q = Query::new("docs").order_by("profile.address.city", true);
    let hits = db.query(q).unwrap();
    let order: Vec<String> = hits.iter().map(|(i, _)| i.clone()).collect();
    assert_eq!(order, ["b", "c", "a"]);

    // TopN lane (limit below row count) must match the legacy lane.
    let q = Query::new("docs").order_by("profile.address.city", true).limit(2);
    let hits = db.query(q).unwrap();
    assert_eq!(hits.iter().map(|(i, _)| i.as_str()).collect::<Vec<_>>(), ["b", "c"]);
}

#[test]
fn dotted_composite_index_probe_matches_full_sweep() {
    let dir = tmp_dir("dotted-index");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    db.create_composite_index_ex(
        "docs",
        vec![(
            "profile.address.city".into(),
            SortDirection::Asc,
        )],
        false, true, None,
    ).unwrap();
    for (i, c) in ["Rome", "Oslo", "Rome"].iter().enumerate() {
        db.put("docs", &format!("p{i}"), &nested(Some(c), None)).unwrap();
    }
    // Skip-doc: no nested path.
    db.put("docs", "missing", &HakoDoc::default()).unwrap();

    assert!(db.await_quiescent(std::time::Duration::from_secs(10)), "settle");
    let eq = db.query(
        query_for("docs", "profile.address.city", Operator::Eq, Value::String("Rome".into()))
    ).unwrap();
    assert_eq!(eq.len(), 2);
    let ne = db.query(
        query_for("docs", "profile.address.city", Operator::Ne, Value::String("Rome".into()))
    ).unwrap();
    assert_eq!(ne.len(), 1, "missing-path doc must not match ne");
    // Range probe over the dotted tree: strictly above "Oslo" leaves
    // the two Romes (Oslo sorts above "G" too, so "G" is no divider).
    let range = db.query(
        query_for("docs", "profile.address.city", Operator::Gt, Value::String("Oslo".into()))
    ).unwrap();
    assert_eq!(range.len(), 2, "Rome x2 strictly above Oslo");
}

#[test]
fn dotted_unique_and_filtered_planner_consistency() {
    let dir = tmp_dir("dotted-cons");
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();

    // Unique over a dotted path.
    db.create_composite_index_ex(
        "uniq",
        vec![("profile.address.city".into(), SortDirection::Asc)],
        true, false, None,
    ).unwrap();
    db.put("uniq", "u1", &nested(Some("Rome"), None)).unwrap();
    let err = db.put("uniq", "u2", &nested(Some("Rome"), None)).unwrap_err();
    assert!(matches!(err.kind(), hakodb::error::ErrorKind::UniqueViolation), "{err:?}");
    // Payload now carries context (fkip-234 report).
    match err {
        hakodb::error::HakoError::UniqueViolation { collection, index, key } => {
            assert_eq!(collection, "uniq");
            assert!(!index.is_empty());
            assert!(!key.is_empty(), "violation payload must name the holder");
        }
        _ => unreachable!(),
    }

    // Filtered composite index: the planner may ride it ONLY when the
    // query implies the predicate. A plain city query must still see
    // the non-eu doc (full-sweep parity).
    db.create_composite_index_ex(
        "cons",
        vec![("profile.address.city".into(), SortDirection::Asc)],
        false, true,
        Some(FilterGroup {
            logic: FilterLogic::And,
            filters: vec![Filter {
                field: "region".into(),
                op: Operator::Eq,
                value: Value::String("eu".into()),
            }],
        }),
    ).unwrap();
    db.put("cons", "eu1", &{
        let mut d = nested(Some("Rome"), None);
        d.insert("region", Value::String("eu".into()));
        d
    }).unwrap();
    db.put("cons", "us1", &{
        let mut d = nested(Some("Rome"), None);
        d.insert("region", Value::String("us".into()));
        d
    }).unwrap();
    assert!(db.await_quiescent(std::time::Duration::from_secs(10)), "settle");

    let unrestricted = db.query(
        query_for("cons", "profile.address.city", Operator::Eq, Value::String("Rome".into()))
    ).unwrap();
    assert_eq!(unrestricted.len(), 2, "query does not imply the filter → full sweep");

    // Query that implies the predicate: rides the filtered tree, same
    // result as the full sweep.
    let implied = {
        let mut q = query_for("cons", "profile.address.city", Operator::Eq, Value::String("Rome".into()));
        q.filters.push(Filter { field: "region".into(), op: Operator::Eq, value: Value::String("eu".into()) });
        db.query(q).unwrap()
    };
    assert_eq!(implied.len(), 1);
    assert_eq!(implied[0].0, "eu1");

    // Create-time dotted-path validation.
    assert!(db.create_composite_index_ex(
        "cons",
        vec![("profile..zip".into(), SortDirection::Asc)],
        false, true, None,
    ).is_err(), "empty dotted segment refused");
    assert!(db.create_composite_index_ex(
        "cons",
        vec![("a.".into(), SortDirection::Asc)],
        false, true, None,
    ).is_err(), "trailing dot refused");
}
