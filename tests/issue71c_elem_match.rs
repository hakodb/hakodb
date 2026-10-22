//! hakodb#71 Release C: F3 array element match (`elemMatch`) + whole-value
//! `all_eq`. Semantics under test: an element must satisfy ALL sub-filters;
//! empty arrays / missing fields / non-array values / absent element paths
//! are fail-closed (no match); raw/walk lanes refuse instead of leaking rows.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::query::Query;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71c-elem-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn f(field: &str, op: Operator, v: Value) -> Filter {
    Filter { field: field.into(), op, value: v }
}

fn order(items: Vec<(&str, i64)>, tags: Option<Vec<&str>>) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert(
        "items",
        Value::Array(
            items
                .into_iter()
                .map(|(sku, qty)| {
                    Value::Map(vec![
                        ("sku".into(), Value::String(sku.into())),
                        ("qty".into(), Value::Int(qty)),
                    ])
                })
                .collect(),
        ),
    );
    if let Some(t) = tags {
        d.insert(
            "tags",
            Value::Array(t.into_iter().map(|s| Value::String(s.into())).collect()),
        );
    }
    d
}

#[test]
fn elem_match_requires_one_element_with_all_subfilters() {
    let db = Hako::open(tmp_dir("basic"), HakoConfig::default()).unwrap();
    db.put("o", "o1", &order(vec![("A1", 1), ("B2", 5)], None)).unwrap();
    // Same sku but qty too low, plus a high-qty item of another sku: no
    // single element satisfies BOTH sub-filters.
    db.put("o", "o2", &order(vec![("A1", 1), ("C3", 9)], None)).unwrap();
    db.put("o", "o3", &order(vec![("A1", 3), ("B2", 0)], None)).unwrap();

    let q = Query::new("o").elem_match("items", vec![
        f("sku", Operator::Eq, Value::String("A1".into())),
        f("qty", Operator::Gt, Value::Int(2)),
    ]);
    let hits: Vec<String> = db.query(q).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(hits, ["o3"], "only o3 has one element with sku=A1 AND qty>2");
}

#[test]
fn elem_match_fail_closed_edges() {
    let db = Hako::open(tmp_dir("edges"), HakoConfig::default()).unwrap();
    db.put("o", "empty", &order(vec![], None)).unwrap();
    db.put("o", "missing", &HakoDoc::default()).unwrap();
    db.put("o", "nonarray", &{
        let mut d = HakoDoc::default();
        d.insert("items", Value::String("A1".into()));
        d
    }).unwrap();
    db.put("o", "good", &order(vec![("A1", 2)], None)).unwrap();

    let q = || Query::new("o").elem_match("items", vec![
        f("sku", Operator::Eq, Value::String("A1".into())),
    ]);
    let hits: Vec<String> = db.query(q()).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(hits, ["good"], "empty/missing/non-array never match");

    // Element path absent: an element without `sku` is ineligible, and
    // the whole-value path ("") compares the element itself.
    db.put("o", "nosku", &{
        let mut d = HakoDoc::default();
        d.insert("items", Value::Array(vec![
            Value::Map(vec![("qty".into(), Value::Int(4))]),
        ]));
        d
    }).unwrap();
    let hits: Vec<String> = db.query(q()).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(hits, ["good"], "absent element path does not match");

    let whole = Query::new("o").elem_match("items", vec![
        f("", Operator::Eq, Value::Int(7)),
    ]);
    db.put("o", "scalar", &{
        let mut d = HakoDoc::default();
        d.insert("items", Value::Array(vec![Value::Int(7), Value::Int(9)]));
        d
    }).unwrap();
    let hits: Vec<String> = db.query(whole).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(hits, ["scalar"], "empty element path compares the value itself");
}

#[test]
fn array_all_eq_is_whole_value_membership() {
    let db = Hako::open(tmp_dir("all-eq"), HakoConfig::default()).unwrap();
    db.put("d", "all", &order(vec![], Some(vec!["a", "a"]))).unwrap();
    db.put("d", "mixed", &order(vec![], Some(vec!["a", "b"]))).unwrap();
    db.put("d", "empty", &order(vec![], Some(vec![]))).unwrap();
    db.put("d", "missing", &HakoDoc::default()).unwrap();

    let q = Query::new("d").where_filter(
        "tags",
        Operator::ArrayAllEq,
        Value::String("a".into()),
    );
    let hits: Vec<String> = db.query(q).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(hits, ["all"], "mixed/empty/missing fail closed");
}

#[test]
fn elem_match_combines_with_indexed_filters() {
    let db = Hako::open(tmp_dir("combo"), HakoConfig::default()).unwrap();
    db.create_index("o", "status").unwrap();
    let mut a = order(vec![("A1", 5)], None);
    a.insert("status", Value::String("open".into()));
    let mut b = order(vec![("A1", 5)], None);
    b.insert("status", Value::String("done".into()));
    let mut c = order(vec![("B2", 5)], None);
    c.insert("status", Value::String("open".into()));
    db.put("o", "a", &a).unwrap();
    db.put("o", "b", &b).unwrap();
    db.put("o", "c", &c).unwrap();

    let q = Query::new("o")
        .where_eq("status", Value::String("open".into()))
        .elem_match("items", vec![f("sku", Operator::Eq, Value::String("A1".into()))]);
    let hits: Vec<String> = db.query(q).unwrap().into_iter().map(|(id, _)| id).collect();
    assert_eq!(hits, ["a"], "index filter AND elemMatch both apply");
}

#[test]
fn raw_and_walk_refuse_elem_match() {
    let db = Hako::open(tmp_dir("guard"), HakoConfig::default()).unwrap();
    db.put("o", "o1", &order(vec![("A1", 5)], None)).unwrap();
    let mut q = Query::new("o").elem_match("items", vec![
        f("sku", Operator::Eq, Value::String("A1".into())),
    ]);
    q = q.order_by("id", true);
    let walk_err = db.walk(q, &mut |_, _| true).is_err();
    assert!(walk_err, "walk cannot evaluate elemMatch â€” must error");

    let mut rawq = Query::new("o").elem_match("items", vec![
        f("sku", Operator::Eq, Value::String("A1".into())),
    ]);
    rawq = rawq.order_by("id", true);
    let raw_err = db.query_raw(rawq).is_err();
    assert!(raw_err, "raw bytes cannot evaluate elemMatch â€” must error");
}
