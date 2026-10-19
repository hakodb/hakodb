//! hakodb#71 Release C: F4 aggregation pipeline (match/project/sort/
//! limit/offset + unwind/group with sum/count/min/max/avg). Checks compare
//! pipeline output against hand-computed expectations; edge behaviour
//! (unwind skipping non-arrays, global group without key, fail-closed
//! nested project/unwind) is pinned here.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::query::filter::{Filter, Operator};
use hakodb::query::pipeline::{Accum, Pipeline};
use hakodb::query::query::Query;

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71c-pipe-{}-{}", tag, std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn order(id: &str, status: &str, items: Vec<(&str, i64)>) -> (String, HakoDoc) {
    let mut d = HakoDoc::default();
    d.insert("status", Value::String(status.into()));
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
    (id.to_string(), d)
}

fn seed(db: &Hako) {
    for (id, doc) in [
        order("o1", "paid", vec![("A", 2), ("B", 3)]),
        order("o2", "paid", vec![("A", 4)]),
        order("o3", "open", vec![("B", 1)]),
    ] {
        db.put("orders", &id, &doc).unwrap();
    }
}

fn get(doc: &HakoDoc, field: &str) -> Value {
    doc.get(field).cloned().unwrap_or(Value::Null)
}

#[test]
fn match_project_sort_limit_offset() {
    let db = Hako::open(&tmp_dir("basic"), HakoConfig::default()).unwrap();
    seed(&db);

    let p = Pipeline::new("orders")
        .match_query(Query::new("orders").where_eq("status", Value::String("paid".into())))
        .sort("status", true)
        .offset(1)
        .limit(1)
        .project(vec!["status"]);
    let rows = db.run_pipeline(p).unwrap();
    assert_eq!(rows.len(), 1, "2 paid, offset 1 → 1 row");
    assert_eq!(rows[0].1.fields.len(), 1, "project keeps status only");
    assert_eq!(get(&rows[0].1, "status"), Value::String("paid".into()));
}

#[test]
fn unwind_and_group_by_element_path() {
    let db = Hako::open(&tmp_dir("unwind"), HakoConfig::default()).unwrap();
    seed(&db);

    let p = Pipeline::new("orders")
        .unwind("items")
        .group(
            Some("items.sku"),
            vec![
                ("total_qty", Accum::Sum("items.qty".into())),
                ("orders", Accum::Count),
            ],
        )
        .sort("items.sku", true);
    let rows = db.run_pipeline(p).unwrap();
    assert_eq!(rows.len(), 2, "two skus");
    // A: 2 + 4 = 6 across 2 elements; B: 3 + 1 = 4 across 2 elements.
    let simplify = |r: &(String, HakoDoc)| {
        (
            r.1.get("sku").cloned().unwrap_or(Value::Null),
            r.1.get("total_qty").cloned().unwrap_or(Value::Null),
            r.1.get("orders").cloned().unwrap_or(Value::Null),
        )
    };
    assert_eq!(
        rows.iter().map(simplify).collect::<Vec<_>>(),
        [
            (Value::String("A".into()), Value::Int(6), Value::Int(2)),
            (Value::String("B".into()), Value::Int(4), Value::Int(2)),
        ]
    );
}

#[test]
fn group_without_key_is_one_global_group() {
    let db = Hako::open(&tmp_dir("global"), HakoConfig::default()).unwrap();
    seed(&db);

    let p = Pipeline::new("orders")
        .unwind("items")
        .group(None, vec![
            ("n", Accum::Count),
            ("qty", Accum::Sum("items.qty".into())),
            ("avg_qty", Accum::Avg("items.qty".into())),
            ("min_qty", Accum::Min("items.qty".into())),
            ("max_qty", Accum::Max("items.qty".into())),
        ]);
    let rows = db.run_pipeline(p).unwrap();
    assert_eq!(rows.len(), 1);
    let doc = &rows[0].1;
    assert_eq!(get(doc, "n"), Value::Int(4));
    assert_eq!(get(doc, "qty"), Value::Int(10));
    assert_eq!(get(doc, "avg_qty"), Value::Float(2.5));
    assert_eq!(get(doc, "min_qty"), Value::Int(1));
    assert_eq!(get(doc, "max_qty"), Value::Int(4));
}

#[test]
fn unwind_skips_missing_and_non_array_fields() {
    let db = Hako::open(&tmp_dir("skip"), HakoConfig::default()).unwrap();
    seed(&db);
    let mut flat = HakoDoc::default();
    flat.insert("status", Value::String("paid".into()));
    flat.insert("items", Value::String("nope".into()));
    db.put("orders", "flat", &flat).unwrap();
    let mut missing = HakoDoc::default();
    missing.insert("status", Value::String("paid".into()));
    db.put("orders", "missing", &missing).unwrap();

    let p = Pipeline::new("orders")
        .match_query(Query::new("orders").where_eq("status", Value::String("paid".into())))
        .unwind("items")
        .group(None, vec![("n", Accum::Count)]);
    let rows = db.run_pipeline(p).unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(get(&rows[0].1, "n"), Value::Int(3), "only o1(2)+o2(1) elements");
}

#[test]
fn elem_match_inside_pipeline_match() {
    let db = Hako::open(&tmp_dir("elem"), HakoConfig::default()).unwrap();
    seed(&db);

    let q = Query::new("orders").elem_match("items", vec![
        Filter { field: "sku".into(), op: Operator::Eq, value: Value::String("A".into()) },
        Filter { field: "qty".into(), op: Operator::Gt, value: Value::Int(3) },
    ]);
    let p = Pipeline::new("orders").match_query(q).project(vec!["status"]);
    let rows = db.run_pipeline(p).unwrap();
    assert_eq!(rows.len(), 1, "only o2 has A with qty > 3");
    assert_eq!(rows[0].0, "o2");
}

#[test]
fn nested_unwind_is_refused_but_nested_project_prunes() {
    let db = Hako::open(&tmp_dir("refuse"), HakoConfig::default()).unwrap();
    seed(&db);

    let p = Pipeline::new("orders").unwind("meta.items");
    assert!(db.run_pipeline(p).is_err(), "nested unwind refused");

    let p = Pipeline::new("orders").project(vec!["items.sku"]);
    let rows = db.run_pipeline(p).unwrap();
    assert_eq!(rows.len(), 3);
    for (_, doc) in &rows {
        let items = doc.get("items").expect("items prefix kept");
        match items {
            Value::Array(arr) => {
                for el in arr {
                    match el {
                        Value::Map(m) => assert!(m.iter().all(|(k, _)| k.as_ref() == "sku")),
                        other => panic!("unexpected element {other:?}"),
                    }
                }
            }
            other => panic!("unexpected items value {other:?}"),
        }
    }
}
