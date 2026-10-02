//! DHP regression: `==` on a numeric-looking field must behave like the
//! range workaround (`>=` + `<=`), including across Int/String storage.
//! F1251251023 / passw=15072007 returned 0 rows for `==` while ranges hit.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-eqx-{}-{}",
        label,
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn seed_int(db: &Hako) {
    let mut doc = HakoDoc::default();
    doc.insert("nim", Value::String("F1251251023".into()));
    doc.insert("passw", Value::Int(15072007));
    db.write_batch(vec![BatchMutation::Put {
        collection: "mhs".into(),
        doc_id: "F1251251023".into(),
        doc,
    }])
    .expect("seed");
}

fn seed_str(db: &Hako) {
    let mut doc = HakoDoc::default();
    doc.insert("nim", Value::String("F1251251023".into()));
    doc.insert("passw", Value::String("15072007".into()));
    db.write_batch(vec![BatchMutation::Put {
        collection: "mhs".into(),
        doc_id: "F1251251023".into(),
        doc,
    }])
    .expect("seed");
}

fn count(db: &Hako, q: Query) -> usize {
    db.query(q).expect("query").len()
}

fn eq(field: &str, v: Value) -> Query {
    Query::new("mhs").where_filter(field, Operator::Eq, v)
}

#[test]
fn eq_int_stored_int_filter() {
    let dir = tmp("ii");
    let db = open_db(&dir);
    seed_int(&db);
    // Same-type Eq must hit (the DHP report: even this shape failed there —
    // if it passes here, the divergence is data/type, not the operator).
    assert_eq!(count(&db, eq("passw", Value::Int(15072007))), 1, "int==int");
    assert_eq!(count(&db, eq("nim", Value::String("F1251251023".into()))), 1, "str==str");
    assert_eq!(count(&db, eq("passw", Value::Int(1))), 0, "int==wrong");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn eq_cross_type_matches_range_semantics() {
    for stored_str in [false, true] {
        let dir = tmp(if stored_str { "s" } else { "i" });
        let db = open_db(&dir);
        if stored_str {
            seed_str(&db);
        } else {
            seed_int(&db);
        }
        // Range workaround hits in both worlds (the verified DHP behavior).
        let lo = Query::new("mhs").where_filter("passw", Operator::Gte, Value::Int(15072007));
        let hi = Query::new("mhs").where_filter("passw", Operator::Lte, Value::Int(15072007));
        assert_eq!(count(&db, lo), 1, "gte hits (stored_str={stored_str})");
        assert_eq!(count(&db, hi), 1, "lte hits (stored_str={stored_str})");
        // So must ==, regardless of which side carries the string.
        assert_eq!(
            count(&db, eq("passw", Value::Int(15072007))),
            1,
            "int-filter == hits (stored_str={stored_str})"
        );
        assert_eq!(
            count(&db, eq("passw", Value::String("15072007".into()))),
            1,
            "str-filter == hits (stored_str={stored_str})"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

#[test]
fn eq_cross_type_with_secondary_index() {    for stored_str in [false, true] {
        let dir = tmp(if stored_str { "is" } else { "ii" });
        let db = open_db(&dir);
        if stored_str {
            seed_str(&db);
        } else {
            seed_int(&db);
        }
        db.create_index("mhs", "passw").expect("index");
        assert_eq!(
            count(&db, eq("passw", Value::Int(15072007))),
            1,
            "indexed int-filter == hits (stored_str={stored_str})"
        );
        assert_eq!(
            count(&db, eq("passw", Value::String("15072007".into()))),
            1,
            "indexed str-filter == hits (stored_str={stored_str})"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Exact DHP login shape: nim == X AND passw == Y, both filter-type
/// directions, with and without a secondary index on passw.
#[test]
fn login_combo_both_directions() {
    for stored_str in [false, true] {
        for indexed in [false, true] {
            let dir = tmp("combo");
            let db = open_db(&dir);
            if stored_str {
                seed_str(&db);
            } else {
                seed_int(&db);
            }
            if indexed {
                db.create_index("mhs", "passw").expect("index");
            }
            let login = |pv: Value| {
                Query::new("mhs")
                    .where_eq("nim", Value::String("F1251251023".into()))
                    .where_filter("passw", Operator::Eq, pv)
                    .limit(1)
            };
            assert_eq!(count(&db, login(Value::Int(15072007))), 1, "login int");
            assert_eq!(
                count(&db, login(Value::String("15072007".into()))),
                1,
                "login str"
            );
            assert_eq!(count(&db, login(Value::Int(0))), 0, "wrong passw");
            let _ = std::fs::remove_dir_all(&dir);
        }
    }
}

/// Ne cross-type: different bytes must still verify semantically -
/// cross-type-EQUAL values are excluded, everything else kept.
#[test]
fn ne_cross_type_verifies() {
    let dir = tmp("ne");
    let db = open_db(&dir);
    seed_int(&db);
    assert_eq!(
        count(
            &db,
            Query::new("mhs").where_filter(
                "passw",
                Operator::Ne,
                Value::String("15072007".into())
            )
        ),
        0,
        "ne excludes cross-type-equal"
    );
    assert_eq!(
        count(
            &db,
            Query::new("mhs").where_filter(
                "passw",
                Operator::Ne,
                Value::String("nope".into())
            )
        ),
        1,
        "ne keeps different"
    );
    let _ = std::fs::remove_dir_all(&dir);
}
