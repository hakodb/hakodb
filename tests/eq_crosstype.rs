//! Cross-type Eq regression: `==` on a numeric-looking field must
//! behave like the range workaround (`>=` + `<=`), including across
//! Int/String storage.
//! A numeric-string secret returned 0 rows for `==` while ranges hit.
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
    doc.insert("sid", Value::String("S25000001".into()));
    doc.insert("pin", Value::Int(7062007));
    db.write_batch(vec![BatchMutation::Put {
        collection: "students".into(),
        doc_id: "S25000001".into(),
        doc,
    }])
    .expect("seed");
}

fn seed_str(db: &Hako) {
    let mut doc = HakoDoc::default();
    doc.insert("sid", Value::String("S25000001".into()));
    doc.insert("pin", Value::String("7062007".into()));
    db.write_batch(vec![BatchMutation::Put {
        collection: "students".into(),
        doc_id: "S25000001".into(),
        doc,
    }])
    .expect("seed");
}

fn count(db: &Hako, q: Query) -> usize {
    db.query(q).expect("query").len()
}

fn eq(field: &str, v: Value) -> Query {
    Query::new("students").where_filter(field, Operator::Eq, v)
}

#[test]
fn eq_int_stored_int_filter() {
    let dir = tmp("ii");
    let db = open_db(&dir);
    seed_int(&db);
    // Same-type Eq must hit (the field report: even this shape failed there —
    // if it passes here, the divergence is data/type, not the operator).
    assert_eq!(count(&db, eq("pin", Value::Int(7062007))), 1, "int==int");
    assert_eq!(count(&db, eq("sid", Value::String("S25000001".into()))), 1, "str==str");
    assert_eq!(count(&db, eq("pin", Value::Int(1))), 0, "int==wrong");
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
        // Range workaround hits in both worlds (the verified field behavior).
        let lo = Query::new("students").where_filter("pin", Operator::Gte, Value::Int(7062007));
        let hi = Query::new("students").where_filter("pin", Operator::Lte, Value::Int(7062007));
        assert_eq!(count(&db, lo), 1, "gte hits (stored_str={stored_str})");
        assert_eq!(count(&db, hi), 1, "lte hits (stored_str={stored_str})");
        // So must ==, regardless of which side carries the string.
        assert_eq!(
            count(&db, eq("pin", Value::Int(7062007))),
            1,
            "int-filter == hits (stored_str={stored_str})"
        );
        assert_eq!(
            count(&db, eq("pin", Value::String("7062007".into()))),
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
        db.create_index("students", "pin").expect("index");
        assert_eq!(
            count(&db, eq("pin", Value::Int(7062007))),
            1,
            "indexed int-filter == hits (stored_str={stored_str})"
        );
        assert_eq!(
            count(&db, eq("pin", Value::String("7062007".into()))),
            1,
            "indexed str-filter == hits (stored_str={stored_str})"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }
}

/// Portal login shape: sid == X AND pin == Y, both filter-type
/// directions, with and without a secondary index on pin.
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
                db.create_index("students", "pin").expect("index");
            }
            let login = |pv: Value| {
                Query::new("students")
                    .where_eq("sid", Value::String("S25000001".into()))
                    .where_filter("pin", Operator::Eq, pv)
                    .limit(1)
            };
            assert_eq!(count(&db, login(Value::Int(7062007))), 1, "login int");
            assert_eq!(
                count(&db, login(Value::String("7062007".into()))),
                1,
                "login str"
            );
            assert_eq!(count(&db, login(Value::Int(0))), 0, "wrong pin");
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
            Query::new("students").where_filter(
                "pin",
                Operator::Ne,
                Value::String("7062007".into())
            )
        ),
        0,
        "ne excludes cross-type-equal"
    );
    assert_eq!(
        count(
            &db,
            Query::new("students").where_filter(
                "pin",
                Operator::Ne,
                Value::String("nope".into())
            )
        ),
        1,
        "ne keeps different"
    );
    let _ = std::fs::remove_dir_all(&dir);
}

/// Scale probe: 25k docs (portal roster shape) + composite(pin,sid) +
/// secondary(sid). Times combo (union path) vs sid-only (secondary trust)
/// vs the same combo with the composite dropped (sweep+verify).
#[test]
fn scale_union_vs_secondary() {
    use std::time::Instant;
    let dir = tmp("scale25k");
    let db = open_db(&dir);
    const N: usize = 25_000;
    let mut batch = Vec::with_capacity(N);
    for i in 0..N {
        let mut doc = HakoDoc::default();
        doc.insert("sid", Value::String(format!("F{i:09}")));
        doc.insert("pin", Value::String(format!("{}", 15000000 + (i % 9999))));
        doc.insert("nama", Value::String(format!("Nama {i}")));
        batch.push(BatchMutation::Put {
            collection: "students".into(),
            doc_id: format!("d{i:06}"),
            doc,
        });
    }
    db.write_batch(batch).expect("seed");
    db.create_index("students", "sid").expect("sec sid");
    db.create_index("students", "pin").expect("sec pin");
    db.create_composite_index(
        "students",
        vec![
            ("pin".to_string(), hakodb::index::composite::definition::SortDirection::Asc),
            ("sid".to_string(), hakodb::index::composite::definition::SortDirection::Asc),
        ],
    )
    .expect("composite");
    // Target row: deterministic pick.
    let target = 12345usize;
    let tsid = format!("F{target:09}");
    let tpass = format!("{}", 15000000 + (target % 9999));
    let q_sid = || {
        Query::new("students")
            .where_eq("sid", Value::String(tsid.clone()))
            .limit(1)
    };
    let q_combo = || {
        Query::new("students")
            .where_eq("sid", Value::String(tsid.clone()))
            .where_filter("pin", Operator::Eq, Value::String(tpass.clone()))
            .limit(1)
    };
    // Wait for index backfills (else we time contention, not queries).
    let t0 = std::time::Instant::now();
    loop {
        if db.quiescence_status().index_backfills == 0 {
            break;
        }
        assert!(
            t0.elapsed() < std::time::Duration::from_secs(120),
            "backfill never drains"
        );
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    // Warmup.
    assert_eq!(count(&db, q_sid()), 1);
    assert_eq!(count(&db, q_combo()), 1);
    let t = Instant::now();
    let mut n = 0;
    for _ in 0..50 {
        n += count(&db, q_sid());
    }
    let sid_us = t.elapsed().as_micros() / 50;
    let t = Instant::now();
    for _ in 0..50 {
        n += count(&db, q_combo());
    }
    let combo_us = t.elapsed().as_micros() / 50;
    eprintln!("SCALE sid-only={sid_us}us/req combo={combo_us}us/req rows={n}");
    assert_eq!(n, 100);
    let _ = std::fs::remove_dir_all(&dir);
}
