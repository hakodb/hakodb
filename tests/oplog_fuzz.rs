// Op-log differential fuzzer: seeded pseudo-random puts/deletes/gets/
// queries across collections, with an in-memory model checked after every
// step and full cross-path verification at intervals + across a reopen.
// Deterministic: the seed prints always; override with OPLOG_SEED, ops
// with OPLOG_OPS. No indexes are created on purpose — unindexed paths
// (FullCollection scan+match) are exactly what this exercises, and every
// read here is synchronous (no async-index races by construction).
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::query::query::Query;
use rand::{Rng, SeedableRng};
use rand::rngs::StdRng;
use std::collections::{BTreeMap, HashMap};

type Model = HashMap<(String, String), BTreeMap<String, Value>>;

fn model_fields(doc: &HakoDoc) -> BTreeMap<String, Value> {
    doc.fields.iter().map(|(k, v)| (k.to_string(), v.clone())).collect()
}

fn engine_fields(doc: &HakoDoc) -> BTreeMap<String, Value> {
    model_fields(doc)
}

fn gen_value(rng: &mut StdRng) -> Value {
    match rng.gen_range(0..8) {
        0 => Value::Int(rng.gen_range(-5..20)),
        1 => Value::Int(rng.gen_range(i64::MIN / 2..i64::MAX / 2)),
        2 => Value::Bool(rng.gen_bool(0.5)),
        3 => Value::Null,
        4 => {
            // Short (super-tag) and longer strings, occasional unicode.
            let words = ["a", "abcdefg", "hello-world", "caf\u{e9}", "x", "tn"];
            Value::String(words[rng.gen_range(0..words.len())].to_string())
        }
        5 => Value::String(format!("v{}", rng.gen_range(0..1000))),
        6 => Value::Binary(vec![rng.gen::<u8>(); 3]),
        _ => Value::Binary((0..32).map(|i| (i * 7 + 1) as u8).collect()),
    }
}

fn gen_doc(rng: &mut StdRng) -> HakoDoc {
    // Low-cardinality `grp` field supports filtered-query checks.
    let mut doc = HakoDoc::default();
    doc.insert("grp", Value::String(format!("g{}", rng.gen_range(0..3))));
    let extra = rng.gen_range(0..3);
    for i in 0..extra {
        doc.insert(format!("f{i}"), gen_value(rng));
    }
    doc
}

/// Full cross-path verification of one collection against the model.
fn verify_collection(db: &Hako, model: &Model, col: &str, ctx: &str) {
    let want_ids: Vec<String> = model
        .iter()
        .filter(|((c, _), _)| c == col)
        .map(|((_, id), _)| id.clone())
        .collect();
    let mut want_sorted = want_ids.clone();
    want_sorted.sort();

    // Decoded scans, both directions.
    for ascending in [true, false] {
        let mut ids = Vec::new();
        let mut anchor: Option<String> = None;
        loop {
            let mut q = Query::new(col);
            q = q.order_by("id", ascending);
            q.limit = Some(25);
            if let Some(a) = &anchor {
                q = q.start_after(vec![Value::String(a.clone())]);
            }
            let rows = db.query(q).unwrap_or_else(|e| panic!("{ctx} scan: {e:?}"));
            if rows.is_empty() {
                break;
            }
            anchor = Some(rows.last().unwrap().0.clone());
            for (id, doc) in rows {
                let want = model
                    .get(&(col.to_string(), id.clone()))
                    .unwrap_or_else(|| panic!("{ctx} unknown id {id}"));
                assert_eq!(&engine_fields(&doc), want, "{ctx} fields {id}");
                ids.push(id);
            }
        }
        assert_eq!(ids.len(), want_sorted.len(), "{ctx} scan count asc={ascending}");
        if ascending {
            assert_eq!(ids, want_sorted, "{ctx} ascending order");
        } else {
            let mut rev = want_sorted.clone();
            rev.reverse();
            assert_eq!(ids, rev, "{ctx} descending order");
        }
    }

    // Unindexed filtered query vs model (the v0.8.5 regression class).
    // Unordered scan: ANY matching subset of the right size is valid —
    // assert membership + predicate, never exact set (hash order).
    let g = format!("g{}", want_sorted.len() % 3);
    let mut q = Query::new(col);
    q = q.where_filter("grp", hakodb::query::filter::Operator::Eq, Value::String(g.clone()));
    q.limit = Some(10);
    let rows = db.query(q).unwrap_or_else(|e| panic!("{ctx} filtered: {e:?}"));
    let total_matches = model
        .iter()
        .filter(|((c, _), f)| c == col && f.get("grp") == Some(&Value::String(g.clone())))
        .count();
    assert_eq!(rows.len(), total_matches.min(10), "{ctx} filtered count");
    for (rid, doc) in &rows {
        assert_eq!(
            model.get(&(col.to_string(), rid.clone())),
            Some(&engine_fields(doc)),
            "{ctx} filtered row {rid}"
        );
        assert_eq!(
            doc.get("grp"),
            Some(&Value::String(g.clone())),
            "{ctx} predicate {rid}"
        );
    }

    // Walk ids + raw decode spot check.
    let q = Query::new(col).order_by("id", true);
    let mut walked = Vec::new();
    db.walk(q, &mut |id: &str, _| {
        walked.push(id.to_string());
        true
    })
    .unwrap_or_else(|e| panic!("{ctx} walk: {e:?}"));
    walked.sort();
    assert_eq!(walked, want_sorted, "{ctx} walk ids");

    if let Some(sample) = want_sorted.first() {
        let q = Query::new(col).order_by("id", true);
        let rows = db.query_raw(q).unwrap_or_else(|e| panic!("{ctx} raw: {e:?}"));
        let found = rows.iter().find(|(id, _)| id == sample).expect("sample present");
        let doc = HakoDoc::decode(&found.1).expect("raw decodes");
        assert_eq!(
            &engine_fields(&doc),
            &model[&(col.to_string(), sample.clone())],
            "{ctx} raw sample"
        );
    }
}

fn wait_ready(db: &Hako) {
    let t0 = std::time::Instant::now();
    while !db.is_indexes_ready() {
        assert!(t0.elapsed() < std::time::Duration::from_secs(30), "indexes never ready");
        std::thread::sleep(std::time::Duration::from_millis(5));
    }
}

#[test]
fn oplog_fuzz() {
    let seed: u64 = std::env::var("OPLOG_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(0xC0FFEE);
    let ops: usize = std::env::var("OPLOG_OPS")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or(1200);
    eprintln!("oplog seed: {seed} (override with OPLOG_SEED), ops: {ops} (OPLOG_OPS)");
    let mut rng = StdRng::seed_from_u64(seed);

    let dir = std::env::temp_dir().join(format!("fl-test-oplog-{seed}"));
    let _ = std::fs::remove_dir_all(&dir);
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let mut db = Hako::open(&dir, cfg.clone()).expect("open");
    wait_ready(&db);

    let cols = ["a", "b"];
    let ids: Vec<String> = (0..60).map(|i| format!("k{i:02}")).collect();
    let mut model: Model = HashMap::new();

    for step in 0..ops {
        let col = cols[rng.gen_range(0..cols.len())].to_string();
        let id = ids[rng.gen_range(0..ids.len())].clone();
        match rng.gen_range(0..100) {
            0..=49 => {
                let doc = gen_doc(&mut rng);
                model.insert((col.clone(), id.clone()), model_fields(&doc));
                db.write_batch(vec![BatchMutation::Put {
                    collection: col.clone(),
                    doc_id: id.clone(),
                    doc,
                }])
                .unwrap_or_else(|e| panic!("step {step} put: {e:?}"));
            }
            50..=64 => {
                model.remove(&(col.clone(), id.clone()));
                db.write_batch(vec![BatchMutation::Delete {
                    collection: col.clone(),
                    doc_id: id.clone(),
                }])
                .unwrap_or_else(|e| panic!("step {step} del: {e:?}"));
            }
            65..=79 => {
                // Point read, verified immediately.
                let got = db
                    .get(&col, &id)
                    .unwrap_or_else(|e| panic!("step {step} get: {e:?}"));
                match model.get(&(col.clone(), id.clone())) {
                    Some(want) => {
                        let got = got.unwrap_or_else(|| panic!("step {step} {id} missing"));
                        assert_eq!(&engine_fields(&got), want, "step {step} get {id}");
                    }
                    None => assert!(got.is_none(), "step {step} {id} should be absent"),
                }
            }
            80..=89 => {
                // Filtered spot check (unindexed path).
                let g = format!("g{}", rng.gen_range(0..3));
                let mut q = Query::new(&col);
                q = q.where_filter(
                    "grp",
                    hakodb::query::filter::Operator::Eq,
                    Value::String(g.clone()),
                );
                q.limit = Some(5);
                let rows = db.query(q).unwrap_or_else(|e| panic!("step {step} fq: {e:?}"));
                assert!(rows.len() <= 5, "step {step} limit respected");
                for (rid, doc) in &rows {
                    assert_eq!(
                        model.get(&(col.clone(), rid.clone())),
                        Some(&engine_fields(doc)),
                        "step {step} fq row {rid}"
                    );
                    assert_eq!(
                        doc.get("grp"),
                        Some(&Value::String(g.clone())),
                        "step {step} predicate {rid}"
                    );
                }
            }
            _ => {
                // Paged keyset walk over one collection.
                let mut anchor: Option<String> = None;
                let mut seen = Vec::new();
                for _ in 0..8 {
                    let mut q = Query::new(&col);
                    q = q.order_by("id", true);
                    q.limit = Some(7);
                    if let Some(a) = &anchor {
                        q = q.start_after(vec![Value::String(a.clone())]);
                    }
                    let rows = db.query(q).unwrap_or_else(|e| panic!("step {step} page: {e:?}"));
                    if rows.is_empty() {
                        break;
                    }
                    anchor = Some(rows.last().unwrap().0.clone());
                    seen.extend(rows.into_iter().map(|(id, _)| id));
                }
                let mut want: Vec<String> = model
                    .keys()
                    .filter(|(c, _)| c == &col)
                    .map(|(_, id)| id.clone())
                    .collect();
                want.sort();
                want.truncate(seen.len().min(56));
                assert_eq!(seen.len(), want.len(), "step {step} page walk count");
                assert_eq!(seen, want, "step {step} page walk order");
            }
        }

        if step == ops / 2 {
            // Reopen mid-run: explicit close flushes + snapshots, the fresh
            // handle replays, then the run CONTINUES (post-reopen writes
            // exercise recovery-yearned state, not just a verified stop).
            drop(db);
            db = Hako::open(&dir, cfg.clone()).expect("reopen");
            wait_ready(&db);
            for c in cols {
                verify_collection(&db, &model, c, &format!("reopen@{step}"));
            }
        }

        if step % 200 == 199 {
            for c in cols {
                verify_collection(&db, &model, c, &format!("step{step}"));
            }
        }
    }

    for c in cols {
        verify_collection(&db, &model, c, "final");
    }
    drop(db);
    std::fs::remove_dir_all(&dir).ok();
    eprintln!("oplog seed {seed}: all {ops} ops verified");
}
