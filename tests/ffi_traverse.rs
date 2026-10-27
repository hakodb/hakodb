//! C-API round trip for graph traversal: `hk_engine_create_traverse_index`
//! and `hk_engine_find_paths` return BFS-ordered
//! `[{"collection","id","depth","doc"}]`, hard-error on untracked
//! incoming, fail-close to `"[]"` on missing anchors.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::ffi::{
    hk_engine_await_quiescent, hk_engine_create_traverse_index, hk_engine_find_paths,
    hk_engine_open, hk_last_error, hk_string_free,
};
use std::ffi::{CStr, CString};

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-ffi-trav-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn refdoc(tgt: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("link", Value::Reference { collection: "pts".into(), doc_id: tgt.into() });
    doc
}

fn seed(dir: &std::path::Path) {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(dir, cfg).expect("open");
    db.create_traverse_index("pts").expect("create");
    for (id, tgt) in [("a", "b"), ("b", "c")] {
        db.write_batch(vec![BatchMutation::Put {
            collection: "pts".into(),
            doc_id: id.to_string(),
            doc: refdoc(tgt),
        }])
        .expect("seed");
    }
    let mut blank = HakoDoc::default();
    blank.insert("v", Value::Int(1));
    db.write_batch(vec![BatchMutation::Put {
        collection: "pts".into(),
        doc_id: "c".into(),
        doc: blank,
    }])
    .expect("seed");
    db.flush().expect("flush");
}

fn open(dir: &std::path::Path) -> *mut hakodb::ffi::HK_Engine {
    let path = CString::new(dir.to_str().expect("path")).expect("cstr");
    let eng = { hk_engine_open(path.as_ptr()) };
    assert!(!eng.is_null(), "hk_engine_open failed");
    assert!({ hk_engine_await_quiescent(eng, 60_000) }, "never quiescent");
    eng
}

fn to_json(ptr: *mut std::ffi::c_char) -> serde_json::Value {
    assert!(!ptr.is_null(), "null return");
    let s = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    hk_string_free(ptr);
    serde_json::from_str(&s).expect("json")
}

fn last_err() -> String {
    unsafe {
        let p = hk_last_error();
        assert!(!p.is_null());
        CStr::from_ptr(p).to_string_lossy().into_owned()
    }
}

#[test]
fn find_paths_returns_bfs_hits_with_depth() {
    let dir = tmp("ordered");
    seed(&dir);
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let id = CString::new("a").unwrap();
    let out = {
        hk_engine_find_paths(eng, col.as_ptr(), id.as_ptr(), 0, std::ptr::null(), 10, 10)
    };
    let v = to_json(out);
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], "b");
    assert_eq!(arr[0]["depth"], 1);
    assert_eq!(arr[0]["collection"], "pts");
    assert_eq!(arr[1]["id"], "c");
    assert_eq!(arr[1]["depth"], 2);
    assert!(arr[1]["doc"].is_object());
    // G1 (#168): frozen v1 keeps the v0.19.3 row shape — no route keys.
    assert!(!arr[0].as_object().unwrap().contains_key("path"));
    assert!(!arr[1].as_object().unwrap().contains_key("path"));
    // Opt-in path rows ride the v4 generation (see find_paths_v4 test).
}

#[test]
fn find_paths_missing_anchor_returns_empty() {
    let dir = tmp("missing");
    seed(&dir);
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let id = CString::new("ghost").unwrap();
    let out = {
        hk_engine_find_paths(eng, col.as_ptr(), id.as_ptr(), 0, std::ptr::null(), 10, 10)
    };
    assert_eq!(to_json(out), serde_json::json!([]));
}

#[test]
fn find_paths_untracked_incoming_is_hard_error() {
    let dir = tmp("untracked");
    seed(&dir);
    let eng = open(&dir);
    // No traverse index on "other": incoming must null + last_error.
    let col = CString::new("other").unwrap();
    let id = CString::new("x").unwrap();
    let out = {
        hk_engine_find_paths(eng, col.as_ptr(), id.as_ptr(), 1, std::ptr::null(), 10, 10)
    };
    assert!(out.is_null(), "untracked incoming must return null");
    assert!(last_err().contains("create_traverse_index"), "got: {}", last_err());
    // create_traverse_index via FFI works (idempotent: pts already has it).
    let col2 = CString::new("pts").unwrap();
    let rc = { hk_engine_create_traverse_index(eng, col2.as_ptr()) };
    assert_eq!(rc, 0);
}

#[test]
fn shortest_paths_weighted_over_ffi() {
    use hakodb::ffi::{hk_engine_create_edge_index, hk_engine_shortest_paths};
    let dir = tmp("shortest");
    seed_edge_db(&dir);
    let eng = open(&dir);
    // a -1-> b -1-> c plus a -10-> c: Dijkstra orders b@1.0, c@2.0.
    let col = CString::new("n").unwrap();
    let id = CString::new("a").unwrap();
    let via = CString::new("rel").unwrap();
    let w = CString::new("w").unwrap();
    let out = {
        hk_engine_shortest_paths(
            eng,
            col.as_ptr(),
            id.as_ptr(),
            0,
            std::ptr::null(),
            via.as_ptr(),
            w.as_ptr(),
            10,
            10,
        )
    };
    let v = to_json(out);
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], "b");
    assert!((arr[0]["cost"].as_f64().unwrap() - 1.0).abs() < 1e-9);
    assert_eq!(arr[1]["id"], "c");
    assert!((arr[1]["cost"].as_f64().unwrap() - 2.0).abs() < 1e-9);
    // create_edge_index via FFI works (idempotent here).
    let rel = CString::new("rel").unwrap();
    let rc = { hk_engine_create_edge_index(eng, rel.as_ptr()) };
    assert_eq!(rc, 0);
}

fn seed_edge_db(dir: &std::path::Path) {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(dir, cfg).expect("open");
    db.create_edge_index("rel").expect("edge");
    let edge = |f: &str, t: &str, w: f64| {
        let mut doc = HakoDoc::default();
        doc.insert("_from", Value::Reference { collection: "n".into(), doc_id: f.into() });
        doc.insert("_to", Value::Reference { collection: "n".into(), doc_id: t.into() });
        doc.insert("w", Value::Float(w));
        doc
    };
    for (id, f, t, w) in [("e1", "a", "b", 1.0), ("e2", "b", "c", 1.0), ("e3", "a", "c", 10.0)] {
        db.write_batch(vec![BatchMutation::Put {
            collection: "rel".into(),
            doc_id: id.to_string(),
            doc: edge(f, t, w),
        }])
        .expect("seed");
    }
    for id in ["a", "b", "c"] {
        db.write_batch(vec![BatchMutation::Put {
            collection: "n".into(),
            doc_id: id.to_string(),
            doc: HakoDoc::default(),
        }])
        .expect("seed");
    }
    db.flush().expect("flush");
}

#[test]
fn find_paths_v2_filter_prunes_descent() {
    use hakodb::ffi::hk_engine_find_paths_v2;
    let dir = tmp("v2filter");
    seed(&dir);
    // Stamp kinds: b = "hop" (matches), c = "stop" (does not). The start
    // node "a" has no kind at all — it is expanded regardless.
    {
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        let mut bdoc = HakoDoc::default();
        bdoc.insert("kind", Value::String("hop".into()));
        bdoc.insert("link", Value::Reference { collection: "pts".into(), doc_id: "c".into() });
        let mut cdoc = HakoDoc::default();
        cdoc.insert("kind", Value::String("stop".into()));
        cdoc.insert("v", Value::Int(1));
        db.write_batch(vec![
            BatchMutation::Put { collection: "pts".into(), doc_id: "b".into(), doc: bdoc },
            BatchMutation::Put { collection: "pts".into(), doc_id: "c".into(), doc: cdoc },
        ])
        .expect("stamp");
        db.flush().expect("flush");
    }
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let id = CString::new("a").unwrap();
    let kind = CString::new("kind").unwrap();
    let hop = CString::new("\"hop\"").unwrap();
    // Filtered: c is pruned (non-matching), so only b serves.
    let out = {
        hk_engine_find_paths_v2(
            eng,
            col.as_ptr(),
            id.as_ptr(),
            0,
            std::ptr::null(),
            10,
            10,
            kind.as_ptr(),
            hop.as_ptr(),
        )
    };
    let arr = to_json(out).as_array().expect("array").clone();
    assert_eq!(arr.len(), 1, "c must be pruned: {arr:?}");
    assert_eq!(arr[0]["id"], "b");
    // G1 (#168): frozen v2 rows carry no route keys — depth still rides.
    assert!(!arr[0].as_object().unwrap().contains_key("path"));
    // Unfiltered (nulls): the plain v1-equivalent walk sees c too.
    let out2 = {
        hk_engine_find_paths_v2(
            eng, col.as_ptr(), id.as_ptr(), 0, std::ptr::null(), 10, 10,
            std::ptr::null(), std::ptr::null(),
        )
    };
    assert_eq!(to_json(out2).as_array().expect("array").len(), 2);
    // One-sided filter is a loud error, never a silent partial filter.
    let bad = {
        hk_engine_find_paths_v2(
            eng, col.as_ptr(), id.as_ptr(), 0, std::ptr::null(), 10, 10,
            kind.as_ptr(), std::ptr::null(),
        )
    };
    assert!(bad.is_null(), "one-sided filter must fail");
    assert!(last_err().contains("filter_json"), "got: {}", last_err());
}

#[test]
fn find_simple_paths_over_ffi_route_rows() {
    use hakodb::ffi::hk_engine_find_simple_paths;
    let dir = tmp("simpleffi");
    seed(&dir);
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let id = CString::new("a").unwrap();
    // Chain a -> b -> c: each distinct simple path is one row, cheapest
    // first, start row included (depth 0).
    let out = {
        hk_engine_find_simple_paths(
            eng,
            col.as_ptr(),
            id.as_ptr(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            10,
            10,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    let v = to_json(out);
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 3);
    assert_eq!(arr[0]["id"], "a");
    assert_eq!(arr[0]["depth"], 0);
    // G1 (#168): frozen v1 rows carry no route keys — simple-pruning
    // semantics (ordering, depth, cost) are untouched.
    for r in arr {
        assert!(!r.as_object().unwrap().contains_key("path"), "{r:?}");
    }
}

#[test]
fn find_simple_paths_weighted_over_ffi() {
    use hakodb::ffi::hk_engine_find_simple_paths;
    let dir = tmp("simplewffi");
    seed_edge_db(&dir);
    let eng = open(&dir);
    let col = CString::new("n").unwrap();
    let id = CString::new("a").unwrap();
    let via = CString::new("rel").unwrap();
    let w = CString::new("w").unwrap();
    // Two routes to c: via b costs 2, direct costs 10 — both are rows.
    let out = {
        hk_engine_find_simple_paths(
            eng,
            col.as_ptr(),
            id.as_ptr(),
            0,
            std::ptr::null(),
            via.as_ptr(),
            w.as_ptr(),
            10,
            10,
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    let v = to_json(out);
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 4);
    assert_eq!(arr[2]["id"], "c");
    assert!((arr[2]["cost"].as_f64().unwrap() - 2.0).abs() < 1e-9);
    assert_eq!(arr[3]["id"], "c");
    assert!((arr[3]["cost"].as_f64().unwrap() - 10.0).abs() < 1e-9);
    assert!(!arr[3].as_object().unwrap().contains_key("path"), "frozen v1: {arr:?}");
}

#[test]
fn graph_analysis_ops_over_ffi() {
    use hakodb::ffi::hk_engine_graph_analysis;
    let dir = tmp("ganalysis");
    seed(&dir); // pts: a -> b -> c
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let call = |op: &str| {
        let opc = CString::new(op).unwrap();
        let out = {
            hk_engine_graph_analysis(
                eng,
                col.as_ptr(),
                opc.as_ptr(),
                0,
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
                std::ptr::null(),
            )
        };
        assert!(!out.is_null(), "op {op} returned null: {}", last_err());
        to_json(out)
    };
    // degrees: total-degree desc, node asc — b wins (1+1), then a/c tie.
    let deg = call("degrees");
    let arr = deg.as_array().expect("degrees array");
    assert_eq!(arr[0]["node"], "pts/b");
    assert_eq!(
        arr[0]["in"].as_i64().unwrap() + arr[0]["out"].as_i64().unwrap(),
        2
    );
    assert_eq!(arr[1]["node"], "pts/a");
    assert_eq!(arr[2]["node"], "pts/c");
    assert_eq!(arr.len(), 3);
    // topo: chain order; cycle: none; components: one.
    let topo = call("topo");
    assert_eq!(
        topo.as_array().expect("topo array"),
        &vec![
            serde_json::json!("pts/a"),
            serde_json::json!("pts/b"),
            serde_json::json!("pts/c")
        ]
    );
    let cyc = call("cycle");
    assert!(cyc.is_null(), "acyclic fixture: {cyc:?}");
    let comps = call("components");
    assert_eq!(
        comps.as_array().expect("comps array"),
        &vec![serde_json::json!(["pts/a", "pts/b", "pts/c"])]
    );
    // Unknown op is a loud error, never a silent empty.
    let opc = CString::new("mystery").unwrap();
    let bad = {
        hk_engine_graph_analysis(
            eng,
            col.as_ptr(),
            opc.as_ptr(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    assert!(bad.is_null(), "unknown op must fail");
    assert!(last_err().contains("unknown graph analysis op"), "got: {}", last_err());
}

#[test]
fn export_graph_over_ffi() {
    use hakodb::ffi::hk_engine_export_graph;
    let dir = tmp("gexport");
    seed(&dir);
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let out = {
        hk_engine_export_graph(
            eng,
            col.as_ptr(),
            0,
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
            std::ptr::null(),
        )
    };
    let v = to_json(out);
    assert_eq!(v["nodes"].as_array().expect("nodes").len(), 3);
    assert!(v["nodes"][0]["doc"].is_object(), "docs ride inline");
    assert_eq!(
        v["edges"],
        serde_json::json!([
            { "from": "pts/a", "to": "pts/b" },
            { "from": "pts/b", "to": "pts/c" }
        ])
    );
}


#[test]
fn find_paths_v3_routes_and_projects_via() {
    use hakodb::ffi::hk_engine_find_paths_v3;
    let dir = tmp("v3");
    {
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        db.create_edge_index("rel").expect("edge");
        let edge = |f: &str, t: &str, typ: &str| {
            let mut doc = HakoDoc::default();
            doc.insert("_from", Value::Reference { collection: "n".into(), doc_id: f.into() });
            doc.insert("_to", Value::Reference { collection: "n".into(), doc_id: t.into() });
            doc.insert("w", Value::Float(1.0));
            doc.insert("type", Value::String(typ.into()));
            doc
        };
        db.write_batch(vec![
            BatchMutation::Put { collection: "rel".into(), doc_id: "e1".into(), doc: edge("a", "b", "follows") },
            BatchMutation::Put { collection: "rel".into(), doc_id: "e2".into(), doc: edge("b", "c", "blocks") },
        ])
        .expect("seed");
        for id in ["a", "b", "c"] {
            db.write_batch(vec![BatchMutation::Put {
                collection: "n".into(),
                doc_id: id.to_string(),
                doc: HakoDoc::default(),
            }])
            .expect("seed");
        }
        db.flush().expect("flush");
    }
    let eng = open(&dir);
    let col = CString::new("n").unwrap();
    let id = CString::new("a").unwrap();
    let rel = CString::new("rel").unwrap();
    let tf = CString::new("type").unwrap();
    let follows = CString::new("\"follows\"").unwrap();
    let nul = std::ptr::null();

    // Follows only: c unreachable through the blocks edge.
    let rows = to_json({
        hk_engine_find_paths_v3(
            eng, col.as_ptr(), id.as_ptr(), 0, nul, rel.as_ptr(), 5, 10,
            nul, nul, tf.as_ptr(), follows.as_ptr(), nul, nul,
        )
    });
    let arr = rows.as_array().expect("rows");
    let ids: Vec<&str> = arr.iter().map(|r| r["id"].as_str().unwrap()).collect();
    assert_eq!(ids, vec!["b"], "blocks edge filtered: {rows:?}");
    // G1 (#168): v3 is frozen via-free — rows keep the v0.19.3 shape.
    for r in arr {
        assert!(
            !r.as_object().unwrap().contains_key("via"),
            "frozen v3 must not carry via: {r:?}"
        );
    }

    // No predicates (all-null pair args): both hops serve.
    let rows = to_json({
        hk_engine_find_paths_v3(
            eng, col.as_ptr(), id.as_ptr(), 0, nul, rel.as_ptr(), 5, 10,
            nul, nul, nul, nul, nul, nul,
        )
    });
    let arr = rows.as_array().expect("rows");
    assert_eq!(arr.iter().map(|r| r["id"].as_str().unwrap()).collect::<Vec<_>>(), vec!["b", "c"]);

    // Mismatched filter pair is a loud error.
    let past = {
        hk_engine_find_paths_v3(
            eng, col.as_ptr(), id.as_ptr(), 0, nul, rel.as_ptr(), 5, 10,
            nul, nul, tf.as_ptr(), nul, nul, nul,
        )
    };
    assert!(past.is_null(), "field without json = loud error");
    assert!(last_err().contains("filter_json"));
}

#[test]
fn find_paths_v4_include_via_opt_in() {
    use hakodb::ffi::hk_engine_find_paths_v4;
    let dir = tmp("v4via");
    {
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        db.create_edge_index("rel").expect("edge");
        let edge = |f: &str, t: &str| {
            let mut doc = HakoDoc::default();
            doc.insert("_from", Value::Reference { collection: "n".into(), doc_id: f.into() });
            doc.insert("_to", Value::Reference { collection: "n".into(), doc_id: t.into() });
            doc
        };
        db.write_batch(vec![
            BatchMutation::Put { collection: "rel".into(), doc_id: "e1".into(), doc: edge("a", "b") },
        ])
        .expect("seed");
        for id in ["a", "b"] {
            db.write_batch(vec![BatchMutation::Put {
                collection: "n".into(),
                doc_id: id.to_string(),
                doc: HakoDoc::default(),
            }])
            .expect("seed");
        }
        db.flush().expect("flush");
    }
    let eng = open(&dir);
    let col = CString::new("n").unwrap();
    let id = CString::new("a").unwrap();
    let rel = CString::new("rel").unwrap();
    let nul = std::ptr::null();

    // include_path = 1, include_via = 1: route keys ride together.
    let rows = to_json({
        hk_engine_find_paths_v4(
            eng, col.as_ptr(), id.as_ptr(), 0, nul, rel.as_ptr(), 5, 10, 1, 1,
            nul, nul, nul, nul, nul, nul,
        )
    });
    let arr = rows.as_array().expect("rows");
    assert_eq!(arr[0]["id"], "b");
    assert_eq!(arr[0]["path"], serde_json::json!(["n/a", "n/b"]), "opt-in path: {rows:?}");
    assert_eq!(arr[0]["via"], serde_json::json!(["rel/e1"]), "opt-in via: {rows:?}");

    // Both flags = 0: frozen v0.19.3 shape, byte-for-byte (no route keys).
    let rows = to_json({
        hk_engine_find_paths_v4(
            eng, col.as_ptr(), id.as_ptr(), 0, nul, rel.as_ptr(), 5, 10, 0, 0,
            nul, nul, nul, nul, nul, nul,
        )
    });
    let arr = rows.as_array().expect("rows");
    assert_eq!(arr[0]["id"], "b");
    let obj = arr[0].as_object().unwrap();
    assert!(
        !obj.contains_key("path") && !obj.contains_key("via"),
        "flags off = frozen shape: {rows:?}"
    );
}

#[test]
fn find_simple_paths_v2_match_frozen_and_v3_via() {
    use hakodb::ffi::{hk_engine_find_simple_paths_v2, hk_engine_find_simple_paths_v3};
    let dir = tmp("simplev2");
    seed_edge_db(&dir);
    let eng = open(&dir);
    let col = CString::new("n").unwrap();
    let id = CString::new("a").unwrap();
    let rel = CString::new("rel").unwrap();
    let w = CString::new("w").unwrap();
    let one = CString::new("1.0").unwrap();
    let nul = std::ptr::null();

    // edge_match w == 1.0 excludes the direct a->c shortcut (w = 10).
    let out = {
        hk_engine_find_simple_paths_v2(
            eng, col.as_ptr(), id.as_ptr(), 0,
            nul, rel.as_ptr(), nul, 5, 10,
            nul, nul, nul, nul, w.as_ptr(), one.as_ptr(),
        )
    };
    let rows = to_json(out);
    let arr = rows.as_array().expect("rows");
    let got: Vec<(&str, usize)> = arr
        .iter()
        .map(|r| (r["id"].as_str().unwrap(), r["depth"].as_u64().unwrap() as usize))
        .collect();
    assert_eq!(
        got,
        vec![("a", 0), ("b", 1), ("c", 2)],
        "route rows order by cost; v2 is frozen via-free (G1 #168): {rows:?}"
    );
    for r in arr {
        assert!(!r.as_object().unwrap().contains_key("via"), "frozen v2: {r:?}");
    }

    // simple_v3 include_via = 1: via rides again (opt-in).
    let rows = to_json({
        hk_engine_find_simple_paths_v3(
            eng, col.as_ptr(), id.as_ptr(), 0,
            nul, rel.as_ptr(), nul, 5, 10, 1, 1,
            nul, nul, nul, nul, w.as_ptr(), one.as_ptr(),
        )
    });
    let arr = rows.as_array().expect("rows");
    let got: Vec<(&str, Vec<&str>)> = arr
        .iter()
        .map(|r| {
            (
                r["id"].as_str().unwrap(),
                r["via"].as_array().unwrap().iter().map(|v| v.as_str().unwrap_or("")).collect(),
            )
        })
        .collect();
    assert_eq!(
        got,
        vec![
            ("a", vec![]),
            ("b", vec!["rel/e1"]),
            ("c", vec!["rel/e1", "rel/e2"]),
        ],
        "opt-in via on v3 rows ride the edge docs: {rows:?}"
    );

    // Half a (field, json) pair is a loud error, not a silent no-op.
    let out = {
        hk_engine_find_simple_paths_v2(
            eng, col.as_ptr(), id.as_ptr(), 0,
            nul, rel.as_ptr(), nul, 5, 10,
            nul, nul, w.as_ptr(), nul, nul, nul,
        )
    };
    assert!(out.is_null(), "half pair must error");
    assert!(last_err().contains("filter_json required"), "err: {}", last_err());
}
