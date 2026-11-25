//! #217 wave FFI mirrors: `hk_engine_open_exclusive` (loud refusal +
//! release-on-drop), `hk_engine_ensure_indexes` (idempotent boot schema),
//! `hk_engine_hybrid_search_filtered` / `hk_engine_find_near_scored_filtered`
//! (filters pre-truncation), and `hk_engine_subgraph_json_projected`
//! (node-doc field projection). Engine side is covered by
//! `tests/{open_exclusive,ensure_indexes,hybrid_search,traverse}.rs`; these
//! tests exercise the C boundary only â€” null/last_error discipline, JSON
//! grammars, and handover.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::ffi::{
    hk_engine_await_quiescent, hk_engine_ensure_indexes, hk_engine_find_near_scored_filtered,
    hk_engine_free, hk_engine_hybrid_search_filtered, hk_engine_open_exclusive,
    hk_engine_subgraph_json_projected, hk_last_error, hk_string_free,
};
use hakodb::index::vector::{encode_f32s, Metric};
use std::ffi::{CStr, CString};

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-ffi-w217-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn doc(name: &str, emb: [f32; 2]) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("name", Value::String(name.into()));
    d.insert("emb", Value::Binary(encode_f32s(&emb)));
    d
}

fn seed(dir: &std::path::Path) {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open_exclusive(dir, cfg).expect("open_exclusive");
    // d1 -ref-> d2: the traversal lane follows inline Reference fields
    // (no edge collection needed at the FFI boundary).
    let mut d1 = doc("alpha beta", [1.0, 0.0]);
    d1.insert(
        "link",
        Value::Reference {
            collection: "items".into(),
            doc_id: "d2".into(),
        },
    );
    db.put("items", "d1", &d1).expect("d1");
    db.put("items", "d2", &doc("alpha gamma", [0.0, 1.0]))
        .expect("d2");
    db.put("items", "d3", &doc("zeta", [0.9, 0.1]))
        .expect("d3");
    let schema = hakodb::engine::IndexSchema {
        fts: vec![("items".into(), "name".into())],
        vector: vec![("items".into(), "emb".into(), 2, Metric::Cosine)],
        tracked: vec!["items".into()],
        ..Default::default()
    };
    db.ensure_indexes(&schema).expect("ensure_indexes");
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
}

fn c(s: &str) -> CString {
    CString::new(s).expect("cstr")
}

fn last_err() -> String {
    let p = hk_last_error();
    if p.is_null() {
        String::new()
    } else {
        unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
    }
}

fn json_out(ptr: *mut std::ffi::c_char) -> serde_json::Value {
    assert!(!ptr.is_null(), "null return (error: {})", last_err());
    let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    hk_string_free(ptr);
    serde_json::from_str(&s).expect("json")
}

#[test]
fn open_exclusive_mirror_refuses_second_holder() {
    let dir = tmp("xopen");
    let path = c(dir.to_str().expect("path"));
    let a = hk_engine_open_exclusive(path.as_ptr());
    assert!(!a.is_null(), "first exclusive open failed: {}", last_err());
    let b = hk_engine_open_exclusive(path.as_ptr());
    assert!(b.is_null(), "second holder must fail");
    assert!(
        last_err().contains("already open"),
        "error must name the conflict: {}",
        last_err()
    );
    // Drop releases the lock: the next open wins.
    hk_engine_free(a);
    let c3 = hk_engine_open_exclusive(path.as_ptr());
    assert!(!c3.is_null(), "lock must release on drop: {}", last_err());
    hk_engine_free(c3);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn ensure_indexes_mirror_parses_and_is_idempotent() {
    let dir = tmp("ensure");
    seed(&dir);
    let path = c(dir.to_str().expect("path"));
    let eng = hk_engine_open_exclusive(path.as_ptr());
    assert!(!eng.is_null(), "{}", last_err());
    // Same schema twice: both succeed (declared-or-existing), the second
    // must NOT wipe the populated FTS index (immediate hybrid works).
    let schema = c(
        r#"{"fts":[["items","name"]],
            "vector":[["items","emb",2,0]],
            "tracked":["items","edges"]}"#,
    );
    assert_eq!(hk_engine_ensure_indexes(eng, schema.as_ptr()), 0);
    assert_eq!(hk_engine_ensure_indexes(eng, schema.as_ptr()), 0);
    // Bad metric id errors loudly.
    let bad = c(r#"{"vector":[["items","emb",2,7]]}"#);
    assert_eq!(hk_engine_ensure_indexes(eng, bad.as_ptr()), -1);
    assert!(last_err().contains("metric"), "{}", last_err());
    hk_engine_free(eng);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn hybrid_filtered_mirror_applies_filters_before_ranking() {
    let dir = tmp("hybf");
    seed(&dir);
    let path = c(dir.to_str().expect("path"));
    let eng = hk_engine_open_exclusive(path.as_ptr());
    assert!(!eng.is_null(), "{}", last_err());
    // Recovery replays in background — Near fail-closes to zero rows
    // before the indexes are ready (same rule as ffi_vector.rs).
    assert!(hk_engine_await_quiescent(eng, 60_000));
    let col = c("items");
    let tf = c("name");
    let tq = c("alpha");
    let vf = c("emb");
    let q: [f32; 2] = [1.0, 0.0];
    // name CONTAINS "beta" admits only d1.
    let filters = c(r#"[{"field":"name","op":"contains","value":"beta"}]"#);
    let rows = json_out(hk_engine_hybrid_search_filtered(
        eng,
        col.as_ptr(),
        tf.as_ptr(),
        tq.as_ptr(),
        vf.as_ptr(),
        q.as_ptr(),
        q.len(),
        3,
        60,
        filters.as_ptr(),
    ));
    assert_eq!(rows.as_array().map(Vec::len), Some(1), "{rows}");
    assert_eq!(rows[0]["id"], "d1");
    // null filters = unfiltered hybrid: text side matches d1+d2 ("alpha"),
    // vector side adds d3 (near [1,0]) — fused union, 3 rows.
    let rows = json_out(hk_engine_hybrid_search_filtered(
        eng,
        col.as_ptr(),
        tf.as_ptr(),
        tq.as_ptr(),
        vf.as_ptr(),
        q.as_ptr(),
        q.len(),
        3,
        60,
        std::ptr::null(),
    ));
    assert_eq!(rows.as_array().map(Vec::len), Some(3), "{rows}");
    // Malformed filters_json errors (null return + last_error).
    let bad = c(r#"[{"field":"name"}]"#);
    let r = hk_engine_hybrid_search_filtered(
        eng,
        col.as_ptr(),
        tf.as_ptr(),
        tq.as_ptr(),
        vf.as_ptr(),
        q.as_ptr(),
        q.len(),
        3,
        60,
        bad.as_ptr(),
    );
    assert!(r.is_null(), "malformed filters must fail");
    assert!(last_err().contains("op"), "{}", last_err());
    hk_engine_free(eng);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn find_near_scored_filtered_mirror_scores_and_filters() {
    let dir = tmp("nearsf");
    seed(&dir);
    let path = c(dir.to_str().expect("path"));
    let eng = hk_engine_open_exclusive(path.as_ptr());
    assert!(!eng.is_null(), "{}", last_err());
    assert!(hk_engine_await_quiescent(eng, 60_000));
    let col = c("items");
    let vf = c("emb");
    let q: [f32; 2] = [1.0, 0.0];
    let filters = c(r#"[{"field":"name","op":"contains","value":"beta"}]"#);
    let rows = json_out(hk_engine_find_near_scored_filtered(
        eng,
        col.as_ptr(),
        vf.as_ptr(),
        q.as_ptr(),
        q.len(),
        3,
        f64::NAN,
        filters.as_ptr(),
    ));
    assert_eq!(rows.as_array().map(Vec::len), Some(1), "{rows}");
    assert_eq!(rows[0]["id"], "d1");
    // Distance cap: 0.0/NaN = no cap; a tiny cap on the same query kills
    // the far hit.
    let rows = json_out(hk_engine_find_near_scored_filtered(
        eng,
        col.as_ptr(),
        vf.as_ptr(),
        q.as_ptr(),
        q.len(),
        3,
        0.0,
        std::ptr::null(),
    ));
    let n: usize = rows.as_array().map(Vec::len).unwrap_or(0);
    assert!(n >= 1, "{rows}");
    hk_engine_free(eng);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn subgraph_projected_mirror_scopes_fields() {
    let dir = tmp("sgproj");
    seed(&dir);
    let path = c(dir.to_str().expect("path"));
    let eng = hk_engine_open_exclusive(path.as_ptr());
    assert!(!eng.is_null(), "{}", last_err());
    let col = c("items");
    let seed_id = c("d1");
    let fields = c(r#"["name"]"#);
    let out = json_out(hk_engine_subgraph_json_projected(
        eng,
        col.as_ptr(),
        seed_id.as_ptr(),
        1,
        2, // outbound
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        10,
        fields.as_ptr(),
    ));
    // Node docs keep ONLY "name" (no "emb", no "_time" beyond the engine's
    // own stamping rules); the edge doc rides in nodes as before.
    let nodes = out["nodes"].as_array().expect("nodes");
    assert!(nodes.iter().any(|n| n["id"] == "d2"), "{out}");
    for n in nodes {
        if n["id"] == "d2" {
            assert!(n["doc"].get("name").is_some(), "keep-listed field: {n}");
            assert!(n["doc"].get("emb").is_none(), "projection must drop emb: {n}");
        }
    }
    // null fields = legacy full output (emb present).
    let out = json_out(hk_engine_subgraph_json_projected(
        eng,
        col.as_ptr(),
        seed_id.as_ptr(),
        1,
        2,
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        std::ptr::null(),
        10,
        std::ptr::null(),
    ));
    let nodes = out["nodes"].as_array().expect("nodes");
    let d2 = nodes.iter().find(|n| n["id"] == "d2").expect("d2");
    assert!(d2["doc"].get("emb").is_some(), "legacy keeps emb: {out}");
    hk_engine_free(eng);
    let _ = std::fs::remove_dir_all(&dir);
}
