//! C-API round trip for vector search: `hk_engine_find_near` returns
//! nearest-first `[{"id","score","doc"}]` with exact scores, hard-errors
//! on dim mismatch, and fail-closes to `"[]"` on unindexed fields.
//! Setup goes through the Rust API (same shapes as `vector_knn.rs`);
//! only the new FFI surface is exercised through raw pointers.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::ffi::{
    hk_engine_await_quiescent, hk_engine_find_near, hk_engine_open, hk_last_error,
    hk_string_free,
};
use hakodb::index::vector::{distance, encode_f32s, Metric};
use std::ffi::{CStr, CString};

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-ffi-vec-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn vdoc(emb: &[f32]) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("emb", Value::Binary(encode_f32s(emb)));
    doc
}

fn seed(dir: &std::path::Path) {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(dir, cfg).expect("open");
    db.create_vector_index("pts", "emb", 2, Metric::Cosine)
        .expect("create");
    let pts: &[(&str, [f32; 2])] = &[
        ("a", [1.0, 0.0]),
        ("b", [0.0, 1.0]),
        ("c", [1.0, 1.0]),
        ("d", [10.0, 0.0]),
    ];
    db.write_batch(
        pts.iter()
            .map(|(id, p)| BatchMutation::Put {
                collection: "pts".into(),
                doc_id: id.to_string(),
                doc: vdoc(p),
            })
            .collect(),
    )
    .expect("seed");
    db.flush().expect("flush");
    // Drop closes; reopen through the C API below (recovery reloads defs).
}

/// Call `hk_engine_find_near` on a freshly C-opened handle; returns the
/// parsed JSON array. The handle leaks (process-lifetime test).
/// Recovery replays in background, so await quiescence first — same rule
/// as `vector_knn.rs::wait_ready` (before `indexes_ready`, Near
/// fail-closes to zero rows).
fn call_near(dir: &std::path::Path, field: &str, q: &[f32], k: usize) -> *mut std::ffi::c_char {
    let path = CString::new(dir.to_str().expect("path")).expect("cstr");
    let eng = { hk_engine_open(path.as_ptr()) };
    assert!(!eng.is_null(), "hk_engine_open failed");
    assert!(
        { hk_engine_await_quiescent(eng, 60_000) },
        "engine never quiescent"
    );
    let col = CString::new("pts").unwrap();
    let fld = CString::new(field).unwrap();
    hk_engine_find_near(eng, col.as_ptr(), fld.as_ptr(), q.as_ptr(), q.len(), k)
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
fn find_near_returns_ordered_hits_with_exact_scores() {
    let dir = tmp("ordered");
    seed(&dir);
    // q=(1,0.5): cosine order c,a,... — c=(1,1) is closer to q than
    // a=(1,0) (dist ~0.051 vs ~0.106; same verdict as
    // vector_knn::def_survives_restart asserting ["c","a"]).
    let q = [1.0f32, 0.5];
    let v = to_json(call_near(&dir, "emb", &q, 2));
    let arr = v.as_array().expect("array");
    assert_eq!(arr.len(), 2);
    assert_eq!(arr[0]["id"], "c");
    assert_eq!(arr[1]["id"], "a");
    for (i, id) in ["c", "a"].iter().enumerate() {
        let stored = match *id {
            "c" => [1.0, 1.0],
            _ => [1.0, 0.0],
        };
        let want = distance(Metric::Cosine, &q, &stored);
        let got = arr[i]["score"].as_f64().expect("score");
        assert!((got - want).abs() < 1e-9, "{id}: {got} vs {want}");
        assert!(arr[i]["doc"].is_object(), "doc embedded");
    }
}

#[test]
fn find_near_dim_mismatch_is_hard_error() {
    let dir = tmp("mismatch");
    seed(&dir);
    let q = [1.0f32, 0.5, 0.0];
    let ptr = call_near(&dir, "emb", &q, 2);
    assert!(ptr.is_null(), "mismatch must return null");
    assert!(last_err().contains("dim mismatch"), "got: {}", last_err());
}

#[test]
fn find_near_unindexed_field_returns_empty() {
    let dir = tmp("unindexed");
    seed(&dir);
    let q = [1.0f32, 0.5];
    let v = to_json(call_near(&dir, "nope", &q, 2));
    assert_eq!(v, serde_json::json!([]));
}
