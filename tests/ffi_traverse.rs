//! C-API round trip for graph traversal: `hk_engine_create_traverse_index`
//! + `hk_engine_find_paths` return BFS-ordered
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
    let eng = unsafe { hk_engine_open(path.as_ptr()) };
    assert!(!eng.is_null(), "hk_engine_open failed");
    assert!(unsafe { hk_engine_await_quiescent(eng, 60_000) }, "never quiescent");
    eng
}

fn to_json(ptr: *mut std::ffi::c_char) -> serde_json::Value {
    assert!(!ptr.is_null(), "null return");
    let s = unsafe { CStr::from_ptr(ptr).to_string_lossy().into_owned() };
    unsafe { hk_string_free(ptr) };
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
    let out = unsafe {
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
}

#[test]
fn find_paths_missing_anchor_returns_empty() {
    let dir = tmp("missing");
    seed(&dir);
    let eng = open(&dir);
    let col = CString::new("pts").unwrap();
    let id = CString::new("ghost").unwrap();
    let out = unsafe {
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
    let out = unsafe {
        hk_engine_find_paths(eng, col.as_ptr(), id.as_ptr(), 1, std::ptr::null(), 10, 10)
    };
    assert!(out.is_null(), "untracked incoming must return null");
    assert!(last_err().contains("create_traverse_index"), "got: {}", last_err());
    // create_traverse_index via FFI works (idempotent: pts already has it).
    let col2 = CString::new("pts").unwrap();
    let rc = unsafe { hk_engine_create_traverse_index(eng, col2.as_ptr()) };
    assert_eq!(rc, 0);
}
