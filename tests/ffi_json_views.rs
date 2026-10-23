//! P2 FFI: JSON read paths and borrowed view getters. Pins
//! - `hk_engine_get_json` / `hk_view_to_json` byte parity with the owned
//!   `hk_engine_get` + `hk_doc_to_json` path,
//! - `hk_view_get_str/bytes` returning spans borrowed from the pinned view
//!   (the old code returned pointers into a dropped `Value`),
//! - dotted-path getters, and
//! - `hk_query_execute_json` / `hk_cursor_walk_json` row parity.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::ffi::{
    hk_cursor_walk_json, hk_doc_to_json, hk_engine_await_quiescent, hk_engine_free,
    hk_engine_get, hk_engine_get_json, hk_engine_open, hk_last_error, hk_query_execute,
    hk_query_execute_json, hk_query_free, hk_query_new, hk_query_select_field, hk_string_free,
    hk_view_free, hk_view_get, hk_view_get_bytes, hk_view_get_path_bool,
    hk_view_get_path_bytes, hk_view_get_path_float, hk_view_get_path_int,
    hk_view_get_path_str, hk_view_get_str, hk_view_to_json, HK_Engine, HK_ViewDoc,
};
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};
use std::sync::Arc;

fn tmp(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!(
        "hakodb-ffi-json-{label}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("cstr")
}

fn seed(dir: &Path) {
    std::fs::create_dir_all(dir).unwrap();
    let db = Hako::open(dir, HakoConfig::default()).unwrap();
    let mut d1 = HakoDoc::default();
    d1.insert("short", Value::String("abcé".into()));
    d1.insert("long", Value::String("x".repeat(300)));
    d1.insert("bin", Value::Binary(vec![0, 1, 2, 255]));
    d1.insert(
        "nested",
        Value::Map(vec![(
            Arc::from("inner"),
            Value::Map(vec![
                (Arc::from("name"), Value::String("deep".into())),
                (Arc::from("num"), Value::Int(42)),
                (Arc::from("flt"), Value::Float(1.5)),
                (Arc::from("ok"), Value::Bool(true)),
            ]),
        )]),
    );
    d1._time = 111;
    db.put("docs", "d1", &d1).unwrap();

    let mut d2 = HakoDoc::default();
    d2.insert("short", Value::String("other".into()));
    d2._time = 222;
    db.put("docs", "d2", &d2).unwrap();
    db.flush().unwrap();
}

fn open(dir: &Path) -> *mut HK_Engine {
    let path = cstr(dir.to_str().unwrap());
    let eng = hk_engine_open(path.as_ptr());
    assert!(!eng.is_null(), "open failed: {}", last_error());
    assert!(hk_engine_await_quiescent(eng, 60_000), "never quiescent");
    eng
}

fn last_error() -> String {
    let p = hk_last_error();
    if p.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

fn take_string(p: *mut std::ffi::c_char) -> String {
    assert!(!p.is_null(), "null result: {}", last_error());
    let s = unsafe { CStr::from_ptr(p) }.to_str().unwrap().to_string();
    hk_string_free(p);
    s
}

fn view_str(view: *const HK_ViewDoc, path: &str) -> Option<String> {
    let key = cstr(path);
    let mut len = 0usize;
    let p = hk_view_get_str(view, key.as_ptr(), &mut len);
    if p.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(p as *const u8, len) };
    Some(std::str::from_utf8(bytes).unwrap().to_string())
}

fn view_path_str(view: *const HK_ViewDoc, path: &str) -> Option<String> {
    let key = cstr(path);
    let mut len = 0usize;
    let p = hk_view_get_path_str(view, key.as_ptr(), &mut len);
    if p.is_null() {
        return None;
    }
    let bytes = unsafe { std::slice::from_raw_parts(p as *const u8, len) };
    Some(std::str::from_utf8(bytes).unwrap().to_string())
}

#[test]
fn json_point_reads_match_owned_bytes_and_view_getters_borrow() {
    let dir = tmp("point");
    seed(&dir);
    let eng = open(&dir);
    let col = cstr("docs");
    let id = cstr("d1");

    let owned = {
        let doc = hk_engine_get(eng, col.as_ptr(), id.as_ptr());
        assert!(!doc.is_null(), "get failed: {}", last_error());
        take_string(hk_doc_to_json(doc))
    };
    let fast = take_string(hk_engine_get_json(eng, col.as_ptr(), id.as_ptr()));
    assert_eq!(fast, owned, "get_json must match owned get + to_json");

    let view = hk_view_get(eng, col.as_ptr(), id.as_ptr());
    assert!(!view.is_null(), "view failed: {}", last_error());
    let view_json = take_string(hk_view_to_json(view));
    assert_eq!(view_json, owned, "view JSON must match owned bytes");

    // Borrowed point getters: read after the call returns. The old code
    // returned a pointer into a Value dropped inside the getter.
    assert_eq!(view_str(view, "short").as_deref(), Some("abcé"));
    assert_eq!(view_str(view, "long").as_deref(), Some("x".repeat(300).as_str()));
    assert_eq!(view_str(view, "nested"), None, "non-String stays null");

    let key = cstr("bin");
    let mut len = 0usize;
    let p = hk_view_get_bytes(view, key.as_ptr(), &mut len);
    assert!(!p.is_null());
    assert_eq!(unsafe { std::slice::from_raw_parts(p, len) }, &[0, 1, 2, 255]);

    // Dotted paths: string/int/float/bool, plus misses.
    assert_eq!(
        view_path_str(view, "nested.inner.name").as_deref(),
        Some("deep")
    );
    let path = cstr("nested.inner.num");
    let mut i = 0i64;
    assert!(hk_view_get_path_int(view, path.as_ptr(), &mut i));
    assert_eq!(i, 42);
    let path = cstr("nested.inner.flt");
    let mut f = 0f64;
    assert!(hk_view_get_path_float(view, path.as_ptr(), &mut f));
    assert_eq!(f, 1.5);
    let path = cstr("nested.inner.ok");
    assert_eq!(hk_view_get_path_bool(view, path.as_ptr()), 1);
    assert_eq!(view_path_str(view, "nested.inner.missing"), None);

    let path = cstr("bin");
    let p = hk_view_get_path_bytes(view, path.as_ptr(), &mut len);
    assert!(!p.is_null());
    assert_eq!(unsafe { std::slice::from_raw_parts(p, len) }, &[0, 1, 2, 255]);
    let path = cstr("nested.inner.name");
    assert!(
        hk_view_get_path_bytes(view, path.as_ptr(), &mut len).is_null(),
        "non-Binary path"
    );

    hk_view_free(view);
    hk_engine_free(eng);
    std::fs::remove_dir_all(&dir).ok();
}

#[derive(Default)]
struct Walk {
    rows: usize,
    stop_after: usize,
}

unsafe extern "C" fn walk_json_cb(
    _id: *const std::ffi::c_char,
    _id_len: usize,
    json: *const u8,
    json_len: usize,
    userdata: *mut std::ffi::c_void,
) -> bool {
    let st = &mut *(userdata as *mut Walk);
    let bytes = std::slice::from_raw_parts(json, json_len);
    let v: serde_json::Value = serde_json::from_slice(bytes).expect("row is JSON");
    assert!(v.is_object(), "row is a JSON object");
    st.rows += 1;
    st.stop_after == 0 || st.rows < st.stop_after
}

#[test]
fn query_json_matches_execute_and_walk_json_visits_rows() {
    let dir = tmp("query");
    seed(&dir);
    let eng = open(&dir);
    let col = cstr("docs");

    let q = hk_query_new(col.as_ptr());
    assert!(!q.is_null());
    let owned = take_string(hk_query_execute(eng, q));
    let fast = take_string(hk_query_execute_json(eng, q));
    assert_eq!(fast, owned, "query_json must match query_execute bytes");

    let mut st = Walk::default();
    let n = hk_cursor_walk_json(
        eng,
        q,
        Some(walk_json_cb),
        &mut st as *mut Walk as *mut std::ffi::c_void,
    );
    assert_eq!(n, 2);
    assert_eq!(st.rows, 2);

    // Early stop and null callback.
    let mut st = Walk {
        stop_after: 1,
        ..Default::default()
    };
    let n = hk_cursor_walk_json(
        eng,
        q,
        Some(walk_json_cb),
        &mut st as *mut Walk as *mut std::ffi::c_void,
    );
    assert_eq!(n, 1);
    assert_eq!(st.rows, 1);
    let n = hk_cursor_walk_json(eng, q, None, std::ptr::null_mut());
    assert_eq!(n, -1);

    // Projection queries keep hk_query_execute's shape (the fast path
    // delegates when a projection is set).
    let field = cstr("short");
    assert_eq!(hk_query_select_field(q, field.as_ptr()), 0);
    let owned = take_string(hk_query_execute(eng, q));
    let fast = take_string(hk_query_execute_json(eng, q));
    assert_eq!(fast, owned, "projection bytes must match");

    hk_query_free(q);
    hk_engine_free(eng);
    std::fs::remove_dir_all(&dir).ok();
}
