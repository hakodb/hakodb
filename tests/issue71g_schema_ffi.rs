//! Issue #71 F8: per-collection schema validation over the C ABI.
//!
//! Rust-level semantics are pinned in `tests/issue71d_explain_schema.rs`;
//! these cases prove the FFI carries set/get/clear/list, the JSON shape, the
//! typed error kind (4 = ConstraintMismatch), and persistence across reopen.

use hakodb::ffi::{
    hk_doc_free, hk_doc_insert_int, hk_doc_insert_str, hk_doc_new, hk_engine_clear_schema,
    hk_engine_free, hk_engine_get_schema, hk_engine_insert, hk_engine_list_schemas,
    hk_engine_open, hk_engine_set_schema, hk_error_last_kind, hk_last_error, hk_string_free,
    HK_Engine,
};
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};

const SCHEMA: &str = r#"{"fields":[
    {"name":"status","kind":"string","required":true},
    {"name":"qty","kind":"int","default":1}
]}"#;

fn tmp(label: &str) -> PathBuf {
    let dir = std::env::temp_dir().join(format!("issue71g-{label}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("cstr")
}

fn open(dir: &Path) -> *mut HK_Engine {
    let path = cstr(dir.to_str().unwrap());
    let eng = hk_engine_open(path.as_ptr());
    assert!(!eng.is_null(), "open failed: {}", last_error());
    eng
}

fn last_error() -> String {
    let p = hk_last_error();
    if p.is_null() {
        return String::new();
    }
    unsafe { CStr::from_ptr(p) }.to_string_lossy().into_owned()
}

fn take_json(p: *mut std::ffi::c_char) -> serde_json::Value {
    assert!(!p.is_null(), "null result: {}", last_error());
    let s = unsafe { CStr::from_ptr(p) }.to_str().unwrap().to_string();
    hk_string_free(p);
    serde_json::from_str(&s).expect("valid JSON")
}

fn set(eng: *mut HK_Engine, collection: &str, json: &str) -> i32 {
    hk_engine_set_schema(eng, cstr(collection).as_ptr(), cstr(json).as_ptr())
}

fn insert(eng: *mut HK_Engine, id: &str, status: Option<&str>) -> i32 {
    let doc = hk_doc_new();
    if let Some(s) = status {
        hk_doc_insert_str(doc, cstr("status").as_ptr(), cstr(s).as_ptr());
    }
    let rc = hk_engine_insert(eng, cstr("orders").as_ptr(), cstr(id).as_ptr(), doc);
    hk_doc_free(doc);
    rc
}

fn insert_int_status(eng: *mut HK_Engine, id: &str, status: i64) -> i32 {
    let doc = hk_doc_new();
    hk_doc_insert_int(doc, cstr("status").as_ptr(), status);
    let rc = hk_engine_insert(eng, cstr("orders").as_ptr(), cstr(id).as_ptr(), doc);
    hk_doc_free(doc);
    rc
}

#[test]
fn schema_set_get_list_and_write_gate() {
    let dir = tmp("gate");
    let eng = open(&dir);
    assert_eq!(set(eng, "orders", SCHEMA), 0, "{}", last_error());
    assert_eq!(hk_error_last_kind(), 0);

    let got = take_json(hk_engine_get_schema(eng, cstr("orders").as_ptr()));
    assert_eq!(got["fields"][0]["name"], "status");
    assert_eq!(got["fields"][0]["kind"], "string");
    assert_eq!(got["fields"][0]["required"], true);
    assert_eq!(got["fields"][1]["kind"], "int");
    assert_eq!(got["fields"][1]["default"], 1);

    let all = take_json(hk_engine_list_schemas(eng));
    assert_eq!(all["orders"]["fields"].as_array().unwrap().len(), 2);

    assert_eq!(insert(eng, "ok", Some("paid")), 0, "{}", last_error());

    assert_eq!(insert(eng, "missing", None), -1);
    assert_eq!(hk_error_last_kind(), 4, "missing required field");
    assert!(last_error().contains("schema violation"), "{}", last_error());

    assert_eq!(insert_int_status(eng, "wrong", 3), -1);
    assert_eq!(hk_error_last_kind(), 4, "wrong type");

    assert_eq!(hk_engine_clear_schema(eng, cstr("orders").as_ptr()), 0);
    assert_eq!(insert(eng, "after-clear", None), 0, "{}", last_error());

    let absent = hk_engine_get_schema(eng, cstr("orders").as_ptr());
    assert!(absent.is_null());
    assert!(last_error().is_empty(), "absence is not an error");

    hk_engine_free(eng);
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn schema_persists_across_reopen() {
    let dir = tmp("persist");
    {
        let eng = open(&dir);
        assert_eq!(set(eng, "orders", SCHEMA), 0, "{}", last_error());
        assert_eq!(insert(eng, "ok", Some("paid")), 0, "{}", last_error());
        hk_engine_free(eng);
    }
    {
        let eng = open(&dir);
        let got = take_json(hk_engine_get_schema(eng, cstr("orders").as_ptr()));
        assert_eq!(got["fields"][0]["kind"], "string");
        assert_eq!(insert(eng, "missing", None), -1);
        assert_eq!(hk_error_last_kind(), 4);
        hk_engine_free(eng);
    }
    let _ = std::fs::remove_dir_all(dir);
}

#[test]
fn schema_declaration_errors_are_loud() {
    let dir = tmp("bad-decl");
    let eng = open(&dir);

    assert_eq!(
        set(eng, "orders", r#"{"fields":[{"name":"a.b","kind":"string"}]}"#),
        -1
    );
    assert!(last_error().contains("nested path"), "{}", last_error());

    assert_eq!(
        set(
            eng,
            "orders",
            r#"{"fields":[{"name":"n","kind":"int","default":"x"}]}"#
        ),
        -1
    );
    assert!(last_error().contains("default"), "{}", last_error());

    assert_eq!(
        set(eng, "orders", r#"{"fields":[{"name":"n","kind":"decimal"}]}"#),
        -1
    );
    assert!(last_error().contains("invalid schema JSON"), "{}", last_error());

    // `{}` means "no declared fields" -> clears, matching the engine API.
    assert_eq!(set(eng, "orders", "{}"), 0, "{}", last_error());
    assert!(hk_engine_get_schema(eng, cstr("orders").as_ptr()).is_null());

    hk_engine_free(eng);
    let _ = std::fs::remove_dir_all(dir);
}
