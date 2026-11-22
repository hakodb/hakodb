//! FFI cancellation handle and projected-query cancellation (issue #240).

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::ffi::{
    hk_cancel, hk_cancel_free, hk_cancel_new, hk_doc_free, hk_doc_insert_str, hk_doc_new,
    hk_engine_free, hk_engine_insert, hk_engine_open, hk_query_execute, hk_query_execute_with_cancel, hk_query_free, hk_query_reset, hk_query_new,
    hk_string_free, HK_Engine,
};
use hakodb::query::query::Query;
use std::ffi::{CStr, CString};

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!("hako-cancel-{label}-{}", std::process::id()))
}

#[test]
fn ffi_cancel_token_aborts_query() {
    let path = tmp("ffi");
    let _ = std::fs::remove_dir_all(&path);
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();
    let engine = hk_engine_open(cpath.as_ptr());
    assert!(!engine.is_null(), "engine open");
    let engine: &mut HK_Engine = unsafe { &mut *engine };

    let doc = hk_doc_new();
    {
        let key = CString::new("name").unwrap();
        let value = CString::new("alice").unwrap();
        hk_doc_insert_str(doc, key.as_ptr(), value.as_ptr());
    }
    let id = CString::new("u1").unwrap();
    hk_engine_insert(engine, c"users".as_ptr(), id.as_ptr(), doc);
    hk_doc_free(doc);

    // Live token: query succeeds and emits JSON.
    let token = hk_cancel_new();
    assert!(!token.is_null(), "cancel handle");
    let query = hk_query_new(c"users".as_ptr());
    let json = hk_query_execute_with_cancel(engine, query, token);
    assert!(!json.is_null(), "query with live cancel token must succeed");
    let text = unsafe { CStr::from_ptr(json) }.to_string_lossy().into_owned();
    assert!(text.contains("alice"), "{text}");
    hk_string_free(json);
    hk_query_free(query);

    // Cancelled token: query aborts cooperatively.
    hk_cancel(token);
    let query = hk_query_new(c"users".as_ptr());
    let json = hk_query_execute_with_cancel(engine, query, token);
    assert!(json.is_null(), "cancelled query must return null");
    hk_query_free(query);
    hk_cancel_free(token);
    hk_engine_free(engine);
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn projected_query_with_cancel_token() {
    let path = tmp("rust");
    let _ = std::fs::remove_dir_all(&path);
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    let db = Hako::open(&path, cfg).expect("open");
    let mut doc = HakoDoc::default();
    doc.insert("name", Value::String("alice".into()));
    db.write_batch(vec![BatchMutation::Put {
        collection: "users".into(),
        doc_id: "u1".into(),
        doc,
    }])
    .expect("seed");

    let token = hakodb::CancelToken::new();
    let rows = db
        .query_projected_with_cancel(Query::new("users"), &["name".into()], Some(&token))
        .expect("live token");
    assert_eq!(rows.len(), 1);

    token.cancel();
    let err = db
        .query_projected_with_cancel(Query::new("users"), &["name".into()], Some(&token))
        .expect_err("cancelled token");
    assert!(matches!(err, hakodb::error::HakoError::Cancelled), "{err:?}");
    let _ = std::fs::remove_dir_all(&path);
}

#[test]
fn query_handle_reset_reuses_one_allocation() {
    let path = tmp("reset");
    let _ = std::fs::remove_dir_all(&path);
    let cpath = CString::new(path.to_string_lossy().as_bytes()).unwrap();
    let engine = hk_engine_open(cpath.as_ptr());
    assert!(!engine.is_null());
    let engine: &mut HK_Engine = unsafe { &mut *engine };

    let doc = hk_doc_new();
    let key = CString::new("name").unwrap();
    let value = CString::new("alice").unwrap();
    hk_doc_insert_str(doc, key.as_ptr(), value.as_ptr());
    let id = CString::new("u1").unwrap();
    hk_engine_insert(engine, c"users".as_ptr(), id.as_ptr(), doc);
    hk_doc_free(doc);

    let query = hk_query_new(c"users".as_ptr());
    assert!(!query.is_null());
    for _ in 0..3 {
        assert_eq!(hk_query_reset(query, c"users".as_ptr()), 0, "reset must succeed");
        let json = hk_query_execute(engine, query);
        assert!(!json.is_null(), "reset query must execute");
        let text = unsafe { CStr::from_ptr(json) }.to_string_lossy().into_owned();
        assert!(text.contains("alice"), "{text}");
        hk_string_free(json);
    }
    hk_query_free(query);
    hk_engine_free(engine);
    let _ = std::fs::remove_dir_all(&path);
}