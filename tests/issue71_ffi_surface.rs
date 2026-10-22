//! Issue #71 FFI surface: F3 elemMatch/all_eq, F4 pipeline, F5 versioned get
//! + transaction patch/get_many + retry classification, F7 explain JSON.
//! Rust-level behavior is pinned in the issue71c/71d tests; these only prove
//! the C ABI carries it faithfully.

use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako, RetryPolicy};
use hakodb::ffi::{
    hk_doc_free, hk_doc_insert_int, hk_doc_new, hk_doc_to_json, hk_engine_await_quiescent,
    hk_engine_free,
    hk_engine_get_versioned, hk_engine_insert, hk_engine_open, hk_error_is_retryable,
    hk_error_last_kind, hk_last_error, hk_pipeline_execute, hk_pipeline_execute_to_handles,
    hk_pipeline_free, hk_pipeline_group, hk_pipeline_limit, hk_pipeline_match, hk_pipeline_new,
    hk_pipeline_sort, hk_pipeline_unwind, hk_query_execute, hk_query_explain, hk_query_free,
    hk_query_new, hk_query_where_array_all_eq, hk_query_where_elem_match, hk_result_set_count,
    hk_result_set_free, hk_string_free, hk_transaction_begin, hk_transaction_commit,
    hk_transaction_free, hk_transaction_get, hk_transaction_get_many, hk_transaction_patch, HK_Engine,
};
use std::ffi::{CStr, CString};
use std::path::{Path, PathBuf};

fn tmp(label: &str) -> PathBuf {
    std::env::temp_dir().join(format!("issue71-ffi-{label}-{}", std::process::id()))
}

fn cstr(s: &str) -> CString {
    CString::new(s).expect("cstr")
}

fn items(pairs: &[(&str, i64)]) -> Value {
    Value::Array(
        pairs
            .iter()
            .map(|(sku, qty)| {
                Value::Map(vec![
                    (std::sync::Arc::from("sku"), Value::String((*sku).into())),
                    (std::sync::Arc::from("qty"), Value::Int(*qty)),
                ])
            })
            .collect(),
    )
}

fn order(name: &str, pairs: &[(&str, i64)], tags: &[&str]) -> HakoDoc {
    let mut d = HakoDoc::default();
    d.insert("name", Value::String(name.into()));
    d.insert("items", items(pairs));
    d.insert(
        "tags",
        Value::Array(tags.iter().map(|t| Value::String((*t).into())).collect()),
    );
    d
}

fn seed(dir: &Path) {
    let _ = std::fs::remove_dir_all(dir);
    std::fs::create_dir_all(dir).unwrap();
    let db = Hako::open(dir, HakoConfig::default()).unwrap();
    let batch = vec![
        ("o1", order("o1", &[("A", 2), ("B", 3)], &["x", "x"])),
        ("o2", order("o2", &[("A", 4), ("B", 1)], &["x", "y"])),
        ("o3", order("o3", &[("C", 9)], &[])),
    ]
    .into_iter()
    .map(|(id, doc)| BatchMutation::Put {
        collection: "orders".into(),
        doc_id: id.into(),
        doc,
    })
    .collect();
    db.write_batch(batch).unwrap();
    db.flush().unwrap();
}

fn open(dir: &Path) -> *mut HK_Engine {
    let path = cstr(dir.to_str().unwrap());
    let eng = { hk_engine_open(path.as_ptr()) };
    assert!(!eng.is_null(), "open failed: {}", last_error());
    assert!({ hk_engine_await_quiescent(eng, 60_000) }, "never quiescent");
    eng
}

fn last_error() -> String {
    let p = { hk_last_error() };
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

#[test]
fn elem_match_and_all_eq_through_ffi() {
    let dir = tmp("elem");
    seed(&dir);
    let eng = open(&dir);

    let q = { hk_query_new(cstr("orders").as_ptr()) };
    assert!(!q.is_null());
    let filters = cstr(r#"[{"field":"sku","op":"eq","value":"B"},{"field":"qty","op":"gt","value":2}]"#);
    let rc = { hk_query_where_elem_match(q, cstr("items").as_ptr(), filters.as_ptr()) };
    assert_eq!(rc, 0, "elem_match: {}", last_error());
    let rows = take_json(hk_query_execute(eng, q));
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows}");
    assert_eq!(rows[0]["name"], "o1");

    // Empty sub-filter list is refused instead of matching everything.
    let empty = cstr("[]");
    let rc = { hk_query_where_elem_match(q, cstr("items").as_ptr(), empty.as_ptr()) };
    assert_eq!(rc, -1);
    assert!(!last_error().is_empty());

    let all = { hk_query_new(cstr("orders").as_ptr()) };
    let rc = { hk_query_where_array_all_eq(all, cstr("tags").as_ptr(), cstr("\"x\"").as_ptr()) };
    assert_eq!(rc, 0, "all_eq: {}", last_error());
    let rows = take_json(hk_query_execute(eng, all));
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows}");
    assert_eq!(rows[0]["name"], "o1", "empty tags fail closed, x+y excluded");

    {
        hk_query_free(q);
        hk_query_free(all);
        hk_engine_free(eng);
    }
}

#[test]
fn pipeline_through_ffi_matches_rust_semantics() {
    let dir = tmp("pipe");
    seed(&dir);
    let eng = open(&dir);

    let p = { hk_pipeline_new(cstr("orders").as_ptr()) };
    assert!(!p.is_null(), "{}", last_error());
    assert_eq!({ hk_pipeline_unwind(p, cstr("items").as_ptr()) }, 0);
    let accums = cstr(r#"[{"out":"total","op":"sum","field":"items.qty"},{"out":"orders","op":"count"}]"#);
    assert_eq!(
        { hk_pipeline_group(p, cstr("items.sku").as_ptr(), accums.as_ptr()) },
        0,
        "{}",
        last_error()
    );
    assert_eq!({ hk_pipeline_sort(p, cstr("total").as_ptr(), false) }, 0);
    assert_eq!({ hk_pipeline_limit(p, 2) }, 0);

    let rows = take_json(hk_pipeline_execute(eng, p));
    assert_eq!(rows.as_array().unwrap().len(), 2, "{rows}");
    assert_eq!(rows[0]["sku"], "C");
    assert_eq!(rows[0]["total"], 9);
    assert_eq!(rows[0]["orders"], 1);
    assert_eq!(rows[1]["sku"], "A");
    assert_eq!(rows[1]["total"], 6);
    assert_eq!(rows[1]["orders"], 2);

    let rs = { hk_pipeline_execute_to_handles(eng, p) };
    assert!(!rs.is_null(), "{}", last_error());
    assert_eq!({ hk_result_set_count(rs) }, 2);
    {
        hk_result_set_free(rs);
        hk_pipeline_free(p);
        hk_engine_free(eng);
    }
}

#[test]
fn pipeline_match_stage_accepts_a_query_handle() {
    let dir = tmp("pipeline-match");
    seed(&dir);
    let eng = open(&dir);

    let q = { hk_query_new(cstr("orders").as_ptr()) };
    let rc = { hk_query_where_elem_match(q, cstr("items").as_ptr(), cstr(r#"[{"field":"sku","op":"eq","value":"C"}]"#).as_ptr()) };
    assert_eq!(rc, 0);

    let p = { hk_pipeline_new(cstr("orders").as_ptr()) };
    assert_eq!({ hk_pipeline_match(p, q) }, 0);
    let rows = take_json(hk_pipeline_execute(eng, p));
    assert_eq!(rows.as_array().unwrap().len(), 1, "{rows}");
    assert_eq!(rows[0]["name"], "o3");

    {
        hk_pipeline_free(p);
        hk_query_free(q);
        hk_engine_free(eng);
    }
}

#[test]
fn versioned_get_and_transaction_surface() {
    let dir = tmp("tx");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let eng = open(&dir);

    let doc = { hk_doc_new() };
    hk_doc_insert_int(doc, cstr("n").as_ptr(), 1);
    assert_eq!(
        { hk_engine_insert(eng, cstr("c").as_ptr(), cstr("a").as_ptr(), doc) },
        0,
        "{}",
        last_error()
    );
    hk_doc_free(doc);

    let mut v1 = 0u64;
    let got = { hk_engine_get_versioned(eng, cstr("c").as_ptr(), cstr("a").as_ptr(), &mut v1) };
    assert!(!got.is_null(), "{}", last_error());
    assert!(v1 > 0, "version captured atomically");
    hk_doc_free(got);

    let mut missing_version = 7u64;
    let absent = {
        hk_engine_get_versioned(eng, cstr("c").as_ptr(), cstr("nope").as_ptr(), &mut missing_version)
    };
    assert!(absent.is_null());
    assert_eq!(missing_version, 0, "absent doc zeroes the out version");

    let doc = { hk_doc_new() };
    hk_doc_insert_int(doc, cstr("n").as_ptr(), 2);
    assert_eq!({ hk_engine_insert(eng, cstr("c").as_ptr(), cstr("a").as_ptr(), doc) }, 0);
    hk_doc_free(doc);
    let mut v2 = 0u64;
    let got = { hk_engine_get_versioned(eng, cstr("c").as_ptr(), cstr("a").as_ptr(), &mut v2) };
    assert!(!got.is_null());
    assert!(v2 > v1, "a rewrite bumps the version ({v1} -> {v2})");
    hk_doc_free(got);

    let tx = { hk_transaction_begin(eng) };
    assert!(!tx.is_null());
    let many = take_json({
        hk_transaction_get_many(eng, tx, cstr("c").as_ptr(), cstr(r#"["a","missing"]"#).as_ptr())
    });
    assert_eq!(many[0]["n"], 2);
    assert!(many[1].is_null(), "absent id is null in the same order: {many}");
    assert_eq!(
        { hk_transaction_patch(eng, tx, cstr("c").as_ptr(), cstr("a").as_ptr(), cstr(r#"{"n":42}"#).as_ptr()) },
        0,
        "{}",
        last_error()
    );
    assert_eq!({ hk_transaction_commit(eng, tx) }, 0, "{}", last_error());
    hk_transaction_free(tx);

    let mut v3 = 0u64;
    let got = { hk_engine_get_versioned(eng, cstr("c").as_ptr(), cstr("a").as_ptr(), &mut v3) };
    assert!(!got.is_null());
    let json = take_json(hk_doc_to_json(got));
    assert_eq!(json["n"], 42, "patch applied at commit: {json}");
    hk_doc_free(got);

    // Conflict: read in tx1, external write, then commit.
    let tx1 = { hk_transaction_begin(eng) };
    let seen = { hk_transaction_get(eng, tx1, cstr("c").as_ptr(), cstr("a").as_ptr()) };
    assert!(!seen.is_null());
    hk_doc_free(seen);
    let doc = { hk_doc_new() };
    hk_doc_insert_int(doc, cstr("n").as_ptr(), 3);
    assert_eq!({ hk_engine_insert(eng, cstr("c").as_ptr(), cstr("a").as_ptr(), doc) }, 0);
    hk_doc_free(doc);
    assert_eq!(
        { hk_transaction_patch(eng, tx1, cstr("c").as_ptr(), cstr("a").as_ptr(), cstr(r#"{"n":99}"#).as_ptr()) },
        0
    );
    assert_eq!({ hk_transaction_commit(eng, tx1) }, -1, "stale read-set must conflict");
    assert!({ hk_error_is_retryable() }, "kind {}", { hk_error_last_kind() });
    assert_eq!({ hk_error_last_kind() }, 2);
    {
        hk_transaction_free(tx1);
        hk_engine_free(eng);
    }
}

#[test]
fn retry_helper_reruns_the_body_on_conflict() {
    let dir = tmp("retry");
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let db = Hako::open(&dir, HakoConfig::default()).unwrap();
    let mut start = HakoDoc::default();
    start.insert("n", Value::Int(0));
    db.put("c", "k", &start).unwrap();

    let mut attempts = 0u32;
    db.transaction(
        RetryPolicy::exponential(4, std::time::Duration::from_millis(1), std::time::Duration::from_millis(10)),
        |tx| {
            let mut doc = tx.get(&db, "c", "k")?.expect("seeded doc");
            attempts += 1;
            if attempts == 1 {
                // A competing writer lands between our read and commit.
                let mut rival = HakoDoc::default();
                rival.insert("n", Value::Int(100));
                db.put("c", "k", &rival)?;
            }
            doc.insert("n", Value::Int(attempts as i64));
            tx.put("c", "k", doc);
            Ok(())
        },
    )
    .unwrap();

    assert_eq!(attempts, 2, "first commit conflicted, retry succeeded");
    let doc = db.get("c", "k").unwrap().expect("doc");
    assert_eq!(doc.get("n"), Some(&Value::Int(2)));
}

#[test]
fn explain_returns_plan_json() {
    let dir = tmp("explain");
    seed(&dir);
    let eng = open(&dir);

    let q = { hk_query_new(cstr("orders").as_ptr()) };
    let report = take_json(hk_query_explain(eng, q));
    assert_eq!(report["collection"], "orders");
    assert_eq!(report["collection_rows"], 3);
    assert!(report["scan"].is_string(), "{report}");
    assert!(report["available_indexes"].is_array(), "{report}");
    assert_eq!(report["filter_count"], 0);

    {
        hk_query_free(q);
        hk_engine_free(eng);
    }
}
