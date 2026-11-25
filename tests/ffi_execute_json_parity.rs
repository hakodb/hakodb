//! Byte parity between the canonical FFI JSON executor and the decoded
//! document reference.

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};
use hakodb::ffi::*;
use hakodb::query::filter::Operator;
use hakodb::query::query::Query;
use std::ffi::{CStr, CString};

fn dir(tag: &str) -> std::path::PathBuf {
    let p = std::env::temp_dir().join(format!(
        "hako-exec-json-{}-{}-{}",
        tag,
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let _ = std::fs::remove_dir_all(&p);
    p
}

#[test]
#[allow(unused_unsafe)]
fn execute_ex_json_matches_decoded_reference() {
    let d = dir("parity");
    let mut batch = Vec::new();
    for i in 0..1000usize {
        let mut doc = HakoDoc::default();
        doc.insert("key", Value::String(format!("{:010}", i)));
        doc.insert("name", Value::String(format!("name-{}", i % 25)));
        doc.insert("payload", Value::String("x".repeat(64)));
        batch.push(BatchMutation::Put {
            collection: "records".into(),
            doc_id: format!("r{i:04}"),
            doc,
        });
    }
    {
        let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
        let db = Hako::open(&d, cfg).expect("open");
        db.write_batch(batch).expect("seed");
    }

    let path = CString::new(d.to_str().unwrap()).unwrap();
    let engine = unsafe { hk_engine_open(path.as_ptr()) };
    assert!(!engine.is_null(), "engine open");
    let query = unsafe { hk_query_new(c"records".as_ptr()) };
    assert!(!query.is_null(), "query new");
    unsafe {
        assert_eq!(hk_query_where_eq_str(query, c"name".as_ptr(), c"name-7".as_ptr()), 0);
        assert_eq!(hk_query_limit(query, 100), 0);
    }
    let json = unsafe { hk_query_execute_ex(engine, query, std::ptr::null_mut()) };
    assert!(!json.is_null(), "execute_ex returns JSON");
    let got = unsafe { CStr::from_ptr(json).to_bytes() }.to_vec();

    let cfg = HakoConfig { durability_mode: DurabilityMode::Manual, ..Default::default() };
    let db = Hako::open(&d, cfg).expect("reopen");
    let q = Query::new("records")
        .where_filter("name", Operator::Eq, Value::String("name-7".into()))
        .limit(100);
    let mut expected = String::from("[");
    for (i, (_, doc)) in db.query(q).expect("decoded").into_iter().enumerate() {
        if i > 0 { expected.push(','); }
        expected.push_str(&serde_json::to_string(&doc.to_json()).expect("json"));
    }
    expected.push(']');
    assert_eq!(got, expected.as_bytes(), "JSON bytes differ");

    unsafe { hk_string_free(json); hk_query_free(query); hk_engine_free(engine); }
    let _ = std::fs::remove_dir_all(&d);
}
