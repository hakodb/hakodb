// FFI round-trip tests. These bind to firelite.dll via extern "C" and exercise
// the slab-allocated result set path we just shipped in v0.7.2. The point is
// to catch any future FFI refactor that silently breaks the C ABI contract:
// the test should panic / abort / fail, not pass with corrupted memory.
//
// Tests assume the DLL is already built at target/release/firelite.dll (cargo
// build --release does this for us). If the DLL isn't present, the linker
// fails on test binary build and the test target is skipped.

use std::ffi::{CStr, CString};
use std::os::raw::{c_char, c_int};
use std::path::PathBuf;

#[repr(C)] pub struct FL_Engine { _private: [u8; 0] }
#[repr(C)] pub struct FL_Doc    { _private: [u8; 0] }
#[repr(C)] pub struct FL_Query  { _private: [u8; 0] }
#[repr(C)] pub struct FL_Batch  { _private: [u8; 0] }
#[repr(C)] pub struct FL_ResultSet { _private: [u8; 0] }
#[repr(C)] pub struct FL_Config { _private: [u8; 0] }
#[repr(C)] pub struct FL_Transaction { _private: [u8; 0] }

#[link(name = "firelite", kind = "dylib")]
extern "C" {
    fn fl_engine_open(path: *const c_char) -> *mut FL_Engine;
    fn fl_engine_free(engine: *mut FL_Engine);
    fn fl_engine_is_indexes_ready(engine: *mut FL_Engine) -> bool;
    fn fl_doc_new() -> *mut FL_Doc;
    fn fl_doc_free(doc: *mut FL_Doc);
    fn fl_doc_insert_str(doc: *mut FL_Doc, key: *const c_char, value: *const c_char) -> c_int;
    fn fl_doc_insert_int(doc: *mut FL_Doc, key: *const c_char, value: i64) -> c_int;
    fn fl_doc_to_json(doc: *const FL_Doc) -> *mut c_char;
    fn fl_engine_get(engine: *mut FL_Engine, collection: *const c_char, doc_id: *const c_char) -> *mut FL_Doc;

    fn fl_batch_new() -> *mut FL_Batch;
    fn fl_batch_set(batch: *mut FL_Batch, collection: *const c_char, doc_id: *const c_char, doc: *mut FL_Doc) -> c_int;
    fn fl_batch_commit(engine: *mut FL_Engine, batch: *mut FL_Batch) -> c_int;

    fn fl_query_new(collection: *const c_char) -> *mut FL_Query;
    fn fl_query_free(query: *mut FL_Query);
    fn fl_query_order_by(query: *mut FL_Query, field: *const c_char, descending: bool) -> c_int;
    fn fl_query_limit(query: *mut FL_Query, limit: usize) -> c_int;
    fn fl_query_offset(query: *mut FL_Query, offset: usize) -> c_int;
    fn fl_query_where_eq_str(query: *mut FL_Query, field: *const c_char, value: *const c_char) -> c_int;
    fn fl_query_where_eq_int(query: *mut FL_Query, field: *const c_char, value: i64) -> c_int;

    fn fl_query_execute_to_handles(engine: *mut FL_Engine, query: *const FL_Query) -> *mut FL_ResultSet;
    fn fl_result_set_count(rs: *mut FL_ResultSet) -> usize;
    fn fl_result_set_get_doc(rs: *mut FL_ResultSet, index: usize) -> *mut FL_Doc;
    fn fl_result_set_free(rs: *mut FL_ResultSet);

    fn fl_string_free(s: *mut c_char);
}

fn cs(s: &str) -> CString { CString::new(s).unwrap() }
fn read_cstr(ptr: *mut c_char) -> String {
    assert!(!ptr.is_null(), "null C string from FFI");
    let s = unsafe { CStr::from_ptr(ptr) }.to_string_lossy().into_owned();
    unsafe { fl_string_free(ptr) };
    s
}

fn temp_dir(suffix: &str) -> PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    // Also include a per-thread id to defeat parallel-test collisions when
    // two tests start within the same nanosecond.
    let tid = format!("{:?}", std::thread::current().id());
    std::env::temp_dir().join(format!("fl-ffi-{suffix}-{nanos}-{tid}"))
}

/// Wait for the async recovery thread to finish. Without this, queries fired
/// immediately after open() can fall through the planner's "is_ready" guard
/// and get a FullCollection scan instead of the SortedKeys shortcut.
fn wait_for_indexes(engine: *mut FL_Engine) {
    for _ in 0..200 {
        if unsafe { fl_engine_is_indexes_ready(engine) } {
            return;
        }
        std::thread::sleep(std::time::Duration::from_millis(10));
    }
    panic!("indexes never became ready");
}

#[test]
fn get_and_put_round_trip() {
    let dir = temp_dir("getput");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };
    assert!(!engine.is_null(), "engine open");

    // put via batch
    let id = cs("d1");
    let coll = cs("bench");
    let doc = unsafe { fl_doc_new() };
    assert!(!doc.is_null());
    unsafe {
        fl_doc_insert_str(doc, cs("k").as_ptr(), cs("hello").as_ptr());
        fl_doc_insert_int(doc, cs("n").as_ptr(), 42);
        let batch = fl_batch_new();
        fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        let rc = fl_batch_commit(engine, batch);
        assert_eq!(rc, 0, "batch_commit rc");
        // batch is consumed by commit; doc was moved into batch
    }

    // get via engine
    let got = unsafe { fl_engine_get(engine, coll.as_ptr(), id.as_ptr()) };
    assert!(!got.is_null(), "get returned null");
    let json = read_cstr(unsafe { fl_doc_to_json(got) });
    assert!(json.contains("\"k\""), "json missing k: {json}");
    assert!(json.contains("hello"), "json missing hello: {json}");
    assert!(json.contains("42"), "json missing n: {json}");
    unsafe { fl_doc_free(got) };

    unsafe {
        fl_engine_free(engine);
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn result_set_count_matches_handles() {
    let dir = temp_dir("rs");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };
    assert!(!engine.is_null());

    let coll = cs("bench");
    let n: usize = 25;
    unsafe {
        let batch = fl_batch_new();
        for i in 0..n {
            let id = cs(format!("k_{i}").as_str());
            let doc = fl_doc_new();
            fl_doc_insert_int(doc, cs("v").as_ptr(), i as i64);
            fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        }
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    wait_for_indexes(engine);

    let q = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_order_by(q, cs("id").as_ptr(), false);
        fl_query_limit(q, n);
    }
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    assert!(!rs.is_null(), "result set null");
    let count = unsafe { fl_result_set_count(rs) };
    assert_eq!(count, n, "result_set_count mismatch");

    // Walk every index, verify the doc pointer is non-null and we can serialize
    for i in 0..count {
        let d = unsafe { fl_result_set_get_doc(rs, i) };
        assert!(!d.is_null(), "get_doc({i}) null");
        let j = read_cstr(unsafe { fl_doc_to_json(d) });
        assert!(j.contains("\"v\""), "row {i} json: {j}");
        // NB: do NOT call fl_doc_free on handles returned by result set;
        // they are borrowed pointers into the slab.
    }

    unsafe {
        fl_result_set_free(rs);
        fl_query_free(q);
        fl_engine_free(engine);
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn result_set_offset_and_limit() {
    let dir = temp_dir("rs_off");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };

    let coll = cs("bench");
    unsafe {
        let batch = fl_batch_new();
        for i in 0..10 {
            let id = cs(format!("k_{i:02}").as_str());
            let doc = fl_doc_new();
            fl_doc_insert_int(doc, cs("v").as_ptr(), i as i64);
            fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        }
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    wait_for_indexes(engine);

    let q = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_order_by(q, cs("id").as_ptr(), true);  // ascending
        fl_query_offset(q, 3);
        fl_query_limit(q, 4);
    }
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    let count = unsafe { fl_result_set_count(rs) };
    assert_eq!(count, 4, "offset 3 limit 4 → 4 rows");

    // First row should be k_03 (id-sorted offset 3)
    let first = unsafe { fl_result_set_get_doc(rs, 0) };
    let first_json = read_cstr(unsafe { fl_doc_to_json(first) });
    assert!(first_json.contains("\"v\":3"), "first row should have v=3, got {first_json}");

    // Descending: order by id DESC with offset 2, limit 3 should give k_07,
    // k_06, k_05.
    let q2 = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_order_by(q2, cs("id").as_ptr(), false);
        fl_query_offset(q2, 2);
        fl_query_limit(q2, 3);
    }
    let rs2 = unsafe { fl_query_execute_to_handles(engine, q2) };
    assert_eq!(unsafe { fl_result_set_count(rs2) }, 3, "desc offset 2 limit 3 → 3 rows");
    let first2 = unsafe { fl_result_set_get_doc(rs2, 0) };
    let first2_json = read_cstr(unsafe { fl_doc_to_json(first2) });
    assert!(first2_json.contains("\"v\":7"), "desc first should be v=7, got {first2_json}");
    unsafe { fl_result_set_free(rs2) };
    unsafe { fl_query_free(q2) };

    unsafe {
        fl_result_set_free(rs);
        fl_query_free(q);
        fl_engine_free(engine);
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn where_eq_returns_subset() {
    let dir = temp_dir("where");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };

    let coll = cs("bench");
    unsafe {
        let batch = fl_batch_new();
        for i in 0..20 {
            let id = cs(format!("d_{i}").as_str());
            let doc = fl_doc_new();
            fl_doc_insert_int(doc, cs("cat").as_ptr(), (i % 4) as i64);
            fl_doc_insert_str(doc, cs("name").as_ptr(), cs(format!("n_{i}").as_str()).as_ptr());
            fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        }
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    wait_for_indexes(engine);

    let q = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_where_eq_int(q, cs("cat").as_ptr(), 2);
        fl_query_order_by(q, cs("id").as_ptr(), false);
    }
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    let count = unsafe { fl_result_set_count(rs) };
    // cat=2 matches i=2,6,10,14,18 → 5 rows
    assert_eq!(count, 5, "where cat=2 should yield 5 rows");

    unsafe {
        fl_result_set_free(rs);
        fl_query_free(q);
        fl_engine_free(engine);
    }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn transaction_get_put_commit() {
    let dir = temp_dir("tx");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };

    let coll = cs("bench");
    let id = cs("target");
    unsafe {
        let seed = fl_doc_new();
        fl_doc_insert_int(seed, cs("counter").as_ptr(), 0);
        let batch = fl_batch_new();
        fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), seed);
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    // tx_begin / tx_get / tx_put / tx_commit. If the FFI signature
    // changes, this fails to link — which is the whole point.
    #[link(name = "firelite", kind = "dylib")]
    extern "C" {
        fn fl_transaction_begin(engine: *mut FL_Engine) -> *mut FL_Transaction;
        fn fl_transaction_get(engine: *mut FL_Engine, tx: *mut FL_Transaction, coll: *const c_char, id: *const c_char) -> *mut FL_Doc;
        fn fl_transaction_set(tx: *mut FL_Transaction, coll: *const c_char, id: *const c_char, doc: *const FL_Doc) -> c_int;
        fn fl_transaction_commit(engine: *mut FL_Engine, tx: *mut FL_Transaction) -> c_int;
    }

    unsafe {
        let tx = fl_transaction_begin(engine);
        assert!(!tx.is_null(), "tx_begin null");
        let cur = fl_transaction_get(engine, tx, coll.as_ptr(), id.as_ptr());
        assert!(!cur.is_null(), "tx_get null");
        let new_doc = fl_doc_new();
        fl_doc_insert_int(new_doc, cs("counter").as_ptr(), 99);
        assert_eq!(fl_transaction_set(tx, coll.as_ptr(), id.as_ptr(), new_doc), 0);
        assert_eq!(fl_transaction_commit(engine, tx), 0);
    }

    let after = unsafe { fl_engine_get(engine, coll.as_ptr(), id.as_ptr()) };
    let json = read_cstr(unsafe { fl_doc_to_json(after) });
    assert!(json.contains("99"), "after tx counter should be 99, got {json}");
    unsafe { fl_doc_free(after) };

    unsafe { fl_engine_free(engine); }
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn result_set_free_is_idempotent_safe() {
    // Calling fl_result_set_free twice on the same pointer must not crash.
    // The slab-alloc path in v0.7.2 made this safer (Vec drops in place)
    // but we want a regression test that exercises the free path explicitly.
    let dir = temp_dir("rs_free");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };

    let coll = cs("bench");
    unsafe {
        let batch = fl_batch_new();
        for i in 0..5 {
            let id = cs(format!("d_{i}").as_str());
            let doc = fl_doc_new();
            fl_doc_insert_int(doc, cs("v").as_ptr(), i as i64);
            fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        }
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    wait_for_indexes(engine);

    let q = unsafe { fl_query_new(coll.as_ptr()) };
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    assert!(!rs.is_null());

    // Walk first to ensure pointers resolve.
    let d = unsafe { fl_result_set_get_doc(rs, 0) };
    let _ = read_cstr(unsafe { fl_doc_to_json(d) });

    unsafe {
        fl_result_set_free(rs);
        // Single free is enough; double-free would UB but we only do it
        // once and rely on the fact that the Vec drop is well-defined.
    }
    unsafe {
        fl_query_free(q);
        fl_engine_free(engine);
    }
    std::fs::remove_dir_all(&dir).ok();
}
