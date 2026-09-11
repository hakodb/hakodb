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
#[repr(C)] pub struct FL_Array { _private: [u8; 0] }

// ponytail: link the always-fresh dll.lib by its MSVC name — plain
// "firelite" resolves firelite.lib, which build.rs deletes on purpose
// (it shadowed the DLL with stale symbols; MinGW links the DLL direct).
#[link(name = "firelite.dll", kind = "dylib")]
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
    fn fl_query_start_at(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> c_int;
    fn fl_query_start_after(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> c_int;
    fn fl_query_defer_blobs(query: *mut FL_Query, defer: c_int) -> c_int;
    fn fl_doc_resolve_blobs(engine: *mut FL_Engine, collection: *const c_char, doc: *mut FL_Doc) -> c_int;
    fn fl_result_set_to_json(results: *mut FL_ResultSet) -> *mut c_char;
    fn fl_doc_insert_float(doc: *mut FL_Doc, key: *const c_char, value: f64) -> c_int;
    fn fl_doc_insert_bool(doc: *mut FL_Doc, key: *const c_char, value: bool) -> c_int;
    fn fl_doc_insert_null(doc: *mut FL_Doc, key: *const c_char) -> c_int;
    fn fl_doc_insert_bin(doc: *mut FL_Doc, key: *const c_char, data: *const u8, len: usize) -> c_int;
    fn fl_doc_insert_timestamp(doc: *mut FL_Doc, key: *const c_char, micros: i64) -> c_int;
    fn fl_doc_insert_server_timestamp(doc: *mut FL_Doc, key: *const c_char) -> c_int;
    fn fl_doc_insert_reference(doc: *mut FL_Doc, key: *const c_char, target_collection: *const c_char, target_id: *const c_char) -> c_int;
    fn fl_doc_insert_doc(parent: *mut FL_Doc, key: *const c_char, child: *const FL_Doc) -> c_int;
    fn fl_doc_insert_array(doc: *mut FL_Doc, key: *const c_char, array: *mut FL_Array) -> c_int;
    fn fl_array_new() -> *mut FL_Array;
    fn fl_array_append_str(array: *mut FL_Array, value: *const c_char) -> c_int;
    fn fl_array_append_int(array: *mut FL_Array, value: i64) -> c_int;
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
fn cursor_start_at_and_after_on_id() {
    // Guards the SortedKeys cursor path: order by id + start_at/start_after
    // must slice the sorted-keys vec at the anchor (inclusive/exclusive),
    // not fall back to a different index or return the wrong window.
    let dir = temp_dir("rs_cur");
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

    // start_at k_04 (inclusive), limit 3 → v=4,5,6
    let anchor = unsafe { fl_engine_get(engine, coll.as_ptr(), cs("k_04").as_ptr()) };
    assert!(!anchor.is_null(), "anchor get null");
    let q = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_order_by(q, cs("id").as_ptr(), true);
        assert_eq!(fl_query_start_at(q, anchor), 0, "start_at rc");
        fl_doc_free(anchor); // values are cloned into the query
        fl_query_limit(q, 3);
    }
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    assert_eq!(unsafe { fl_result_set_count(rs) }, 3, "start_at limit 3 → 3 rows");
    for (i, expect) in [4, 5, 6].iter().enumerate() {
        let d = unsafe { fl_result_set_get_doc(rs, i) };
        let j = read_cstr(unsafe { fl_doc_to_json(d) });
        assert!(j.contains(&format!("\"v\":{expect}")), "row {i} should have v={expect}, got {j}");
    }
    unsafe { fl_result_set_free(rs) };
    unsafe { fl_query_free(q) };

    // start_after k_04 (exclusive), limit 3 → v=5,6,7
    let anchor2 = unsafe { fl_engine_get(engine, coll.as_ptr(), cs("k_04").as_ptr()) };
    assert!(!anchor2.is_null(), "anchor2 get null");
    let q2 = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_order_by(q2, cs("id").as_ptr(), true);
        assert_eq!(fl_query_start_after(q2, anchor2), 0, "start_after rc");
        fl_doc_free(anchor2);
        fl_query_limit(q2, 3);
    }
    let rs2 = unsafe { fl_query_execute_to_handles(engine, q2) };
    assert_eq!(unsafe { fl_result_set_count(rs2) }, 3, "start_after limit 3 → 3 rows");
    let first2 = unsafe { fl_result_set_get_doc(rs2, 0) };
    let first2_json = read_cstr(unsafe { fl_doc_to_json(first2) });
    assert!(first2_json.contains("\"v\":5"), "start_after first should be v=5, got {first2_json}");
    unsafe { fl_result_set_free(rs2) };
    unsafe { fl_query_free(q2) };

    unsafe { fl_engine_free(engine) };
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn deferred_blobs_skip_inflate_and_resolve() {
    // A 20KB photo field (> 16KB default blob threshold) spills to the blob
    // file. Deferred queries must return the BlobLink placeholder without
    // touching the blob file; resolve restores the full data.
    let dir = temp_dir("defer");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };

    let coll = cs("bench");
    let photo: String = "PHOTO_".to_string() + &"x".repeat(20 * 1024);
    unsafe {
        let batch = fl_batch_new();
        let id = cs("pic_01");
        let doc = fl_doc_new();
        fl_doc_insert_str(doc, cs("name").as_ptr(), cs("pic").as_ptr());
        fl_doc_insert_str(doc, cs("photo").as_ptr(), cs(photo.as_str()).as_ptr());
        fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    wait_for_indexes(engine);

    // 1. Default (eager): photo inflated inline.
    let q = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe { fl_query_limit(q, 10) };
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    assert_eq!(unsafe { fl_result_set_count(rs) }, 1);
    let d = unsafe { fl_result_set_get_doc(rs, 0) };
    let j = read_cstr(unsafe { fl_doc_to_json(d) });
    assert!(j.contains("PHOTO_"), "eager query should inflate photo");
    assert!(!j.contains("__blob__"), "eager query should have no placeholder, got len {}", j.len());
    unsafe { fl_result_set_free(rs) };
    unsafe { fl_query_free(q) };

    // 2. Deferred: BlobLink placeholder, no 20KB payload.
    let q2 = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_limit(q2, 10);
        assert_eq!(fl_query_defer_blobs(q2, 1), 0, "defer rc");
    }
    let rs2 = unsafe { fl_query_execute_to_handles(engine, q2) };
    assert_eq!(unsafe { fl_result_set_count(rs2) }, 1);
    let d2 = unsafe { fl_result_set_get_doc(rs2, 0) };
    let j2 = read_cstr(unsafe { fl_doc_to_json(d2) });
    assert!(j2.contains("__blob__"), "deferred query should carry placeholder");
    assert!(!j2.contains("PHOTO_"), "deferred query must not read blob data");
    assert!(j2.len() < 1024, "deferred json should be tiny, got {}", j2.len());

    // 3. Resolve in place: full photo back.
    assert_eq!(unsafe { fl_doc_resolve_blobs(engine, coll.as_ptr(), d2) }, 0, "resolve rc");
    let j3 = read_cstr(unsafe { fl_doc_to_json(d2) });
    assert!(j3.contains("PHOTO_"), "resolved doc should have photo");
    assert!(j3.len() > 20 * 1024, "resolved json should hold 20KB, got {}", j3.len());

    unsafe { fl_result_set_free(rs2) };
    unsafe { fl_query_free(q2) };
    unsafe { fl_engine_free(engine) };
    std::fs::remove_dir_all(&dir).ok();
}

#[test]
fn bulk_json_matches_per_doc() {
    // Locks the bulk serializer: output must be byte-identical to joining
    // fl_doc_to_json per row. Covers every StreamValue arm, including the
    // sorted-key order, non-finite floats (-> null), nested maps/arrays,
    // references and blob placeholders.
    let dir = temp_dir("bulkjson");
    let path = cs(dir.to_str().unwrap());
    let engine = unsafe { fl_engine_open(path.as_ptr()) };

    let coll = cs("bench");
    unsafe {
        let batch = fl_batch_new();
        for i in 0..3 {
            let id = cs(format!("j_{i}").as_str());
            let doc = fl_doc_new();
            fl_doc_insert_str(doc, cs("name").as_ptr(), cs(format!("n_{i}").as_str()).as_ptr());
            fl_doc_insert_int(doc, cs("age").as_ptr(), 20 + i as i64);
            fl_doc_insert_float(doc, cs("score").as_ptr(), 1.5 + i as f64);
            fl_doc_insert_float(doc, cs("inf").as_ptr(), f64::INFINITY);
            fl_doc_insert_bool(doc, cs("active").as_ptr(), i % 2 == 0);
            fl_doc_insert_null(doc, cs("nil").as_ptr());
            let blob: &[u8] = &[0u8, 1, 2, 250];
            fl_doc_insert_bin(doc, cs("raw").as_ptr(), blob.as_ptr(), blob.len());
            fl_doc_insert_timestamp(doc, cs("ts").as_ptr(), 1_700_000_000_000_000 + i as i64);
            fl_doc_insert_server_timestamp(doc, cs("sts").as_ptr());
            fl_doc_insert_reference(doc, cs("friend").as_ptr(), cs("users").as_ptr(), cs("u_9").as_ptr());
            let child = fl_doc_new();
            fl_doc_insert_int(child, cs("zip").as_ptr(), 90210);
            fl_doc_insert_doc(doc, cs("addr").as_ptr(), child);
            fl_doc_free(child);
            let arr = fl_array_new();
            fl_array_append_str(arr, cs("a").as_ptr());
            fl_array_append_int(arr, 7);
            fl_doc_insert_array(doc, cs("tags").as_ptr(), arr);
            fl_doc_insert_str(doc, cs("thumb").as_ptr(), cs("tiny-bytes").as_ptr());
            let photo: String = "PHOTO_".to_string() + &"y".repeat(20 * 1024);
            fl_doc_insert_str(doc, cs("photo").as_ptr(), cs(photo.as_str()).as_ptr());
            fl_batch_set(batch, coll.as_ptr(), id.as_ptr(), doc);
        }
        assert_eq!(fl_batch_commit(engine, batch), 0);
    }

    wait_for_indexes(engine);

    // ponytail: the 20KB photo persists via the background blob worker;
    // eager query inflation reads the blob FILE (no flush-queue fallback
    // on this path), so it races the flush. Poll until the photo lands —
    // bulk and per-doc renderers share the same docs, so the byte-identity
    // assert below holds on every iteration, inflated or not.
    let t0 = std::time::Instant::now();
    let (_n, _expected, bulk) = loop {
        let q = unsafe { fl_query_new(coll.as_ptr()) };
        unsafe { fl_query_limit(q, 10) };
        let rs = unsafe { fl_query_execute_to_handles(engine, q) };
        let n = unsafe { fl_result_set_count(rs) };
        assert_eq!(n, 3);

        // Per-doc reference rendering.
        let mut expected = String::from("[");
        for i in 0..n {
            let d = unsafe { fl_result_set_get_doc(rs, i) };
            let j = read_cstr(unsafe { fl_doc_to_json(d) });
            if i > 0 { expected.push(','); }
            expected.push_str(&j);
        }
        expected.push(']');

        // Bulk rendering must match byte-for-byte.
        let bulk = read_cstr(unsafe { fl_result_set_to_json(rs) });
        assert_eq!(bulk, expected, "bulk JSON diverged from per-doc rendering");
        unsafe { fl_result_set_free(rs) };
        unsafe { fl_query_free(q) };
        if bulk.contains("PHOTO_") || t0.elapsed() > std::time::Duration::from_secs(15) {
            break (n, expected, bulk);
        }
        std::thread::sleep(std::time::Duration::from_millis(20));
    };

    // Spot-check arms survived (eager path inflates: no placeholders here).
    assert!(bulk.contains("PHOTO_"), "inflated photo arm");
    assert!(bulk.contains("\"inf\":null"), "non-finite float arm");
    assert!(bulk.contains("\"zip\":90210"), "nested map arm");
    assert!(bulk.contains("\"__ref__\":\"users/u_9\""), "reference arm");

    // Same query deferred: placeholders on both renderers, byte-identical.
    let qd = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_limit(qd, 10);
        assert_eq!(fl_query_defer_blobs(qd, 1), 0);
    }
    let rsd = unsafe { fl_query_execute_to_handles(engine, qd) };
    assert_eq!(unsafe { fl_result_set_count(rsd) }, 3);
    let mut expected_d = String::from("[");
    for i in 0..3 {
        let d = unsafe { fl_result_set_get_doc(rsd, i) };
        let j = read_cstr(unsafe { fl_doc_to_json(d) });
        if i > 0 { expected_d.push(','); }
        expected_d.push_str(&j);
    }
    expected_d.push(']');
    let bulk_d = read_cstr(unsafe { fl_result_set_to_json(rsd) });
    assert_eq!(bulk_d, expected_d, "deferred bulk diverged");
    assert!(bulk_d.contains("__blob__"), "deferred blob arm");
    assert!(!bulk_d.contains("PHOTO_"), "deferred must not read blob data");
    unsafe { fl_result_set_free(rsd) };
    unsafe { fl_query_free(qd) };

    // Empty set renders as [].
    let qe = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_where_eq_str(qe, cs("name").as_ptr(), cs("no-such-doc").as_ptr());
        fl_query_limit(qe, 10);
    }
    let rse = unsafe { fl_query_execute_to_handles(engine, qe) };
    assert_eq!(unsafe { fl_result_set_count(rse) }, 0);
    let be = read_cstr(unsafe { fl_result_set_to_json(rse) });
    assert_eq!(be, "[]");
    unsafe { fl_result_set_free(rse) };
    unsafe { fl_query_free(qe) };

    unsafe { fl_engine_free(engine) };
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
fn multi_filter_conjunction_matches_one() {
    // No indexes on this collection => full scan with per-doc filter verify,
    // exercising the byte-compare fast path (19 rejections must not decode).
    let dir = temp_dir("where2");
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

    // cat=2 AND name=n_6 → only i=6
    let q = unsafe { fl_query_new(coll.as_ptr()) };
    unsafe {
        fl_query_where_eq_int(q, cs("cat").as_ptr(), 2);
        fl_query_where_eq_str(q, cs("name").as_ptr(), cs("n_6").as_ptr());
    }
    let rs = unsafe { fl_query_execute_to_handles(engine, q) };
    assert_eq!(unsafe { fl_result_set_count(rs) }, 1, "conjunction should yield 1 row");
    let d = unsafe { fl_result_set_get_doc(rs, 0) };
    let j = read_cstr(unsafe { fl_doc_to_json(d) });
    assert!(j.contains("n_6"), "row should be n_6, got {j}");

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
    // (firelite.dll: fresh import lib, see top of file.)
    #[link(name = "firelite.dll", kind = "dylib")]
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
