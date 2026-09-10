use std::cell::RefCell;
// use crate::engine::Engine;
use std::ffi::{c_char, c_int, CStr, CString};
// use std::os::raw::c_char;
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{channel, Sender};
use std::time::Duration;
use std::{ptr, thread};
use std::collections::HashSet;

// use std::sync::Arc;
use hashbrown::HashMap;

use crate::config::{DurabilityMode, FireLiteConfig};
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::engine::{BatchMutation, FireLite};
use crate::index::composite::definition::SortDirection;
use crate::query::filter::Operator;
use crate::query::query::{AggregateOp, Query};
// use crate::query::planner::QueryPlanner;

macro_rules! safety_shield {
    ($fallback:expr, $block:block) => {
        match catch_unwind(AssertUnwindSafe(|| $block)) {
            Ok(val) => val,
            Err(_) => {
                // Set the thread-local error so C++ can see it
                crate::ffi::set_last_error("CRITICAL: Internal Engine Panic. The operation was aborted to prevent a process crash.");
                $fallback
            }
        }
    };
}

#[allow(non_camel_case_types)]
pub struct FL_Engine {
    // db: FireLite,
    db: std::sync::Arc<FireLite>,
}

#[allow(non_camel_case_types)]
pub struct FL_Doc {
    pub id: String,
    doc: FireLiteDoc,
}

#[allow(non_camel_case_types)]
pub struct FL_Batch {
    ops: Vec<BatchMutation>,
}

#[allow(non_camel_case_types)]
pub struct FL_Query {
    query: Query,
}

// config
#[allow(non_camel_case_types)]
pub struct FL_Config {
    pub inner: FireLiteConfig,
}

#[allow(non_camel_case_types)]
pub struct FL_Watch {
    stop_tx: Sender<()>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

// 1. Fixed type naming warning with #[allow]
#[allow(non_camel_case_types)]
pub type FL_OnSnapshotCallback = unsafe extern "C" fn(
    collection: *const c_char,
    path: *const c_char,
    kind: i32,
    user_data: *mut std::ffi::c_void,
);

#[allow(non_camel_case_types)]
pub struct FL_Array {
    pub items: Vec<Value>,
}

#[allow(non_camel_case_types)]
pub struct FL_Transaction {
    pub tx: crate::engine::SerializableTransaction,
}

#[allow(non_camel_case_types)]
pub struct FL_ResultSet {
    pub docs: Vec<FL_Doc>,
}

#[cfg(feature = "net-sync")]
#[allow(non_camel_case_types)]
pub struct FL_NetSyncer {
    inner: std::sync::Arc<crate::net_sync::NetSyncer>,
}

// ============================================================================
// CLOUD SYNC FFI BINDINGS
// ============================================================================

#[cfg(feature = "cloud-sync")]
#[allow(non_camel_case_types)]
pub struct FL_CloudSync {
    inner: std::sync::Arc<crate::cloud_sync::CloudSync>,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
}

fn set_last_error(msg: impl Into<String>) -> i32 {
    let message = CString::new(msg.into())
        .unwrap_or_else(|_| CString::new("ffi error").expect("valid static cstring"));
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(message);
    });
    -1
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
}

fn cstr_to_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("null pointer".into());
    }
    let s = unsafe { CStr::from_ptr(ptr) };
    s.to_str()
        .map(|v| v.to_string())
        .map_err(|_| "invalid utf8".into())
}

fn value_to_json(v: &Value) -> serde_json::Value {
    v.to_json()
}

/// ponytail: 1-3 digit push without fmt machinery (~5ns/byte).
/// serde_json renders numbers as plain digits — this matches it exactly.
fn push_u8(out: &mut String, b: u8) {
    if b >= 100 {
        out.push((b / 100 + b'0') as char);
        out.push(((b / 10) % 10 + b'0') as char);
    } else if b >= 10 {
        out.push((b / 10 + b'0') as char);
    }
    out.push((b % 10 + b'0') as char);
}

fn doc_to_json(doc: &FireLiteDoc) -> Result<String, String> {
    safety_shield!(Err("Internal Panic".into()), {
        // ponytail: stream instead of boxing every value into
        // serde_json::Value first. A 100-byte Binary used to become 100
        // boxed Numbers (~8µs/op on point-gets); digits need no escaping
        // so they format straight into the buffer. Keys and all other
        // values still go through serde_json (escaping identical). Fields
        // emit in sorted-key order — byte-identical to the old BTreeMap
        // output (see test below).
        let mut fields: Vec<&(std::sync::Arc<str>, Value)> = doc.fields.iter().collect();
        fields.sort_by(|a, b| a.0.as_ref().cmp(b.0.as_ref()));
        let mut out = String::with_capacity(256);
        out.push('{');
        for (i, (k, v)) in fields.iter().enumerate() {
            if i > 0 {
                out.push(',');
            }
            out.push_str(&serde_json::to_string(k.as_ref()).map_err(|e| e.to_string())?);
            out.push(':');
            match v {
                Value::Binary(bytes) => {
                    out.push('[');
                    for (j, b) in bytes.iter().enumerate() {
                        if j > 0 {
                            out.push(',');
                        }
                        push_u8(&mut out, *b);
                    }
                    out.push(']');
                }
                _ => {
                    let s = serde_json::to_string(&value_to_json(v)).map_err(|e| e.to_string())?;
                    out.push_str(&s);
                }
            }
        }
        out.push('}');
        Ok(out)
    })
}

fn projection_to_json(fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert(k, value_to_json(&v));
    }
    Ok(serde_json::Value::Object(map))
}

#[no_mangle]
pub extern "C" fn fl_engine_open(path: *const c_char) -> *mut FL_Engine {
    let path = match cstr_to_string(path) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(e);
            return ptr::null_mut();
        }
    };

    match FireLite::open(path, FireLiteConfig::default()) {
        Ok(db) => {
            clear_last_error();
            // Box::into_raw(Box::new(FL_Engine { db }))
            Box::into_raw(Box::new(FL_Engine { db: std::sync::Arc::new(db) }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_engine_is_indexes_ready(engine: *mut FL_Engine) -> bool {
    safety_shield!(false, {
        if engine.is_null() { return false; }
        let engine = unsafe { &*engine };
        engine.db.indexes_ready.load(std::sync::atomic::Ordering::Acquire)
    })
}

#[no_mangle]
pub extern "C" fn fl_config_new() -> *mut FL_Config {
    Box::into_raw(Box::new(FL_Config {
        inner: FireLiteConfig::default(),
    }))
}

#[no_mangle]
pub extern "C" fn fl_config_free(config: *mut FL_Config) {
    if !config.is_null() {
        unsafe { drop(Box::from_raw(config)) };
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_durability(config: *mut FL_Config, mode: i32) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.durability_mode = match mode {
            1 => DurabilityMode::Interval,
            2 => DurabilityMode::Manual,
            3 => DurabilityMode::OnCommit,
            _ => DurabilityMode::Always,
        };
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_encryption_key(config: *mut FL_Config, key: *const c_char) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.encryption_key = cstr_to_string(key).ok();
    }
}

/// Set which collections should be encrypted. 
/// collections_json: A JSON array of strings, e.g., '["secrets", "private_messages"]'
#[no_mangle]
pub extern "C" fn fl_config_set_encrypted_collections(
    config: *mut FL_Config,
    collections_json: *const c_char,
) -> i32 {
    let cfg = unsafe { match config.as_mut() {
        Some(c) => c,
        None => return -1,
    }};

    let json_str = match cstr_to_string(collections_json) {
        Ok(s) => s,
        Err(_) => return -1,
    };

    let cols: HashSet<String> = match serde_json::from_str(&json_str) {
        Ok(v) => v,
        Err(_) => return -1,
    };

    cfg.inner.encrypted_cols = Some(cols);
    0
}

#[no_mangle]
pub extern "C" fn fl_config_set_audit_log(
    config: *mut FL_Config,
    enabled: bool,
    path: *const c_char,
) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.enable_audit_log = enabled;
        cfg.inner.audit_log_path = cstr_to_string(path).ok();
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_query_workers(config: *mut FL_Config, count: usize) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.query_workers = count;
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_memory_limits(
    config: *mut FL_Config,
    mmap_size: usize,
    max_inlined_bytes: usize,
) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.mmap_size = mmap_size;
        cfg.inner.max_inlined_memory_bytes = max_inlined_bytes;
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_storage_tuning(
    config: *mut FL_Config,
    page_size: usize,
    compaction_threshold: usize,
    group_commit_max_ops: usize,
) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.page_size = page_size;
        cfg.inner.auto_compaction_threshold_bytes = compaction_threshold;
        cfg.inner.group_commit_max_ops = group_commit_max_ops;
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_blob_threshold(config: *mut FL_Config, threshold_bytes: usize) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.value_blob_threshold_bytes = threshold_bytes;
    }
}

/// WAL headroom reservation in bytes (0 = off, default 4MB). Preallocated
/// ahead of the write position so steady-state appends never extend the
/// file. Sparse: consumes no disk until written. Ignored for Manual.
#[no_mangle]
pub extern "C" fn fl_config_set_wal_reserve_bytes(config: *mut FL_Config, bytes: u64) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.wal_reserve_bytes = bytes;
    }
}

/// Write-path phase breakdown (see engine::write_stats_report). Returns a
/// fresh C string the caller frees with fl_string_free. Counters reset.
#[no_mangle]
pub extern "C" fn fl_debug_write_stats() -> *mut c_char {
    match std::ffi::CString::new(crate::engine::engine::write_stats_report()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Opens the engine using a custom config.
/// Note: This function takes ownership of the config and will free it automatically.
#[no_mangle]
pub extern "C" fn fl_engine_open_with_config(
    path: *const c_char,
    config: *mut FL_Config,
) -> *mut FL_Engine {
    safety_shield!(std::ptr::null_mut(), {
        let path_str = match cstr_to_string(path) {
            Ok(v) => v,
            Err(e) => {
                set_last_error(e);
                return std::ptr::null_mut();
            }
        };

        if config.is_null() {
            set_last_error("Null config provided");
            return std::ptr::null_mut();
        }

        // Take ownership of the config from the FFI caller
        let cfg_box = unsafe { Box::from_raw(config) };

        match FireLite::open(path_str, cfg_box.inner) {
            Ok(db) => {
                clear_last_error();
                // Box::into_raw(Box::new(FL_Engine { db }))
                Box::into_raw(Box::new(FL_Engine { db: std::sync::Arc::new(db) }))
            }
            Err(e) => {
                set_last_error(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

// end config

// emulate snapshoot
#[no_mangle]
pub extern "C" fn fl_engine_watch(
    engine: *mut FL_Engine,
    collection: *const c_char,
    callback: FL_OnSnapshotCallback,
    user_data_ptr: *mut std::ffi::c_void,
) -> *mut FL_Watch {
    if engine.is_null() {
        return std::ptr::null_mut();
    }

    let engine_ref = unsafe { &*engine };
    let col_name = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return std::ptr::null_mut(),
    };

    // Cast pointer to usize to safely move it across thread boundaries
    let user_data_val = user_data_ptr as usize;

    let rx = engine_ref.db.watch_collection(&col_name);
    let (stop_tx, stop_rx) = channel::<()>();

    let col_clone = col_name.clone();
    let handle = thread::spawn(move || {
        let thread_user_data = user_data_val as *mut std::ffi::c_void;

        // OPTIMIZATION: Create the C-compatible collection name once
        let c_col = CString::new(col_clone).unwrap();

        loop {
            if let Ok(_) | Err(std::sync::mpsc::TryRecvError::Disconnected) = stop_rx.try_recv() {
                break;
            }

            match rx.recv_timeout(Duration::from_millis(100)) {
                Ok(event) => {
                    let c_path = CString::new(&*event.path).unwrap();
                    let kind = match event.kind {
                        crate::engine::ChangeKind::Put => 1,
                        crate::engine::ChangeKind::Delete => 2,
                    };

                    unsafe {
                        // Pass the stable c_col and the event-specific c_path
                        callback(c_col.as_ptr(), c_path.as_ptr(), kind, thread_user_data);
                    }
                }
                Err(_) => continue,
            }
        }
    });

    Box::into_raw(Box::new(FL_Watch {
        stop_tx,
        thread_handle: Some(handle),
    }))
}

#[no_mangle]
pub extern "C" fn fl_watch_free(watch: *mut FL_Watch) {
    if !watch.is_null() {
        let mut w = unsafe { Box::from_raw(watch) };
        let _ = w.stop_tx.send(());
        if let Some(h) = w.thread_handle.take() {
            // The compiler now knows h is JoinHandle<()>
            let _ = h.join();
        }
    }
}
// end of snapshoot

#[no_mangle]
pub extern "C" fn fl_engine_free(engine: *mut FL_Engine) {
    safety_shield!((), {
        if !engine.is_null() {
            unsafe { drop(Box::from_raw(engine)) };
        }
    })
}

// #[no_mangle]
// pub extern "C" fn fl_doc_new() -> *mut FL_Doc {
//     Box::into_raw(Box::new(FL_Doc {
//         // id: doc_id.clone(),
//         doc: FireLiteDoc::default(),
//     }))
// }

// 2. Update creation points
#[no_mangle]
pub extern "C" fn fl_doc_new() -> *mut FL_Doc {
    Box::into_raw(Box::new(FL_Doc {
        id: String::new(),
        doc: FireLiteDoc::default(),
    }))
}

#[no_mangle]
pub extern "C" fn fl_doc_free(doc: *mut FL_Doc) {
    if !doc.is_null() {
        unsafe { drop(Box::from_raw(doc)) };
    }
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_str(
    doc: *mut FL_Doc,
    key: *const c_char,
    value: *const c_char,
) -> i32 {
    if doc.is_null() {
        return set_last_error("null FL_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let value = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::String(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_int(doc: *mut FL_Doc, key: *const c_char, value: i64) -> i32 {
    if doc.is_null() {
        return set_last_error("null FL_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Int(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_float(doc: *mut FL_Doc, key: *const c_char, value: f64) -> i32 {
    if doc.is_null() {
        return set_last_error("null FL_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Float(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_bool(doc: *mut FL_Doc, key: *const c_char, value: bool) -> i32 {
    if doc.is_null() {
        return set_last_error("null FL_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Bool(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_null(doc: *mut FL_Doc, key: *const c_char) -> i32 {
    if doc.is_null() {
        return set_last_error("null FL_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Null);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_bin(
    doc: *mut FL_Doc,
    key: *const c_char,
    data: *const u8,
    len: usize,
) -> i32 {
    if doc.is_null() {
        return set_last_error("null FL_Doc");
    }
    if data.is_null() && len > 0 {
        return set_last_error("null binary data");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let bytes = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Binary(bytes));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_insert(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *const FL_Doc,
) -> i32 {
    safety_shield!(-1, {
        // Returns -1 if Rust panics
        if engine.is_null() || doc.is_null() {
            return set_last_error("null engine/doc handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };

        let engine = unsafe { &mut *engine };
        let doc = unsafe { &*doc };
        match engine.db.put(&collection, &doc_id, &doc.doc) {
            Ok(_) => 0,
            Err(e) => set_last_error(format!("{}", e)),
        }
    })
}

/// Owned-doc insert: takes over the FL_Doc handle (no deep clone).
/// The handle is ALWAYS consumed — success or failure — do not use or free
/// `doc` after the call.
#[no_mangle]
pub extern "C" fn fl_engine_insert_take(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *mut FL_Doc,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || doc.is_null() {
            return set_last_error("null engine/doc handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };

        let engine = unsafe { &mut *engine };
        // Move the document out of its Box without cloning every field.
        let owned = unsafe { *Box::from_raw(doc) };
        match engine.db.put_owned(&collection, &doc_id, owned.doc) {
            Ok(_) => 0,
            Err(e) => set_last_error(format!("{}", e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_engine_get(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut FL_Doc {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error("null engine handle");
            return ptr::null_mut();
        }
        // ponytail: borrow the caller's C strings — the old code allocated
        // two Strings per get (strlen + validate + copy each). Only doc_id
        // still needs ownership (moved into the FL_Doc handle).
        if collection.is_null() {
            set_last_error("null collection handle");
            return ptr::null_mut();
        }
        if doc_id.is_null() {
            set_last_error("null doc_id handle");
            return ptr::null_mut();
        }
        let collection_c = unsafe { CStr::from_ptr(collection) };
        let doc_id_c = unsafe { CStr::from_ptr(doc_id) };
        let collection = match collection_c.to_str() {
            Ok(v) => v,
            Err(_) => {
                set_last_error("invalid utf8");
                return ptr::null_mut();
            }
        };
        let doc_id_owned = match doc_id_c.to_str() {
            Ok(v) => v.to_string(),
            Err(_) => {
                set_last_error("invalid utf8");
                return ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        match engine.db.get(collection, &doc_id_owned) {
            // PONYTAIL: move doc_id into FL_Doc instead of clone — doc_id is
            // already a freshly-allocated String and we don't reuse it.
            Ok(Some(doc)) => Box::into_raw(Box::new(FL_Doc { id: doc_id_owned, doc })),
            _ => std::ptr::null_mut(),
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_engine_delete(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    let engine = unsafe { &mut *engine };
    match engine.db.delete(&collection, &doc_id) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1 // or ptr::null_mut() depending on function return type
        }
    }
}

/// Local-only delete: marks the key so no sync tailer or handshake
/// catch-up ever transmits it, then deletes normally (fresh tombstone
/// timestamp keeps the version clock advanced — handshake-stable).
#[no_mangle]
pub extern "C" fn fl_engine_delete_local(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    let engine = unsafe { &mut *engine };
    match engine.db.delete_local(&collection, &doc_id) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

/// Marks a collection local-only (`local != 0`) or rejoins it to sync.
/// A local-only collection never emits nor is caught up from the network.
#[no_mangle]
pub extern "C" fn fl_engine_set_collection_local(
    engine: *mut FL_Engine,
    collection: *const c_char,
    local: i32,
) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let engine = unsafe { &mut *engine };
    engine.db.set_collection_local(&collection, local != 0);
    clear_last_error();
    0
}

/// Opts a key back into replication (future ops only).
#[no_mangle]
pub extern "C" fn fl_engine_replicate_key(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let engine = unsafe { &mut *engine };
    engine.db.replicate_key(&collection, &doc_id);
    clear_last_error();
    0
}

/// Opts a whole collection back into replication (clears flag + key marks).
#[no_mangle]
pub extern "C" fn fl_engine_replicate_collection(
    engine: *mut FL_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let engine = unsafe { &mut *engine };
    engine.db.replicate_collection(&collection);
    clear_last_error();
    0
}

/// Vacuum: purge a collection's tombstones. Emits no WAL op (never
/// replicates); drops the version so the next handshake pulls peer state.
/// Returns tombstones purged, or -1 on error.
#[no_mangle]
pub extern "C" fn fl_engine_vacuum_collection(
    engine: *mut FL_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let engine = unsafe { &mut *engine };
    match engine.db.vacuum_collection(&collection) {
        Ok(n) => {
            clear_last_error();
            n as i32
        }
        Err(e) => set_last_error(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn fl_batch_new() -> *mut FL_Batch {
    Box::into_raw(Box::new(FL_Batch { ops: Vec::new() }))
}

#[no_mangle]
pub extern "C" fn fl_batch_free(batch: *mut FL_Batch) {
    if !batch.is_null() {
        unsafe { drop(Box::from_raw(batch)) };
    }
}

#[no_mangle]
pub extern "C" fn fl_batch_set(
    batch: *mut FL_Batch,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *mut FL_Doc,
) -> i32 {
    if batch.is_null() || doc.is_null() {
        return set_last_error("null batch/doc handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    let batch = unsafe { &mut *batch };
    let doc_ptr = unsafe { &mut *doc };

    let internal_doc = std::mem::take(&mut doc_ptr.doc);

    batch.ops.push(BatchMutation::Put {
        collection,
        doc_id,
        doc: internal_doc,
    });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_batch_delete(
    batch: *mut FL_Batch,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if batch.is_null() {
        return set_last_error("null batch handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    let batch = unsafe { &mut *batch };
    batch.ops.push(BatchMutation::Delete { collection, doc_id });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_batch_commit(engine: *mut FL_Engine, batch: *mut FL_Batch) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || batch.is_null() {
            return set_last_error("null engine/batch handle");
        }

        let engine = unsafe { &mut *engine };
        let batch = unsafe { &mut *batch };
        let ops = std::mem::take(&mut batch.ops);

        match engine.db.write_batch(ops) {
            Ok(_) => {
                clear_last_error();
                0
            }
            Err(e) => {
                set_last_error(e.to_string());
                -1 // or ptr::null_mut() depending on function return type
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_query_new(collection: *const c_char) -> *mut FL_Query {
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(e);
            return ptr::null_mut();
        }
    };

    clear_last_error();
    Box::into_raw(Box::new(FL_Query {
        query: Query::new(&collection),
    }))
}

#[no_mangle]
pub extern "C" fn fl_query_free(query: *mut FL_Query) {
    if !query.is_null() {
        unsafe { drop(Box::from_raw(query)) };
    }
}

#[no_mangle]
pub extern "C" fn fl_query_where_eq_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let value = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query
    //     .query
    //     .clone()
    //     .where_filter(&field, Operator::Eq, Value::String(value));
    // clear_last_error();
    // 0
    apply_string_filter(query, field, value, Operator::Eq)
}

#[no_mangle]
pub extern "C" fn fl_query_where_eq_bool(
    query: *mut FL_Query,
    field: *const c_char,
    value: bool, // Receive the bool directly
) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }

    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };

    // let query_ptr = unsafe { &mut *query };

    // // Update the query with Value::Bool directly
    // query_ptr.query = query_ptr
    //     .query
    //     .clone()
    //     .where_filter(&field, Operator::Eq, Value::Bool(value));

    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e), };
    let query_ptr = unsafe { &mut *query };
    
    // FIX: Push directly
    query_ptr.query.filters.push(crate::query::filter::Filter {
        field,
        op: Operator::Eq,
        value: Value::Bool(value)
    });
    clear_last_error();
    0
}

/// Executes the query and deletes all matching documents.
/// Returns the number of deleted documents, or -1 on error.
#[no_mangle]
pub extern "C" fn fl_query_delete(engine: *mut FL_Engine, query: *mut FL_Query) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() { return -1; }
        let engine = unsafe { &*engine };
        let query_ptr = unsafe { &*query };

        match engine.db.delete_where(query_ptr.query.clone()) {
            Ok(count) => {
                clear_last_error();
                count as i32
            },
            Err(e) => set_last_error(e.to_string()),
        }
    })
}

/// Local-only mass delete: marks every match so the wipe never leaves
/// this device, then deletes. See `fl_engine_delete_local`.
#[no_mangle]
pub extern "C" fn fl_query_delete_local(engine: *mut FL_Engine, query: *mut FL_Query) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() { return -1; }
        let engine = unsafe { &*engine };
        let query_ptr = unsafe { &*query };

        match engine.db.delete_where_local(query_ptr.query.clone()) {
            Ok(count) => {
                clear_last_error();
                count as i32
            },
            Err(e) => set_last_error(e.to_string()),
        }
    })
}

/// Executes the query and applies the updates from 'patch_doc' to all matches.
/// Returns the number of updated documents, or -1 on error.
#[no_mangle]
pub extern "C" fn fl_query_patch(
    engine: *mut FL_Engine, 
    query: *mut FL_Query, 
    patch_doc: *const FL_Doc
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() || patch_doc.is_null() { return -1; }
        let engine = unsafe { &*engine };
        let query_ptr = unsafe { &*query };
        let patch_ptr = unsafe { &*patch_doc };

        // Convert FL_Doc fields to the internal updates vector
        let updates: Vec<(String, Value)> = patch_ptr.doc.fields.iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        match engine.db.patch_where(query_ptr.query.clone(), updates) {
            Ok(count) => {
                clear_last_error();
                count as i32
            },
            Err(e) => set_last_error(e.to_string()),
        }
    })
}

fn apply_string_filter(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
    op: Operator,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let value = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query
    //     .query
    //     .clone()
    //     .where_filter(&field, op, Value::String(value));
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e), };
    let value = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error(e), };
    let query = unsafe { &mut *query };
    
    // FIX: Push directly without cloning the query struct
    query.query.filters.push(crate::query::filter::Filter {
        field,
        op,
        value: Value::String(value)
    });
    clear_last_error();
    0
}

fn apply_int_filter(query: *mut FL_Query, field: *const c_char, value: i64, op: Operator) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query
    //     .query
    //     .clone()
    //     .where_filter(&field, op, Value::Int(value));
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e), };
    let query = unsafe { &mut *query };
    
    // FIX: Push directly
    query.query.filters.push(crate::query::filter::Filter {
        field,
        op,
        value: Value::Int(value)
    });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_eq_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query
    //     .query
    //     .clone()
    //     .where_filter(&field, Operator::Eq, Value::Int(value));
    // clear_last_error();
    // 0
    apply_int_filter(query, field, value, Operator::Eq)
}

#[no_mangle]
pub extern "C" fn fl_query_where_ne_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Ne)
}

#[no_mangle]
pub extern "C" fn fl_query_where_ne_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Ne)
}

#[no_mangle]
pub extern "C" fn fl_query_where_gt_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Gt)
}

#[no_mangle]
pub extern "C" fn fl_query_where_gt_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Gt)
}

#[no_mangle]
pub extern "C" fn fl_query_where_gte_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Gte)
}

#[no_mangle]
pub extern "C" fn fl_query_where_gte_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Gte)
}

#[no_mangle]
pub extern "C" fn fl_query_where_lt_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Lt)
}

#[no_mangle]
pub extern "C" fn fl_query_where_lt_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Lt)
}

#[no_mangle]
pub extern "C" fn fl_query_where_lte_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Lte)
}

#[no_mangle]
pub extern "C" fn fl_query_where_lte_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Lte)
}

#[no_mangle]
pub extern "C" fn fl_query_where_array_contains(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let v = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let q = unsafe { &mut *query };
    q.query = q
        .query
        .clone()
        .where_filter(&f, Operator::ArrayContains, Value::String(v));
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_array_contains_any(
    query: *mut FL_Query,
    field: *const c_char,
    array: *mut FL_Array,
) -> i32 {
    if query.is_null() || array.is_null() {
        return -1;
    }
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let array_inner = unsafe { Box::from_raw(array) };
    q.query.filters.push(crate::query::filter::Filter {
        field: f,
        op: Operator::ArrayContainsAny,
        value: Value::Array(array_inner.items),
    });
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_not_in(
    query: *mut FL_Query,
    field: *const c_char,
    array: *mut FL_Array,
) -> i32 {
    if query.is_null() || array.is_null() {
        return -1;
    }
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let array_inner = unsafe { Box::from_raw(array) };
    q.query.filters.push(crate::query::filter::Filter {
        field: f,
        op: Operator::NotIn,
        value: Value::Array(array_inner.items),
    });
    0
}

#[no_mangle]
pub extern "C" fn fl_query_order_by(
    query: *mut FL_Query,
    field: *const c_char,
    ascending: bool,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query.query.clone().order_by(&field, ascending);
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e), };
    let query = unsafe { &mut *query };
    
    // FIX: Push directly
    query.query.order_by.push(crate::query::order::OrderBy {
        field,
        ascending,
    });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_limit(query: *mut FL_Query, limit: usize) -> i32 {
    if query.is_null() {
        return -1;
    }
    let query = unsafe { &mut *query };
    query.query.limit = Some(limit);
    0
}

/// Opt in to deferred blobs: matching docs come back with blob-backed
/// fields as `Value::BlobLink` placeholders (no blob-file reads).
/// Resolve later with `fl_doc_resolve_blobs`. Default off (eager).
#[no_mangle]
pub extern "C" fn fl_query_defer_blobs(query: *mut FL_Query, defer: c_int) -> i32 {
    if query.is_null() {
        return -1;
    }
    let query = unsafe { &mut *query };
    query.query.defer_blobs = defer != 0;
    0
}

/// Resolve deferred blob fields of a query-returned doc in place.
/// No-op for docs without BlobLinks. Needs the owning collection (blob
/// addresses are per-shard).
#[no_mangle]
pub extern "C" fn fl_doc_resolve_blobs(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc: *mut FL_Doc,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || doc.is_null() {
            return set_last_error("null engine/doc handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let engine = unsafe { &*engine };
        let doc = unsafe { &mut *doc };
        match engine.db.resolve_document_blobs(&mut doc.doc, &collection) {
            Ok(_) => 0,
            Err(e) => set_last_error(format!("{}", e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_query_offset(query: *mut FL_Query, offset: usize) -> i32 {
    // <--- NEW FFI
    if query.is_null() {
        return -1;
    }
    let query = unsafe { &mut *query };
    query.query.offset = Some(offset);
    0
}

#[no_mangle]
pub extern "C" fn fl_query_select_field(query: *mut FL_Query, field: *const c_char) -> i32 {
    // if query.is_null() {
    //     return set_last_error("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query.query.clone().select(&field);
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e), };
    let query = unsafe { &mut *query };
    
    // FIX: Push directly
    query.query.projection.push(field);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_execute(engine: *mut FL_Engine, query: *const FL_Query) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error("null engine/query handle");
            return ptr::null_mut();
        }

        let engine = unsafe { &mut *engine };
        let query = unsafe { &*query };

        let query_obj = query.query.clone();
        let rows_res: Result<Vec<serde_json::Value>, String> = if query_obj.projection.is_empty() {
            engine
                .db
                .query(query_obj)
                .map_err(|e| e.to_string())
                .and_then(|rows| {
                    rows.into_iter()
                        .map(|(_, doc)| {
                            doc_to_json(&doc).and_then(|s| {
                                serde_json::from_str::<serde_json::Value>(&s)
                                    .map_err(|e| e.to_string())
                            })
                        })
                        .collect()
                })
        } else {
            engine
                .db
                .query_projected_zero_copy(query_obj.clone(), &query_obj.projection)
                .map_err(|e| e.to_string())
                .and_then(|rows| {
                    rows.into_iter()
                        .map(|(_, fields)| projection_to_json(fields))
                        .collect()
                })
        };

        match rows_res {
            Ok(arr) => {
                match CString::new(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string()))
                {
                    Ok(s) => {
                        clear_last_error();
                        s.into_raw()
                    }
                    Err(e) => {
                        set_last_error(e.to_string());
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(e);
                ptr::null_mut()
            }
        }
    })
}

// #[no_mangle]
// pub extern "C" fn fl_query_execute_to_handles(
//     engine: *mut FL_Engine,
//     query: *const FL_Query,
// ) -> *mut FL_ResultSet {
//     safety_shield!(std::ptr::null_mut(), {
//         let engine = unsafe { &*engine };
//         let query_obj = unsafe { &*query };

//         // 1. Run the actual query (Fast logic)
//         let results = engine.db.query(query_obj.query.clone()).unwrap_or_default();

//         // 2. Convert each result into a handle (*mut FL_Doc), just like 'get' does
//         let doc_handles: Vec<*mut FL_Doc> = results
//             .into_iter()
//             .map(|(_id, doc)| Box::into_raw(Box::new(FL_Doc { doc })))
//             .collect();

//         // 3. Wrap the list of handles in a ResultSet handle
//         Box::into_raw(Box::new(FL_ResultSet { docs: doc_handles }))
//     })
// }

#[no_mangle]
pub extern "C" fn fl_query_execute_to_handles(
    engine: *mut FL_Engine,
    query: *const FL_Query,
) -> *mut FL_ResultSet {
    safety_shield!(std::ptr::null_mut(), {
        let engine = unsafe { &*engine };
        let query_obj = unsafe { &*query };
        let results = engine.db.query(query_obj.query.clone()).unwrap_or_default();

        // PONYTAIL: store FL_Doc values directly in the ResultSet slab
        // (one allocation) instead of N individual Box::new(FL_Doc) per
        // result. fl_result_set_get_doc returns a borrowed pointer into
        // the slab; fl_result_set_free drops the whole Vec in one shot.
        // The C++ side already treats returned handles as borrowed (it
        // calls fl_doc_to_json or reads fields, never frees them itself),
        // so this is safe as long as fl_result_set_free is called before
        // the handles go out of scope.
        let docs: Vec<FL_Doc> = results
            .into_iter()
            .map(|(id, doc)| FL_Doc { id, doc })
            .collect();
        Box::into_raw(Box::new(FL_ResultSet { docs }))
    })
}

#[no_mangle]
pub extern "C" fn fl_result_set_count(results: *mut FL_ResultSet) -> usize {
    if results.is_null() { return 0; }
    unsafe {
        // results.as_ref() returns Option<&FL_ResultSet>
        results.as_ref().map(|rs| rs.docs.len()).unwrap_or(0)
    }
}

#[no_mangle]
pub extern "C" fn fl_result_set_get_doc(results: *mut FL_ResultSet, index: usize) -> *mut FL_Doc {
    if results.is_null() { return std::ptr::null_mut(); }
    unsafe {
        // Borrowed pointer into the FL_ResultSet's docs slab. The caller
        // MUST not free this handle and MUST call fl_result_set_free
        // before the handle goes out of scope. C++ code already follows
        // this contract (it reads fields or passes the handle to
        // fl_doc_to_json without calling fl_doc_free).
        if let Some(rs) = results.as_ref() {
            rs.docs.get(index)
                .map(|d| d as *const FL_Doc as *mut FL_Doc)
                .unwrap_or(std::ptr::null_mut())
        } else {
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_result_set_free(results: *mut FL_ResultSet) {
    if !results.is_null() {
        // PONYTAIL: docs is now Vec<FL_Doc> (owned values), not Vec<*mut
        // FL_Doc>. Dropping the Box<Vec<FL_Doc>> drops every FL_Doc in
        // one shot — no per-doc Box::from_raw walk needed.
        unsafe { drop(Box::from_raw(results)); }
    }
}

// --- Bulk JSON (ponytail) ---
// Streams a result set as one JSON array string with NO intermediate
// serde_json::Value DOM (the per-doc path builds a full Map DOM per row).
// Output is byte-identical to "[" + fl_doc_to_json(row) joined + "]" —
// locked by `bulk_json_matches_per_doc` below. Notes:
// - serde_json::Map is a BTreeMap (no preserve_order): keys are SORTED, so
//   fields are collected and sorted (one small Vec per doc, still ~15x
//   fewer allocs than the DOM path), including nested Maps.
// - The inner __blob__ meta sorts as {"len","offset"} — emitted in that
//   order explicitly.
// - No `id` field: mirrors fl_doc_to_json exactly (correlate by index,
//   same as fl_result_set_get_doc today).
use serde::ser::{Serialize, Serializer, SerializeMap, SerializeSeq};

struct StreamValue<'a>(&'a Value);
impl<'a> Serialize for StreamValue<'a> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        match self.0 {
            Value::Null | Value::ServerTimestamp => s.serialize_unit(),
            Value::Bool(b) => s.serialize_bool(*b),
            Value::Int(i) => s.serialize_i64(*i),
            Value::Float(f) => match serde_json::Number::from_f64(*f) {
                Some(n) => n.serialize(s),
                None => s.serialize_unit(),
            },
            Value::String(st) => s.serialize_str(st),
            Value::Binary(bytes) => {
                let mut seq = s.serialize_seq(Some(bytes.len()))?;
                for b in bytes.iter() {
                    seq.serialize_element(&(*b as u64))?;
                }
                seq.end()
            }
            Value::Timestamp(m) => s.serialize_i64(*m),
            Value::Reference { collection, doc_id } => {
                let mut m = s.serialize_map(Some(1))?;
                m.serialize_entry("__ref__", &format!("{collection}/{doc_id}"))?;
                m.end()
            }
            Value::Map(fields) => {
                let mut pairs: Vec<(&str, &Value)> =
                    fields.iter().map(|(k, v)| (AsRef::<str>::as_ref(k), v)).collect();
                pairs.sort_by(|a, b| a.0.cmp(b.0));
                let mut m = s.serialize_map(Some(pairs.len()))?;
                for (k, v) in pairs {
                    m.serialize_entry(k, &StreamValue(v))?;
                }
                m.end()
            }
            Value::BlobLink { offset, len } => {
                let mut m = s.serialize_map(Some(1))?;
                // Inner keys sorted to match BTreeMap output: len < offset.
                let inner = BlobMeta { len: *len, offset: *offset };
                m.serialize_entry("__blob__", &inner)?;
                m.end()
            }
            Value::Array(items) => {
                let mut seq = s.serialize_seq(Some(items.len()))?;
                for v in items.iter() {
                    seq.serialize_element(&StreamValue(v))?;
                }
                seq.end()
            }
        }
    }
}

#[derive(serde::Serialize)]
struct BlobMeta {
    len: u32,
    offset: u64,
}

struct SlabJson<'a>(&'a [FL_Doc]);

/// One document's fields as a sorted map (mirrors `doc_to_json` shape).
struct DocFields<'a>(&'a FireLiteDoc);
impl<'a> Serialize for DocFields<'a> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut pairs: Vec<(&str, &Value)> = self
            .0
            .fields
            .iter()
            .map(|(k, v)| (AsRef::<str>::as_ref(k), v))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let mut m = s.serialize_map(Some(pairs.len()))?;
        for (k, v) in pairs {
            m.serialize_entry(k, &StreamValue(v))?;
        }
        m.end()
    }
}

impl<'a> Serialize for SlabJson<'a> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for row in self.0.iter() {
            seq.serialize_element(&DocFields(&row.doc))?;
        }
        seq.end()
    }
}

/// Bulk result-set to JSON: one call, one JSON array string, no per-doc
/// DOM and no per-doc FFI round trips. Byte-identical to joining
/// `fl_doc_to_json` per row. Caller frees with `fl_string_free`.
#[no_mangle]
pub extern "C" fn fl_result_set_to_json(results: *mut FL_ResultSet) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if results.is_null() {
            set_last_error("null result set handle");
            return ptr::null_mut();
        }
        let rs = unsafe { &*results };
        match serde_json::to_string(&SlabJson(&rs.docs)) {
            Ok(s) => match CString::new(s) {
                Ok(c) => {
                    clear_last_error();
                    c.into_raw()
                }
                Err(e) => {
                    set_last_error(e.to_string());
                    ptr::null_mut()
                }
            },
            Err(e) => {
                set_last_error(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_doc_to_json(doc: *const FL_Doc) -> *mut c_char {
    if doc.is_null() {
        set_last_error("null doc handle");
        return ptr::null_mut();
    }

    let doc = unsafe { &*doc };
    match doc_to_json(&doc.doc).and_then(|s| CString::new(s).map_err(|e| e.to_string())) {
        Ok(s) => {
            clear_last_error();
            s.into_raw()
        }
        Err(e) => {
            set_last_error(e);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    })
}

/// Enable library diagnostic logging to stderr. Default OFF. Idempotent.
#[no_mangle]
pub extern "C" fn fl_log_enable_stderr() {
    crate::util::log::enable_stderr();
}

/// Disable library diagnostic logging to stderr. Default OFF. Idempotent.
#[no_mangle]
pub extern "C" fn fl_log_disable_stderr() {
    crate::util::log::disable_stderr();
}

#[no_mangle]
pub extern "C" fn fl_string_free(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            let _ = CString::from_raw(value);
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_query_aggregate_count(query: *mut FL_Query) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Count);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_aggregate_sum(query: *mut FL_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(s) => s,
        Err(e) => return set_last_error(e),
    };
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Sum(field));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_aggregate_avg(query: *mut FL_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(s) => s,
        Err(e) => return set_last_error(e),
    };
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Avg(field));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_execute_aggregation(
    engine: *mut FL_Engine,
    query: *const FL_Query,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error("null engine or query handle");
            return std::ptr::null_mut();
        }

        let engine = unsafe { &*engine };
        let query_wrapper = unsafe { &*query };

        // Call the public method on FireLite.
        // This performs planning and parallel execution inside the Rust core.
        match engine.db.execute_aggregation(query_wrapper.query.clone()) {
            Ok(result) => {
                let res_map: HashMap<String, f64> = result;

                match serde_json::to_string(&res_map) {
                    Ok(json) => match CString::new(json) {
                        Ok(c_str) => {
                            clear_last_error();
                            c_str.into_raw()
                        }
                        Err(e) => {
                            set_last_error(e.to_string());
                            std::ptr::null_mut()
                        }
                    },
                    Err(e) => {
                        set_last_error(e.to_string());
                        std::ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_timestamp(
    doc: *mut FL_Doc,
    key: *const c_char,
    micros: i64,
) -> i32 {
    if doc.is_null() {
        return set_last_error("null doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Timestamp(micros));
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_server_timestamp(doc: *mut FL_Doc, key: *const c_char) -> i32 {
    if doc.is_null() {
        return set_last_error("null doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::ServerTimestamp);
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_backup(engine: *mut FL_Engine, path: *const c_char) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() {
            return set_last_error("null engine");
        }
        let engine = unsafe { &*engine };
        let path = match cstr_to_string(path) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        match engine.db.backup(path) {
            Ok(_) => 0,
            Err(e) => {
                set_last_error(e.to_string());
                -1 // or ptr::null_mut() depending on function return type
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_query_where_match(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // let f = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let v = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let q = unsafe { &mut *query };
    // q.query = q
    //     .query
    //     .clone()
    //     .where_filter(&f, Operator::Match, Value::String(v));
    // 0
    apply_string_filter(query, field, value, Operator::Match)
}

#[no_mangle]
pub extern "C" fn fl_query_where_match_prefix(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::MatchPrefix)
}

#[no_mangle]
pub extern "C" fn fl_query_where_contains(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // let f = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let v = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let q = unsafe { &mut *query };
    // q.query = q
    //     .query
    //     .clone()
    //     .where_filter(&f, Operator::Contains, Value::String(v));
    // 0
    apply_string_filter(query, field, value, Operator::Contains)
}

#[no_mangle]
pub extern "C" fn fl_query_where_starts_with(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // let f = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let v = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error(e),
    // };
    // let q = unsafe { &mut *query };
    // q.query = q
    //     .query
    //     .clone()
    //     .where_filter(&f, Operator::StartsWith, Value::String(v));
    // 0
    apply_string_filter(query, field, value, Operator::StartsWith)
}

#[no_mangle]
pub extern "C" fn fl_engine_list_collections(engine: *mut FL_Engine) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error("null engine handle");
            return std::ptr::null_mut();
        }

        let engine = unsafe { &*engine };
        match engine.db.list_collections() {
            Ok(cols) => {
                // Serialize the Vec<String> to a JSON array: ["users", "posts"]
                let json = serde_json::to_string(&cols).unwrap_or_else(|_| "[]".to_string());
                match CString::new(json) {
                    Ok(c_str) => {
                        clear_last_error();
                        c_str.into_raw()
                    }
                    Err(_) => std::ptr::null_mut(),
                }
            }
            Err(e) => {
                set_last_error(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_array_new() -> *mut FL_Array {
    Box::into_raw(Box::new(FL_Array { items: Vec::new() }))
}

#[no_mangle]
pub extern "C" fn fl_array_free(array: *mut FL_Array) {
    if !array.is_null() {
        unsafe { drop(Box::from_raw(array)) };
    }
}

// --- ARRAY PUSH METHODS ---
#[no_mangle]
pub extern "C" fn fl_array_append_str(array: *mut FL_Array, value: *const c_char) -> i32 {
    let s = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    unsafe {
        (*array).items.push(Value::String(s));
    }
    0
}

#[no_mangle]
pub extern "C" fn fl_array_append_int(array: *mut FL_Array, value: i64) -> i32 {
    unsafe {
        (*array).items.push(Value::Int(value));
    }
    0
}

#[no_mangle]
pub extern "C" fn fl_array_append_doc(array: *mut FL_Array, doc: *const FL_Doc) -> i32 {
    safety_shield!(-1, {
        if array.is_null() || doc.is_null() {
            return set_last_error("null array or doc handle");
        }

        let array_ptr = unsafe { &mut *array };
        let doc_ptr = unsafe { &*doc };

        // Convert the document's fields into a Value::Map and push to the array
        array_ptr.items.push(Value::Map(doc_ptr.doc.fields.clone()));

        clear_last_error();
        0
    })
}

// --- DOCUMENT NESTING METHODS ---

/// Takes the contents of 'child' and inserts it as a Map into 'parent'
#[no_mangle]
pub extern "C" fn fl_doc_insert_doc(
    parent: *mut FL_Doc,
    key: *const c_char,
    child: *const FL_Doc,
) -> i32 {
    safety_shield!(-1, {
        let key = match cstr_to_string(key) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let parent_doc = unsafe { &mut *parent };
        let child_doc = unsafe { &*child };

        // Convert the child document's fields into a Value::Map
        parent_doc
            .doc
            .insert(key, Value::Map(child_doc.doc.fields.clone()));
        0
    })
}

/// Takes the contents of 'array' and inserts it into the document
#[no_mangle]
pub extern "C" fn fl_doc_insert_array(
    doc: *mut FL_Doc,
    key: *const c_char,
    array: *mut FL_Array,
) -> i32 {
    safety_shield!(-1, {
        let key = match cstr_to_string(key) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let doc = unsafe { &mut *doc };
        let array_inner = unsafe { Box::from_raw(array) }; // Take ownership and free FL_Array

        doc.doc.insert(key, Value::Array(array_inner.items));
        0
    })
}

#[no_mangle]
pub extern "C" fn fl_engine_patch(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    updates: *const FL_Doc,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || updates.is_null() {
            return -1;
        }
        let engine = unsafe { &*engine };
        let col = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error(e),
        };
        let update_doc = unsafe { &*updates };

        let mut updates_vec: Vec<(String, Value)> = Vec::new();
        for (k, v) in &update_doc.doc.fields {
            updates_vec.push((k.to_string(), v.clone()));
        }

        // FIX: Access .db and ensure set_last_error returns correctly
        match engine.db.patch(&col, &id, updates_vec) {
            Ok(_) => 0,
            Err(e) => {
                set_last_error(e.to_string());
                -1
            }
        }
    })
}

// --- 1. INDEX MANAGEMENT ---

/// Creates a composite index from C++.
/// fields_json should be like: [{"field": "age", "desc": false}]
#[no_mangle]
pub extern "C" fn fl_engine_create_index(
    engine: *mut FL_Engine,
    collection: *const c_char,
    fields_json: *const c_char,
) -> u32 {
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return 0,
    };
    let json_str = match cstr_to_string(fields_json) {
        Ok(v) => v,
        Err(_) => return 0,
    };

    // Parse the JSON into the SortDirection vector
    let Ok(fields_raw): std::result::Result<Vec<serde_json::Value>, _> =
        serde_json::from_str(&json_str)
    else {
        return 0;
    };

    let mut fields = Vec::new();
    for item in fields_raw {
        let f = item["field"].as_str().unwrap_or("").to_string();
        let desc = item["desc"].as_bool().unwrap_or(false);
        let dir = if desc {
            SortDirection::Desc
        } else {
            SortDirection::Asc
        };
        fields.push((f, dir));
    }

    match engine.db.create_composite_index(&col, fields) {
        Ok(id) => id,
        Err(e) => {
            set_last_error(e.to_string());
            0 // Return 0 to indicate failure
        }
    }
}

/// Simplified indexer: Create an index for a single field.
#[no_mangle]
pub extern "C" fn fl_engine_create_simple_index(
    engine: *mut FL_Engine,
    collection: *const c_char,
    field: *const c_char,
) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_e) => return -1,
    };
    let fld = match cstr_to_string(field) {
        Ok(v) => v,
        Err(_e) => return -1,
    };

    match engine.db.create_index(&col, &fld) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn fl_engine_create_fts_index(
    engine: *mut FL_Engine,
    collection: *const c_char,
    field: *const c_char,
) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let fld = match cstr_to_string(field) {
        Ok(v) => v,
        Err(_) => return -1,
    };

    match engine.db.create_fts_index(&col, &fld) {
        Ok(_) => 0,
        Err(_) => -1,
    }
}

#[no_mangle]
pub extern "C" fn fl_engine_list_indexes(
    engine: *mut FL_Engine,
    collection: *const c_char,
) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let collection = if collection.is_null() {
            None
        } else {
            match cstr_to_string(collection) {
                Ok(v) => Some(v),
                Err(e) => {
                    set_last_error(e);
                    return ptr::null_mut();
                }
            }
        };

        let indexes = engine.db.list_indexes(collection.as_deref());
        match serde_json::to_string(&indexes)
            .ok()
            .and_then(|s| CString::new(s).ok())
        {
            Some(json) => json.into_raw(),
            None => ptr::null_mut(),
        }
    })
}

// --- 2. SERIALIZABLE TRANSACTIONS (Read-Modify-Write) ---

#[no_mangle]
pub extern "C" fn fl_transaction_begin(engine: *mut FL_Engine) -> *mut FL_Transaction {
    safety_shield!(ptr::null_mut(), {
        let engine = unsafe { &*engine };
        Box::into_raw(Box::new(FL_Transaction {
            tx: engine.db.begin_serializable_transaction(),
        }))
    })
}

#[no_mangle]
pub extern "C" fn fl_transaction_get(
    engine: *mut FL_Engine,
    tx: *mut FL_Transaction,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut FL_Doc {
    let engine = unsafe { &*engine };
    let tx = unsafe { &mut *tx };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };
    let id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };

    match tx.tx.get(&engine.db, &col, &id) {
        Ok(Some(doc)) => Box::into_raw(Box::new(FL_Doc { doc, id: id.clone() })),
        _ => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn fl_transaction_set(
    tx: *mut FL_Transaction,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *const FL_Doc,
) -> i32 {
    let tx = unsafe { &mut *tx };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let doc = unsafe { &*doc };

    tx.tx.put(&col, &id, doc.doc.clone());
    0
}

#[no_mangle]
pub extern "C" fn fl_transaction_commit(engine: *mut FL_Engine, tx: *mut FL_Transaction) -> i32 {
    safety_shield!(-1, {
        let engine = unsafe { &*engine };
        // Borrow only: the caller still owns the handle and must release it
        // with fl_transaction_free (matching the fl_batch_commit contract).
        let tx = unsafe { &*tx };

        match tx.tx.commit(&engine.db) {
            Ok(_) => 0,
            Err(e) => {
                set_last_error(e.to_string());
                -1 // or ptr::null_mut() depending on function return type
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_transaction_free(tx: *mut FL_Transaction) {
    if !tx.is_null() {
        unsafe { drop(Box::from_raw(tx)) };
    }
}

// --- 3. SUBCOLLECTION HELPERS ---

#[no_mangle]
pub extern "C" fn fl_engine_insert_subdoc(
    engine: *mut FL_Engine,
    col: *const c_char,
    id: *const c_char,
    sub_col: *const c_char,
    sub_id: *const c_char,
    doc: *const FL_Doc,
) -> i32 {
    let engine = unsafe { &*engine };
    let c = match cstr_to_string(col) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let i = match cstr_to_string(id) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let sc = match cstr_to_string(sub_col) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let si = match cstr_to_string(sub_id) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    let d = unsafe { &*doc };

    match engine.db.put_subdocument(&c, &i, &sc, &si, &d.doc) {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1 // or ptr::null_mut() depending on function return type
        }
    }
}

// --- 4. DIAGNOSTICS & MAINTENANCE ---

#[no_mangle]
pub extern "C" fn fl_engine_compact(engine: *mut FL_Engine) -> i32 {
    safety_shield!(-1, {
        let engine = unsafe { &*engine };
        match engine.db.compact() {
            Ok(_) => 0,
            Err(e) => {
                set_last_error(e.to_string());
                -1 // or ptr::null_mut() depending on function return type
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_engine_get_stats(engine: *mut FL_Engine) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        let engine = unsafe { &*engine };
        let stats = engine.db.get_stats();

        let json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
        match CString::new(json) {
            Ok(s) => s.into_raw(),
            Err(_) => ptr::null_mut(),
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_reference(
    doc: *mut FL_Doc,
    key: *const c_char,
    target_collection: *const c_char,
    target_id: *const c_char,
) -> i32 {
    if doc.is_null() {
        return -1;
    }
    let key_str = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let col = match cstr_to_string(target_collection) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let id = match cstr_to_string(target_id) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    let doc_ptr = unsafe { &mut *doc };
    doc_ptr.doc.insert(
        key_str,
        Value::Reference {
            collection: col,
            doc_id: id,
        },
    );
    0
}

/// Given a document and a field name containing a Reference, fetch the target document.
/// Returns a new FL_Doc handle, or null if the field is not a reference or target not found.
#[no_mangle]
pub extern "C" fn fl_engine_get_by_ref(
    engine: *mut FL_Engine,
    doc: *const FL_Doc,
    field_key: *const c_char,
) -> *mut FL_Doc {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || doc.is_null() || field_key.is_null() {
            return ptr::null_mut();
        }

        let engine = unsafe { &*engine };
        let doc_ptr = unsafe { &*doc };
        let key = match cstr_to_string(field_key) {
            Ok(k) => k,
            Err(e) => {
                set_last_error(e);
                return ptr::null_mut();
            }
        };

        // 1. Find the value in the provided document
        let Some(val) = doc_ptr.doc.get(&key) else {
            set_last_error("Field not found in document");
            return ptr::null_mut();
        };

        // 2. Resolve the reference using the engine
        match engine.db.get_by_reference(val) {
            Ok(Some(target_doc)) => Box::into_raw(Box::new(FL_Doc { doc: target_doc, id: doc_ptr.id.clone() })),
            Ok(None) => ptr::null_mut(), // Document doesn't exist (Dangling reference)
            Err(e) => {
                set_last_error(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

// /// Internal helper to extract values from an anchor document based on the query's sort order
// fn get_anchor_values(q: &crate::query::query::Query, doc: &FireLiteDoc) -> Option<Vec<Value>> {
//     if q.order_by.is_empty() { return None; }
    
//     let mut vals = Vec::with_capacity(q.order_by.len());
//     for order in &q.order_by {
//         vals.push(doc.get(&order.field)?.clone());
//     }
//     Some(vals)
// }
// 3. Fix get_anchor_values logic
fn get_anchor_values(q: &crate::query::query::Query, fl_doc: &FL_Doc) -> Option<Vec<Value>> {
    if q.order_by.is_empty() { return None; }
    
    let mut vals = Vec::with_capacity(q.order_by.len());
    for order in &q.order_by {
        // MAGIC FIX: Extract internal metadata manually
        if order.field == "id" {
            vals.push(Value::String(fl_doc.id.clone()));
        } else if order.field == "_time" {
            vals.push(Value::Int(fl_doc.doc._time));
        } else {
            vals.push(fl_doc.doc.get(&order.field)?.clone());
        }
    }
    Some(vals)
}

// #[no_mangle]
// pub extern "C" fn fl_query_start_after(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> i32 {
//     safety_shield!(-1, {
//         if query.is_null() || anchor_doc.is_null() { return -1; }
//         let q = unsafe { &mut *query };
//         let doc = unsafe { &*anchor_doc };

//         if let Some(vals) = get_anchor_values(&q.query, &doc.doc) {
//             q.query.start_after = Some(vals);
//             0
//         } else {
//             set_last_error("Anchor document missing one or more fields from sort chain");
//             -1
//         }
//     })
// }

#[no_mangle]
pub extern "C" fn fl_query_start_after(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() { return -1; }
        let q = unsafe { &mut *query };
        let doc = unsafe { &*anchor_doc };
        
        if let Some(vals) = get_anchor_values(&q.query, doc) {
            q.query.start_after = Some(vals);
            0
        } else {
            set_last_error("Anchor document missing one or more fields from sort chain");
            -1
        }
    })
}

// #[no_mangle]
// pub extern "C" fn fl_query_start_at(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> i32 {
//     safety_shield!(-1, {
//         if query.is_null() || anchor_doc.is_null() {
//             return -1;
//         }
//         let q = unsafe { &mut *query };
//         let doc = unsafe { &*anchor_doc };
//         if let Some(vals) = get_anchor_values(&q.query, &doc.doc) {
//             q.query.start_at = Some(vals);
//             0
//         } else {
//             set_last_error("Anchor document missing sort field");
//             -1
//         }
//     })
// }
// 4. Update pointer usages for the anchor logic
#[no_mangle]
pub extern "C" fn fl_query_start_at(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() { return -1; }
        let q = unsafe { &mut *query };
        let doc = unsafe { &*anchor_doc };
        
        if let Some(vals) = get_anchor_values(&q.query, doc) {
            q.query.start_at = Some(vals);
            0
        } else {
            set_last_error("Anchor document missing one or more fields from sort chain");
            -1
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_query_end_at(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() {
            return -1;
        }
        let q = unsafe { &mut *query };
        let doc = unsafe { &*anchor_doc };
        if let Some(vals) = get_anchor_values(&q.query, doc) {
            q.query.end_at = Some(vals);
            0
        } else {
            set_last_error("Anchor document missing sort field");
            -1
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_query_end_before(query: *mut FL_Query, anchor_doc: *const FL_Doc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() {
            return -1;
        }
        let q = unsafe { &mut *query };
        let doc = unsafe { &*anchor_doc };
        if let Some(vals) = get_anchor_values(&q.query, doc) {
            q.query.end_before = Some(vals);
            0
        } else {
            set_last_error("Anchor document missing sort field");
            -1
        }
    })
}

#[no_mangle]
pub extern "C" fn fl_engine_get_audit_log(engine: *mut FL_Engine) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            return std::ptr::null_mut();
        }
        let engine = unsafe { &*engine };

        let entries = engine.db.audit_entries();

        // Convert to JSON
        // Note: Ensure AuditEntry and AccessOp derive serde::Serialize
        let json = serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string());

        match CString::new(json) {
            Ok(s) => s.into_raw(),
            Err(_) => std::ptr::null_mut(),
        }
    })
}

// --- OR LOGIC ---

#[no_mangle]
pub extern "C" fn fl_query_where_or_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let v = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    // Each call to fl_query_where_or creates a new standalone OR group
    q.query.or_groups.push(vec![crate::query::filter::Filter {
        field: f,
        op: Operator::Eq,
        value: Value::String(v),
    }]);
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_or_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    q.query.or_groups.push(vec![crate::query::filter::Filter {
        field: f,
        op: Operator::Eq,
        value: Value::Int(value),
    }]);
    0
}

// --- IN LOGIC ---

/// Adds an IN filter: field IN [array_items]
/// This takes ownership of the FL_Array and frees it.
#[no_mangle]
pub extern "C" fn fl_query_where_in(
    query: *mut FL_Query,
    field: *const c_char,
    array: *mut FL_Array,
) -> i32 {
    if query.is_null() || array.is_null() {
        return -1;
    }
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };

    // Take the items from the FFI array and destroy the handle
    let array_inner = unsafe { Box::from_raw(array) };

    q.query.filters.push(crate::query::filter::Filter {
        field: f,
        op: Operator::In,
        value: Value::Array(array_inner.items),
    });
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_snapshot_indices(engine: *mut FL_Engine) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };

    match engine.db.save_index_snapshots() {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_config_set_compression(config: *mut FL_Config, enabled: bool, level: i32) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.use_compression = enabled;
        cfg.inner.compression_level = level;
    }
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn fl_net_syncer_new(
    engine: *mut FL_Engine,
    name: *const c_char,
    room_key: *const c_char,
) -> *mut FL_NetSyncer {
    let engine_ref = unsafe { &*engine };
    let name_str = cstr_to_string(name).unwrap_or_else(|_| "node".into());
    let room_str = cstr_to_string(room_key).unwrap_or_else(|_| "default".into());

    // We need to clone the Arc<FireLite> logically. 
    // Since FL_Engine wraps FireLite (which is not an Arc inside FL_Engine), 
    // we use a temporary wrap to pass it to the syncer.
    // let db_ptr:std::sync::Arc<FireLite> = unsafe { std::sync::Arc::from_raw(&engine_ref.db as *const _) };
    let syncer = crate::net_sync::NetSyncer::new(
        engine_ref.db.clone(),
        &name_str,
        &room_str,
        vec![],
    );
    // Important: Forget the raw pointer so we don't drop the engine!
    // std::mem::forget(db_ptr);

    clear_last_error();
    Box::into_raw(Box::new(FL_NetSyncer {
        inner: std::sync::Arc::new(syncer),
    }))
}

/// Select discovery transports: 0 = mDNS (desktop default), 1 = UDP
/// broadcast (mobile default, no multicast), 2 = both (mixed groups — a
/// desktop joining mobile peers must opt into both or broadcast).
/// Takes effect at the next start().
#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn fl_net_syncer_set_discovery(syncer: *mut FL_NetSyncer, mode: i32) -> i32 {
    if syncer.is_null() { return -1; }
    let s_ref = unsafe { &*syncer };
    let m = match mode {
        0 => crate::net_sync::DiscoveryMode::Mdns,
        1 => crate::net_sync::DiscoveryMode::Broadcast,
        2 => crate::net_sync::DiscoveryMode::Both,
        _ => return set_last_error("invalid discovery mode (0=mdns, 1=broadcast, 2=both)"),
    };
    s_ref.inner.set_discovery(m);
    clear_last_error();
    0
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn fl_net_syncer_start(syncer: *mut FL_NetSyncer, port: u16) -> i32 {
    if syncer.is_null() { return -1; }
    let s_ref = unsafe { &*syncer };
    let inner = s_ref.inner.clone();

    // Use block_on to bridge synchronous FFI to the async start method
    let rt = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => return set_last_error("No tokio runtime found"),
    };

    match rt.block_on(async move { inner.start(port).await }) {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn fl_net_syncer_status(syncer: *mut FL_NetSyncer) -> *mut c_char {
    if syncer.is_null() { return ptr::null_mut(); }
    let s_ref = unsafe { &*syncer };
    let status = s_ref.inner.status();
    
    match serde_json::to_string(&status) {
        Ok(json) => CString::new(json).unwrap().into_raw(),
        Err(_) => ptr::null_mut()
    }
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn fl_net_syncer_free(syncer: *mut FL_NetSyncer) {
    if !syncer.is_null() {
        let s = unsafe { Box::from_raw(syncer) };
        s.inner.stop();
    }
}


// CLOUD SYNC
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_new(
    engine: *mut FL_Engine,
    mode: i32, // 0 = Server, 1 = Client
    client_id: *const c_char,
    room_name: *const c_char,
    room_key: *const c_char,
    auth_token: *const c_char,
) -> *mut FL_CloudSync {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error("Null engine handle");
            return ptr::null_mut();
        }

        let engine_ref = unsafe { &*engine };
        let cid_str = cstr_to_string(client_id).unwrap_or_else(|_| "node".into());
        let room_name_str = cstr_to_string(room_name).unwrap_or_else(|_| "default".into());
        let room_str = cstr_to_string(room_key).unwrap_or_else(|_| "default".into());
        let token_str = cstr_to_string(auth_token).unwrap_or_default();

        let sync_mode = match mode {
            0 => crate::cloud_sync::CloudSyncMode::Server,
            _ => crate::cloud_sync::CloudSyncMode::Client,
        };

        // 100% Safe Arc cloning (No UB / No Arc::from_raw hack)
        let cloud_sync = crate::cloud_sync::CloudSync::new(
            engine_ref.db.clone(),
            sync_mode,
            &cid_str,
            &room_name_str,
            &room_str,
            &token_str,
        );

        clear_last_error();
        Box::into_raw(Box::new(FL_CloudSync {
            inner: std::sync::Arc::new(cloud_sync),
        }))
    })
}

/// Creates a room-agnostic cloud SERVER. Not bound to any room: the server
/// accepts and persists any (room_name, room_key) pair its clients ask for and
/// routes sync to the matching room group.
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_server_new(
    engine: *mut FL_Engine,
    server_id: *const c_char,
    auth_token: *const c_char,
) -> *mut FL_CloudSync {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error("Null engine handle");
            return ptr::null_mut();
        }

        let engine_ref = unsafe { &*engine };
        let sid_str = cstr_to_string(server_id).unwrap_or_else(|_| "server".into());
        let token_str = cstr_to_string(auth_token).unwrap_or_default();

        let cloud_sync =
            crate::cloud_sync::CloudSync::server(engine_ref.db.clone(), &sid_str, &token_str);

        clear_last_error();
        Box::into_raw(Box::new(FL_CloudSync {
            inner: std::sync::Arc::new(cloud_sync),
        }))
    })
}

/// Creates an offline-first cloud CLIENT bound to a room of the caller's
/// choosing. The client picks the room (room_name + room_key) and later picks
/// the server via `fl_cloud_sync_start`.
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_client_new(
    engine: *mut FL_Engine,
    client_id: *const c_char,
    room_name: *const c_char,
    room_key: *const c_char,
    auth_token: *const c_char,
) -> *mut FL_CloudSync {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error("Null engine handle");
            return ptr::null_mut();
        }

        let engine_ref = unsafe { &*engine };
        let cid_str = cstr_to_string(client_id).unwrap_or_else(|_| "node".into());
        let room_name_str = cstr_to_string(room_name).unwrap_or_else(|_| "default".into());
        let room_str = cstr_to_string(room_key).unwrap_or_else(|_| "default".into());
        let token_str = cstr_to_string(auth_token).unwrap_or_default();

        let cloud_sync = crate::cloud_sync::CloudSync::client(
            engine_ref.db.clone(),
            &cid_str,
            &room_name_str,
            &room_str,
            &token_str,
        );

        clear_last_error();
        Box::into_raw(Box::new(FL_CloudSync {
            inner: std::sync::Arc::new(cloud_sync),
        }))
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_start(cloud_sync: *mut FL_CloudSync, address: *const c_char) -> i32 {
    safety_shield!(-1, {
        if cloud_sync.is_null() {
            return set_last_error("Null cloud_sync handle");
        }
        let cs_ref = unsafe { &*cloud_sync };
        let addr_str = match cstr_to_string(address) {
            Ok(a) => a,
            Err(e) => return set_last_error(e),
        };
        let inner = cs_ref.inner.clone();

        // Support both existing Tokio threads and plain C/C++ threads
        let run_sync = async move { inner.start(&addr_str).await };

        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            match handle.block_on(run_sync) {
                Ok(_) => {
                    clear_last_error();
                    0
                }
                Err(e) => set_last_error(e.to_string()),
            }
        } else {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(r) => r,
                Err(e) => return set_last_error(format!("Failed to create Tokio runtime: {}", e)),
            };
            match rt.block_on(run_sync) {
                Ok(_) => {
                    clear_last_error();
                    0
                }
                Err(e) => set_last_error(e.to_string()),
            }
        }
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_status(cloud_sync: *mut FL_CloudSync) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if cloud_sync.is_null() {
            set_last_error("Null cloud_sync handle");
            return ptr::null_mut();
        }
        let cs_ref = unsafe { &*cloud_sync };
        // Returns JSON string representation of current CloudStatus
        let status = cs_ref.inner.status();
        match serde_json::to_string(&status) {
            Ok(json) => match CString::new(json) {
                Ok(c_str) => {
                    clear_last_error();
                    c_str.into_raw()
                }
                Err(e) => {
                    set_last_error(e.to_string());
                    ptr::null_mut()
                }
            },
            Err(e) => {
                set_last_error(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_stop(cloud_sync: *mut FL_CloudSync) {
    safety_shield!((), {
        if !cloud_sync.is_null() {
            let cs_ref = unsafe { &*cloud_sync };
            cs_ref.inner.stop();
        }
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn fl_cloud_sync_free(cloud_sync: *mut FL_CloudSync) {
    safety_shield!((), {
        if !cloud_sync.is_null() {
            let cs = unsafe { Box::from_raw(cloud_sync) };
            cs.inner.stop();
        }
    })
}

#[cfg(test)]
mod ffi_json_tests {
    use super::*;

    /// The streaming `doc_to_json` must emit byte-identical output to the
    /// old Box-everything-into-serde-Value approach (BTreeMap = sorted
    /// keys), including Binary byte arrays and escaping.
    #[test]
    fn doc_to_json_matches_serde_map_output() {
        let mut doc = FireLiteDoc::default();
        // Deliberately unsorted insertion + escaping-sensitive strings.
        doc.insert("v", Value::Binary(vec![0u8, 1, 9, 10, 99, 100, 171, 255]));
        doc.insert("z", Value::Int(-42));
        doc.insert("a", Value::String("q\"\\qé".to_string()));
        doc.insert("m", Value::Bool(true));

        let mut reference = serde_json::Map::new();
        for (k, v) in &doc.fields {
            reference.insert(k.to_string(), value_to_json(v));
        }
        let expected = serde_json::to_string(&serde_json::Value::Object(reference)).unwrap();

        assert_eq!(doc_to_json(&doc).unwrap(), expected);
        // Spot-check the binary arm shape (no spaces, plain digits).
        assert!(expected.contains("\"v\":[0,1,9,10,99,100,171,255]"));
    }
}
