use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::{ptr, thread};
use std::time::Duration;
use std::sync::mpsc::{channel, Sender};

use hashbrown::HashMap; 

use crate::config::{FireLiteConfig, DurabilityMode};
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::engine::{BatchMutation, FireLite};
use crate::query::filter::Operator;
use crate::query::query::{Query, AggregateOp};
use crate::index::composite::definition::SortDirection;
// use crate::query::planner::QueryPlanner;

#[allow(non_camel_case_types)]
pub struct FL_Engine {
    db: FireLite,
}

#[allow(non_camel_case_types)]
pub struct FL_Doc {
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
    user_data: *mut std::ffi::c_void
);

#[allow(non_camel_case_types)]
pub struct FL_Array {
    pub items: Vec<Value>,
}

#[allow(non_camel_case_types)]
pub struct FL_Transaction {
    pub tx: crate::engine::SerializableTransaction,
}

// struct SendPtr(*mut std::ffi::c_void);
// unsafe impl Send for SendPtr {}
// unsafe impl Sync for SendPtr {} 

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
    match v {
        Value::Null => serde_json::Value::Null,
        Value::Bool(v) => serde_json::Value::Bool(*v),
        Value::Int(v) => serde_json::Value::Number((*v).into()),
        Value::Float(v) => serde_json::Number::from_f64(*v)
            .map(serde_json::Value::Number)
            .unwrap_or(serde_json::Value::Null),
        Value::String(v) => serde_json::Value::String(v.clone()),
        Value::Binary(v) => serde_json::Value::Array(
            v.iter().map(|b| serde_json::Value::Number((*b as u64).into())).collect()
        ),
        Value::Timestamp(v) => serde_json::Value::Number((*v).into()),
        Value::Array(items) => { // <--- ADD THIS
            serde_json::Value::Array(items.iter().map(value_to_json).collect())
        },
        Value::Map(fields) => {
            let mut map = serde_json::Map::new();
            for (k, sv) in fields {
                map.insert(k.clone(), value_to_json(sv));
            }
            serde_json::Value::Object(map)
        },
        Value::Reference { collection, doc_id } => {
            let mut map = serde_json::Map::new();
            map.insert("__ref__".to_string(), serde_json::Value::String(format!("{}/{}", collection, doc_id)));
            serde_json::Value::Object(map)
        },
        Value::ServerTimestamp => serde_json::Value::Null,
    }
}

fn doc_to_json(doc: &FireLiteDoc) -> Result<String, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in &doc.fields {
        map.insert(k.clone(), value_to_json(v));
    }
    serde_json::to_string(&serde_json::Value::Object(map)).map_err(|e| e.to_string())
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
            Box::into_raw(Box::new(FL_Engine { db }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
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

#[no_mangle]
pub extern "C" fn fl_config_set_audit_log(config: *mut FL_Config, enabled: bool, path: *const c_char) {
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
    max_inlined_bytes: usize
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

/// Opens the engine using a custom config. 
/// Note: This function takes ownership of the config and will free it automatically.
#[no_mangle]
pub extern "C" fn fl_engine_open_with_config(path: *const c_char, config: *mut FL_Config) -> *mut FL_Engine {
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
            Box::into_raw(Box::new(FL_Engine { db }))
        }
        Err(e) => {
            set_last_error(e.to_string());
            std::ptr::null_mut()
        }
    }
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
    if engine.is_null() { return std::ptr::null_mut(); }
    
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
                    let c_path = CString::new(event.path).unwrap();
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
    if !engine.is_null() {
        unsafe { drop(Box::from_raw(engine)) };
    }
}

#[no_mangle]
pub extern "C" fn fl_doc_new() -> *mut FL_Doc {
    Box::into_raw(Box::new(FL_Doc {
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
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => {
    set_last_error(e.to_string());
    -1 // or ptr::null_mut() depending on function return type
},
    }
}

#[no_mangle]
pub extern "C" fn fl_engine_get(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut FL_Doc {
    if engine.is_null() {
        set_last_error("null engine handle");
        return ptr::null_mut();
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(e);
            return ptr::null_mut();
        }
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => {
            set_last_error(e);
            return ptr::null_mut();
        }
    };

    let engine = unsafe { &mut *engine };
    match engine.db.get(&collection, &doc_id) {
        Ok(Some(doc)) => {
            clear_last_error();
            Box::into_raw(Box::new(FL_Doc { doc }))
        }
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
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
},
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
    doc: *const FL_Doc,
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
    let doc = unsafe { &*doc };
    batch.ops.push(BatchMutation::Put {
        collection,
        doc_id,
        doc: doc.doc.clone(),
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
},
    }
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
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let value = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let query = unsafe { &mut *query };
    query.query = query
        .query
        .clone()
        .where_filter(&field, Operator::Eq, Value::String(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_eq_int(
    query: *mut FL_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let query = unsafe { &mut *query };
    query.query = query
        .query
        .clone()
        .where_filter(&field, Operator::Eq, Value::Int(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_order_by(
    query: *mut FL_Query,
    field: *const c_char,
    ascending: bool,
) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let query = unsafe { &mut *query };
    query.query = query.query.clone().order_by(&field, ascending);
    clear_last_error();
    0
}

// #[no_mangle]
// pub extern "C" fn fl_query_limit(query: *mut FL_Query, limit: usize) -> i32 {
//     if query.is_null() {
//         return set_last_error("null query handle");
//     }
//     let query = unsafe { &mut *query };
//     query.query = query.query.clone().limit(limit);
//     clear_last_error();
//     0
// }

#[no_mangle]
pub extern "C" fn fl_query_limit(query: *mut FL_Query, limit: usize) -> i32 {
    if query.is_null() { return -1; }
    let query = unsafe { &mut *query };
    query.query.limit = Some(limit);
    0
}

#[no_mangle]
pub extern "C" fn fl_query_offset(query: *mut FL_Query, offset: usize) -> i32 { // <--- NEW FFI
    if query.is_null() { return -1; }
    let query = unsafe { &mut *query };
    query.query.offset = Some(offset);
    0
}

#[no_mangle]
pub extern "C" fn fl_query_select_field(query: *mut FL_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error(e),
    };
    let query = unsafe { &mut *query };
    query.query = query.query.clone().select(&field);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_execute(engine: *mut FL_Engine, query: *const FL_Query) -> *mut c_char {
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
                            serde_json::from_str::<serde_json::Value>(&s).map_err(|e| e.to_string())
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
            match CString::new(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())) {
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
    if query.is_null() { return set_last_error("null query handle"); }
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Count);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn fl_query_aggregate_sum(query: *mut FL_Query, field: *const c_char) -> i32 {
    if query.is_null() { return set_last_error("null query handle"); }
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
    if query.is_null() { return set_last_error("null query handle"); }
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
    query: *const FL_Query
) -> *mut c_char {
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
                Ok(json) => {
                    match CString::new(json) {
                        Ok(c_str) => {
                            clear_last_error();
                            c_str.into_raw()
                        }
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
        }
        Err(e) => {
            set_last_error(e.to_string());
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_timestamp(doc: *mut FL_Doc, key: *const c_char, micros: i64) -> i32 {
    if doc.is_null() { return set_last_error("null doc"); }
    let key = match cstr_to_string(key) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::Timestamp(micros));
    0
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_server_timestamp(doc: *mut FL_Doc, key: *const c_char) -> i32 {
    if doc.is_null() { return set_last_error("null doc"); }
    let key = match cstr_to_string(key) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let doc = unsafe { &mut *doc };
    doc.doc.insert(key, Value::ServerTimestamp);
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_backup(engine: *mut FL_Engine, path: *const c_char) -> i32 {
    if engine.is_null() { return set_last_error("null engine"); }
    let engine = unsafe { &*engine };
    let path = match cstr_to_string(path) { Ok(v) => v, Err(e) => return set_last_error(e) };
    match engine.db.backup(path) {
        Ok(_) => 0,
        Err(e) => {
    set_last_error(e.to_string());
    -1 // or ptr::null_mut() depending on function return type
},
    }
}

#[no_mangle]
pub extern "C" fn fl_query_where_match(query: *mut FL_Query, field: *const c_char, value: *const c_char) -> i32 {
    let f = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let v = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let q = unsafe { &mut *query };
    q.query = q.query.clone().where_filter(&f, Operator::Match, Value::String(v));
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_contains(query: *mut FL_Query, field: *const c_char, value: *const c_char) -> i32 {
    let f = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let v = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let q = unsafe { &mut *query };
    q.query = q.query.clone().where_filter(&f, Operator::Contains, Value::String(v));
    0
}

#[no_mangle]
pub extern "C" fn fl_query_where_starts_with(query: *mut FL_Query, field: *const c_char, value: *const c_char) -> i32 {
    let f = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let v = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let q = unsafe { &mut *query };
    q.query = q.query.clone().where_filter(&f, Operator::StartsWith, Value::String(v));
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_list_collections(engine: *mut FL_Engine) -> *mut c_char {
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
}

#[no_mangle]
pub extern "C" fn fl_array_new() -> *mut FL_Array {
    Box::into_raw(Box::new(FL_Array { items: Vec::new() }))
}

#[no_mangle]
pub extern "C" fn fl_array_free(array: *mut FL_Array) {
    if !array.is_null() { unsafe { drop(Box::from_raw(array)) }; }
}

// --- ARRAY PUSH METHODS ---
#[no_mangle]
pub extern "C" fn fl_array_append_str(array: *mut FL_Array, value: *const c_char) -> i32 {
    let s = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error(e) };
    unsafe { (*array).items.push(Value::String(s)); }
    0
}

#[no_mangle]
pub extern "C" fn fl_array_append_int(array: *mut FL_Array, value: i64) -> i32 {
    unsafe { (*array).items.push(Value::Int(value)); }
    0
}

// --- DOCUMENT NESTING METHODS ---

/// Takes the contents of 'child' and inserts it as a Map into 'parent'
#[no_mangle]
pub extern "C" fn fl_doc_insert_doc(parent: *mut FL_Doc, key: *const c_char, child: *const FL_Doc) -> i32 {
    let key = match cstr_to_string(key) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let parent_doc = unsafe { &mut *parent };
    let child_doc = unsafe { &*child };
    
    // Convert the child document's fields into a Value::Map
    parent_doc.doc.insert(key, Value::Map(child_doc.doc.fields.clone()));
    0
}

/// Takes the contents of 'array' and inserts it into the document
#[no_mangle]
pub extern "C" fn fl_doc_insert_array(doc: *mut FL_Doc, key: *const c_char, array: *mut FL_Array) -> i32 {
    let key = match cstr_to_string(key) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let doc = unsafe { &mut *doc };
    let array_inner = unsafe { Box::from_raw(array) }; // Take ownership and free FL_Array
    
    doc.doc.insert(key, Value::Array(array_inner.items));
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_patch(
    engine: *mut FL_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    updates: *const FL_Doc,
) -> i32 {
    if engine.is_null() || updates.is_null() { return -1; }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let id = match cstr_to_string(doc_id) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let update_doc = unsafe { &*updates };

    // FIX: Access .db and ensure set_last_error returns correctly
    match engine.db.patch(&col, &id, update_doc.doc.fields.clone()) {
        Ok(_) => 0,
        Err(e) => {
            set_last_error(e.to_string());
            -1
        }
    }
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
    let col = match cstr_to_string(collection) { Ok(v) => v, Err(_) => return 0 };
    let json_str = match cstr_to_string(fields_json) { Ok(v) => v, Err(_) => return 0 };
    
    // Parse the JSON into the SortDirection vector
    let Ok(fields_raw): std::result::Result<Vec<serde_json::Value>, _> = serde_json::from_str(&json_str) else { return 0 };
    
    let mut fields = Vec::new();
    for item in fields_raw {
        let f = item["field"].as_str().unwrap_or("").to_string();
        let desc = item["desc"].as_bool().unwrap_or(false);
        let dir = if desc { SortDirection::Desc } else { SortDirection::Asc };
        fields.push((f, dir));
    }

    engine.db.create_composite_index(&col, fields)
}

// --- 2. SERIALIZABLE TRANSACTIONS (Read-Modify-Write) ---

#[no_mangle]
pub extern "C" fn fl_transaction_begin(engine: *mut FL_Engine) -> *mut FL_Transaction {
    let engine = unsafe { &*engine };
    Box::into_raw(Box::new(FL_Transaction {
        tx: engine.db.begin_serializable_transaction(),
    }))
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
    let col = match cstr_to_string(collection) { Ok(v) => v, Err(_) => return ptr::null_mut() };
    let id = match cstr_to_string(doc_id) { Ok(v) => v, Err(_) => return ptr::null_mut() };

    match tx.tx.get(&engine.db, &col, &id) {
        Ok(Some(doc)) => Box::into_raw(Box::new(FL_Doc { doc })),
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
    let col = match cstr_to_string(collection) { Ok(v) => v, Err(_) => return -1 };
    let id = match cstr_to_string(doc_id) { Ok(v) => v, Err(_) => return -1 };
    let doc = unsafe { &*doc };
    
    tx.tx.put(&col, &id, doc.doc.clone());
    0
}

#[no_mangle]
pub extern "C" fn fl_transaction_commit(engine: *mut FL_Engine, tx: *mut FL_Transaction) -> i32 {
    let engine = unsafe { &*engine };
    let tx_box = unsafe { Box::from_raw(tx) }; // Take ownership to free memory
    
    match tx_box.tx.commit(&engine.db) {
        Ok(_) => 0,
        Err(e) => {
    set_last_error(e.to_string());
    -1 // or ptr::null_mut() depending on function return type
},
    }
}

#[no_mangle]
pub extern "C" fn fl_transaction_free(tx: *mut FL_Transaction) {
    if !tx.is_null() { unsafe { drop(Box::from_raw(tx)) }; }
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
    let c = match cstr_to_string(col) { Ok(v) => v, Err(_) => return -1 };
    let i = match cstr_to_string(id) { Ok(v) => v, Err(_) => return -1 };
    let sc = match cstr_to_string(sub_col) { Ok(v) => v, Err(_) => return -1 };
    let si = match cstr_to_string(sub_id) { Ok(v) => v, Err(_) => return -1 };
    let d = unsafe { &*doc };

    match engine.db.put_subdocument(&c, &i, &sc, &si, &d.doc) {
        Ok(_) => 0,
        Err(e) => {
    set_last_error(e.to_string());
    -1 // or ptr::null_mut() depending on function return type
},
    }
}

// --- 4. DIAGNOSTICS & MAINTENANCE ---

#[no_mangle]
pub extern "C" fn fl_engine_compact(engine: *mut FL_Engine) -> i32 {
    let engine = unsafe { &*engine };
    match engine.db.compact() {
        Ok(_) => 0,
        Err(e) => {
    set_last_error(e.to_string());
    -1 // or ptr::null_mut() depending on function return type
},
    }
}

#[no_mangle]
pub extern "C" fn fl_engine_get_stats(engine: *mut FL_Engine) -> *mut c_char {
    let engine = unsafe { &*engine };
    let stats = engine.db.get_stats();
    
    let json = serde_json::to_string(&stats).unwrap_or_else(|_| "{}".to_string());
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn fl_doc_insert_reference(
    doc: *mut FL_Doc,
    key: *const c_char,
    target_collection: *const c_char,
    target_id: *const c_char,
) -> i32 {
    if doc.is_null() { return -1; }
    let key_str = match cstr_to_string(key) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let col = match cstr_to_string(target_collection) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let id = match cstr_to_string(target_id) { Ok(v) => v, Err(e) => return set_last_error(e) };
    
    let doc_ptr = unsafe { &mut *doc };
    doc_ptr.doc.insert(key_str, Value::Reference { collection: col, doc_id: id });
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
    if engine.is_null() || doc.is_null() || field_key.is_null() {
        return ptr::null_mut();
    }

    let engine = unsafe { &*engine };
    let doc_ptr = unsafe { &*doc };
    let key = match cstr_to_string(field_key) { 
        Ok(k) => k, 
        Err(e) => { set_last_error(e); return ptr::null_mut(); } 
    };

    // 1. Find the value in the provided document
    let Some(val) = doc_ptr.doc.get(&key) else {
        set_last_error("Field not found in document");
        return ptr::null_mut();
    };

    // 2. Resolve the reference using the engine
    match engine.db.get_by_reference(val) {
        Ok(Some(target_doc)) => {
            Box::into_raw(Box::new(FL_Doc { doc: target_doc }))
        }
        Ok(None) => ptr::null_mut(), // Document doesn't exist (Dangling reference)
        Err(e) => {
            set_last_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn fl_query_start_after(
    query: *mut FL_Query,
    anchor_doc: *const FL_Doc,
) -> i32 {
    if query.is_null() || anchor_doc.is_null() { return -1; }
    let q = unsafe { &mut *query };
    let doc = unsafe { &*anchor_doc };
    
    // Logic: Look at what the query is sorting by, 
    // and extract those values from the anchor document.
    if let Some(order) = &q.query.order_by {
        if let Some(val) = doc.doc.get(&order.field) {
            q.query.start_after = Some(vec![val.clone()]);
            return 0;
        }
    }
    set_last_error("Anchor document missing sort field");
    -1
}

#[no_mangle]
pub extern "C" fn fl_query_where_or_str(
    query: *mut FL_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error(e) };
    let v = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error(e) };
    
    q.query.or_groups.push(vec![crate::query::filter::Filter {
        field: f,
        op: Operator::Eq,
        value: Value::String(v),
    }]);
    0
}

#[no_mangle]
pub extern "C" fn fl_engine_get_audit_log(engine: *mut FL_Engine) -> *mut c_char {
    if engine.is_null() { return std::ptr::null_mut(); }
    let engine = unsafe { &*engine };
    
    let entries = engine.db.audit_entries();
    
    // Convert to JSON
    // Note: Ensure AuditEntry and AccessOp derive serde::Serialize
    let json = serde_json::to_string(&entries).unwrap_or_else(|_| "[]".to_string());
    
    match CString::new(json) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}