use std::cell::RefCell;
use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use crate::config::{FireLiteConfig, DurabilityMode};
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::engine::{BatchMutation, FireLite};
use crate::query::filter::Operator;
use crate::query::query::Query;

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

fn doc_to_json(doc: &FireLiteDoc) -> Result<String, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in &doc.fields {
        let jv = match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(v) => serde_json::Value::Bool(*v),
            Value::Int(v) => serde_json::Value::Number((*v).into()),
            Value::Float(v) => serde_json::Number::from_f64(*v)
                .map(serde_json::Value::Number)
                .ok_or_else(|| "invalid float".to_string())?,
            Value::String(v) => serde_json::Value::String(v.clone()),
            Value::Binary(v) => serde_json::Value::Array(
                v.iter()
                    .map(|b| serde_json::Value::Number((*b as u64).into()))
                    .collect(),
            ),
        };
        map.insert(k.clone(), jv);
    }
    serde_json::to_string(&serde_json::Value::Object(map)).map_err(|e| e.to_string())
}

fn projection_to_json(fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        let jv = match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(v) => serde_json::Value::Bool(v),
            Value::Int(v) => serde_json::Value::Number(v.into()),
            Value::Float(v) => serde_json::Number::from_f64(v)
                .map(serde_json::Value::Number)
                .ok_or_else(|| "invalid float".to_string())?,
            Value::String(v) => serde_json::Value::String(v),
            Value::Binary(v) => serde_json::Value::Array(
                v.into_iter()
                    .map(|b| serde_json::Value::Number((b as u64).into()))
                    .collect(),
            ),
        };
        map.insert(k, jv);
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
pub extern "C" fn fl_engine_set_durability(engine: *mut FL_Engine, mode: i32) -> i32 {
    if engine.is_null() {
        return set_last_error("null engine handle");
    }
    
    let d_mode = match mode {
        1 => DurabilityMode::OnCommit,
        2 => DurabilityMode::Interval,
        3 => DurabilityMode::Manual,
        _ => DurabilityMode::Always,
    };

    let engine = unsafe { &*engine };
    engine.db.set_durability_mode(d_mode);
    clear_last_error();
    0
}

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
        Err(e) => set_last_error(e.to_string()),
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
        Err(e) => set_last_error(e.to_string()),
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

#[no_mangle]
pub extern "C" fn fl_query_limit(query: *mut FL_Query, limit: usize) -> i32 {
    if query.is_null() {
        return set_last_error("null query handle");
    }
    let query = unsafe { &mut *query };
    query.query = query.query.clone().limit(limit);
    clear_last_error();
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
