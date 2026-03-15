use std::ffi::{CStr, CString};
use std::os::raw::c_char;
use std::ptr;

use crate::config::FireLiteConfig;
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::engine::FireLite;

pub struct FireLiteHandle {
    db: FireLite,
    last_error: Option<CString>,
}

impl FireLiteHandle {
    fn set_error(&mut self, msg: impl Into<String>) -> i32 {
        self.last_error = CString::new(msg.into()).ok();
        -1
    }

    fn clear_error(&mut self) {
        self.last_error = None;
    }
}

fn cstr_to_string(ptr: *const c_char) -> Result<String, String> {
    if ptr.is_null() {
        return Err("null pointer".to_string());
    }
    let s = unsafe { CStr::from_ptr(ptr) };
    s.to_str()
        .map(|v| v.to_string())
        .map_err(|_| "invalid utf-8 string".to_string())
}

fn json_to_doc(json: &str) -> Result<FireLiteDoc, String> {
    let value: serde_json::Value = serde_json::from_str(json).map_err(|e| e.to_string())?;
    let obj = value
        .as_object()
        .ok_or_else(|| "json document must be an object".to_string())?;

    let mut doc = FireLiteDoc::default();
    for (k, v) in obj {
        let fv = match v {
            serde_json::Value::Null => Value::Null,
            serde_json::Value::Bool(b) => Value::Bool(*b),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() {
                    Value::Int(i)
                } else if let Some(f) = n.as_f64() {
                    Value::Float(f)
                } else {
                    return Err(format!("unsupported numeric value for key '{k}'"));
                }
            }
            serde_json::Value::String(s) => Value::String(s.clone()),
            _ => return Err(format!("unsupported nested json type for key '{k}'")),
        };
        doc.insert(k.clone(), fv);
    }

    Ok(doc)
}

fn doc_to_json(doc: &FireLiteDoc) -> Result<String, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in &doc.fields {
        let jv = match v {
            Value::Null => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .ok_or_else(|| "invalid floating-point value".to_string())?,
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Binary(bytes) => serde_json::Value::Array(
                bytes
                    .iter()
                    .map(|b| serde_json::Value::Number((*b as u64).into()))
                    .collect(),
            ),
        };
        map.insert(k.clone(), jv);
    }

    serde_json::to_string(&serde_json::Value::Object(map)).map_err(|e| e.to_string())
}

#[no_mangle]
pub extern "C" fn firelite_open(path: *const c_char) -> *mut FireLiteHandle {
    let path = match cstr_to_string(path) {
        Ok(v) => v,
        Err(_) => return ptr::null_mut(),
    };

    match FireLite::open(path, FireLiteConfig::default()) {
        Ok(db) => Box::into_raw(Box::new(FireLiteHandle {
            db,
            last_error: None,
        })),
        Err(_) => ptr::null_mut(),
    }
}

#[no_mangle]
pub extern "C" fn firelite_close(handle: *mut FireLiteHandle) {
    if !handle.is_null() {
        unsafe { drop(Box::from_raw(handle)) };
    }
}

#[no_mangle]
pub extern "C" fn firelite_put_json(
    handle: *mut FireLiteHandle,
    collection: *const c_char,
    doc_id: *const c_char,
    json: *const c_char,
) -> i32 {
    if handle.is_null() {
        return -1;
    }

    let h = unsafe { &mut *handle };
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return h.set_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return h.set_error(e),
    };
    let json = match cstr_to_string(json) {
        Ok(v) => v,
        Err(e) => return h.set_error(e),
    };

    let doc = match json_to_doc(&json) {
        Ok(v) => v,
        Err(e) => return h.set_error(e),
    };

    match h.db.put(&collection, &doc_id, &doc) {
        Ok(_) => {
            h.clear_error();
            0
        }
        Err(e) => h.set_error(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn firelite_get_json(
    handle: *mut FireLiteHandle,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut c_char {
    if handle.is_null() {
        return ptr::null_mut();
    }

    let h = unsafe { &mut *handle };
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => {
            h.set_error(e);
            return ptr::null_mut();
        }
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => {
            h.set_error(e);
            return ptr::null_mut();
        }
    };

    match h.db.get(&collection, &doc_id) {
        Ok(Some(doc)) => {
            match doc_to_json(&doc).and_then(|s| CString::new(s).map_err(|e| e.to_string())) {
                Ok(cstr) => {
                    h.clear_error();
                    cstr.into_raw()
                }
                Err(e) => {
                    h.set_error(e);
                    ptr::null_mut()
                }
            }
        }
        Ok(None) => ptr::null_mut(),
        Err(e) => {
            h.set_error(e.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn firelite_delete(
    handle: *mut FireLiteHandle,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if handle.is_null() {
        return -1;
    }

    let h = unsafe { &mut *handle };
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return h.set_error(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return h.set_error(e),
    };

    match h.db.delete(&collection, &doc_id) {
        Ok(_) => {
            h.clear_error();
            0
        }
        Err(e) => h.set_error(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn firelite_last_error_message(handle: *mut FireLiteHandle) -> *const c_char {
    if handle.is_null() {
        return ptr::null();
    }

    let h = unsafe { &mut *handle };
    h.last_error
        .as_ref()
        .map(|s| s.as_ptr())
        .unwrap_or(ptr::null())
}

#[no_mangle]
pub extern "C" fn firelite_free_string(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            let _ = CString::from_raw(value);
        }
    }
}
