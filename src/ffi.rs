#![allow(clippy::not_unsafe_ptr_arg_deref)] // outer C ABI: callers guarantee pointer validity

use std::cell::RefCell;
use std::ffi::{c_char, c_int, CStr, CString};
use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::mpsc::{channel, Sender};
use std::time::Duration;
use std::{ptr, thread};
use std::collections::HashSet;

use hashbrown::HashMap;

use crate::config::{DurabilityMode, HakoConfig};
use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::engine::{BatchMutation, Hako};
use crate::index::composite::definition::SortDirection;
use crate::query::filter::Operator;
use crate::query::query::{AggregateOp, Query};

macro_rules! safety_shield {
    ($fallback:expr, $block:block) => {
        match catch_unwind(AssertUnwindSafe(|| $block)) {
            Ok(val) => val,
            Err(_) => {
                // Set the thread-local error so C++ can see it
                crate::ffi::set_last_error_err("CRITICAL: Internal Engine Panic. The operation was aborted to prevent a process crash.");
                $fallback
            }
        }
    };
}

#[allow(non_camel_case_types)]
pub struct HK_Engine {
    // db: Hako,
    db: std::sync::Arc<Hako>,
}

pub enum DocRef {
    /// Owned document (fresh handle, query row, materialized view).
    Owned(HakoDoc),
    /// Shared with the engine's doc cache. A point read returns the resident
    /// `Arc` (zero-copy); the first mutation clones it (COW). This is the
    /// difference between a ~900 ns and a ~250 ns `hk_engine_get`.
    Shared(std::sync::Arc<HakoDoc>),
}

impl DocRef {
    #[inline]
    fn into_hako(self) -> HakoDoc {
        match self {
            DocRef::Owned(d) => d,
            DocRef::Shared(a) => std::sync::Arc::try_unwrap(a).unwrap_or_else(|x| (*x).clone()),
        }
    }
}

impl Default for DocRef {
    fn default() -> Self {
        DocRef::Owned(HakoDoc::default())
    }
}

#[allow(non_camel_case_types)]
pub struct HK_Doc {
    pub id: String,
    doc: DocRef,
}

impl HK_Doc {
    #[inline]
    fn hako(&self) -> &HakoDoc {
        match &self.doc {
            DocRef::Owned(d) => d,
            DocRef::Shared(a) => a,
        }
    }
    #[inline]
    fn hako_mut(&mut self) -> &mut HakoDoc {
        match &mut self.doc {
            DocRef::Owned(d) => d,
            DocRef::Shared(a) => std::sync::Arc::make_mut(a),
        }
    }
    #[inline]
    fn into_hako(self) -> HakoDoc {
        self.doc.into_hako()
    }
}

#[allow(non_camel_case_types)]
pub struct HK_Batch {
    ops: Vec<BatchMutation>,
}

#[allow(non_camel_case_types)]
pub struct HK_Query {
    query: Query,
}

// config
#[allow(non_camel_case_types)]
pub struct HK_Config {
    pub inner: HakoConfig,
}

#[allow(non_camel_case_types)]
pub struct HK_Watch {
    stop_tx: Sender<()>,
    thread_handle: Option<thread::JoinHandle<()>>,
}

// 1. Fixed type naming warning with #[allow]
#[allow(non_camel_case_types)]
pub type HK_OnSnapshotCallback = unsafe extern "C" fn(
    collection: *const c_char,
    path: *const c_char,
    kind: i32,
    user_data: *mut std::ffi::c_void,
);

#[allow(non_camel_case_types)]
pub struct HK_Array {
    pub items: Vec<Value>,
}

#[allow(non_camel_case_types)]
pub struct HK_Transaction {
    pub tx: crate::engine::SerializableTransaction,
}

/// F4 aggregation pipeline builder handle (`hk_pipeline_*`).
#[allow(non_camel_case_types)]
pub struct HK_Pipeline {
    pipeline: crate::query::pipeline::Pipeline,
}

#[allow(non_camel_case_types)]
pub struct HK_ResultSet {
    pub docs: Vec<HK_Doc>,
}

/// Raw (undecoded) document handle. `bytes` pins the storage-encoded
/// buffer (zero copies for inlined docs); decode on demand with
/// `hk_rawdoc_to_doc`. Bytes are opaque storage encoding — do not persist
/// or compare across versions.
#[allow(non_camel_case_types)]
pub struct HK_RawDoc {
    pub id: String,
    pub bytes: std::sync::Arc<Vec<u8>>,
}

/// Slab of raw docs. Borrowed-handle contract mirrors HK_ResultSet:
/// `hk_rawresult_get` pointers die with `hk_rawresult_free`.
#[allow(non_camel_case_types)]
pub struct HK_RawResultSet {
    pub docs: Vec<HK_RawDoc>,
}

#[cfg(feature = "net-sync")]
#[allow(non_camel_case_types)]
pub struct HK_NetSyncer {
    inner: std::sync::Arc<crate::net_sync::NetSyncer>,
}

// ============================================================================
// CLOUD SYNC FFI BINDINGS
// ============================================================================

#[cfg(feature = "cloud-sync")]
#[allow(non_camel_case_types)]
pub struct HK_CloudSync {
    inner: std::sync::Arc<crate::cloud_sync::CloudSync>,
}

thread_local! {
    static LAST_ERROR: RefCell<Option<CString>> = const { RefCell::new(None) };
    /// Issue #71/F5: retry policy must never string-match. Written from
    /// `HakoError::kind()` wherever a typed error reaches the FFI
    /// boundary; 1 (generic) defaults for the untyped sites.
    static LAST_ERROR_KIND: std::cell::Cell<i32> = const { std::cell::Cell::new(0) };
}

fn set_last_error(msg: impl Into<String>) -> i32 {
    let message = CString::new(msg.into())
        .unwrap_or_else(|_| CString::new("ffi error").expect("valid static cstring"));
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = Some(message);
    });
    LAST_ERROR_KIND.with(|k| k.set(crate::error::ErrorKind::Generic as i32));
    -1
}

/// Anything an `Err(e) => set_last_error_err(e)` arm wants to report. Allows
/// one mechanical sweep of all sites without type-specific call sites —
/// typed errors keep their retry classification (issue #71/F5), strings
/// degrade to Generic, exactly as before.
pub(crate) trait LastErrorPayload {
    fn payload(self) -> (String, crate::error::ErrorKind);
}

impl LastErrorPayload for crate::error::HakoError {
    fn payload(self) -> (String, crate::error::ErrorKind) {
        (self.to_string(), self.kind())
    }
}

impl LastErrorPayload for &crate::error::HakoError {
    fn payload(self) -> (String, crate::error::ErrorKind) {
        (self.to_string(), self.kind())
    }
}

impl LastErrorPayload for std::io::Error {
    fn payload(self) -> (String, crate::error::ErrorKind) {
        (self.to_string(), crate::error::ErrorKind::Generic)
    }
}

impl LastErrorPayload for String {
    fn payload(self) -> (String, crate::error::ErrorKind) {
        (self, crate::error::ErrorKind::Generic)
    }
}

impl LastErrorPayload for &str {
    fn payload(self) -> (String, crate::error::ErrorKind) {
        (self.to_string(), crate::error::ErrorKind::Generic)
    }
}

impl LastErrorPayload for &&str {
    fn payload(self) -> (String, crate::error::ErrorKind) {
        (self.to_string(), crate::error::ErrorKind::Generic)
    }
}

/// Set the last error from a typed engine error — records its retry
/// classification alongside the message (issue #71/F5).
pub(crate) fn set_last_error_err(payload: impl LastErrorPayload) -> i32 {
    let (msg, kind) = payload.payload();
    set_last_error(msg);
    LAST_ERROR_KIND.with(|k| k.set(kind as i32));
    -1
}

fn clear_last_error() {
    LAST_ERROR.with(|slot| {
        *slot.borrow_mut() = None;
    });
    LAST_ERROR_KIND.with(|k| k.set(crate::error::ErrorKind::None as i32));
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

fn doc_to_json(doc: &HakoDoc) -> Result<String, String> {
    safety_shield!(Err("Internal Panic".into()), {
        // Single-pass emit, byte-identical to serde's output (pinned by
        // tests/json_bytes_parity) with no intermediate serde_json DOM.
        // Long runs are bulk-copied, so there is no size-crossover arm.
        let out = doc.to_json_bytes();
        String::from_utf8(out).map_err(|e| e.to_string())
    })
}

/// Hot FFI variant of [`doc_to_json`]: same bytes, handed to the caller as a
/// C string without the UTF-8 validation pass, the interior-NUL scan, and
/// the extra copy `String` + `CString::new` cost.
fn doc_to_cstring(doc: &HakoDoc) -> Result<CString, String> {
    safety_shield!(Err("Internal Panic".into()), {
        let mut out = doc.to_json_bytes();
        out.push(0);
        // SAFETY: `write_json` emits only valid UTF-8 (string values are
        // `&str`, keys are `&str`, everything else is ASCII) and escapes
        // every U+0000 as `\u0000`, so the buffer has no interior NUL and
        // the byte we just appended is the only terminator.
        Ok(unsafe { CString::from_vec_unchecked(out) })
    })
}

/// NUL-terminate a JSON byte buffer produced by a `write_json` path. Those
/// writers escape U+0000, so the appended byte is the only terminator.
fn json_to_cstring(mut out: Vec<u8>) -> CString {
    out.push(0);
    unsafe { CString::from_vec_unchecked(out) }
}

fn projection_to_json(fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in fields {
        map.insert(k, value_to_json(&v));
    }
    Ok(serde_json::Value::Object(map))
}

/// JSON -> `Value` for FFI arguments (filters, patches, pipeline accums).
/// Written out instead of `serde_json::from_value` because `Value` is
/// untagged and its serde `Map` arm expects the internal pair encoding, so
/// a plain JSON object would fail to deserialize.
fn json_to_value(v: &serde_json::Value) -> Value {
    match v {
        serde_json::Value::Null => Value::Null,
        serde_json::Value::Bool(b) => Value::Bool(*b),
        serde_json::Value::Number(n) => match n.as_i64() {
            Some(i) => Value::Int(i),
            None => Value::Float(n.as_f64().unwrap_or(0.0)),
        },
        serde_json::Value::String(s) => Value::String(s.clone()),
        serde_json::Value::Array(items) => Value::Array(items.iter().map(json_to_value).collect()),
        serde_json::Value::Object(map) => Value::Map(
            map.iter()
                .map(|(k, v)| (std::sync::Arc::from(k.as_str()), json_to_value(v)))
                .collect(),
        ),
    }
}

fn parse_json_arg(raw: *const c_char, what: &str) -> Result<serde_json::Value, String> {
    let s = cstr_to_string(raw)?;
    serde_json::from_str(&s).map_err(|e| format!("invalid {what} JSON: {e}"))
}

/// Build a [`HakoDoc`] from a JSON object string (W4, #143): one FFI call
/// replaces the hk_doc_new + N×hk_doc_insert_* + engine-call chain the
/// client paid per document (~2.5–3 µs of client-side cost). Values keep
/// the `Value::from_json` semantics (references, blob links, nested maps).
fn hako_doc_from_json(raw: *const c_char) -> Result<HakoDoc, String> {
    let obj = match parse_json_arg(raw, "doc")? {
        serde_json::Value::Object(map) => map,
        _ => return Err("doc JSON must be an object".to_string()),
    };
    let mut doc = HakoDoc::default();
    for (k, v) in obj {
        doc.insert(k, Value::from_json(v)?);
    }
    Ok(doc)
}

fn parse_filter_op(name: &str) -> Result<Operator, String> {
    Ok(match name {
        "eq" => Operator::Eq,
        "ne" => Operator::Ne,
        "gt" => Operator::Gt,
        "gte" => Operator::Gte,
        "lt" => Operator::Lt,
        "lte" => Operator::Lte,
        "match" => Operator::Match,
        "match_prefix" => Operator::MatchPrefix,
        "contains" => Operator::Contains,
        "starts_with" => Operator::StartsWith,
        "in" => Operator::In,
        "array_contains" => Operator::ArrayContains,
        "array_contains_any" => Operator::ArrayContainsAny,
        "array_all_eq" => Operator::ArrayAllEq,
        "not_in" => Operator::NotIn,
        other => return Err(format!("unknown filter op `{other}`")),
    })
}

/// `[{"field":"qty","op":"gt","value":2}, ...]` -> filters (F3 elemMatch).
fn parse_filters_json(raw: *const c_char) -> Result<Vec<crate::query::filter::Filter>, String> {
    let value = parse_json_arg(raw, "filters")?;
    let arr = value
        .as_array()
        .ok_or("filters must be a JSON array")?;
    arr.iter()
        .map(|entry| {
            let obj = entry
                .as_object()
                .ok_or("each filter must be a JSON object")?;
            let field = obj
                .get("field")
                .and_then(|v| v.as_str())
                .ok_or("filter.field must be a string")?;
            let op = obj
                .get("op")
                .and_then(|v| v.as_str())
                .ok_or("filter.op must be a string")?;
            let value = obj.get("value").ok_or("filter.value is required")?;
            Ok(crate::query::filter::Filter {
                field: field.to_string(),
                op: parse_filter_op(op)?,
                value: json_to_value(value),
            })
        })
        .collect()
}

/// `{"field": <json>, ...}` -> top-level patch updates.
fn parse_updates_json(raw: *const c_char) -> Result<Vec<(String, Value)>, String> {
    let value = parse_json_arg(raw, "updates")?;
    let obj = value
        .as_object()
        .ok_or("updates must be a JSON object")?;
    Ok(obj.iter().map(|(k, v)| (k.clone(), json_to_value(v))).collect())
}

#[no_mangle]
pub extern "C" fn hk_engine_open(path: *const c_char) -> *mut HK_Engine {
    let path = match cstr_to_string(path) {
        Ok(v) => v,
        Err(e) => {
            set_last_error_err(e);
            return ptr::null_mut();
        }
    };

    match Hako::open(path, HakoConfig::default()) {
        Ok(db) => {
            clear_last_error();
            // Box::into_raw(Box::new(HK_Engine { db }))
            Box::into_raw(Box::new(HK_Engine { db: std::sync::Arc::new(db) }))
        }
        Err(e) => {
            set_last_error_err(e.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_engine_is_indexes_ready(engine: *mut HK_Engine) -> bool {
    safety_shield!(false, {
        if engine.is_null() { return false; }
        let engine = unsafe { &*engine };
        engine.db.is_indexes_ready()
    })
}

/// Block until background work settles (index recovery + async index
/// updates + blob persistence + maintenance) or `timeout_ms` lapses.
/// Requires two consecutive clear samples, so use it before measuring.
/// Returns true when settled. See `Hako::await_quiescent`.
#[no_mangle]
pub extern "C" fn hk_engine_await_quiescent(engine: *mut HK_Engine, timeout_ms: u64) -> bool {
    safety_shield!(false, {
        if engine.is_null() { return false; }
        let engine = unsafe { &*engine };
        engine.db.await_quiescent(std::time::Duration::from_millis(timeout_ms))
    })
}

/// Point-sample diagnostic as JSON (free with hk_string_free):
/// {"indexes_ready":b,"pending_index_ops":n,"index_backfills":n,
///  "pending_blob_bytes":n,"queued_blob_items":n,
///  "maintenance_running":b,"quiescent":b}.
/// Tells you WHAT is outstanding instead of guessing.
#[no_mangle]
pub extern "C" fn hk_engine_quiescence_status(engine: *mut HK_Engine) -> *mut c_char {
    if engine.is_null() {
        set_last_error_err("null engine handle");
        return ptr::null_mut();
    }
    let engine = unsafe { &*engine };
    let s = engine.db.quiescence_status();
    let text = serde_json::json!({
        "indexes_ready": s.indexes_ready,
        "pending_index_ops": s.pending_index_ops,
        "index_backfills": s.index_backfills,
        "pending_blob_bytes": s.pending_blob_bytes,
        "queued_blob_items": s.queued_blob_items,
        "maintenance_running": s.maintenance_running,
        "quiescent": s.is_quiescent(),
    })
    .to_string();
    match CString::new(text) {
        Ok(cs) => {
            clear_last_error();
            cs.into_raw()
        }
        Err(e) => {
            set_last_error_err(e.to_string());
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_config_new() -> *mut HK_Config {
    Box::into_raw(Box::new(HK_Config {
        inner: HakoConfig::default(),
    }))
}

#[no_mangle]
pub extern "C" fn hk_config_free(config: *mut HK_Config) {
    if !config.is_null() {
        unsafe { drop(Box::from_raw(config)) };
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_durability(config: *mut HK_Config, mode: i32) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.durability_mode = match mode {
            1 => DurabilityMode::Interval,
            2 => DurabilityMode::Manual,
            3 => DurabilityMode::OnCommit,
            _ => DurabilityMode::Always,
        };
    }
}

/// Group-commit window in ms (Interval mode). Stored raw; clamped to
/// 1..=30_000 at open (see HakoConfig). Cluster use-case: per-instance
/// intervals (+ staggered starts) spread fsync storms.
#[no_mangle]
pub extern "C" fn hk_config_set_group_commit_interval_ms(config: *mut HK_Config, ms: u64) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.group_commit_interval_ms = ms;
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_encryption_key(config: *mut HK_Config, key: *const c_char) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.encryption_key = cstr_to_string(key).ok();
    }
}

/// Set which collections should be encrypted. 
/// collections_json: A JSON array of strings, e.g., '["secrets", "private_messages"]'
#[no_mangle]
pub extern "C" fn hk_config_set_encrypted_collections(
    config: *mut HK_Config,
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
pub extern "C" fn hk_config_set_audit_log(
    config: *mut HK_Config,
    enabled: bool,
    path: *const c_char,
) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.enable_audit_log = enabled;
        cfg.inner.audit_log_path = cstr_to_string(path).ok();
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_query_workers(config: *mut HK_Config, count: usize) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.query_workers = count;
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_memory_limits(
    config: *mut HK_Config,
    mmap_size: usize,
    max_inlined_bytes: usize,
) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.mmap_size = mmap_size;
        cfg.inner.max_inlined_memory_bytes = max_inlined_bytes;
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_storage_tuning(
    config: *mut HK_Config,
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
pub extern "C" fn hk_config_set_blob_threshold(config: *mut HK_Config, threshold_bytes: usize) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.value_blob_threshold_bytes = threshold_bytes;
    }
}

/// WAL headroom reservation in bytes (0 = off, default 4MB). Preallocated
/// ahead of the write position so steady-state appends never extend the
/// file. Sparse: consumes no disk until written. Ignored for Manual.
#[no_mangle]
pub extern "C" fn hk_config_set_wal_reserve_bytes(config: *mut HK_Config, bytes: u64) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.wal_reserve_bytes = bytes;
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_page_cache_capacity(config: *mut HK_Config, capacity: usize) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.page_cache_capacity = capacity;
    }
}

/// JSON string-array parameter shared by the collection-list setters
/// (issue #33: sync_excluded / lazy_collections / replication_collections
/// were unreachable from non-Rust bindings).
fn config_set_string_list(
    config: *mut HK_Config,
    json: *const c_char,
    apply: impl FnOnce(&mut HakoConfig, Vec<String>),
) -> i32 {
    let Some(cfg) = (unsafe { config.as_mut() }) else { return -1 };
    let Ok(s) = cstr_to_string(json) else { return -1 };
    match serde_json::from_str::<Vec<String>>(&s) {
        Ok(list) => {
            apply(&mut cfg.inner, list);
            0
        }
        Err(_) => -1,
    }
}

/// Collections pinned OUT of every sync plane (mesh/cloud/socket), both
/// directions. JSON array of names.
#[no_mangle]
pub extern "C" fn hk_config_set_sync_excluded(
    config: *mut HK_Config,
    collections_json: *const c_char,
) -> i32 {
    config_set_string_list(config, collections_json, |cfg, list| {
        cfg.sync_excluded = list;
    })
}

/// Lazy-residency groups (exact names or trailing-`*` prefixes). JSON
/// array; archived collections stay on disk, load on touch.
#[no_mangle]
pub extern "C" fn hk_config_set_lazy_collections(
    config: *mut HK_Config,
    collections_json: *const c_char,
) -> i32 {
    config_set_string_list(config, collections_json, |cfg, list| {
        cfg.lazy_collections = list;
    })
}

/// Replication opt-in list (None = all collections). JSON array of names.
#[no_mangle]
pub extern "C" fn hk_config_set_replication_collections(
    config: *mut HK_Config,
    collections_json: *const c_char,
) -> i32 {
    config_set_string_list(config, collections_json, |cfg, list| {
        cfg.replication_collections = Some(list);
    })
}

/// Write-path phase breakdown (see engine::write_stats_report). Returns a
/// fresh C string the caller frees with hk_string_free. Counters reset.
#[no_mangle]
pub extern "C" fn hk_debug_write_stats() -> *mut c_char {
    match std::ffi::CString::new(crate::engine::engine::write_stats_report()) {
        Ok(s) => s.into_raw(),
        Err(_) => std::ptr::null_mut(),
    }
}

/// Opens the engine using a custom config.
/// Note: This function takes ownership of the config and will free it automatically.
#[no_mangle]
pub extern "C" fn hk_engine_open_with_config(
    path: *const c_char,
    config: *mut HK_Config,
) -> *mut HK_Engine {
    safety_shield!(std::ptr::null_mut(), {
        let path_str = match cstr_to_string(path) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return std::ptr::null_mut();
            }
        };

        if config.is_null() {
            set_last_error_err("Null config provided");
            return std::ptr::null_mut();
        }

        // Take ownership of the config from the FFI caller
        let cfg_box = unsafe { Box::from_raw(config) };

        match Hako::open(path_str, cfg_box.inner) {
            Ok(db) => {
                clear_last_error();
                // Box::into_raw(Box::new(HK_Engine { db }))
                Box::into_raw(Box::new(HK_Engine { db: std::sync::Arc::new(db) }))
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

// end config

// emulate snapshoot
#[no_mangle]
pub extern "C" fn hk_engine_watch(
    engine: *mut HK_Engine,
    collection: *const c_char,
    callback: HK_OnSnapshotCallback,
    user_data_ptr: *mut std::ffi::c_void,
) -> *mut HK_Watch {
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

    Box::into_raw(Box::new(HK_Watch {
        stop_tx,
        thread_handle: Some(handle),
    }))
}

#[no_mangle]
pub extern "C" fn hk_watch_free(watch: *mut HK_Watch) {
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
pub extern "C" fn hk_engine_free(engine: *mut HK_Engine) {
    safety_shield!((), {
        if !engine.is_null() {
            unsafe { drop(Box::from_raw(engine)) };
        }
    })
}

// #[no_mangle]
// pub extern "C" fn hk_doc_new() -> *mut HK_Doc {
//     Box::into_raw(Box::new(HK_Doc {
//         // id: doc_id.clone(),
//         doc: HakoDoc::default(),
//     }))
// }

// 2. Update creation points
#[no_mangle]
pub extern "C" fn hk_doc_new() -> *mut HK_Doc {
    Box::into_raw(Box::new(HK_Doc {
        id: String::new(),
        doc: DocRef::Owned(HakoDoc::default()),
    }))
}

#[no_mangle]
pub extern "C" fn hk_doc_free(doc: *mut HK_Doc) {
    if !doc.is_null() {
        unsafe { drop(Box::from_raw(doc)) };
    }
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_str(
    doc: *mut HK_Doc,
    key: *const c_char,
    value: *const c_char,
) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null HK_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let value = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::String(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_int(doc: *mut HK_Doc, key: *const c_char, value: i64) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null HK_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::Int(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_float(doc: *mut HK_Doc, key: *const c_char, value: f64) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null HK_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::Float(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_bool(doc: *mut HK_Doc, key: *const c_char, value: bool) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null HK_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::Bool(value));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_null(doc: *mut HK_Doc, key: *const c_char) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null HK_Doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::Null);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_bin(
    doc: *mut HK_Doc,
    key: *const c_char,
    data: *const u8,
    len: usize,
) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null HK_Doc");
    }
    if data.is_null() && len > 0 {
        return set_last_error_err("null binary data");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let bytes = if len == 0 {
        Vec::new()
    } else {
        unsafe { std::slice::from_raw_parts(data, len) }.to_vec()
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::Binary(bytes));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_engine_insert(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *const HK_Doc,
) -> i32 {
    safety_shield!(-1, {
        // Returns -1 if Rust panics
        if engine.is_null() || doc.is_null() {
            return set_last_error_err("null engine/doc handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };

        let engine = unsafe { &mut *engine };
        let doc = unsafe { &*doc };
        match engine.db.put(&collection, &doc_id, doc.hako()) {
            Ok(_) => 0,
            Err(e) => set_last_error_err(e),
        }
    })
}

/// Owned-doc insert: takes over the HK_Doc handle (no deep clone).
/// The handle is ALWAYS consumed — success or failure — do not use or free
/// `doc` after the call.
#[no_mangle]
pub extern "C" fn hk_engine_insert_take(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *mut HK_Doc,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || doc.is_null() {
            return set_last_error_err("null engine/doc handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };

        let engine = unsafe { &mut *engine };
        // Move the document out of its Box without cloning every field.
        let owned = unsafe { *Box::from_raw(doc) };
        match engine.db.put_owned(&collection, &doc_id, owned.into_hako()) {
            Ok(_) => 0,
            Err(e) => set_last_error_err(e),
        }
    })
}

/// Single-call JSON insert (W4, #143): builds the doc from a JSON object
/// string and puts it — replaces the hk_doc_new + N×hk_doc_insert_*
/// + hk_engine_insert chain per document on the client side.
#[no_mangle]
pub extern "C" fn hk_engine_insert_json(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    json: *const c_char,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() {
            return set_last_error_err("null engine handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let doc = match hako_doc_from_json(json) {
            Ok(d) => d,
            Err(e) => return set_last_error_err(e),
        };

        let engine = unsafe { &mut *engine };
        // Owned put: the JSON-built doc is fresh — the &put would deep-clone
        // it once more on the way in.
        match engine.db.put_owned(&collection, &doc_id, doc) {
            Ok(_) => 0,
            Err(e) => set_last_error_err(e),
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_engine_get(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut HK_Doc {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        // ponytail: borrow the caller's C strings — the old code allocated
        // two Strings per get (strlen + validate + copy each). Only doc_id
        // still needs ownership (moved into the HK_Doc handle).
        if collection.is_null() {
            set_last_error_err("null collection handle");
            return ptr::null_mut();
        }
        if doc_id.is_null() {
            set_last_error_err("null doc_id handle");
            return ptr::null_mut();
        }
        let collection_c = unsafe { CStr::from_ptr(collection) };
        let doc_id_c = unsafe { CStr::from_ptr(doc_id) };
        let collection = match collection_c.to_str() {
            Ok(v) => v,
            Err(_) => {
                set_last_error_err("invalid utf8");
                return ptr::null_mut();
            }
        };
        let doc_id_owned = match doc_id_c.to_str() {
            Ok(v) => v.to_string(),
            Err(_) => {
                set_last_error_err("invalid utf8");
                return ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        match engine.db.get_arc(collection, &doc_id_owned) {
            // Zero-copy: the returned handle shares the cached HakoDoc.
            Ok(Some(doc)) => Box::into_raw(Box::new(HK_Doc { id: doc_id_owned, doc: DocRef::Shared(doc) })),
            _ => std::ptr::null_mut(),
        }
    })
}

/// Point-read JSON straight from the pinned bytes; blob links are resolved
/// so the output matches `hk_engine_get` + `hk_doc_to_json`. Caller frees
/// with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_engine_get_json(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || collection.is_null() || doc_id.is_null() {
            set_last_error_err("null engine/collection/doc_id handle");
            return std::ptr::null_mut();
        }
        let (collection, doc_id) = match (
            unsafe { CStr::from_ptr(collection) }.to_str(),
            unsafe { CStr::from_ptr(doc_id) }.to_str(),
        ) {
            (Ok(c), Ok(d)) => (c, d),
            _ => {
                set_last_error_err("invalid utf8");
                return std::ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        match engine.db.get_json(collection, doc_id) {
            Ok(Some((_version, json))) => {
                clear_last_error();
                json_to_cstring(json).into_raw()
            }
            Ok(None) => std::ptr::null_mut(),
            Err(e) => {
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

/// Atomic `(doc, version)`: both are captured under one shard read, so an
/// RMW loop cannot pair a new version with a stale doc (issue #71/F5 item
/// 3). `out_version` may be NULL; it receives 0 when the doc is absent.
#[no_mangle]
pub extern "C" fn hk_engine_get_versioned(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    out_version: *mut u64,
) -> *mut HK_Doc {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        if !out_version.is_null() {
            unsafe { *out_version = 0 };
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        match engine.db.get_versioned(&collection, &doc_id) {
            Ok(Some((doc, version))) => {
                if !out_version.is_null() {
                    unsafe { *out_version = version };
                }
                clear_last_error();
                Box::into_raw(Box::new(HK_Doc { id: doc_id, doc: DocRef::Owned(doc) }))
            }
            Ok(None) => {
                clear_last_error();
                ptr::null_mut()
            }
            Err(e) => {
                set_last_error_err(e);
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_engine_delete(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    let engine = unsafe { &mut *engine };
    match engine.db.delete(&collection, &doc_id) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => {
            set_last_error_err(e.to_string());
            -1 // or ptr::null_mut() depending on function return type
        }
    }
}

/// Local-only delete: marks the key so no sync tailer or handshake
/// catch-up ever transmits it, then deletes normally (fresh tombstone
/// timestamp keeps the version clock advanced — handshake-stable).
#[no_mangle]
pub extern "C" fn hk_engine_delete_local(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    let engine = unsafe { &mut *engine };
    match engine.db.delete_local(&collection, &doc_id) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => {
            set_last_error_err(e.to_string());
            -1
        }
    }
}

/// Marks a collection local-only (`local != 0`) or rejoins it to sync.
/// A local-only collection never emits nor is caught up from the network.
#[no_mangle]
pub extern "C" fn hk_engine_set_collection_local(
    engine: *mut HK_Engine,
    collection: *const c_char,
    local: i32,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let engine = unsafe { &mut *engine };
    engine.db.set_collection_local(&collection, local != 0);
    clear_last_error();
    0
}

/// Opts a key back into replication (future ops only).
#[no_mangle]
pub extern "C" fn hk_engine_replicate_key(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let engine = unsafe { &mut *engine };
    engine.db.replicate_key(&collection, &doc_id);
    clear_last_error();
    0
}

/// Opts a whole collection back into replication (clears flag + key marks).
#[no_mangle]
pub extern "C" fn hk_engine_replicate_collection(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_engine_vacuum_collection(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let engine = unsafe { &mut *engine };
    match engine.db.vacuum_collection(&collection) {
        Ok(n) => {
            clear_last_error();
            n as i32
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

/// Relocate docs by id from one collection to another (same engine).
/// `ids_json`: JSON array of doc ids, e.g. `["a","b"]`. Returns a JSON
/// report `{"moved":[...],"missing":[...]}`, or NULL on error (refusals:
/// excluded sides, src == dst). Caller frees with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_engine_relocate_docs(
    engine: *mut HK_Engine,
    src: *const c_char,
    dst: *const c_char,
    ids_json: *const c_char,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return std::ptr::null_mut();
        }
        let src = match cstr_to_string(src) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return std::ptr::null_mut();
            }
        };
        let dst = match cstr_to_string(dst) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return std::ptr::null_mut();
            }
        };
        let ids_raw = match cstr_to_string(ids_json) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return std::ptr::null_mut();
            }
        };
        let ids: Vec<String> = match serde_json::from_str(&ids_raw) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(format!("ids_json must be a JSON string array: {e}"));
                return std::ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        match engine.db.relocate_docs(&src, &dst, &ids) {
            Ok(rep) => {
                let json = serde_json::json!({"moved": rep.moved, "missing": rep.missing}).to_string();
                match CString::new(json) {
                    Ok(c_str) => {
                        clear_last_error();
                        c_str.into_raw()
                    }
                    Err(_) => std::ptr::null_mut(),
                }
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

/// Load a lazy collection's snapshot into the index now (synchronous).
/// Returns 0, or -1 on error (e.g. not a lazy collection).
#[no_mangle]
pub extern "C" fn hk_engine_load_collection(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let engine = unsafe { &*engine };
    match engine.db.load_collection(&collection) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

/// Unload a lazy collection from the index (frees RAM; snapshot stays).
/// Returns 0, or -1 on error (e.g. not a lazy collection).
#[no_mangle]
pub extern "C" fn hk_engine_unload_collection(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let engine = unsafe { &*engine };
    match engine.db.unload_collection(&collection) {
        Ok(()) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

/// List lazy collections currently unloaded (not in the index), as a
/// JSON array. Caller frees with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_engine_unloaded_collections(engine: *mut HK_Engine) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return std::ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let cols = engine.db.unloaded_lazy_collections();
        let json = serde_json::to_string(&cols).unwrap_or_else(|_| "[]".to_string());
        match CString::new(json) {
            Ok(c_str) => {
                clear_last_error();
                c_str.into_raw()
            }
            Err(_) => std::ptr::null_mut(),
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_batch_new() -> *mut HK_Batch {
    Box::into_raw(Box::new(HK_Batch { ops: Vec::new() }))
}

#[no_mangle]
pub extern "C" fn hk_batch_free(batch: *mut HK_Batch) {
    if !batch.is_null() {
        unsafe { drop(Box::from_raw(batch)) };
    }
}

#[no_mangle]
pub extern "C" fn hk_batch_set(
    batch: *mut HK_Batch,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *mut HK_Doc,
) -> i32 {
    if batch.is_null() || doc.is_null() {
        return set_last_error_err("null batch/doc handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    let batch = unsafe { &mut *batch };
    let doc_ptr = unsafe { &mut *doc };

    let internal_doc = std::mem::take(&mut doc_ptr.doc).into_hako();

    batch.ops.push(BatchMutation::Put {
        collection,
        doc_id,
        doc: internal_doc,
    });
    clear_last_error();
    0
}

/// Single-call JSON batch set (W4, #143): builds the doc from a JSON object
/// string and queues it on the batch — the batch-construction twin of
/// [`hk_engine_insert_json`].
#[no_mangle]
pub extern "C" fn hk_batch_set_json(
    batch: *mut HK_Batch,
    collection: *const c_char,
    doc_id: *const c_char,
    json: *const c_char,
) -> i32 {
    if batch.is_null() {
        return set_last_error_err("null batch handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = match hako_doc_from_json(json) {
        Ok(d) => d,
        Err(e) => return set_last_error_err(e),
    };

    let batch = unsafe { &mut *batch };
    batch.ops.push(BatchMutation::Put {
        collection,
        doc_id,
        doc,
    });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_batch_delete(
    batch: *mut HK_Batch,
    collection: *const c_char,
    doc_id: *const c_char,
) -> i32 {
    if batch.is_null() {
        return set_last_error_err("null batch handle");
    }
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc_id = match cstr_to_string(doc_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    let batch = unsafe { &mut *batch };
    batch.ops.push(BatchMutation::Delete { collection, doc_id });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_batch_commit(engine: *mut HK_Engine, batch: *mut HK_Batch) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || batch.is_null() {
            return set_last_error_err("null engine/batch handle");
        }

        let engine = unsafe { &mut *engine };
        let batch = unsafe { &mut *batch };
        let ops = std::mem::take(&mut batch.ops);

        match engine.db.write_batch(ops) {
            Ok(_) => {
                clear_last_error();
                0
            }
            // Typed payload: unique violations arrive as kind 3 (issue
            // #71/F1/F5), not generic.
            Err(e) => set_last_error_err(e),
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_query_new(collection: *const c_char) -> *mut HK_Query {
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => {
            set_last_error_err(e);
            return ptr::null_mut();
        }
    };

    clear_last_error();
    Box::into_raw(Box::new(HK_Query {
        query: Query::new(&collection),
    }))
}

#[no_mangle]
pub extern "C" fn hk_query_free(query: *mut HK_Query) {
    if !query.is_null() {
        unsafe { drop(Box::from_raw(query)) };
    }
}

#[no_mangle]
pub extern "C" fn hk_query_where_eq_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let value = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_query_where_eq_bool(
    query: *mut HK_Query,
    field: *const c_char,
    value: bool, // Receive the bool directly
) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }

    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };

    // let query_ptr = unsafe { &mut *query };

    // // Update the query with Value::Bool directly
    // query_ptr.query = query_ptr
    //     .query
    //     .clone()
    //     .where_filter(&field, Operator::Eq, Value::Bool(value));

    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error_err("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
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
pub extern "C" fn hk_query_delete(engine: *mut HK_Engine, query: *mut HK_Query) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() { return -1; }
        let engine = unsafe { &*engine };
        let query_ptr = unsafe { &*query };

        match engine.db.delete_where(query_ptr.query.clone()) {
            Ok(count) => {
                clear_last_error();
                count as i32
            },
            Err(e) => set_last_error_err(e.to_string()),
        }
    })
}

/// Local-only mass delete: marks every match so the wipe never leaves
/// this device, then deletes. See `hk_engine_delete_local`.
#[no_mangle]
pub extern "C" fn hk_query_delete_local(engine: *mut HK_Engine, query: *mut HK_Query) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() { return -1; }
        let engine = unsafe { &*engine };
        let query_ptr = unsafe { &*query };

        match engine.db.delete_where_local(query_ptr.query.clone()) {
            Ok(count) => {
                clear_last_error();
                count as i32
            },
            Err(e) => set_last_error_err(e.to_string()),
        }
    })
}

/// Executes the query and applies the updates from 'patch_doc' to all matches.
/// Returns the number of updated documents, or -1 on error.
#[no_mangle]
pub extern "C" fn hk_query_patch(
    engine: *mut HK_Engine, 
    query: *mut HK_Query, 
    patch_doc: *const HK_Doc
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() || patch_doc.is_null() { return -1; }
        let engine = unsafe { &*engine };
        let query_ptr = unsafe { &*query };
        let patch_ptr = unsafe { &*patch_doc };

        // Convert HK_Doc fields to the internal updates vector
        let updates: Vec<(String, Value)> = patch_ptr.hako().fields.iter()
            .map(|(k, v)| (k.to_string(), v.clone()))
            .collect();

        match engine.db.patch_where(query_ptr.query.clone(), updates) {
            Ok(count) => {
                clear_last_error();
                count as i32
            },
            Err(e) => set_last_error_err(e.to_string()),
        }
    })
}

fn apply_string_filter(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
    op: Operator,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let value = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query
    //     .query
    //     .clone()
    //     .where_filter(&field, op, Value::String(value));
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error_err("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
    let value = match cstr_to_string(value) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
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

fn apply_int_filter(query: *mut HK_Query, field: *const c_char, value: i64, op: Operator) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query
    //     .query
    //     .clone()
    //     .where_filter(&field, op, Value::Int(value));
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error_err("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
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
pub extern "C" fn hk_query_where_eq_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_query_where_ne_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Ne)
}

#[no_mangle]
pub extern "C" fn hk_query_where_ne_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Ne)
}

#[no_mangle]
pub extern "C" fn hk_query_where_gt_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Gt)
}

#[no_mangle]
pub extern "C" fn hk_query_where_gt_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Gt)
}

#[no_mangle]
pub extern "C" fn hk_query_where_gte_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Gte)
}

#[no_mangle]
pub extern "C" fn hk_query_where_gte_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Gte)
}

#[no_mangle]
pub extern "C" fn hk_query_where_lt_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Lt)
}

#[no_mangle]
pub extern "C" fn hk_query_where_lt_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Lt)
}

#[no_mangle]
pub extern "C" fn hk_query_where_lte_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::Lte)
}

#[no_mangle]
pub extern "C" fn hk_query_where_lte_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    apply_int_filter(query, field, value, Operator::Lte)
}

#[no_mangle]
pub extern "C" fn hk_query_where_array_contains(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let v = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let q = unsafe { &mut *query };
    q.query = q
        .query
        .clone()
        .where_filter(&f, Operator::ArrayContains, Value::String(v));
    0
}

#[no_mangle]
pub extern "C" fn hk_query_where_array_contains_any(
    query: *mut HK_Query,
    field: *const c_char,
    array: *mut HK_Array,
) -> i32 {
    if query.is_null() || array.is_null() {
        return -1;
    }
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let array_inner = unsafe { Box::from_raw(array) };
    q.query.filters.push(crate::query::filter::Filter {
        field: f,
        op: Operator::ArrayContainsAny,
        value: Value::Array(array_inner.items),
    });
    0
}

/// F3 `all_eq`: every element of `field` compares equal to the value.
/// `value_json` is a scalar JSON value (`2`, `"open"`, `true`, ...).
#[no_mangle]
pub extern "C" fn hk_query_where_array_all_eq(
    query: *mut HK_Query,
    field: *const c_char,
    value_json: *const c_char,
) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let raw = match parse_json_arg(value_json, "value") {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    if raw.is_array() {
        return set_last_error_err("all_eq value must be a scalar, not an array");
    }
    let q = unsafe { &mut *query };
    q.query.filters.push(crate::query::filter::Filter {
        field: f,
        op: Operator::ArrayAllEq,
        value: json_to_value(&raw),
    });
    clear_last_error();
    0
}

/// F3 `elemMatch`: some element of the array at `field` satisfies ALL the
/// filters in `filters_json`. Sub-filter fields are element-relative paths
/// (`"sku"`, `"meta.qty"`, `""` for the element itself).
///
/// `filters_json`: `[{"field":"qty","op":"gt","value":2}, ...]`.
#[no_mangle]
pub extern "C" fn hk_query_where_elem_match(
    query: *mut HK_Query,
    field: *const c_char,
    filters_json: *const c_char,
) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let filters = match parse_filters_json(filters_json) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    if filters.is_empty() {
        return set_last_error_err("elemMatch requires at least one filter");
    }
    let q = unsafe { &mut *query };
    q.query.elem_match.push(crate::query::query::ElemMatch {
        field: f,
        filters,
    });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_where_not_in(
    query: *mut HK_Query,
    field: *const c_char,
    array: *mut HK_Array,
) -> i32 {
    if query.is_null() || array.is_null() {
        return -1;
    }
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_query_order_by(
    query: *mut HK_Query,
    field: *const c_char,
    ascending: bool,
) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query.query.clone().order_by(&field, ascending);
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error_err("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
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
pub extern "C" fn hk_query_limit(query: *mut HK_Query, limit: usize) -> i32 {
    if query.is_null() {
        return -1;
    }
    let query = unsafe { &mut *query };
    query.query.limit = Some(limit);
    0
}

/// Opt in to deferred blobs: matching docs come back with blob-backed
/// fields as `Value::BlobLink` placeholders (no blob-file reads).
/// Resolve later with `hk_doc_resolve_blobs`. Default off (eager).
#[no_mangle]
pub extern "C" fn hk_query_defer_blobs(query: *mut HK_Query, defer: c_int) -> i32 {
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
pub extern "C" fn hk_doc_resolve_blobs(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc: *mut HK_Doc,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || doc.is_null() {
            return set_last_error_err("null engine/doc handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let engine = unsafe { &*engine };
        let doc = unsafe { &mut *doc };
        match engine.db.resolve_document_blobs(doc.hako_mut(), &collection) {
            Ok(_) => 0,
            Err(e) => set_last_error_err(format!("{}", e)),
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_query_offset(query: *mut HK_Query, offset: usize) -> i32 {
    // <--- NEW FFI
    if query.is_null() {
        return -1;
    }
    let query = unsafe { &mut *query };
    query.query.offset = Some(offset);
    0
}

#[no_mangle]
pub extern "C" fn hk_query_select_field(query: *mut HK_Query, field: *const c_char) -> i32 {
    // if query.is_null() {
    //     return set_last_error_err("null query handle");
    // }
    // let field = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let query = unsafe { &mut *query };
    // query.query = query.query.clone().select(&field);
    // clear_last_error();
    // 0
    if query.is_null() { return set_last_error_err("null query handle"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
    let query = unsafe { &mut *query };
    
    // FIX: Push directly
    query.query.projection.push(field);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_execute(engine: *mut HK_Engine, query: *const HK_Query) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
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
                        set_last_error_err(e.to_string());
                        ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error_err(e);
                ptr::null_mut()
            }
        }
    })
}

/// Query rows as one JSON array string, emitted from pinned bytes (no owned
/// `HakoDoc`). Output matches `hk_query_execute`; projections reuse that
/// path because the projected executor has its own field shape. Caller
/// frees with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_query_execute_json(
    engine: *mut HK_Engine,
    query: *const HK_Query,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
            return std::ptr::null_mut();
        }
        let query_obj = unsafe { &*query };
        if !query_obj.query.projection.is_empty() {
            return hk_query_execute(engine, query);
        }
        let engine = unsafe { &*engine };
        let mut out = Vec::with_capacity(4096);
        match engine.db.query_json_into(query_obj.query.clone(), &mut out) {
            Ok(_) => {
                clear_last_error();
                json_to_cstring(out).into_raw()
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

/// F7: plan explanation without executing the query. Returns JSON
/// (`ExplainReport`: scan/index, counts, covering, available indexes).
/// Caller frees with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_query_explain(
    engine: *mut HK_Engine,
    query: *const HK_Query,
) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let query = unsafe { &*query };
        match engine.db.explain_report(query.query.clone()) {
            Ok(report) => match serde_json::to_string(&report)
                .ok()
                .and_then(|s| CString::new(s).ok())
            {
                Some(s) => {
                    clear_last_error();
                    s.into_raw()
                }
                None => {
                    set_last_error_err("failed to encode explain report");
                    ptr::null_mut()
                }
            },
            Err(e) => {
                set_last_error_err(e);
                ptr::null_mut()
            }
        }
    })
}

// #[no_mangle]
// pub extern "C" fn hk_query_execute_to_handles(
//     engine: *mut HK_Engine,
//     query: *const HK_Query,
// ) -> *mut HK_ResultSet {
//     safety_shield!(std::ptr::null_mut(), {
//         let engine = unsafe { &*engine };
//         let query_obj = unsafe { &*query };

//         // 1. Run the actual query (Fast logic)
//         let results = engine.db.query(query_obj.query.clone()).unwrap_or_default();

//         // 2. Convert each result into a handle (*mut HK_Doc), just like 'get' does
//         let doc_handles: Vec<*mut HK_Doc> = results
//             .into_iter()
//             .map(|(_id, doc)| Box::into_raw(Box::new(HK_Doc { doc })))
//             .collect();

//         // 3. Wrap the list of handles in a ResultSet handle
//         Box::into_raw(Box::new(HK_ResultSet { docs: doc_handles }))
//     })
// }

#[no_mangle]
pub extern "C" fn hk_query_execute_to_handles(
    engine: *mut HK_Engine,
    query: *const HK_Query,
) -> *mut HK_ResultSet {
    safety_shield!(std::ptr::null_mut(), {
        let engine = unsafe { &*engine };
        let query_obj = unsafe { &*query };
        // Zero-copy rows (#142 Q3, M1 complete per #161): rows served by the
        // query row cache are shared with the engine (one refcount bump, no
        // deep clone); fresh rows stay owned `HakoDoc`s — no per-row
        // Arc::new/drop churn on scan lanes. Handles mutate via COW in
        // DocRef, so a cache entry is never aliased-mutated.
        let results = engine.db.query_row_refs(query_obj.query.clone()).unwrap_or_default();

        // PONYTAIL: store HK_Doc values directly in the ResultSet slab
        // (one allocation) instead of N individual Box::new(HK_Doc) per
        // result. hk_result_set_get_doc returns a borrowed pointer into
        // the slab; hk_result_set_free drops the whole Vec in one shot.
        // The C++ side already treats returned handles as borrowed (it
        // calls hk_doc_to_json or reads fields, never frees them itself),
        // so this is safe as long as hk_result_set_free is called before
        // the handles go out of scope.
        let docs: Vec<HK_Doc> = results
            .into_iter()
            .map(|(id, row)| {
                let doc = match row {
                    crate::query::executor::executor::RowRef::Shared(a) => DocRef::Shared(a),
                    crate::query::executor::executor::RowRef::Owned(d) => DocRef::Owned(d),
                };
                HK_Doc { id, doc }
            })
            .collect();
        Box::into_raw(Box::new(HK_ResultSet { docs }))
    })
}

#[no_mangle]
pub extern "C" fn hk_result_set_count(results: *mut HK_ResultSet) -> usize {
    if results.is_null() { return 0; }
    unsafe {
        // results.as_ref() returns Option<&HK_ResultSet>
        results.as_ref().map(|rs| rs.docs.len()).unwrap_or(0)
    }
}

#[no_mangle]
pub extern "C" fn hk_result_set_get_doc(results: *mut HK_ResultSet, index: usize) -> *mut HK_Doc {
    if results.is_null() { return std::ptr::null_mut(); }
    unsafe {
        // Borrowed pointer into the HK_ResultSet's docs slab. The caller
        // MUST not free this handle and MUST call hk_result_set_free
        // before the handle goes out of scope. C++ code already follows
        // this contract (it reads fields or passes the handle to
        // hk_doc_to_json without calling hk_doc_free).
        if let Some(rs) = results.as_ref() {
            rs.docs.get(index)
                .map(|d| d as *const HK_Doc as *mut HK_Doc)
                .unwrap_or(std::ptr::null_mut())
        } else {
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_result_set_free(results: *mut HK_ResultSet) {
    if !results.is_null() {
        // PONYTAIL: docs is now Vec<HK_Doc> (owned values), not Vec<*mut
        // HK_Doc>. Dropping the Box<Vec<HK_Doc>> drops every HK_Doc in
        // one shot — no per-doc Box::from_raw walk needed.
        unsafe { drop(Box::from_raw(results)); }
    }
}

// --- Raw result sets (ponytail) ---
// Same slab shape as HK_ResultSet, but rows are pinned storage bytes
// instead of decoded docs. The C++ side reuses the same HK_Query builders
// (order/limit/start_after via hk_query_start_after_raw) — query_raw
// forces raw=true internally, so zero new query-builder surface.
// Page like the decoded path: execute -> read rows via hk_rawdoc_bytes
// (borrowed, no copy) -> bound the next page with the last row ->
// free the set -> repeat. Decode any row later with hk_rawdoc_to_doc.
#[no_mangle]
pub extern "C" fn hk_query_execute_raw(
    engine: *mut HK_Engine,
    query: *const HK_Query,
) -> *mut HK_RawResultSet {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
            return std::ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let query_obj = unsafe { &*query };
        let results = engine.db.query_raw(query_obj.query.clone()).unwrap_or_default();
        let docs: Vec<HK_RawDoc> = results
            .into_iter()
            .map(|(id, bytes)| HK_RawDoc { id, bytes })
            .collect();
        Box::into_raw(Box::new(HK_RawResultSet { docs }))
    })
}

#[no_mangle]
pub extern "C" fn hk_rawresult_count(results: *mut HK_RawResultSet) -> usize {
    if results.is_null() { return 0; }
    unsafe {
        results.as_ref().map(|rs| rs.docs.len()).unwrap_or(0)
    }
}

#[no_mangle]
pub extern "C" fn hk_rawresult_get(results: *mut HK_RawResultSet, index: usize) -> *mut HK_RawDoc {
    if results.is_null() { return std::ptr::null_mut(); }
    unsafe {
        // Borrowed pointer into the slab — same contract as
        // hk_result_set_get_doc: do not free, free the set first.
        if let Some(rs) = results.as_ref() {
            rs.docs.get(index)
                .map(|d| d as *const HK_RawDoc as *mut HK_RawDoc)
                .unwrap_or(std::ptr::null_mut())
        } else {
            std::ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_rawresult_free(results: *mut HK_RawResultSet) {
    if !results.is_null() {
        unsafe { drop(Box::from_raw(results)); }
    }
}

/// Borrowed byte view of a raw doc. Returns null on null handle; `*len_out`
/// (when non-null) receives the length. Valid until hk_rawresult_free —
/// zero copies for inlined docs.
#[no_mangle]
pub extern "C" fn hk_rawdoc_bytes(doc: *const HK_RawDoc, len_out: *mut usize) -> *const u8 {
    if doc.is_null() { return std::ptr::null(); }
    unsafe {
        if let Some(d) = doc.as_ref() {
            if !len_out.is_null() {
                *len_out = d.bytes.len();
            }
            d.bytes.as_ptr()
        } else {
            std::ptr::null()
        }
    }
}

/// Borrowed id view of a raw doc (for keyset paging without decoding).
/// Rust Strings are NOT NUL-terminated, so the length goes through
/// `*len_out` (when non-null) — read `(ptr, len)`, do NOT treat as CStr.
/// Valid until hk_rawresult_free.
#[no_mangle]
pub extern "C" fn hk_rawdoc_id(doc: *const HK_RawDoc, len_out: *mut usize) -> *const c_char {
    if doc.is_null() { return std::ptr::null(); }
    unsafe {
        if let Some(d) = doc.as_ref() {
            if !len_out.is_null() {
                *len_out = d.id.len();
            }
            d.id.as_ptr() as *const c_char
        } else {
            std::ptr::null()
        }
    }
}

/// Keyset anchor from a raw row. `id`-ordered queries bind the id with no
/// decode; other order fields decode the row ONCE per page (not per row).
#[no_mangle]
pub extern "C" fn hk_query_start_after_raw(query: *mut HK_Query, anchor_doc: *const HK_RawDoc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() { return -1; }
        let q = unsafe { &mut *query };
        let raw = unsafe { &*anchor_doc };
        if q.query.order_by.is_empty() {
            return set_last_error_err("raw anchor needs order_by");
        }
        let mut vals = Vec::with_capacity(q.query.order_by.len());
        let mut decoded: Option<HakoDoc> = None;
        for order in &q.query.order_by {
            if order.field == "id" {
                vals.push(Value::String(raw.id.clone()));
                continue;
            }
            if decoded.is_none() {
                decoded = match HakoDoc::decode(&raw.bytes) {
                    Some(d) => Some(d),
                    None => return set_last_error_err("raw anchor bytes do not decode"),
                };
            }
            let d = decoded.as_ref().unwrap();
            if order.field == "_time" {
                vals.push(Value::Int(d._time));
            } else {
                match d.get(&order.field) {
                    Some(v) => vals.push(v.clone()),
                    None => return set_last_error_err("Anchor document missing one or more fields from sort chain"),
                }
            }
        }
        q.query.start_after = Some(vals);
        0
    })
}

/// The pointer resolver: decode a raw row into an owned HK_Doc (blob
/// fields inflated via the engine, same as a decoded query row).
#[no_mangle]
pub extern "C" fn hk_rawdoc_to_doc(
    engine: *mut HK_Engine,
    raw_doc: *const HK_RawDoc,
    collection: *const c_char,
) -> *mut HK_Doc {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || raw_doc.is_null() || collection.is_null() {
            set_last_error_err("null engine/raw_doc/collection handle");
            return std::ptr::null_mut();
        }
        let collection_c = unsafe { CStr::from_ptr(collection) };
        let collection = match collection_c.to_str() {
            Ok(v) => v,
            Err(_) => {
                set_last_error_err("invalid utf8");
                return std::ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        let raw = unsafe { &*raw_doc };
        let mut doc = match HakoDoc::decode(&raw.bytes) {
            Some(d) => d,
            None => {
                set_last_error_err("raw bytes do not decode");
                return std::ptr::null_mut();
            }
        };
        if let Err(e) = engine.db.resolve_document_blobs(&mut doc, collection) {
            set_last_error_err(e.to_string());
            return std::ptr::null_mut();
        }
        Box::into_raw(Box::new(HK_Doc { id: raw.id.clone(), doc: DocRef::Owned(doc) }))
    })
}

// --- Cursor walk (ponytail) ---
// Zero-alloc scan: one FFI call per walk (not per row, not per page). The
// engine lends each row — id and bytes borrowed, no String/Arc/Vec per
// row — and the callback returns true to continue, false to stop early.
// Returns rows visited, or -1 on error (see hk_last_error).
//
// CONTRACT (mirrors db.walk): borrowed pointers die with the call; the
// callback MUST NOT re-enter the engine (read lock held for the walk);
// C cannot unwind, so no per-row shield is needed — the outer shield
// covers setup only. Decode any row via hk_rawdoc_to_doc semantics on
// your own copy (copy the bytes first if you must keep them).
#[allow(non_camel_case_types)]
pub type HkWalkCallback = Option<
    unsafe extern "C" fn(
        id: *const c_char,
        id_len: usize,
        bytes: *const u8,
        bytes_len: usize,
        userdata: *mut std::ffi::c_void,
    ) -> bool,
>;

#[no_mangle]
pub extern "C" fn hk_cursor_walk(
    engine: *mut HK_Engine,
    query: *const HK_Query,
    callback: HkWalkCallback,
    userdata: *mut std::ffi::c_void,
) -> i64 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
            return -1;
        }
        let cb = match callback {
            Some(f) => f,
            None => {
                set_last_error_err("null walk callback");
                return -1;
            }
        };
        let engine = unsafe { &*engine };
        let query_obj = unsafe { &*query };
        // ponytail: C callbacks cannot unwind, so this direct invocation
        // costs one indirect call per row — no catch_unwind, no copies.
        let res = engine.db.walk(query_obj.query.clone(), &mut |id: &str, bytes: &[u8]| unsafe {
            cb(
                id.as_ptr() as *const c_char,
                id.len(),
                bytes.as_ptr(),
                bytes.len(),
                userdata,
            )
        });
        match res {
            Ok(n) => n as i64,
            Err(e) => {
                set_last_error_err(e.to_string());
                -1
            }
        }
    })
}

/// JSON row callback: same signature as `hk_cursor_walk`, but `bytes` is
/// UTF-8 JSON rather than storage framing. Rows are materialized by
/// `walk_json`; `false` stops early. Returns rows visited, -1 on error.
#[no_mangle]
pub extern "C" fn hk_cursor_walk_json(
    engine: *mut HK_Engine,
    query: *const HK_Query,
    callback: HkWalkCallback,
    userdata: *mut std::ffi::c_void,
) -> i64 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
            return -1;
        }
        let cb = match callback {
            Some(f) => f,
            None => {
                set_last_error_err("null walk callback");
                return -1;
            }
        };
        let engine = unsafe { &*engine };
        let query_obj = unsafe { &*query };
        let res = engine
            .db
            .walk_json(query_obj.query.clone(), &mut |id: &str, json: &[u8]| unsafe {
                cb(
                    id.as_ptr() as *const c_char,
                    id.len(),
                    json.as_ptr(),
                    json.len(),
                    userdata,
                )
            });
        match res {
            Ok(n) => n as i64,
            Err(e) => {
                set_last_error_err(e.to_string());
                -1
            }
        }
    })
}

// --- Borrowed views (ponytail, v0.8.11+) ---
// The sqlite3_column_* analog: an owned HK_ViewDoc handle pins storage
// bytes; typed getters pull single fields with zero owned construction.
// Strict matches only (no cross-type coercion — convert caller-side).
// Views never inflate blobs: BlobLink fields read back as missing here;
// resolve via hk_view_to_doc + hk_doc_resolve_blobs when you need them.
#[allow(non_camel_case_types)]
pub struct HK_ViewDoc {
    view: crate::document::hako_doc::DocView,
}

#[no_mangle]
pub extern "C" fn hk_view_get(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut HK_ViewDoc {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || collection.is_null() || doc_id.is_null() {
            set_last_error_err("null engine/collection/doc_id handle");
            return std::ptr::null_mut();
        }
        let collection_c = unsafe { CStr::from_ptr(collection) };
        let doc_id_c = unsafe { CStr::from_ptr(doc_id) };
        let (collection, doc_id) = match (collection_c.to_str(), doc_id_c.to_str()) {
            (Ok(c), Ok(d)) => (c, d),
            _ => {
                set_last_error_err("invalid utf8");
                return std::ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        match engine.db.get_view(collection, doc_id) {
            Ok(Some(view)) => Box::into_raw(Box::new(HK_ViewDoc { view })),
            Ok(None) => std::ptr::null_mut(),
            Err(e) => {
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_view_free(view: *mut HK_ViewDoc) {
    if !view.is_null() {
        unsafe { drop(Box::from_raw(view)); }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_field_count(view: *const HK_ViewDoc) -> usize {
    if view.is_null() {
        return 0;
    }
    unsafe { view.as_ref().map(|v| v.view.len()).unwrap_or(0) }
}

#[no_mangle]
pub extern "C" fn hk_view_has_field(view: *const HK_ViewDoc, key: *const c_char) -> bool {
    if view.is_null() || key.is_null() {
        return false;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(key).to_str()) {
            (Some(v), Ok(k)) => v.view.get(k).is_some(),
            _ => false,
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_get_int(view: *const HK_ViewDoc, key: *const c_char, out: *mut i64) -> bool {
    if view.is_null() || key.is_null() || out.is_null() {
        return false;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(key).to_str()) {
            (Some(v), Ok(k)) => match v.view.get(k) {
                Some(Value::Int(i)) => {
                    *out = i;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_get_float(view: *const HK_ViewDoc, key: *const c_char, out: *mut f64) -> bool {
    if view.is_null() || key.is_null() || out.is_null() {
        return false;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(key).to_str()) {
            (Some(v), Ok(k)) => match v.view.get(k) {
                Some(Value::Float(f)) => {
                    *out = f;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_get_bool(view: *const HK_ViewDoc, key: *const c_char) -> i32 {
    if view.is_null() || key.is_null() {
        return -1;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(key).to_str()) {
            (Some(v), Ok(k)) => match v.view.get(k) {
                Some(Value::Bool(b)) => {
                    if b {
                        1
                    } else {
                        0
                    }
                }
                _ => -1,
            },
            _ => -1,
        }
    }
}

/// Borrowed UTF-8 view of a String field. Returns null when missing or not
/// a String; `*len_out` (when non-null) receives the byte length. Valid
/// until hk_view_free — same borrowed contract as hk_rawdoc_bytes.
#[no_mangle]
pub extern "C" fn hk_view_get_str(
    view: *const HK_ViewDoc,
    key: *const c_char,
    len_out: *mut usize,
) -> *const c_char {
    if view.is_null() || key.is_null() {
        return std::ptr::null();
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(key).to_str()) {
            (Some(v), Ok(k)) => match v.view.get_str(k) {
                Some(s) => {
                    if !len_out.is_null() {
                        *len_out = s.len();
                    }
                    s.as_ptr() as *const c_char
                }
                None => std::ptr::null(),
            },
            _ => std::ptr::null(),
        }
    }
}

/// Borrowed view of a Binary field. Same lifetime contract as above.
#[no_mangle]
pub extern "C" fn hk_view_get_bytes(
    view: *const HK_ViewDoc,
    key: *const c_char,
    len_out: *mut usize,
) -> *const u8 {
    if view.is_null() || key.is_null() {
        return std::ptr::null();
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(key).to_str()) {
            (Some(v), Ok(k)) => match v.view.get_bytes(k) {
                Some(b) => {
                    if !len_out.is_null() {
                        *len_out = b.len();
                    }
                    b.as_ptr()
                }
                None => std::ptr::null(),
            },
            _ => std::ptr::null(),
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_get_path_int(
    view: *const HK_ViewDoc,
    path: *const c_char,
    out: *mut i64,
) -> bool {
    if view.is_null() || path.is_null() || out.is_null() {
        return false;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(path).to_str()) {
            (Some(v), Ok(p)) => match v.view.get_path(p) {
                Some(Value::Int(i)) => {
                    *out = i;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_get_path_float(
    view: *const HK_ViewDoc,
    path: *const c_char,
    out: *mut f64,
) -> bool {
    if view.is_null() || path.is_null() || out.is_null() {
        return false;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(path).to_str()) {
            (Some(v), Ok(p)) => match v.view.get_path(p) {
                Some(Value::Float(f)) => {
                    *out = f;
                    true
                }
                _ => false,
            },
            _ => false,
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_view_get_path_bool(view: *const HK_ViewDoc, path: *const c_char) -> i32 {
    if view.is_null() || path.is_null() {
        return -1;
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(path).to_str()) {
            (Some(v), Ok(p)) => match v.view.get_path(p) {
                Some(Value::Bool(b)) => {
                    if b {
                        1
                    } else {
                        0
                    }
                }
                _ => -1,
            },
            _ => -1,
        }
    }
}

/// Borrowed UTF-8 at a dotted path. Same lifetime contract as
/// `hk_view_get_str`.
#[no_mangle]
pub extern "C" fn hk_view_get_path_str(
    view: *const HK_ViewDoc,
    path: *const c_char,
    len_out: *mut usize,
) -> *const c_char {
    if view.is_null() || path.is_null() {
        return std::ptr::null();
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(path).to_str()) {
            (Some(v), Ok(p)) => match v.view.get_path_str(p) {
                Some(s) => {
                    if !len_out.is_null() {
                        *len_out = s.len();
                    }
                    s.as_ptr() as *const c_char
                }
                None => std::ptr::null(),
            },
            _ => std::ptr::null(),
        }
    }
}

/// Borrowed Binary at a dotted path. Same lifetime contract as
/// `hk_view_get_bytes`.
#[no_mangle]
pub extern "C" fn hk_view_get_path_bytes(
    view: *const HK_ViewDoc,
    path: *const c_char,
    len_out: *mut usize,
) -> *const u8 {
    if view.is_null() || path.is_null() {
        return std::ptr::null();
    }
    unsafe {
        match (view.as_ref(), CStr::from_ptr(path).to_str()) {
            (Some(v), Ok(p)) => match v.view.get_path_bytes(p) {
                Some(b) => {
                    if !len_out.is_null() {
                        *len_out = b.len();
                    }
                    b.as_ptr()
                }
                None => std::ptr::null(),
            },
            _ => std::ptr::null(),
        }
    }
}

/// Escape hatch: full owned decode of the pinned bytes (links unresolved —
/// follow with hk_doc_resolve_blobs when needed).
#[no_mangle]
pub extern "C" fn hk_view_to_doc(view: *const HK_ViewDoc, doc_id: *const c_char) -> *mut HK_Doc {
    safety_shield!(std::ptr::null_mut(), {
        if view.is_null() || doc_id.is_null() {
            set_last_error_err("null view/doc_id handle");
            return std::ptr::null_mut();
        }
        let id_c = unsafe { CStr::from_ptr(doc_id) };
        let id = match id_c.to_str() {
            Ok(v) => v.to_string(),
            Err(_) => {
                set_last_error_err("invalid utf8");
                return std::ptr::null_mut();
            }
        };
        let view = unsafe { &*view };
        match view.view.to_owned_doc() {
            Some(doc) => Box::into_raw(Box::new(HK_Doc { id, doc: DocRef::Owned(doc) })),
            None => {
                set_last_error_err("view bytes do not decode");
                std::ptr::null_mut()
            }
        }
    })
}

/// Full JSON from the pinned view bytes, no owned decode. Blob links stay
/// as `{"__blob__":...}` (a view cannot inflate); use `hk_engine_get_json`
/// when resolved blob values are required. Caller frees with
/// `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_view_to_json(view: *const HK_ViewDoc) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if view.is_null() {
            set_last_error_err("null view handle");
            return std::ptr::null_mut();
        }
        let view = unsafe { &*view };
        match view.view.to_json_bytes() {
            Some(out) => {
                clear_last_error();
                json_to_cstring(out).into_raw()
            }
            None => {
                set_last_error_err("view bytes do not decode");
                std::ptr::null_mut()
            }
        }
    })
}

/// View-walk callback: borrowed id + a borrowed view handle (valid for the
/// call only — do not free, do not retain). Return true to continue.
#[allow(non_camel_case_types)]
pub type HkViewWalkCallback = Option<
    unsafe extern "C" fn(
        id: *const c_char,
        id_len: usize,
        view: *const HK_ViewDoc,
        userdata: *mut std::ffi::c_void,
    ) -> bool,
>;

/// One-call lazy scan: lends each row as a view (no decode, no owned
/// construction). Returns rows visited, -1 on error. Same no-reentry
/// contract as hk_cursor_walk.
#[no_mangle]
pub extern "C" fn hk_cursor_walk_view(
    engine: *mut HK_Engine,
    query: *const HK_Query,
    callback: HkViewWalkCallback,
    userdata: *mut std::ffi::c_void,
) -> i64 {
    safety_shield!(-1, {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine/query handle");
            return -1;
        }
        let cb = match callback {
            Some(f) => f,
            None => {
                set_last_error_err("null walk callback");
                return -1;
            }
        };
        let engine = unsafe { &*engine };
        let query_obj = unsafe { &*query };
        // ponytail: one Arc bump per row builds the stack-slot view the C
        // side borrows — no handle alloc, no free protocol, no byte copies.
        // The slot dies when the callback returns, so there is nothing to
        // retain or free, by construction.
        let res = engine.db.walk_view(query_obj.query.clone(), &mut |id: &str, view: &_| unsafe {
            let slot = HK_ViewDoc { view: view.clone() };
            cb(
                id.as_ptr() as *const c_char,
                id.len(),
                &slot as *const HK_ViewDoc,
                userdata,
            )
        });
        match res {
            Ok(n) => n as i64,
            Err(e) => {
                set_last_error_err(e.to_string());
                -1
            }
        }
    })
}

// --- Bulk JSON (ponytail) ---
// Streams a result set as one JSON array string with NO intermediate
// serde_json::Value DOM. Output is byte-identical to "[" +
// hk_doc_to_json(row) joined + "]" — locked by `bulk_json_matches_per_doc`
// below (both now carry `_time` at its sorted position). Notes:
// - serde_json::Map is a BTreeMap (no preserve_order): keys are SORTED, so
//   fields are collected and sorted (one small Vec per doc, still ~15x
//   fewer allocs than the DOM path), including nested Maps.
// - The inner __blob__ meta sorts as {"len","offset"} — emitted in that
//   order explicitly.
// - No `id` field: mirrors hk_doc_to_json exactly (correlate by index,
//   same as hk_result_set_get_doc today).
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

struct SlabJson<'a>(&'a [HK_Doc]);

/// One document's fields as a sorted map, now WITH `_time` (mirrors the
/// unified `doc_to_json` shape above — the bulk renderer and per-doc path
/// must stay byte-identical, locked by `bulk_json_matches_per_doc`).
struct DocFields<'a>(&'a HakoDoc);
impl<'a> Serialize for DocFields<'a> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut pairs: Vec<(&str, &Value)> = self
            .0
            .fields
            .iter()
            .map(|(k, v)| (AsRef::<str>::as_ref(k), v))
            .collect();
        pairs.sort_by(|a, b| a.0.cmp(b.0));
        let mut m = s.serialize_map(Some(pairs.len() + 1))?;
        // _time merges at BTreeMap-sorted position; a literal "_time"
        // field wins and swallows the timestamp (map-overwrite parity).
        let mut time_written = false;
        for (k, v) in pairs {
            if !time_written && k > "_time" {
                m.serialize_entry("_time", &self.0._time)?;
                time_written = true;
            }
            if !time_written && k == "_time" {
                time_written = true;
            }
            m.serialize_entry(k, &StreamValue(v))?;
        }
        if !time_written {
            m.serialize_entry("_time", &self.0._time)?;
        }
        m.end()
    }
}

impl<'a> Serialize for SlabJson<'a> {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        let mut seq = s.serialize_seq(Some(self.0.len()))?;
        for row in self.0.iter() {
            seq.serialize_element(&DocFields(row.hako()))?;
        }
        seq.end()
    }
}

/// Bulk result-set to JSON: one call, one JSON array string, no per-doc
/// DOM and no per-doc FFI round trips. Byte-identical to joining
/// `hk_doc_to_json` per row. Caller frees with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_result_set_to_json(results: *mut HK_ResultSet) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if results.is_null() {
            set_last_error_err("null result set handle");
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
                    set_last_error_err(e.to_string());
                    ptr::null_mut()
                }
            },
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_doc_to_json(doc: *const HK_Doc) -> *mut c_char {
    if doc.is_null() {
        set_last_error_err("null doc handle");
        return ptr::null_mut();
    }

    let doc = unsafe { &*doc };
    match doc_to_cstring(doc.hako()) {
        Ok(s) => {
            clear_last_error();
            s.into_raw()
        }
        Err(e) => {
            set_last_error_err(e);
            ptr::null_mut()
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_last_error() -> *const c_char {
    LAST_ERROR.with(|slot| {
        slot.borrow()
            .as_ref()
            .map(|s| s.as_ptr())
            .unwrap_or(ptr::null())
    })
}

/// Classification code of the last error on this thread (issue #71/F5):
/// 0 = none, 1 = generic do-not-retry, 2 = retryable conflict /
/// backfill-in-progress, 3 = unique-index violation. SDK retry loops
/// branch on this — never on the message string. Stable numeric contract.
#[no_mangle]
pub extern "C" fn hk_error_last_kind() -> i32 {
    LAST_ERROR_KIND.with(|k| k.get())
}

/// Convenience predicate for SDK retry loops: true iff the last error on
/// this thread is classified retryable (kind 2, issue #71/F5). Equivalent to
/// `hk_error_last_kind() == 2`; kept so bindings do not hardcode the code.
#[no_mangle]
pub extern "C" fn hk_error_is_retryable() -> bool {
    LAST_ERROR_KIND.with(|k| k.get()) == crate::error::ErrorKind::RetryableConflict as i32
}

/// Enable library diagnostic logging to stderr. Default OFF. Idempotent.
#[no_mangle]
pub extern "C" fn hk_log_enable_stderr() {
    crate::util::log::enable_stderr();
}

/// Disable library diagnostic logging to stderr. Default OFF. Idempotent.
#[no_mangle]
pub extern "C" fn hk_log_disable_stderr() {
    crate::util::log::disable_stderr();
}

#[no_mangle]
pub extern "C" fn hk_string_free(value: *mut c_char) {
    if !value.is_null() {
        unsafe {
            let _ = CString::from_raw(value);
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_query_aggregate_count(query: *mut HK_Query) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Count);
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_aggregate_sum(query: *mut HK_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(s) => s,
        Err(e) => return set_last_error_err(e),
    };
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Sum(field));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_aggregate_avg(query: *mut HK_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(s) => s,
        Err(e) => return set_last_error_err(e),
    };
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Avg(field));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_aggregate_min(query: *mut HK_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(s) => s,
        Err(e) => return set_last_error_err(e),
    };
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Min(field));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_aggregate_max(query: *mut HK_Query, field: *const c_char) -> i32 {
    if query.is_null() {
        return set_last_error_err("null query handle");
    }
    let field = match cstr_to_string(field) {
        Ok(s) => s,
        Err(e) => return set_last_error_err(e),
    };
    let q = unsafe { &mut *query };
    q.query.aggregations.push(AggregateOp::Max(field));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_query_execute_aggregation(
    engine: *mut HK_Engine,
    query: *const HK_Query,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || query.is_null() {
            set_last_error_err("null engine or query handle");
            return std::ptr::null_mut();
        }

        let engine = unsafe { &*engine };
        let query_wrapper = unsafe { &*query };

        // Call the public method on Hako.
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
                            set_last_error_err(e.to_string());
                            std::ptr::null_mut()
                        }
                    },
                    Err(e) => {
                        set_last_error_err(e.to_string());
                        std::ptr::null_mut()
                    }
                }
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_timestamp(
    doc: *mut HK_Doc,
    key: *const c_char,
    micros: i64,
) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::Timestamp(micros));
    0
}

#[no_mangle]
pub extern "C" fn hk_doc_insert_server_timestamp(doc: *mut HK_Doc, key: *const c_char) -> i32 {
    if doc.is_null() {
        return set_last_error_err("null doc");
    }
    let key = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let doc = unsafe { &mut *doc };
    doc.hako_mut().insert(key, Value::ServerTimestamp);
    0
}

#[no_mangle]
pub extern "C" fn hk_engine_backup(engine: *mut HK_Engine, path: *const c_char) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() {
            return set_last_error_err("null engine");
        }
        let engine = unsafe { &*engine };
        let path = match cstr_to_string(path) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        match engine.db.backup(path) {
            Ok(_) => 0,
            Err(e) => {
                set_last_error_err(e.to_string());
                -1 // or ptr::null_mut() depending on function return type
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_query_where_match(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // let f = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let v = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_query_where_match_prefix(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    apply_string_filter(query, field, value, Operator::MatchPrefix)
}

#[no_mangle]
pub extern "C" fn hk_query_where_contains(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // let f = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let v = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_query_where_starts_with(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    // let f = match cstr_to_string(field) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
    // };
    // let v = match cstr_to_string(value) {
    //     Ok(v) => v,
    //     Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_engine_list_collections(engine: *mut HK_Engine) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
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
                set_last_error_err(e.to_string());
                std::ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_array_new() -> *mut HK_Array {
    Box::into_raw(Box::new(HK_Array { items: Vec::new() }))
}

#[no_mangle]
pub extern "C" fn hk_array_free(array: *mut HK_Array) {
    if !array.is_null() {
        unsafe { drop(Box::from_raw(array)) };
    }
}

// --- ARRAY PUSH METHODS ---
#[no_mangle]
pub extern "C" fn hk_array_append_str(array: *mut HK_Array, value: *const c_char) -> i32 {
    let s = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    unsafe {
        (*array).items.push(Value::String(s));
    }
    0
}

#[no_mangle]
pub extern "C" fn hk_array_append_int(array: *mut HK_Array, value: i64) -> i32 {
    unsafe {
        (*array).items.push(Value::Int(value));
    }
    0
}

#[no_mangle]
pub extern "C" fn hk_array_append_doc(array: *mut HK_Array, doc: *const HK_Doc) -> i32 {
    safety_shield!(-1, {
        if array.is_null() || doc.is_null() {
            return set_last_error_err("null array or doc handle");
        }

        let array_ptr = unsafe { &mut *array };
        let doc_ptr = unsafe { &*doc };

        // Convert the document's fields into a Value::Map and push to the array
        array_ptr.items.push(Value::Map(doc_ptr.hako().fields.clone()));

        clear_last_error();
        0
    })
}

// --- DOCUMENT NESTING METHODS ---

/// Takes the contents of 'child' and inserts it as a Map into 'parent'
#[no_mangle]
pub extern "C" fn hk_doc_insert_doc(
    parent: *mut HK_Doc,
    key: *const c_char,
    child: *const HK_Doc,
) -> i32 {
    safety_shield!(-1, {
        let key = match cstr_to_string(key) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let parent_doc = unsafe { &mut *parent };
        let child_doc = unsafe { &*child };

        // Convert the child document's fields into a Value::Map
        parent_doc
            .hako_mut()
            .insert(key, Value::Map(child_doc.hako().fields.clone()));
        0
    })
}

/// Takes the contents of 'array' and inserts it into the document
#[no_mangle]
pub extern "C" fn hk_doc_insert_array(
    doc: *mut HK_Doc,
    key: *const c_char,
    array: *mut HK_Array,
) -> i32 {
    safety_shield!(-1, {
        let key = match cstr_to_string(key) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let doc = unsafe { &mut *doc };
        let array_inner = unsafe { Box::from_raw(array) }; // Take ownership and free HK_Array

        doc.hako_mut().insert(key, Value::Array(array_inner.items));
        0
    })
}

#[no_mangle]
pub extern "C" fn hk_engine_patch(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    updates: *const HK_Doc,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || updates.is_null() {
            return -1;
        }
        let engine = unsafe { &*engine };
        let col = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let update_doc = unsafe { &*updates };

        let mut updates_vec: Vec<(String, Value)> = Vec::new();
        for (k, v) in &update_doc.hako().fields {
            updates_vec.push((k.to_string(), v.clone()));
        }

        // FIX: Access .db and ensure set_last_error returns correctly
        match engine.db.patch(&col, &id, updates_vec) {
            Ok(_) => 0,
            Err(e) => set_last_error_err(e),
        }
    })
}

// --- 1. INDEX MANAGEMENT ---

/// Creates a composite index from C++.
/// fields_json should be like: [{"field": "age", "desc": false}]
/// Optional constraint form (hakodb#71):
///   {"fields": [...], "unique": true, "sparse": false,
///    "filter": {"logic": "And", "filters": [...]}
/// — legacy bare arrays keep working (non-unique, sparse semantics).
#[no_mangle]
pub extern "C" fn hk_engine_create_index(
    engine: *mut HK_Engine,
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
    let mut unique = false;
    let mut sparse = true;
    let mut filter: Option<crate::query::filter::FilterGroup> = None;
    let items: &[serde_json::Value] = match fields_raw.first() {
        None => return 0,
        Some(first) => {
            // Envelope object unless the first item is already a bare
            // field spec ({"field": ...} or {"field","desc"}).
            if first.is_object() && first.get("field").is_none() && first.get("fields").is_some() {
                let env = &fields_raw[0];
                let Some(arr) = env["fields"].as_array() else { return 0 };
                unique = env.get("unique").and_then(serde_json::Value::as_bool).unwrap_or(false);
                sparse = env.get("sparse").and_then(serde_json::Value::as_bool).unwrap_or(true);
                filter = env.get("filter")
                    .map(|v| serde_json::from_value(v.clone()))
                    .transpose()
                    .ok()
                    .flatten();
                arr
            } else {
                &fields_raw
            }
        }
    };

    for item in items {
        let f = item["field"].as_str().unwrap_or("").to_string();
        let desc = item["desc"].as_bool().unwrap_or(false);
        let dir = if desc {
            SortDirection::Desc
        } else {
            SortDirection::Asc
        };
        fields.push((f, dir));
    }

    match engine.db.create_composite_index_ex(&col, fields, unique, sparse, filter) {
        Ok(id) => id,
        Err(e) => {
            set_last_error_err(&e);
            0 // Return 0 to indicate failure
        }
    }
}

/// Drops a composite index by id (idempotent). Returns 0 on success,
/// -1 when the id belongs to another collection or the drop fails.
#[no_mangle]
pub extern "C" fn hk_engine_drop_composite_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
    id: u32,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    match engine.db.drop_composite_index(&col, id) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e),
    }
}

/// Simplified indexer: Create an index for a single field.
#[no_mangle]
pub extern "C" fn hk_engine_create_simple_index(
    engine: *mut HK_Engine,
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
pub extern "C" fn hk_engine_drop_simple_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
    field: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let fld = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    match engine.db.drop_index(&col, &fld) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e),
    }
}

#[no_mangle]
pub extern "C" fn hk_engine_create_fts_index(
    engine: *mut HK_Engine,
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
pub extern "C" fn hk_engine_drop_fts_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
    field: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let fld = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    match engine.db.drop_fts_index(&col, &fld) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e),
    }
}

#[no_mangle]
pub extern "C" fn hk_engine_create_vector_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
    field: *const c_char,
    dim: u32,
    metric: u32,
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
    let m = match crate::index::vector::Metric::from_u32(metric) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e.to_string()),
    };
    match engine.db.create_vector_index(&col, &fld, dim, m) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

/// Vector index with explicit HNSW parameters (#141-1). Pass 0 for a
/// parameter to ride the per-dim derived default. Additive to
/// `hk_engine_create_vector_index` (unchanged; derived defaults).
#[no_mangle]
pub extern "C" fn hk_engine_create_vector_index_v2(
    engine: *mut HK_Engine,
    collection: *const c_char,
    field: *const c_char,
    dim: u32,
    metric: u32,
    m: usize,
    ef_construction: usize,
    ef_search: usize,
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
    let met = match crate::index::vector::Metric::from_u32(metric) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e.to_string()),
    };
    let params = crate::index::vector::HnswParams {
        m: (m > 0).then_some(m),
        ef_construction: (ef_construction > 0).then_some(ef_construction),
        ef_search: (ef_search > 0).then_some(ef_search),
    };
    match engine
        .db
        .create_vector_index_with_params(&col, &fld, dim, met, params)
    {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn hk_engine_drop_vector_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
    field: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let fld = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    match engine.db.drop_vector_index(&col, &fld) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e),
    }
}

#[no_mangle]
pub extern "C" fn hk_query_where_near(
    query: *mut HK_Query,
    field: *const c_char,
    vec_ptr: *const f32,
    vec_len: usize,
) -> i32 {
    if query.is_null() { return set_last_error_err("null query handle"); }
    if vec_ptr.is_null() { return set_last_error_err("null vector pointer"); }
    let field = match cstr_to_string(field) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
    let slice = unsafe { std::slice::from_raw_parts(vec_ptr, vec_len) };
    let query = unsafe { &mut *query };
    query.query.filters.push(crate::query::filter::Filter {
        field,
        op: Operator::Near,
        value: Value::Binary(crate::index::vector::encode_f32s(slice)),
    });
    clear_last_error();
    0
}

/// One-shot KNN for FFI consumers (hakojs, hakogo, hakopascal):
/// nearest-first `[{"id","score","doc"}]` as an owned string (free with
/// `hk_string_free`). The query pipeline drops scores, so each hit's
/// score is the exact metric distance recomputed against the stored
/// document vector — same `distance()` the executor rescore uses, O(k·D),
/// so the graph stays the only pruning stage. Dim mismatch is a hard
/// error (null + `hk_last_error`); unindexed field returns `"[]"`
/// (fail-closed, like `find_near`).
#[no_mangle]
pub extern "C" fn hk_engine_find_near(
    engine: *mut HK_Engine,
    collection: *const c_char,
    field: *const c_char,
    vec_ptr: *const f32,
    vec_len: usize,
    k: usize,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        if vec_ptr.is_null() {
            set_last_error_err("null vector pointer");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let col = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let fld = match cstr_to_string(field) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let query_vec = unsafe { std::slice::from_raw_parts(vec_ptr, vec_len) };
        let metric = match engine.db.vector_metric(&col, &fld) {
            Some(m) => m,
            None => {
                // Unindexed field: fail-closed empty, not an error.
                clear_last_error();
                return CString::new("[]")
                    .map(|s| s.into_raw())
                    .unwrap_or(ptr::null_mut());
            }
        };
        let rows = match engine.db.find_near(&col, &fld, query_vec, k) {
            Ok(r) => r,
            Err(e) => {
                set_last_error_err(e.to_string());
                return ptr::null_mut();
            }
        };
        let mut arr = Vec::with_capacity(rows.len());
        for (id, doc) in &rows {
            let score = match doc.get(&fld) {
                Some(Value::Binary(b)) => {
                    match crate::index::vector::decode_f32s(b) {
                        Ok(stored)
                            if stored.len() == query_vec.len() =>
                        {
                            crate::index::vector::distance(metric, query_vec, &stored)
                        }
                        _ => f64::INFINITY,
                    }
                }
                _ => f64::INFINITY,
            };
            let doc_json: serde_json::Value = match doc_to_json(doc) {
                Ok(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
                Err(_) => serde_json::Value::Null,
            };
            arr.push(serde_json::json!({"id": id, "score": score, "doc": doc_json}));
        }
        match CString::new(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())) {
            Ok(s) => {
                clear_last_error();
                s.into_raw()
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

/// Opt a collection in as a traversal target (issue #53): inbound edges
/// tracked from here on (live writes + backfill). Idempotent.
#[no_mangle]
pub extern "C" fn hk_engine_create_traverse_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    match engine.db.create_traverse_index(&col) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn hk_engine_drop_traverse_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    match engine.db.drop_traverse_index(&col) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e),
    }
}

/// Graph-traversal marker on a query (issue #53): BFS from
/// (`start_col`, `start_id`); `direction` 0/1/2, `edge_field` null = any
/// ref field, `max_depth` bound. Result cap rides query limit.
#[no_mangle]
pub extern "C" fn hk_query_where_traverse(
    query: *mut HK_Query,
    start_col: *const c_char,
    start_id: *const c_char,
    direction: u32,
    edge_field: *const c_char,
    max_depth: usize,
) -> i32 {
    if query.is_null() { return set_last_error_err("null query handle"); }
    let scol = match cstr_to_string(start_col) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
    let sid = match cstr_to_string(start_id) { Ok(v) => v, Err(e) => return set_last_error_err(e), };
    let dir = match crate::index::traverse::TraverseDir::from_u32(direction) {
        Ok(d) => d,
        Err(e) => return set_last_error_err(e.to_string()),
    };
    let edge: Option<String> = if edge_field.is_null() {
        None
    } else {
        match cstr_to_string(edge_field) { Ok(v) => Some(v), Err(e) => return set_last_error_err(e), }
    };
    let f = crate::query::builder::traverse_filter(&scol, &sid, dir, edge.as_deref(), max_depth);
    let query = unsafe { &mut *query };
    query.query.filters.push(crate::query::filter::Filter {
        field: f.field,
        op: Operator::Traverse,
        value: f.value,
    });
    clear_last_error();
    0
}

/// One-shot graph BFS for FFI consumers: nearest-first is meaningless
/// here — hits ride BFS order as `[{"collection","id","depth","doc"}]`
/// (owned string, `hk_string_free`). Missing anchor = `"[]"`
/// (fail-closed); incoming into untracked = null + `hk_last_error`.
#[no_mangle]
pub extern "C" fn hk_engine_find_paths(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    direction: u32,
    edge_field: *const c_char,
    max_depth: usize,
    k: usize,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let col = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let dir = match crate::index::traverse::TraverseDir::from_u32(direction) {
            Ok(d) => d,
            Err(e) => {
                set_last_error_err(e.to_string());
                return ptr::null_mut();
            }
        };
        let edge: Option<String> = if edge_field.is_null() {
            None
        } else {
            match cstr_to_string(edge_field) {
                Ok(v) => Some(v),
                Err(e) => {
                    set_last_error_err(e);
                    return ptr::null_mut();
                }
            }
        };
        let spec = crate::index::traverse::TraverseSpec {
            direction: dir,
            edge_field: edge,
            max_depth,
            limit: if k == 0 { 10 } else { k },
            via_edge: None,
            weight_field: None,
        };
        let rows = match engine.db.find_paths(&col, &id, &spec) {
            Ok(r) => r,
            Err(e) => {
                set_last_error_err(e.to_string());
                return ptr::null_mut();
            }
        };
        let mut arr = Vec::with_capacity(rows.len());
        for hit in &rows {
            let doc_json: serde_json::Value = match doc_to_json(&hit.doc) {
                Ok(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
                Err(_) => serde_json::Value::Null,
            };
            arr.push(serde_json::json!({
                "collection": hit.collection,
                "id": hit.id,
                "depth": hit.depth,
                "cost": hit.cost,
                "path": hit.path.iter().map(|(c, i)| format!("{c}/{i}")).collect::<Vec<_>>(),
                "doc": doc_json,
            }));
        }
        match CString::new(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())) {
            Ok(s) => {
                clear_last_error();
                s.into_raw()
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

/// Register an edge collection (issue #53, Phase B): tracks it for
/// inbound lookup AND indexes `_from`/`_to` for edge lookup — all three
/// ride existing machinery, so sync + persistence need no new shapes.
/// Idempotent.
#[no_mangle]
pub extern "C" fn hk_engine_create_edge_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(_) => return -1,
    };
    match engine.db.create_edge_index(&col) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e.to_string()),
    }
}

#[no_mangle]
pub extern "C" fn hk_engine_drop_edge_index(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    if engine.is_null() {
        return set_last_error_err("null engine handle");
    }
    let engine = unsafe { &*engine };
    let col = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    match engine.db.drop_edge_index(&col) {
        Ok(_) => {
            clear_last_error();
            0
        }
        Err(e) => set_last_error_err(e),
    }
}

/// Weighted shortest paths (Phase B): Dijkstra over inline refs +
/// `via_edge` edge docs, ordered by accumulated cost. Same JSON shape
/// as `find_paths` (with exact `cost` per hit). `weight_field` null =
/// uniform 1.0 per hop (BFS-equivalent order). Missing anchor = `"[]"`
/// (fail-closed); bad depth/direction/untracked incoming = null +
/// `hk_last_error`. `find_paths` is frozen — this additive fn carries
/// the new arity so existing bindings never break.
#[no_mangle]
pub extern "C" fn hk_engine_shortest_paths(
    engine: *mut HK_Engine,
    collection: *const c_char,
    doc_id: *const c_char,
    direction: u32,
    edge_field: *const c_char,
    via_edge: *const c_char,
    weight_field: *const c_char,
    max_depth: usize,
    k: usize,
) -> *mut c_char {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let col = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let dir = match crate::index::traverse::TraverseDir::from_u32(direction) {
            Ok(d) => d,
            Err(e) => {
                set_last_error_err(e.to_string());
                return ptr::null_mut();
            }
        };
        let opt_str = |p: *const c_char| {
            if p.is_null() {
                None
            } else {
                cstr_to_string(p).ok()
            }
        };
        let spec = crate::index::traverse::TraverseSpec {
            direction: dir,
            edge_field: opt_str(edge_field),
            max_depth,
            limit: if k == 0 { 100 } else { k },
            via_edge: opt_str(via_edge),
            weight_field: opt_str(weight_field),
        };
        let rows = match engine.db.find_paths(&col, &id, &spec) {
            Ok(r) => r,
            Err(e) => {
                set_last_error_err(e.to_string());
                return ptr::null_mut();
            }
        };
        let mut arr = Vec::with_capacity(rows.len());
        for hit in &rows {
            let doc_json: serde_json::Value = match doc_to_json(&hit.doc) {
                Ok(s) => serde_json::from_str(&s).unwrap_or(serde_json::Value::Null),
                Err(_) => serde_json::Value::Null,
            };
            arr.push(serde_json::json!({
                "collection": hit.collection,
                "id": hit.id,
                "depth": hit.depth,
                "cost": hit.cost,
                "path": hit.path.iter().map(|(c, i)| format!("{c}/{i}")).collect::<Vec<_>>(),
                "doc": doc_json,
            }));
        }
        match CString::new(serde_json::to_string(&arr).unwrap_or_else(|_| "[]".to_string())) {
            Ok(s) => {
                clear_last_error();
                s.into_raw()
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_engine_list_indexes(
    engine: *mut HK_Engine,
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
                    set_last_error_err(e);
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

// --- PER-COLLECTION SCHEMAS (F8) ---
//
// JSON shape mirrors `crate::schema::CollectionSchema`:
//   {"fields":[{"name":"status","kind":"string","required":true,
//               "default":"paid"}]}
// `kind` is one of string/int/float/bool/null/array/map/bytes/timestamp/any.
// Declared fields are top-level only; undeclared fields stay free. The write
// gate returns ErrorKind 4 (ConstraintMismatch) on a violation.

/// Declare or replace a collection schema. `schema_json` is parsed by serde;
/// an empty `{"fields":[]}` clears the schema (same as `clear`). Returns 0 on
/// success, -1 on error (typed error available via `hk_error_last_kind`).
#[no_mangle]
pub extern "C" fn hk_engine_set_schema(
    engine: *mut HK_Engine,
    collection: *const c_char,
    schema_json: *const c_char,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() {
            return set_last_error_err("null engine handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let raw = match cstr_to_string(schema_json) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let schema: crate::schema::CollectionSchema = match serde_json::from_str(&raw) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(format!("invalid schema JSON: {e}")),
        };
        let engine = unsafe { &*engine };
        match engine.db.set_schema(&collection, schema) {
            Ok(()) => {
                clear_last_error();
                0
            }
            Err(e) => set_last_error_err(e),
        }
    })
}

/// Return the collection's schema as JSON, or NULL when the collection has no
/// schema. NULL is not an error in that case (`hk_last_error` stays clear).
/// Free the returned string with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_engine_get_schema(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let engine = unsafe { &*engine };
        let Some(schema) = engine.db.schema(&collection) else {
            clear_last_error();
            return ptr::null_mut();
        };
        match serde_json::to_string(&schema)
            .ok()
            .and_then(|s| CString::new(s).ok())
        {
            Some(json) => {
                clear_last_error();
                json.into_raw()
            }
            None => {
                set_last_error_err("failed to encode schema");
                ptr::null_mut()
            }
        }
    })
}

/// Remove a collection's schema. Returns 0 on success, -1 on error.
#[no_mangle]
pub extern "C" fn hk_engine_clear_schema(
    engine: *mut HK_Engine,
    collection: *const c_char,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() {
            return set_last_error_err("null engine handle");
        }
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let engine = unsafe { &*engine };
        match engine.db.clear_schema(&collection) {
            Ok(()) => {
                clear_last_error();
                0
            }
            Err(e) => set_last_error_err(e),
        }
    })
}

/// Return every declared schema as a JSON object keyed by collection name.
/// Free the returned string with `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_engine_list_schemas(engine: *mut HK_Engine) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("null engine handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let schemas = engine.db.list_schemas();
        match serde_json::to_string(&schemas)
            .ok()
            .and_then(|s| CString::new(s).ok())
        {
            Some(json) => {
                clear_last_error();
                json.into_raw()
            }
            None => {
                set_last_error_err("failed to encode schemas");
                ptr::null_mut()
            }
        }
    })
}

// --- AGGREGATION PIPELINES (F4) ---

#[no_mangle]
pub extern "C" fn hk_pipeline_new(collection: *const c_char) -> *mut HK_Pipeline {
    let collection = match cstr_to_string(collection) {
        Ok(v) => v,
        Err(e) => {
            set_last_error_err(e);
            return ptr::null_mut();
        }
    };
    clear_last_error();
    Box::into_raw(Box::new(HK_Pipeline {
        pipeline: crate::query::pipeline::Pipeline::new(&collection),
    }))
}

#[no_mangle]
pub extern "C" fn hk_pipeline_free(pipeline: *mut HK_Pipeline) {
    if !pipeline.is_null() {
        unsafe { drop(Box::from_raw(pipeline)) };
    }
}

/// Append a `match` stage from a query handle. The query is copied; the
/// caller keeps ownership of `query`.
#[no_mangle]
pub extern "C" fn hk_pipeline_match(pipeline: *mut HK_Pipeline, query: *const HK_Query) -> i32 {
    if pipeline.is_null() || query.is_null() {
        return set_last_error_err("null pipeline/query handle");
    }
    let p = unsafe { &mut *pipeline };
    let q = unsafe { &*query };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Match(q.query.clone()));
    clear_last_error();
    0
}

/// `fields_json`: JSON array of field names (`["id","items.qty"]`).
#[no_mangle]
pub extern "C" fn hk_pipeline_project(
    pipeline: *mut HK_Pipeline,
    fields_json: *const c_char,
) -> i32 {
    if pipeline.is_null() {
        return set_last_error_err("null pipeline handle");
    }
    let raw = match parse_json_arg(fields_json, "projection") {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let Some(items) = raw.as_array() else {
        return set_last_error_err("projection must be a JSON array");
    };
    let mut fields = Vec::with_capacity(items.len());
    for item in items {
        match item.as_str() {
            Some(s) => fields.push(s.to_string()),
            None => return set_last_error_err("projection entries must be strings"),
        }
    }
    let p = unsafe { &mut *pipeline };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Project(fields));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_pipeline_sort(
    pipeline: *mut HK_Pipeline,
    field: *const c_char,
    ascending: bool,
) -> i32 {
    if pipeline.is_null() {
        return set_last_error_err("null pipeline handle");
    }
    let field = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let p = unsafe { &mut *pipeline };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Sort { field, ascending });
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_pipeline_limit(pipeline: *mut HK_Pipeline, limit: usize) -> i32 {
    if pipeline.is_null() {
        return set_last_error_err("null pipeline handle");
    }
    let p = unsafe { &mut *pipeline };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Limit(limit));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_pipeline_offset(pipeline: *mut HK_Pipeline, offset: usize) -> i32 {
    if pipeline.is_null() {
        return set_last_error_err("null pipeline handle");
    }
    let p = unsafe { &mut *pipeline };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Offset(offset));
    clear_last_error();
    0
}

#[no_mangle]
pub extern "C" fn hk_pipeline_unwind(
    pipeline: *mut HK_Pipeline,
    field: *const c_char,
) -> i32 {
    if pipeline.is_null() {
        return set_last_error_err("null pipeline handle");
    }
    let field = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let p = unsafe { &mut *pipeline };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Unwind { field });
    clear_last_error();
    0
}

/// `key` may be NULL for a single global group.
/// `accums_json`: `[{"out":"total","op":"sum","field":"items.qty"},
/// {"out":"n","op":"count"}]`. `field` is ignored by `count`.
#[no_mangle]
pub extern "C" fn hk_pipeline_group(
    pipeline: *mut HK_Pipeline,
    key: *const c_char,
    accums_json: *const c_char,
) -> i32 {
    use crate::query::pipeline::{Accum, AccumSpec};
    if pipeline.is_null() {
        return set_last_error_err("null pipeline handle");
    }
    let key = if key.is_null() {
        None
    } else {
        match cstr_to_string(key) {
            Ok(v) => Some(v),
            Err(e) => return set_last_error_err(e),
        }
    };
    let raw = match parse_json_arg(accums_json, "accums") {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let Some(items) = raw.as_array() else {
        return set_last_error_err("accums must be a JSON array");
    };
    let mut accums = Vec::with_capacity(items.len());
    for item in items {
        let Some(obj) = item.as_object() else {
            return set_last_error_err("each accum must be a JSON object");
        };
        let out = match obj.get("out").and_then(|v| v.as_str()) {
            Some(v) => v.to_string(),
            None => return set_last_error_err("accum.out must be a string"),
        };
        let op_name = obj.get("op").and_then(|v| v.as_str()).unwrap_or("");
        let field = obj.get("field").and_then(|v| v.as_str()).unwrap_or("");
        let op = match op_name {
            "count" => Accum::Count,
            "sum" | "min" | "max" | "avg" if field.is_empty() => {
                return set_last_error_err(format!("accum `{out}` ({op_name}) needs a field"))
            }
            "sum" => Accum::Sum(field.to_string()),
            "min" => Accum::Min(field.to_string()),
            "max" => Accum::Max(field.to_string()),
            "avg" => Accum::Avg(field.to_string()),
            other => return set_last_error_err(format!("unknown accum op `{other}`")),
        };
        accums.push(AccumSpec { out, op });
    }
    let p = unsafe { &mut *pipeline };
    p.pipeline
        .stages
        .push(crate::query::pipeline::Stage::Group { key, accums });
    clear_last_error();
    0
}

/// Run the pipeline; returns a JSON array of result documents (caller frees
/// with `hk_string_free`).
#[no_mangle]
pub extern "C" fn hk_pipeline_execute(
    engine: *mut HK_Engine,
    pipeline: *const HK_Pipeline,
) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() || pipeline.is_null() {
            set_last_error_err("null engine/pipeline handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let p = unsafe { &*pipeline };
        let rows = match engine.db.run_pipeline(p.pipeline.clone()) {
            Ok(rows) => rows,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let mut docs = Vec::with_capacity(rows.len());
        for (_, doc) in rows {
            match doc_to_json(&doc).and_then(|s| {
                serde_json::from_str::<serde_json::Value>(&s).map_err(|e| e.to_string())
            }) {
                Ok(v) => docs.push(v),
                Err(e) => {
                    set_last_error_err(e);
                    return ptr::null_mut();
                }
            }
        }
        match CString::new(serde_json::to_string(&docs).unwrap_or_else(|_| "[]".to_string())) {
            Ok(s) => {
                clear_last_error();
                s.into_raw()
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

/// Run the pipeline and return owned doc handles (mirrors
/// `hk_query_execute_to_handles`; release with `hk_result_set_free`).
#[no_mangle]
pub extern "C" fn hk_pipeline_execute_to_handles(
    engine: *mut HK_Engine,
    pipeline: *const HK_Pipeline,
) -> *mut HK_ResultSet {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() || pipeline.is_null() {
            set_last_error_err("null engine/pipeline handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let p = unsafe { &*pipeline };
        match engine.db.run_pipeline(p.pipeline.clone()) {
            Ok(rows) => {
                clear_last_error();
                Box::into_raw(Box::new(HK_ResultSet {
                    docs: rows
                        .into_iter()
                        .map(|(id, doc)| HK_Doc { id, doc: DocRef::Owned(doc) })
                        .collect(),
                }))
            }
            Err(e) => {
                set_last_error_err(e);
                ptr::null_mut()
            }
        }
    })
}

// --- 2. SERIALIZABLE TRANSACTIONS (Read-Modify-Write) ---

#[no_mangle]
pub extern "C" fn hk_transaction_begin(engine: *mut HK_Engine) -> *mut HK_Transaction {
    safety_shield!(ptr::null_mut(), {
        let engine = unsafe { &*engine };
        Box::into_raw(Box::new(HK_Transaction {
            tx: engine.db.begin_serializable_transaction(),
        }))
    })
}

#[no_mangle]
pub extern "C" fn hk_transaction_get(
    engine: *mut HK_Engine,
    tx: *mut HK_Transaction,
    collection: *const c_char,
    doc_id: *const c_char,
) -> *mut HK_Doc {
    // ponytail: shielded like commit — an unwinding panic through this
    // frame into C++ is UB (destructors skipped, locks leaked); fail
    // null instead. Hot path: shield costs nothing when no panic.
    safety_shield!(ptr::null_mut(), {
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
        Ok(Some(doc)) => Box::into_raw(Box::new(HK_Doc { doc: DocRef::Owned(doc), id: id.clone() })),
        _ => ptr::null_mut(),
    }
    })
}

#[no_mangle]
pub extern "C" fn hk_transaction_set(
    tx: *mut HK_Transaction,
    collection: *const c_char,
    doc_id: *const c_char,
    doc: *const HK_Doc,
) -> i32 {
    // ponytail: shielded (see hk_transaction_get).
    safety_shield!(-1, {
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

    tx.tx.put(&col, &id, doc.hako().clone());
    0
    })
}

/// Patch inside the transaction (issue #71/F5 item 5): the doc version joins
/// the read-set, then the updates are applied at commit.
/// `updates_json`: `{"field": <json>, ...}` (top-level fields).
#[no_mangle]
pub extern "C" fn hk_transaction_patch(
    engine: *mut HK_Engine,
    tx: *mut HK_Transaction,
    collection: *const c_char,
    doc_id: *const c_char,
    updates_json: *const c_char,
) -> i32 {
    safety_shield!(-1, {
        if engine.is_null() || tx.is_null() {
            return set_last_error_err("null engine/tx handle");
        }
        let engine = unsafe { &*engine };
        let tx = unsafe { &mut *tx };
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let doc_id = match cstr_to_string(doc_id) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        let updates = match parse_updates_json(updates_json) {
            Ok(v) => v,
            Err(e) => return set_last_error_err(e),
        };
        match tx.tx.patch(&engine.db, &collection, &doc_id, updates) {
            Ok(()) => {
                clear_last_error();
                0
            }
            Err(e) => set_last_error_err(e),
        }
    })
}

/// Read many docs into the transaction read-set. Returns a JSON array with
/// one entry per id (`null` when absent). Caller frees with
/// `hk_string_free`.
#[no_mangle]
pub extern "C" fn hk_transaction_get_many(
    engine: *mut HK_Engine,
    tx: *mut HK_Transaction,
    collection: *const c_char,
    ids_json: *const c_char,
) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() || tx.is_null() {
            set_last_error_err("null engine/tx handle");
            return ptr::null_mut();
        }
        let engine = unsafe { &*engine };
        let tx = unsafe { &mut *tx };
        let collection = match cstr_to_string(collection) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let raw = match parse_json_arg(ids_json, "ids") {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let Some(items) = raw.as_array() else {
            set_last_error_err("ids must be a JSON array");
            return ptr::null_mut();
        };
        let mut ids = Vec::with_capacity(items.len());
        for item in items {
            match item.as_str() {
                Some(s) => ids.push(s.to_string()),
                None => {
                    set_last_error_err("id entries must be strings");
                    return ptr::null_mut();
                }
            }
        }
        let rows = match tx.tx.get_many(&engine.db, &collection, &ids) {
            Ok(v) => v,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };
        let mut out = Vec::with_capacity(rows.len());
        for doc in rows {
            match doc {
                Some(doc) => match doc_to_json(&doc) {
                    Ok(s) => match serde_json::from_str::<serde_json::Value>(&s) {
                        Ok(v) => out.push(v),
                        Err(e) => {
                            set_last_error_err(e.to_string());
                            return ptr::null_mut();
                        }
                    },
                    Err(e) => {
                        set_last_error_err(e);
                        return ptr::null_mut();
                    }
                },
                None => out.push(serde_json::Value::Null),
            }
        }
        match CString::new(serde_json::to_string(&out).unwrap_or_else(|_| "[]".to_string())) {
            Ok(s) => {
                clear_last_error();
                s.into_raw()
            }
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_transaction_commit(engine: *mut HK_Engine, tx: *mut HK_Transaction) -> i32 {
    safety_shield!(-1, {
        let engine = unsafe { &*engine };
        // Borrow only: the caller still owns the handle and must release it
        // with hk_transaction_free (matching the hk_batch_commit contract).
        let tx = unsafe { &*tx };

        match tx.tx.commit(&engine.db) {
            Ok(_) => 0,
            // Typed payload keeps the retry classification (issue #71/F5):
            // commit conflicts must arrive as kind 2, not generic.
            Err(e) => set_last_error_err(e),
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_transaction_free(tx: *mut HK_Transaction) {
    if !tx.is_null() {
        unsafe { drop(Box::from_raw(tx)) };
    }
}

// --- 3. SUBCOLLECTION HELPERS ---

#[no_mangle]
pub extern "C" fn hk_engine_insert_subdoc(
    engine: *mut HK_Engine,
    col: *const c_char,
    id: *const c_char,
    sub_col: *const c_char,
    sub_id: *const c_char,
    doc: *const HK_Doc,
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

    match engine.db.put_subdocument(&c, &i, &sc, &si, d.hako()) {
        Ok(_) => 0,
        Err(e) => {
            set_last_error_err(e.to_string());
            -1 // or ptr::null_mut() depending on function return type
        }
    }
}

// --- 4. DIAGNOSTICS & MAINTENANCE ---

#[no_mangle]
pub extern "C" fn hk_engine_compact(engine: *mut HK_Engine) -> i32 {
    safety_shield!(-1, {
        let engine = unsafe { &*engine };
        match engine.db.compact() {
            Ok(_) => 0,
            Err(e) => {
                set_last_error_err(e.to_string());
                -1 // or ptr::null_mut() depending on function return type
            }
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_engine_get_stats(engine: *mut HK_Engine) -> *mut c_char {
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
pub extern "C" fn hk_doc_insert_reference(
    doc: *mut HK_Doc,
    key: *const c_char,
    target_collection: *const c_char,
    target_id: *const c_char,
) -> i32 {
    if doc.is_null() {
        return -1;
    }
    let key_str = match cstr_to_string(key) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let col = match cstr_to_string(target_collection) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let id = match cstr_to_string(target_id) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    let doc_ptr = unsafe { &mut *doc };
    doc_ptr.hako_mut().insert(
        key_str,
        Value::Reference {
            collection: col,
            doc_id: id,
        },
    );
    0
}

/// Given a document and a field name containing a Reference, fetch the target document.
/// Returns a new HK_Doc handle, or null if the field is not a reference or target not found.
#[no_mangle]
pub extern "C" fn hk_engine_get_by_ref(
    engine: *mut HK_Engine,
    doc: *const HK_Doc,
    field_key: *const c_char,
) -> *mut HK_Doc {
    safety_shield!(std::ptr::null_mut(), {
        if engine.is_null() || doc.is_null() || field_key.is_null() {
            return ptr::null_mut();
        }

        let engine = unsafe { &*engine };
        let doc_ptr = unsafe { &*doc };
        let key = match cstr_to_string(field_key) {
            Ok(k) => k,
            Err(e) => {
                set_last_error_err(e);
                return ptr::null_mut();
            }
        };

        // 1. Find the value in the provided document
        let Some(val) = doc_ptr.hako().get(&key) else {
            set_last_error_err("Field not found in document");
            return ptr::null_mut();
        };

        // 2. Resolve the reference using the engine
        match engine.db.get_by_reference(val) {
            Ok(Some(target_doc)) => Box::into_raw(Box::new(HK_Doc { doc: DocRef::Owned(target_doc), id: doc_ptr.id.clone() })),
            Ok(None) => ptr::null_mut(), // Document doesn't exist (Dangling reference)
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

// /// Internal helper to extract values from an anchor document based on the query's sort order
// fn get_anchor_values(q: &crate::query::query::Query, doc: &HakoDoc) -> Option<Vec<Value>> {
//     if q.order_by.is_empty() { return None; }
    
//     let mut vals = Vec::with_capacity(q.order_by.len());
//     for order in &q.order_by {
//         vals.push(doc.get(&order.field)?.clone());
//     }
//     Some(vals)
// }
// 3. Fix get_anchor_values logic
fn get_anchor_values(q: &crate::query::query::Query, hk_doc: &HK_Doc) -> Option<Vec<Value>> {
    if q.order_by.is_empty() { return None; }
    
    let mut vals = Vec::with_capacity(q.order_by.len());
    for order in &q.order_by {
        // MAGIC FIX: Extract internal metadata manually
        if order.field == "id" {
            vals.push(Value::String(hk_doc.id.clone()));
        } else if order.field == "_time" {
            vals.push(Value::Int(hk_doc.hako()._time));
        } else {
            vals.push(hk_doc.hako().get(&order.field)?.clone());
        }
    }
    Some(vals)
}

// #[no_mangle]
// pub extern "C" fn hk_query_start_after(query: *mut HK_Query, anchor_doc: *const HK_Doc) -> i32 {
//     safety_shield!(-1, {
//         if query.is_null() || anchor_doc.is_null() { return -1; }
//         let q = unsafe { &mut *query };
//         let doc = unsafe { &*anchor_doc };

//         if let Some(vals) = get_anchor_values(&q.query, &doc.doc) {
//             q.query.start_after = Some(vals);
//             0
//         } else {
//             set_last_error_err("Anchor document missing one or more fields from sort chain");
//             -1
//         }
//     })
// }

#[no_mangle]
pub extern "C" fn hk_query_start_after(query: *mut HK_Query, anchor_doc: *const HK_Doc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() { return -1; }
        let q = unsafe { &mut *query };
        let doc = unsafe { &*anchor_doc };
        
        if let Some(vals) = get_anchor_values(&q.query, doc) {
            q.query.start_after = Some(vals);
            0
        } else {
            set_last_error_err("Anchor document missing one or more fields from sort chain");
            -1
        }
    })
}

// #[no_mangle]
// pub extern "C" fn hk_query_start_at(query: *mut HK_Query, anchor_doc: *const HK_Doc) -> i32 {
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
//             set_last_error_err("Anchor document missing sort field");
//             -1
//         }
//     })
// }
// 4. Update pointer usages for the anchor logic
#[no_mangle]
pub extern "C" fn hk_query_start_at(query: *mut HK_Query, anchor_doc: *const HK_Doc) -> i32 {
    safety_shield!(-1, {
        if query.is_null() || anchor_doc.is_null() { return -1; }
        let q = unsafe { &mut *query };
        let doc = unsafe { &*anchor_doc };
        
        if let Some(vals) = get_anchor_values(&q.query, doc) {
            q.query.start_at = Some(vals);
            0
        } else {
            set_last_error_err("Anchor document missing one or more fields from sort chain");
            -1
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_query_end_at(query: *mut HK_Query, anchor_doc: *const HK_Doc) -> i32 {
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
            set_last_error_err("Anchor document missing sort field");
            -1
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_query_end_before(query: *mut HK_Query, anchor_doc: *const HK_Doc) -> i32 {
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
            set_last_error_err("Anchor document missing sort field");
            -1
        }
    })
}

#[no_mangle]
pub extern "C" fn hk_engine_get_audit_log(engine: *mut HK_Engine) -> *mut c_char {
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
pub extern "C" fn hk_query_where_or_str(
    query: *mut HK_Query,
    field: *const c_char,
    value: *const c_char,
) -> i32 {
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };
    let v = match cstr_to_string(value) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    // Each call to hk_query_where_or creates a new standalone OR group
    q.query.or_groups.push(vec![crate::query::filter::Filter {
        field: f,
        op: Operator::Eq,
        value: Value::String(v),
    }]);
    0
}

#[no_mangle]
pub extern "C" fn hk_query_where_or_int(
    query: *mut HK_Query,
    field: *const c_char,
    value: i64,
) -> i32 {
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
    };

    q.query.or_groups.push(vec![crate::query::filter::Filter {
        field: f,
        op: Operator::Eq,
        value: Value::Int(value),
    }]);
    0
}

// --- IN LOGIC ---

/// Adds an IN filter: field IN `[array_items]`
/// This takes ownership of the HK_Array and frees it.
#[no_mangle]
pub extern "C" fn hk_query_where_in(
    query: *mut HK_Query,
    field: *const c_char,
    array: *mut HK_Array,
) -> i32 {
    if query.is_null() || array.is_null() {
        return -1;
    }
    let q = unsafe { &mut *query };
    let f = match cstr_to_string(field) {
        Ok(v) => v,
        Err(e) => return set_last_error_err(e),
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
pub extern "C" fn hk_engine_snapshot_indices(engine: *mut HK_Engine) -> i32 {
    if engine.is_null() {
        return -1;
    }
    let engine = unsafe { &*engine };

    match engine.db.save_index_snapshots() {
        Ok(_) => 0,
        Err(e) => {
            set_last_error_err(e.to_string());
            -1
        }
    }
}

#[no_mangle]
pub extern "C" fn hk_config_set_compression(config: *mut HK_Config, enabled: bool, level: i32) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.use_compression = enabled;
        cfg.inner.compression_level = level;
    }
}

/// Hold background maintenance (checkpoint/compaction/purge/snapshots) for
/// deterministic benchmarks or hard latency bounds. Engine stays correct;
/// files grow until re-enabled. Default on.
#[no_mangle]
pub extern "C" fn hk_config_set_background_maintenance(config: *mut HK_Config, enabled: bool) {
    if let Some(cfg) = unsafe { config.as_mut() } {
        cfg.inner.background_maintenance = enabled;
    }
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn hk_net_syncer_new(
    engine: *mut HK_Engine,
    name: *const c_char,
    room_key: *const c_char,
) -> *mut HK_NetSyncer {
    let engine_ref = unsafe { &*engine };
    let name_str = cstr_to_string(name).unwrap_or_else(|_| "node".into());
    let room_str = cstr_to_string(room_key).unwrap_or_else(|_| "default".into());

    // We need to clone the Arc<Hako> logically. 
    // Since HK_Engine wraps Hako (which is not an Arc inside HK_Engine), 
    // we use a temporary wrap to pass it to the syncer.
    // let db_ptr:std::sync::Arc<Hako> = unsafe { std::sync::Arc::from_raw(&engine_ref.db as *const _) };
    let syncer = crate::net_sync::NetSyncer::new(
        engine_ref.db.clone(),
        &name_str,
        &room_str,
        vec![],
    );
    // Important: Forget the raw pointer so we don't drop the engine!
    // std::mem::forget(db_ptr);

    clear_last_error();
    Box::into_raw(Box::new(HK_NetSyncer {
        inner: std::sync::Arc::new(syncer),
    }))
}

/// Select discovery transports: 0 = mDNS (desktop default), 1 = UDP
/// broadcast (mobile default, no multicast), 2 = both (mixed groups — a
/// desktop joining mobile peers must opt into both or broadcast).
/// Takes effect at the next start().
#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn hk_net_syncer_set_discovery(syncer: *mut HK_NetSyncer, mode: i32) -> i32 {
    if syncer.is_null() { return -1; }
    let s_ref = unsafe { &*syncer };
    let m = match mode {
        0 => crate::net_sync::DiscoveryMode::Mdns,
        1 => crate::net_sync::DiscoveryMode::Broadcast,
        2 => crate::net_sync::DiscoveryMode::Both,
        _ => return set_last_error_err("invalid discovery mode (0=mdns, 1=broadcast, 2=both)"),
    };
    s_ref.inner.set_discovery(m);
    clear_last_error();
    0
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn hk_net_syncer_start(syncer: *mut HK_NetSyncer, port: u16) -> i32 {
    if syncer.is_null() { return -1; }
    let s_ref = unsafe { &*syncer };
    let inner = s_ref.inner.clone();

    // Use block_on to bridge synchronous FFI to the async start method
    let rt = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => return set_last_error_err("No tokio runtime found"),
    };

    match rt.block_on(async move { inner.start(port).await }) {
        Ok(_) => 0,
        Err(e) => {
            set_last_error_err(e.to_string());
            -1
        }
    }
}

#[cfg(feature = "net-sync")]
#[no_mangle]
pub extern "C" fn hk_net_syncer_status(syncer: *mut HK_NetSyncer) -> *mut c_char {
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
pub extern "C" fn hk_net_syncer_free(syncer: *mut HK_NetSyncer) {
    if !syncer.is_null() {
        let s = unsafe { Box::from_raw(syncer) };
        s.inner.stop();
    }
}


// SOCKET SYNC (unix-only): co-located instances, explicit paths.
#[cfg(all(unix, feature = "socket-sync"))]
#[allow(non_camel_case_types)]
pub struct HK_SocketSync {
    inner: std::sync::Arc<crate::socket_sync::SocketSync>,
}

#[cfg(all(unix, feature = "socket-sync"))]
#[no_mangle]
pub extern "C" fn hk_socket_sync_new(engine: *mut HK_Engine) -> *mut HK_SocketSync {
    if engine.is_null() {
        return std::ptr::null_mut();
    }
    let engine_ref = unsafe { &*engine };
    let syncer = crate::socket_sync::SocketSync::new(engine_ref.db.clone(), vec![]);
    clear_last_error();
    Box::into_raw(Box::new(HK_SocketSync {
        inner: std::sync::Arc::new(syncer),
    }))
}

#[cfg(all(unix, feature = "socket-sync"))]
#[no_mangle]
pub extern "C" fn hk_socket_sync_serve(syncer: *mut HK_SocketSync, path: *const c_char) -> i32 {
    if syncer.is_null() {
        return -1;
    }
    let s_ref = unsafe { &*syncer };
    let path_str = cstr_to_string(path).unwrap_or_default();
    if path_str.is_empty() {
        return set_last_error_err("empty socket path");
    }
    // tokio::spawn inside serve() needs a runtime context.
    let rt = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => return set_last_error_err("No tokio runtime found"),
    };
    let inner = s_ref.inner.clone();
    match rt.block_on(async move { inner.serve(&path_str) }) {
        Ok(_) => 0,
        Err(e) => set_last_error_err(e.to_string()),
    }
}

#[cfg(all(unix, feature = "socket-sync"))]
#[no_mangle]
pub extern "C" fn hk_socket_sync_dial(syncer: *mut HK_SocketSync, path: *const c_char) -> i32 {
    if syncer.is_null() {
        return -1;
    }
    let s_ref = unsafe { &*syncer };
    let path_str = cstr_to_string(path).unwrap_or_default();
    if path_str.is_empty() {
        return set_last_error_err("empty socket path");
    }
    let rt = match tokio::runtime::Handle::try_current() {
        Ok(h) => h,
        Err(_) => return set_last_error_err("No tokio runtime found"),
    };
    let inner = s_ref.inner.clone();
    match rt.block_on(async move { inner.dial(&path_str).await }) {
        Ok(_) => 0,
        Err(e) => set_last_error_err(e.to_string()),
    }
}

#[cfg(all(unix, feature = "socket-sync"))]
#[no_mangle]
pub extern "C" fn hk_socket_sync_status(syncer: *mut HK_SocketSync) -> *mut c_char {
    if syncer.is_null() {
        return ptr::null_mut();
    }
    let s_ref = unsafe { &*syncer };
    let status = serde_json::json!({ "peers": s_ref.inner.peer_count() });
    match serde_json::to_string(&status) {
        Ok(json) => CString::new(json).unwrap().into_raw(),
        Err(_) => ptr::null_mut(),
    }
}

#[cfg(all(unix, feature = "socket-sync"))]
#[no_mangle]
pub extern "C" fn hk_socket_sync_free(syncer: *mut HK_SocketSync) {
    if !syncer.is_null() {
        let s = unsafe { Box::from_raw(syncer) };
        s.inner.stop();
    }
}


// CLOUD SYNC
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_new(
    engine: *mut HK_Engine,
    mode: i32, // 0 = Server, 1 = Client
    client_id: *const c_char,
    room_name: *const c_char,
    room_key: *const c_char,
    auth_token: *const c_char,
) -> *mut HK_CloudSync {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("Null engine handle");
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
        Box::into_raw(Box::new(HK_CloudSync {
            inner: std::sync::Arc::new(cloud_sync),
        }))
    })
}

/// Creates a room-bound cloud SERVER: serves exactly the configured room
/// on this database (single-database, like the mesh). Clients presenting
/// a different (room_name, room_key) pair are rejected at handshake.
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_server_new(
    engine: *mut HK_Engine,
    server_id: *const c_char,
    auth_token: *const c_char,
    room_name: *const c_char,
    room_key: *const c_char,
) -> *mut HK_CloudSync {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("Null engine handle");
            return ptr::null_mut();
        }

        let engine_ref = unsafe { &*engine };
        let sid_str = cstr_to_string(server_id).unwrap_or_else(|_| "server".into());
        let token_str = cstr_to_string(auth_token).unwrap_or_default();
        let room_name_str = cstr_to_string(room_name).unwrap_or_else(|_| "default".into());
        let room_key_str = cstr_to_string(room_key).unwrap_or_else(|_| "default".into());

        let cloud_sync = crate::cloud_sync::CloudSync::server(
            engine_ref.db.clone(),
            &sid_str,
            &token_str,
            &room_name_str,
            &room_key_str,
        );

        clear_last_error();
        Box::into_raw(Box::new(HK_CloudSync {
            inner: std::sync::Arc::new(cloud_sync),
        }))
    })
}

/// Creates an offline-first cloud CLIENT bound to a room of the caller's
/// choosing. The client picks the room (room_name + room_key) and later picks
/// the server via `hk_cloud_sync_start`.
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_client_new(
    engine: *mut HK_Engine,
    client_id: *const c_char,
    room_name: *const c_char,
    room_key: *const c_char,
    auth_token: *const c_char,
) -> *mut HK_CloudSync {
    safety_shield!(ptr::null_mut(), {
        if engine.is_null() {
            set_last_error_err("Null engine handle");
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
        Box::into_raw(Box::new(HK_CloudSync {
            inner: std::sync::Arc::new(cloud_sync),
        }))
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_start(cloud_sync: *mut HK_CloudSync, address: *const c_char) -> i32 {
    safety_shield!(-1, {
        if cloud_sync.is_null() {
            return set_last_error_err("Null cloud_sync handle");
        }
        let cs_ref = unsafe { &*cloud_sync };
        let addr_str = match cstr_to_string(address) {
            Ok(a) => a,
            Err(e) => return set_last_error_err(e),
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
                Err(e) => set_last_error_err(e.to_string()),
            }
        } else {
            let rt = match tokio::runtime::Runtime::new() {
                Ok(r) => r,
                Err(e) => return set_last_error_err(format!("Failed to create Tokio runtime: {}", e)),
            };
            match rt.block_on(run_sync) {
                Ok(_) => {
                    clear_last_error();
                    0
                }
                Err(e) => set_last_error_err(e.to_string()),
            }
        }
    })
}

/// Redacted status JSON: mode, connected, room_name, active_clients, and
/// queued_writes. The room key is never included.
#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_status(cloud_sync: *mut HK_CloudSync) -> *mut c_char {
    safety_shield!(ptr::null_mut(), {
        if cloud_sync.is_null() {
            set_last_error_err("Null cloud_sync handle");
            return ptr::null_mut();
        }
        let cs_ref = unsafe { &*cloud_sync };
        // Admin/log boundary: never expose the room key.
        let status = cs_ref.inner.status_redacted();
        match serde_json::to_string(&status) {
            Ok(json) => match CString::new(json) {
                Ok(c_str) => {
                    clear_last_error();
                    c_str.into_raw()
                }
                Err(e) => {
                    set_last_error_err(e.to_string());
                    ptr::null_mut()
                }
            },
            Err(e) => {
                set_last_error_err(e.to_string());
                ptr::null_mut()
            }
        }
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_stop(cloud_sync: *mut HK_CloudSync) {
    safety_shield!((), {
        if !cloud_sync.is_null() {
            let cs_ref = unsafe { &*cloud_sync };
            cs_ref.inner.stop();
        }
    })
}

#[cfg(feature = "cloud-sync")]
#[no_mangle]
pub extern "C" fn hk_cloud_sync_free(cloud_sync: *mut HK_CloudSync) {
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
    /// `to_json` + serialize approach (BTreeMap = sorted keys, `_time`
    /// merged at its sorted position), including Binary byte arrays and
    /// escaping.
    #[test]
    fn doc_to_json_matches_serde_map_output() {
        let mut doc = HakoDoc::default();
        // Deliberately unsorted insertion + escaping-sensitive strings.
        doc.insert("v", Value::Binary(vec![0u8, 1, 9, 10, 99, 100, 171, 255]));
        doc.insert("z", Value::Int(-42));
        doc.insert("a", Value::String("q\"\\qé".to_string()));
        doc.insert("m", Value::Bool(true));
        doc._time = 12345;

        let mut reference = serde_json::Map::new();
        reference.insert("_time".to_string(), serde_json::json!(doc._time));
        for (k, v) in &doc.fields {
            reference.insert(k.to_string(), value_to_json(v));
        }
        let expected = serde_json::to_string(&serde_json::Value::Object(reference)).unwrap();

        assert_eq!(doc_to_json(&doc).unwrap(), expected);
        // Spot-check the binary arm shape (no spaces, plain digits) + _time.
        assert!(expected.contains("\"v\":[0,1,9,10,99,100,171,255]"));
        assert!(expected.contains("\"_time\":12345"));
    }

    /// Adaptive emit stays byte-identical on both arms: big-string docs
    /// (serde arm), non-finite floats (null on both), and a literal "_time"
    /// field (timestamp dropped, not doubled — the write_json rule).
    #[test]
    fn doc_to_json_auto_matches_both_arms() {
        let mut big = HakoDoc::default();
        big.insert("content", Value::String("<p>html</p>".repeat(1000)));
        big.insert("n", Value::Int(7));
        big._time = 999;
        let mut out = Vec::new();
        big.write_json(&mut out);
        assert_eq!(doc_to_json(&big).unwrap().as_bytes(), out.as_slice());

        let mut edge = HakoDoc::default();
        edge.insert("nan", Value::Float(f64::NAN));
        edge.insert("inf", Value::Float(f64::INFINITY));
        edge.insert("_time", Value::String("literal".into()));
        // Big string forces the serde arm: null parity must hold there too
        // (to_json maps non-finite to Null, so to_vec cannot fail).
        edge.insert("pad", Value::String("x".repeat(2048)));
        edge._time = 42;
        let mut out2 = Vec::new();
        edge.write_json(&mut out2);
        let s = doc_to_json(&edge).unwrap();
        assert_eq!(s.as_bytes(), out2.as_slice());
        // null parity + single _time (the timestamp, not the literal).
        assert!(s.contains("\"nan\":null") && s.contains("\"inf\":null"));
        assert_eq!(s.matches("\"_time\"").count(), 1);
    }
}
