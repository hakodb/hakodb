//! Data-plane admin API: status, rooms, collections, documents, batch,
//! indexes, and maintenance. Role gates: viewer reads, operator writes
//! data, admin runs heavyweight/destructive maintenance.

use axum::{
    extract::{Path, Query as AxQuery, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};
use firelite::index::composite::definition::SortDirection;
use firelite::query::filter::Operator;
use firelite::query::query::Query as FireQuery;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value as JsonValue};
use std::collections::HashMap;
use std::sync::Arc;

use crate::app::{require_role, AppState, AuthedUser};
use crate::auth::Role;

// ---------------------------------------------------------------------------
// JSON <-> Value mapping
// ---------------------------------------------------------------------------

fn json_to_fire(v: &JsonValue) -> Result<Value, String> {
    match v {
        JsonValue::Null => Ok(Value::Null),
        JsonValue::Bool(b) => Ok(Value::Bool(*b)),
        JsonValue::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err("number out of range".into())
            }
        }
        JsonValue::String(s) => Ok(Value::String(s.clone())),
        JsonValue::Array(items) => items.iter().map(json_to_fire).collect::<Result<Vec<_>, _>>().map(Value::Array),
        JsonValue::Object(map) => map
            .iter()
            .map(|(k, v)| Ok((std::sync::Arc::from(k.as_str()), json_to_fire(v)?)))
            .collect::<Result<Vec<_>, String>>()
            .map(Value::Map),
    }
}

fn fire_to_json(v: &Value) -> JsonValue {
    match v {
        Value::Null => JsonValue::Null,
        Value::Bool(b) => json!(b),
        Value::Int(i) => json!(i),
        Value::Float(f) => json!(f),
        Value::String(s) => json!(s),
        Value::Array(items) => JsonValue::Array(items.iter().map(fire_to_json).collect()),
        Value::Map(entries) => {
            let mut o = serde_json::Map::new();
            for (k, val) in entries {
                o.insert(k.to_string(), fire_to_json(val));
            }
            JsonValue::Object(o)
        }
        Value::Binary(b) => json!({"$binary_bytes": b.len()}),
        Value::BlobLink { offset, len } => json!({"$blob": {"offset": offset, "len": len}}),
        Value::Reference { collection, doc_id } => json!(format!("{collection}/{doc_id}")),
        Value::Timestamp(t) => json!(t),
        Value::ServerTimestamp => json!("$server_timestamp"),
    }
}

fn doc_to_json(id: &str, doc: &FireLiteDoc) -> JsonValue {
    let mut o = serde_json::Map::new();
    o.insert("id".to_string(), json!(id));
    for (k, v) in &doc.fields {
        o.insert(k.to_string(), fire_to_json(v));
    }
    o.insert("_time".to_string(), json!(doc._time));
    JsonValue::Object(o)
}

fn doc_from_json(data: &JsonValue) -> Result<FireLiteDoc, String> {
    let obj = data.as_object().ok_or("data must be a JSON object")?;
    let mut doc = FireLiteDoc::default();
    for (k, v) in obj {
        if k == "id" || k == "_time" {
            continue;
        }
        doc.insert(k.clone(), json_to_fire(v)?);
    }
    Ok(doc)
}

// ---------------------------------------------------------------------------
// Status / rooms / collections
// ---------------------------------------------------------------------------

#[derive(Debug, Serialize)]
struct PeerView {
    peer_key: String,
    prefix: String,
}

async fn status(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    let _ = user;
    let collections = state.db.list_collections().map(|c| c.len()).unwrap_or(0);
    let sync = state.sync.as_ref().map(|s| {
        let peers: Vec<PeerView> = s
            .peer_list()
            .into_iter()
            .map(|p| PeerView {
                peer_key: p.peer_key,
                prefix: p.prefix,
            })
            .collect();
        json!({
            "connected": s.status().connected,
            "active_clients": s.status().active_clients,
            "hosted_rooms": s.status().hosted_rooms,
            "peers": peers,
        })
    });
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "collections": collections,
        "sync": sync,
    }))
    .into_response()
}

#[derive(Debug, Serialize)]
struct RoomView {
    room_name: String,
    prefix: String,
    created_at: i64,
    peers: Vec<String>,
    collections: HashMap<String, i64>,
}

fn room_docs(db: &FireLite) -> Vec<(String, FireLiteDoc)> {
    db.query(FireQuery::new(firelite::cloud_sync::INTERNAL_ROOMS_COLLECTION))
        .unwrap_or_default()
}

/// Storage prefixes of all known rooms (drives version-clock snapshots).
pub(crate) fn room_prefixes(db: &FireLite) -> Vec<String> {
    room_docs(db)
        .iter()
        .filter_map(|(_, doc)| match doc.get("prefix") {
            Some(Value::String(s)) => Some(s.clone()),
            _ => None,
        })
        .collect()
}

async fn rooms(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    let _ = user;
    let peer_prefixes: HashMap<String, Vec<String>> = match state.sync.as_ref() {
        Some(s) => {
            let mut map: HashMap<String, Vec<String>> = HashMap::new();
            for p in s.peer_list() {
                let client = p.peer_key.split(':').nth(1).unwrap_or("?").to_string();
                map.entry(p.prefix).or_default().push(client);
            }
            map
        }
        None => HashMap::new(),
    };
    let mut out = Vec::new();
    for (id, doc) in room_docs(&state.db) {
        let _ = id;
        let name = match doc.get("room_name") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let prefix = match doc.get("prefix") {
            Some(Value::String(s)) => s.clone(),
            _ => continue,
        };
        let created_at = match doc.get("created_at") {
            Some(Value::Int(t)) => *t,
            _ => 0,
        };
        let collections = match state.sync.as_ref() {
            Some(s) => s.room_versions(&prefix),
            None => HashMap::new(),
        };
        out.push(RoomView {
            peers: peer_prefixes.get(&prefix).cloned().unwrap_or_default(),
            room_name: name,
            prefix,
            created_at,
            collections,
        });
    }
    out.sort_by(|a, b| a.room_name.cmp(&b.room_name));
    Json(json!({ "rooms": out })).into_response()
}

#[derive(Debug, Serialize)]
struct CollectionView {
    name: String,
    docs: usize,
}

async fn collections(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    let _ = user;
    let names = state.db.list_collections().unwrap_or_default();
    let stats = state.db.get_stats();
    let mut out: Vec<CollectionView> = names
        .into_iter()
        .map(|name| {
            let docs = stats.get(&format!("{name}_count")).copied().unwrap_or(0);
            CollectionView { name, docs }
        })
        .collect();
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Json(json!({ "collections": out })).into_response()
}

// ---------------------------------------------------------------------------
// Query + documents
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct FilterInput {
    field: String,
    op: String,
    value: JsonValue,
}

#[derive(Debug, Deserialize)]
struct OrderInput {
    field: String,
    #[serde(default = "asc_default")]
    direction: String,
}

fn asc_default() -> String {
    "asc".to_string()
}

#[derive(Debug, Deserialize)]
struct QueryBody {
    collection: String,
    #[serde(default)]
    filters: Vec<FilterInput>,
    #[serde(default)]
    or_groups: Vec<Vec<FilterInput>>,
    #[serde(default)]
    order_by: Vec<OrderInput>,
    limit: Option<usize>,
    offset: Option<usize>,
    projection: Option<Vec<String>>,
}

fn map_operator(op: &str) -> Result<Operator, String> {
    match op {
        "==" => Ok(Operator::Eq),
        "!=" => Ok(Operator::Ne),
        ">" => Ok(Operator::Gt),
        ">=" => Ok(Operator::Gte),
        "<" => Ok(Operator::Lt),
        "<=" => Ok(Operator::Lte),
        "in" => Ok(Operator::In),
        "not-in" => Ok(Operator::NotIn),
        "contains" => Ok(Operator::Contains),
        "starts-with" => Ok(Operator::StartsWith),
        "match" => Ok(Operator::Match),
        "match-prefix" => Ok(Operator::MatchPrefix),
        "array-contains" => Ok(Operator::ArrayContains),
        "array-contains-any" => Ok(Operator::ArrayContainsAny),
        other => Err(format!("unknown operator '{other}'")),
    }
}

fn build_query(body: &QueryBody) -> Result<FireQuery, String> {
    if body.collection.trim().is_empty() {
        return Err("collection required".into());
    }
    // Server-side caps: the admin console must not become a DoS vector
    // against its own database.
    let limit = body.limit.unwrap_or(100).min(1000);
    let mut q = FireQuery::new(&body.collection);
    for f in &body.filters {
        q = q.where_filter(&f.field, map_operator(&f.op)?, json_to_fire(&f.value)?);
    }
    // or_groups ride the engine's cross-filter OR semantics, which the
    // admin console deliberately does not expose (use explicit queries).
    if !body.or_groups.is_empty() {
        return Err("or_groups not supported via admin query yet".into());
    }
    for o in &body.order_by {
        let asc = !o.direction.eq_ignore_ascii_case("desc");
        q = q.order_by(&o.field, asc);
    }
    q = q.limit(limit);
    if let Some(off) = body.offset {
        q = q.offset(off);
    }
    if let Some(proj) = &body.projection {
        if !proj.is_empty() {
            q = q.select_fields(proj.clone());
        }
    }
    Ok(q)
}

async fn query(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Json(body): Json<QueryBody>,
) -> Response {
    let _ = user;
    let q = match build_query(&body) {
        Ok(q) => q,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    };
    match state.db.query(q) {
        Ok(rows) => {
            let out: Vec<JsonValue> =
                rows.iter().map(|(id, doc)| doc_to_json(id, doc)).collect();
            Json(json!({ "rows": out, "count": out.len() })).into_response()
        }
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn get_doc(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path((col, id)): Path<(String, String)>,
) -> Response {
    let _ = user;
    match state.db.get(&col, &id) {
        Ok(Some(doc)) => Json(doc_to_json(&id, &doc)).into_response(),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "not found"})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct DocBody {
    data: JsonValue,
}

async fn put_doc(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path((col, id)): Path<(String, String)>,
    Json(body): Json<DocBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    let doc = match doc_from_json(&body.data) {
        Ok(d) => d,
        Err(e) => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": e})),
            )
                .into_response()
        }
    };
    match state.db.put(&col, &id, &doc) {
        Ok(_) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn patch_doc(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path((col, id)): Path<(String, String)>,
    Json(body): Json<DocBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    let obj = match body.data.as_object() {
        Some(o) => o,
        None => {
            return (
                StatusCode::BAD_REQUEST,
                Json(json!({"error": "data must be a JSON object"})),
            )
                .into_response()
        }
    };
    let mut updates = Vec::new();
    for (k, v) in obj {
        match json_to_fire(v) {
            Ok(fv) => updates.push((k.clone(), fv)),
            Err(e) => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": e})),
                )
                    .into_response()
            }
        }
    }
    match state.db.patch(&col, &id, updates) {
        Ok(_) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn delete_doc(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path((col, id)): Path<(String, String)>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    match state.db.delete(&col, &id) {
        Ok(_) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Batch
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct BatchItem {
    op: String,
    collection: String,
    doc_id: String,
    data: Option<JsonValue>,
}

#[derive(Debug, Deserialize)]
struct BatchBody {
    mutations: Vec<BatchItem>,
}

async fn batch(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Json(body): Json<BatchBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    if body.mutations.len() > 1000 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "batch capped at 1000 mutations"})),
        )
            .into_response();
    }
    let mut ops = Vec::with_capacity(body.mutations.len());
    for m in &body.mutations {
        match m.op.as_str() {
            "set" => {
                let data = match m.data.as_ref() {
                    Some(d) => d,
                    None => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "set requires data"})),
                        )
                            .into_response()
                    }
                };
                match doc_from_json(data) {
                    Ok(doc) => ops.push(BatchMutation::Put {
                        collection: m.collection.clone(),
                        doc_id: m.doc_id.clone(),
                        doc,
                    }),
                    Err(e) => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": e})),
                        )
                            .into_response()
                    }
                }
            }
            "patch" => {
                let obj = match m.data.as_ref().and_then(|d| d.as_object()) {
                    Some(o) => o,
                    None => {
                        return (
                            StatusCode::BAD_REQUEST,
                            Json(json!({"error": "patch requires object data"})),
                        )
                            .into_response()
                    }
                };
                let mut updates = Vec::new();
                for (k, v) in obj {
                    match json_to_fire(v) {
                        Ok(fv) => updates.push((k.clone(), fv)),
                        Err(e) => {
                            return (
                                StatusCode::BAD_REQUEST,
                                Json(json!({"error": e})),
                            )
                                .into_response()
                        }
                    }
                }
                ops.push(BatchMutation::Patch {
                    collection: m.collection.clone(),
                    doc_id: m.doc_id.clone(),
                    updates,
                });
            }
            "delete" => ops.push(BatchMutation::Delete {
                collection: m.collection.clone(),
                doc_id: m.doc_id.clone(),
            }),
            other => {
                return (
                    StatusCode::BAD_REQUEST,
                    Json(json!({"error": format!("unknown op '{other}'")})),
                )
                    .into_response()
            }
        }
    }
    match state.db.write_batch(ops) {
        Ok(ids) => Json(json!({"ok": true, "ids": ids})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Indexes
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct IndexQuery {
    collection: Option<String>,
}

async fn list_indexes(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    AxQuery(q): AxQuery<IndexQuery>,
) -> Response {
    let _ = user;
    Json(json!({ "indexes": state.db.list_indexes(q.collection.as_deref()) })).into_response()
}

#[derive(Debug, Deserialize)]
struct CreateIndexBody {
    kind: String,
    collection: String,
    field: Option<String>,
    fields: Option<Vec<IndexFieldInput>>,
}

#[derive(Debug, Deserialize)]
struct IndexFieldInput {
    field: String,
    #[serde(default)]
    desc: bool,
}

async fn create_index(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Json(body): Json<CreateIndexBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    let res: Result<JsonValue, String> = match body.kind.as_str() {
        "simple" => match body.field {
            Some(f) => state
                .db
                .create_index(&body.collection, &f)
                .map(|_| json!(true))
                .map_err(|e| e.to_string()),
            None => Err("field required".to_string()),
        },
        "fts" => match body.field {
            Some(f) => state
                .db
                .create_fts_index(&body.collection, &f)
                .map(|_| json!(true))
                .map_err(|e| e.to_string()),
            None => Err("field required".to_string()),
        },
        "composite" => match body.fields {
            Some(fs) if !fs.is_empty() => {
                let fields: Vec<(String, SortDirection)> = fs
                    .into_iter()
                    .map(|f| {
                        (
                            f.field,
                            if f.desc {
                                SortDirection::Desc
                            } else {
                                SortDirection::Asc
                            },
                        )
                    })
                    .collect();
                state
                    .db
                    .create_composite_index(&body.collection, fields)
                    .map(|id| json!({ "index_id": id }))
                    .map_err(|e| e.to_string())
            }
            _ => Err("fields required".to_string()),
        },
        other => Err(format!("unknown kind '{other}' (simple|fts|composite)")),
    };
    match res {
        Ok(v) => Json(json!({ "ok": true, "result": v })).into_response(),
        Err(msg) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": msg})),
        )
            .into_response(),
    }
}

// ---------------------------------------------------------------------------
// Maintenance + observability
// ---------------------------------------------------------------------------

#[derive(Debug, Deserialize)]
struct PathBody {
    path: String,
}

async fn backup(State(state): State<Arc<AppState>>, user: AuthedUser, Json(body): Json<PathBody>) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    match state.db.backup(&body.path) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn compact(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    match state.db.compact() {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct VacuumBody {
    collection: String,
}

async fn vacuum(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Json(body): Json<VacuumBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    match state.db.vacuum_collection(&body.collection) {
        Ok(n) => Json(json!({"ok": true, "tombstones": n})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e.to_string()})),
        )
            .into_response(),
    }
}

async fn stats(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    let _ = user;
    Json(json!({ "stats": state.db.get_stats() })).into_response()
}

async fn audit(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    Json(json!({ "entries": state.db.audit_entries() })).into_response()
}

pub fn data_routes() -> Router<Arc<crate::app::AppState>> {
    Router::new()
        .route("/api/status", get(status))
        .route("/api/rooms", get(rooms))
        .route("/api/collections", get(collections))
        .route("/api/query", post(query))
        .route(
            "/api/docs/:col/:id",
            get(get_doc)
                .put(put_doc)
                .patch(patch_doc)
                .delete(delete_doc),
        )
        .route("/api/batch", post(batch))
        .route("/api/indexes", get(list_indexes).post(create_index))
        .route("/api/backup", post(backup))
        .route("/api/compact", post(compact))
        .route("/api/vacuum", post(vacuum))
        .route("/api/stats", get(stats))
        .route("/api/audit", get(audit))
}
