use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tauri::{command, Emitter, Runtime, State, Window};

use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::engine::{BatchMutation, FireLite};
use crate::index::composite::definition::SortDirection;
use crate::query::filter::Operator;
use crate::query::query::Query;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "snake_case")]
pub enum FireLiteOp {
    Get { collection: String, doc_id: String },
    Set { collection: String, doc_id: String, data: serde_json::Value },
    Patch { collection: String, doc_id: String, data: serde_json::Value },
    Delete { collection: String, doc_id: String },
    CreateIndex { collection: String, field: String },
    CreateFtsIndex { collection: String, field: String },
    CreateCompositeIndex { collection: String, fields: Vec<CompositeFieldInput> },
    Query {
        collection: String,
        #[serde(default)]
        action: Option<QueryAction>,
        doc_id_filter: Option<String>,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<Vec<OrderByInput>>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        start_at: Option<Vec<serde_json::Value>>,
        start_after: Option<Vec<serde_json::Value>>,
        end_at: Option<Vec<serde_json::Value>>,
        end_before: Option<Vec<serde_json::Value>>,
    },
    Batch { mutations: Vec<BatchInput> },
    Aggregate {
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        #[serde(default)]
        or_groups: Option<Vec<Vec<FilterInput>>>,
        kind: AggregateKind,
        field: Option<String>,
    },
    Subscribe {
        listener_id: String,
        doc_id_filter: Option<String>,
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<Vec<OrderByInput>>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        event_name: Option<String>,
        start_at: Option<Vec<serde_json::Value>>,
        start_after: Option<Vec<serde_json::Value>>,
        end_at: Option<Vec<serde_json::Value>>,
        end_before: Option<Vec<serde_json::Value>>,
    },
    Unsubscribe { listener_id: String },
    Backup { path: String },
    Compact,
    GetStats,
    ListCollections,
    ListIndexes { collection: Option<String> },
    SnapshotIndices,
    GetAuditLog,
    SetDurability { mode: i32 },
    SetCompression { enabled: bool, level: i32 },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FireLiteResponse {
    Ok,
    Document { data: Option<serde_json::Value> },
    QueryResult { rows: Vec<serde_json::Value> },
    AggregateResult { value: f64 },
    Aggregate(f64), 
    SubscriptionAck { listener_id: String },
    Unsubscribed { listener_id: String },
    Stats { details: serde_json::Value },
    Collections { names: Vec<String> },
    Indexes { list: serde_json::Value },
    AuditLog { entries: Vec<crate::engine::AuditEntry> },
    BulkActionResult { count: usize },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct FilterInput {
    pub field: String,
    pub op: FilterOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct OrderByInput {
    pub field: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct BatchInput {
    pub mutation: BatchMutationKind,
    pub collection: String,
    pub doc_id: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum BatchMutationKind { Set, Patch, Delete }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum FilterOperator {
    Eq, Ne, Gt, Gte, Lt, Lte, Match, Contains, StartsWith, In, NotIn, ArrayContains, ArrayContainsAny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum AggregateKind { Count, Sum, Avg }

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub struct CompositeFieldInput {
    pub field: String,
    #[serde(default)]
    pub desc: bool,
}

#[derive(Clone)]
pub struct FireLiteGateway {
    pub db: Arc<FireLite>,
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,
}

#[derive(Debug, Clone, serde::Serialize)]
#[serde(rename_all = "snake_case")]
pub enum DeltaKind {
    Full,    // Initial bootstrap
    Update,  // Add or Modify
    Delete,  // Removed or no longer matches filter
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DocumentChange {
    pub kind: DeltaKind,
    pub doc_id: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, serde::Serialize)]
pub struct DeltaPayload {
    pub listener_id: String,
    pub changes: Vec<DocumentChange>, // The batch container
}

struct SubscriptionEntry {
    stop_tx: Sender<()>,
    window_label: String,
}

impl FireLiteGateway {
    pub fn new(db: FireLite) -> Self {
        Self {
            db: Arc::new(db),
            subscriptions: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    // --- ADD THIS METHOD ---
    pub fn cleanup_window_subscriptions(&self, window_label: &str) {
        let mut subs = self.subscriptions.lock();
        
        // Find all listener IDs belonging to the closed window
        let ids_to_remove: Vec<String> = subs
            .iter()
            .filter(|(_, entry)| entry.window_label == window_label)
            .map(|(id, _)| id.clone())
            .collect();

        // Stop the threads and remove from the map
        for id in ids_to_remove {
            if let Some(entry) = subs.remove(&id) {
                // Sending this signal causes the loop in the thread to break
                let _ = entry.stop_tx.send(());
            }
        }
    }

    pub fn unsubscribe(&self, listener_id: &str) {
        if let Some(entry) = self.subscriptions.lock().remove(listener_id) {
            let _ = entry.stop_tx.send(());
        }
    }

    fn register_subscription<R: Runtime>(
        &self,
        window: Window<R>,
        listener_id: String,
        query_template: QueryInput,
        event_name: String,
    ) -> Result<(), String> {
        self.unsubscribe(&listener_id);

        let rx = self.db.watch_collection(&query_template.collection);
        let (stop_tx, stop_rx) = mpsc::channel::<()>();
        
        self.subscriptions.lock().insert(
            listener_id.clone(),
            SubscriptionEntry { stop_tx, window_label: window.label().to_string() },
        );

        let db = Arc::clone(&self.db);
        let lid = listener_id.clone();
        let ename = event_name.clone();
        let subscriptions = Arc::clone(&self.subscriptions);
        
        tokio::task::spawn_blocking(move || {
            // --- 1. INITIAL BOOTSTRAP (The "Full" Snapshot) ---
            let initial_rows = match execute_query_input(&db, &query_template) {
                Ok(rows) => rows,
                Err(_) => Vec::new(),
            };

            let _ = window.emit(&ename, DeltaPayload {
                listener_id: lid.clone(),
                changes: vec![DocumentChange {
                    kind: DeltaKind::Full,
                    doc_id: "_all_".into(),
                    data: Some(serde_json::Value::Array(initial_rows)),
                }],
            });

            // --- 2. PREPARE MATCHER PLAN FOR LIVE UPDATES ---
            // let mut base_query = crate::query::query::Query::new(&query_template.collection);
            // for f in &query_template.filters {
            //     base_query = base_query.where_filter(&f.field, map_operator(&f.op), json_value_to_value(&f.value).unwrap_or(Value::Null));
            // }
            // // If we are watching a specific ID, add it to the filter plan
            // if let Some(ref tid) = query_template.doc_id_filter {
            //     base_query = base_query.where_filter("id", Operator::Eq, Value::String(tid.clone()));
            // }
            let query_obj = build_query_from_input(&query_template).unwrap_or_else(|_| {
                crate::query::query::Query::new(&query_template.collection)
            });

            let filter_plan = {
                let indexes = db.indexes.read().unwrap();
                let is_ready = db.indexes_ready.load(std::sync::atomic::Ordering::Acquire);
                crate::query::planner::QueryPlanner::plan(&query_obj, &indexes, 0, 1,is_ready)
            };

            // --- 3. EVENT LOOP ---
            loop {
                // Check if unsubscribed
                if stop_rx.try_recv().is_ok() { break; }

                match rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(first_event) => {
                        let mut events = vec![first_event];
                        while let Ok(extra) = rx.try_recv() { events.push(extra); }

                        let mut changes = Vec::new();
                        for event in events {
                            let doc_id = event.path.clone(); // The path is the ID in FireLite
                            
                            // ID Filtering (Optimization: check before reading disk)
                            if let Some(ref target_id) = query_template.doc_id_filter {
                                if &doc_id != target_id { continue; }
                            }

                            match event.kind {
                                crate::engine::ChangeKind::Delete => {
                                    // Provide a timestamp for the delete so client can ignore stale updates
                                    let mut meta = serde_json::Map::new();
                                    meta.insert("_time".to_string(), serde_json::json!(crate::util::clock::unix_millis() * 1000));
                                    
                                    changes.push(DocumentChange { 
                                        kind: DeltaKind::Delete, 
                                        doc_id, 
                                        data: Some(serde_json::Value::Object(meta)) 
                                    });
                                }
                                crate::engine::ChangeKind::Put => {
                                    // let shard = db.get_shard(&query_template.collection);
                                    let shard = match db.get_shard(&query_template.collection) {
                                        Ok(s) => s,
                                        Err(e) => {
                                            // This is a serious error: we received data but cannot write it
                                            // because the local shard is locked/unreadable.
                                            eprintln!("[Put] CRITICAL: Cannot put data {}. Shard error: {}", &query_template.collection, e);
                                            return; 
                                        }
                                    };
                                    let bytes_res = {
                                        let storage = shard.read().unwrap();
                                        storage.get(&event.path)
                                    };

                                    if let Ok(Some(bytes)) = bytes_res {
                                        // Complex Query Filtering
                                        let doc_time = i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]));
                                        if crate::query::executor::worker::matches_filters_view(&doc_id, &bytes, &filter_plan) {
                                            // Handle Projection
                                            let doc = if let Some(ref p) = query_template.projection {
                                                FireLiteDoc::decode_projected(&bytes, p)
                                            } else {
                                                FireLiteDoc::decode(&bytes)
                                            };

                                            if let Some(mut d) = doc {
                                                let _ = db.resolve_document_blobs(&mut d, &query_template.collection);
                                                changes.push(DocumentChange { 
                                                    kind: DeltaKind::Update, 
                                                    doc_id: doc_id.clone(), 
                                                    data: doc_to_json_value(&doc_id,&d).ok() 
                                                });
                                            }
                                        } else {
                                            // This handles the "Exit" case: 
                                            // Doc existed and matched, but was updated to no longer match.
                                            let mut meta = serde_json::Map::new();
                                            meta.insert("_time".to_string(), serde_json::json!(doc_time));
                                            changes.push(DocumentChange { 
                                                kind: DeltaKind::Delete, 
                                                doc_id, 
                                                data: Some(serde_json::Value::Object(meta)) 
                                            });
                                        }
                                    }
                                }
                            }
                        }

                        if !changes.is_empty() {
                            let _ = window.emit(&ename, DeltaPayload {
                                listener_id: lid.clone(),
                                changes,
                            });
                        }
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            subscriptions.lock().remove(&lid);
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct QueryInput {
    collection: String,
    filters: Vec<FilterInput>,
    or_groups: Option<Vec<Vec<FilterInput>>>,
    order_by: Option<Vec<OrderByInput>>,
    limit: Option<usize>,
    offset: Option<usize>,
    projection: Option<Vec<String>>,
    start_at: Option<Vec<serde_json::Value>>,
    start_after: Option<Vec<serde_json::Value>>,
    end_at: Option<Vec<serde_json::Value>>,
    end_before: Option<Vec<serde_json::Value>>,
    doc_id_filter: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum QueryAction {
    Fetch,
    Delete,
    Patch { data: serde_json::Value },
}

#[command]
pub async fn firelite_exec<R: Runtime>(
    window: Window<R>,
    state: State<'_, FireLiteGateway>,
    op: FireLiteOp,
) -> Result<FireLiteResponse, String> {

    let gateway = state.inner().clone();
    
    tokio::task::spawn_blocking(move || {
        match op {
            FireLiteOp::Get { collection, doc_id } => {
                let doc = gateway.db.get(&collection, &doc_id).map_err(|e| e.to_string())?;
                let data = doc.map(|d| doc_to_json_value(&doc_id, &d)).transpose()?;
                Ok(FireLiteResponse::Document { data })
            }
            FireLiteOp::Set { collection, doc_id, data } => {
                let doc = json_to_doc(&data)?;
                gateway.db.put(&collection, &doc_id, &doc).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::Patch { collection, doc_id, data } => {
                let updates = json_to_vec(&data)?;
                gateway.db.patch(&collection, &doc_id, updates).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::Delete { collection, doc_id } => {
                gateway.db.delete(&collection, &doc_id).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::CreateIndex { collection, field } => {
                gateway.db.create_index(&collection, &field).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::CreateFtsIndex { collection, field } => {
                gateway.db.create_fts_index(&collection, &field).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::CreateCompositeIndex { collection, fields } => {
                let parsed_fields = fields.into_iter().map(|f| (f.field, if f.desc { SortDirection::Desc } else { SortDirection::Asc })).collect();
                let _ = gateway.db.create_composite_index(&collection, parsed_fields);
                gateway.db.persist_index_defs().map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::Query { collection, action, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before } => {
                let input = QueryInput { 
                    collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before 
                };
                
                // Resolve the standard query object
                let query_obj = build_query_from_input(&input)?;

                // if let Some(id) = &input.doc_id_filter {
                //     query_obj = query_obj.where_filter("id", Operator::Eq, Value::String(id.to_string()));
                // }

                match action.unwrap_or(QueryAction::Fetch) {
                    QueryAction::Fetch => {
                        let rows = execute_query_input(&gateway.db, &input)?;
                        Ok(FireLiteResponse::QueryResult { rows })
                    }
                    QueryAction::Delete => {
                        let count = gateway.db.delete_where(query_obj).map_err(|e| e.to_string())?;
                        Ok(FireLiteResponse::BulkActionResult { count })
                    }
                    QueryAction::Patch { data } => {
                        let updates = json_to_vec(&data)?;
                        let count = gateway.db.patch_where(query_obj, updates).map_err(|e| e.to_string())?;
                        Ok(FireLiteResponse::BulkActionResult { count })
                    }
                }
            }
            FireLiteOp::Batch { mutations } => {
                let mut batch = Vec::with_capacity(mutations.len());
                for item in mutations {
                    match item.mutation {
                        BatchMutationKind::Set => {
                            let data = item.data.ok_or("missing data")?;
                            batch.push(BatchMutation::Put { collection: item.collection, doc_id: item.doc_id, doc: json_to_doc(&data)? });
                        }
                        BatchMutationKind::Patch => {
                            let data = item.data.ok_or("missing data")?;
                            batch.push(BatchMutation::Patch { collection: item.collection, doc_id: item.doc_id, updates: json_to_vec(&data)? });
                        }
                        BatchMutationKind::Delete => {
                            batch.push(BatchMutation::Delete { collection: item.collection, doc_id: item.doc_id });
                        }
                    }
                }
                gateway.db.write_batch(batch).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            // FIX: Included or_groups in pattern (Error E0027/E0425)
            FireLiteOp::Aggregate { collection, filters, or_groups, kind, field } => {
                let mut query = Query::new(&collection);
                for filter in filters {
                    query = query.where_filter(&filter.field, map_operator(&filter.op), json_value_to_value(&filter.value)?);
                }
                if let Some(groups) = or_groups {
                    for group in groups {
                        // FIX: Explicit Type for collect (Error E0282)
                        let filters: Vec<crate::query::filter::Filter> = group.iter()
                            .map(|f: &FilterInput| -> Result<crate::query::filter::Filter, String> { 
                                Ok(crate::query::filter::Filter { 
                                    field: f.field.clone(), 
                                    op: map_operator(&f.op), 
                                    value: json_value_to_value(&f.value)? 
                                })
                            })
                            .collect::<Result<Vec<_>, String>>()?;
                        query.or_groups.push(filters);
                    }
                }
                use crate::query::query::AggregateOp;
                query = match kind {
                    AggregateKind::Count => query.aggregate(AggregateOp::Count),
                    AggregateKind::Sum => query.aggregate(AggregateOp::Sum(field.ok_or("missing field")?)),
                    AggregateKind::Avg => query.aggregate(AggregateOp::Avg(field.ok_or("missing field")?)),
                };
                let result = gateway.db.execute_aggregation(query).map_err(|e| e.to_string())?;
                let val = *result.values().next().unwrap_or(&0.0);
                Ok(FireLiteResponse::AggregateResult { value: val })
            }
            FireLiteOp::Subscribe { listener_id, collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, event_name, start_at, start_after, end_at, end_before } => {
                gateway.register_subscription(
                    window,
                    listener_id.clone(),
                    QueryInput { collection, doc_id_filter, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before },
                    event_name.unwrap_or_else(|| "firelite://snapshot".to_string()),
                )?;
                Ok(FireLiteResponse::SubscriptionAck { listener_id })
            }
            FireLiteOp::Unsubscribe { listener_id } => {
                gateway.unsubscribe(&listener_id);
                Ok(FireLiteResponse::Unsubscribed { listener_id })
            }
            FireLiteOp::GetStats => {
                let stats = gateway.db.get_stats();
                Ok(FireLiteResponse::Stats { details: serde_json::to_value(stats).unwrap() })
            }
            FireLiteOp::ListCollections => {
                let names = gateway.db.list_collections().map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Collections { names })
            }
            FireLiteOp::Compact => {
                gateway.db.compact().map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::Backup { path } => {
                gateway.db.backup(path).map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::ListIndexes { collection } => {
                let list = gateway.db.list_indexes(collection.as_deref());
                Ok(FireLiteResponse::Indexes { list: serde_json::to_value(list).unwrap() })
            }
            FireLiteOp::SnapshotIndices => {
                gateway.db.save_index_snapshots().map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::GetAuditLog => {
                let entries = gateway.db.audit_entries();
                Ok(FireLiteResponse::AuditLog { entries })
            }
            FireLiteOp::SetDurability { mode } => {
                use crate::config::DurabilityMode;
                let d_mode = match mode {
                    1 => DurabilityMode::Interval,
                    2 => DurabilityMode::Manual,
                    3 => DurabilityMode::OnCommit,
                    _ => DurabilityMode::Always,
                };
                
                // FIX: Assumes engine.rs change (Error E0616/E0282)
                let shards = gateway.db.shards.read().unwrap();
                for shard in shards.values() {
                    if let Ok(mut s) = shard.write() {
                        s.set_durability_mode(d_mode);
                    }
                }
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::SetCompression { enabled: _, level: _ } => {
                let shards = gateway.db.shards.read().unwrap();
                for shard in shards.values() {
                    if let Ok(mut _s) = shard.write() {
                        // Logic here once setter is added to StorageEngine
                    }
                }
                Ok(FireLiteResponse::Ok)
            }
        }
    })
    .await
    .unwrap_or_else(|e| Err(format!("Tokio Task Error: {}", e))) 
}


fn build_query_from_input(input: &QueryInput) -> Result<Query, String> {
    let mut query = Query::new(&input.collection);

    // 1. CRITICAL FIX: Inject doc_id_filter into the Query filters.
    // The TS client often sends this for single-document snapshots or targeted queries.
    // If we don't add this here, targeted queries return the whole collection.
    if let Some(ref id) = input.doc_id_filter {
        if !id.is_empty() {
            query = query.where_filter("id", Operator::Eq, Value::String(id.clone()));
        }
    }

    // 2. Add Standard Filters (This handles the 'in' operator values)
    for filter in &input.filters {
        let val = json_value_to_value(&filter.value)?;
        query = query.where_filter(
            &filter.field, 
            map_operator(&filter.op), 
            val
        );
    }

    // 2. Add OR groups
    if let Some(groups) = &input.or_groups {
        for group in groups {
            let filters: Vec<crate::query::filter::Filter> = group.iter()
                .map(|f: &FilterInput| -> Result<crate::query::filter::Filter, String> { 
                    Ok(crate::query::filter::Filter { 
                        field: f.field.clone(), 
                        op: map_operator(&f.op), 
                        value: json_value_to_value(&f.value)? 
                    })
                })
                .collect::<Result<Vec<_>, String>>()?;
            query.or_groups.push(filters);
        }
    }

    // 3. Sorting and Pagination
    // if let Some(order) = &input.order_by { query = query.order_by(&order.field, order.ascending); }
    if let Some(ref orders) = &input.order_by {
        for order in orders {
            query = query.order_by(&order.field, order.ascending);
        }
    }
    
    if let Some(limit) = input.limit { query = query.limit(limit); }
    if let Some(offset) = input.offset { query = query.offset(offset); }

    // 5. Select/Projection
    if let Some(proj) = &input.projection { query = query.select_fields(proj.clone()); }

    // 4. Cursor support
    if let Some(v) = &input.start_at { query.start_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.start_after { query.start_after = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_at { query.end_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_before { query.end_before = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }

    Ok(query)
}


fn execute_query_input(db: &FireLite, input: &QueryInput) -> Result<Vec<serde_json::Value>, String> {
    // USE THE NEW HELPER
    let query = build_query_from_input(input)?;

    // 1. Parallel Zero-Copy Projection Path
    if let Some(projection) = &input.projection {
        if !projection.is_empty() {
            let rows = db.query_projected_zero_copy(query.clone(), projection).map_err(|e| e.to_string())?;
            // Use rayon to parallelize JSON construction
            use rayon::prelude::*;
            return Ok(rows.into_par_iter()
                .map(|(id, fields)| projection_fields_to_json(&id, fields).unwrap_or(serde_json::Value::Null))
                .collect());
        }
    }

    // 2. Parallel Standard Query Path
    let rows = db.query(query).map_err(|e| e.to_string())?;
    
    use rayon::prelude::*;
    Ok(rows.into_par_iter()
        .map(|(id, doc)| doc_to_json_value(&id, &doc).unwrap_or(serde_json::Value::Null))
        .collect())
}

fn map_operator(op: &FilterOperator) -> Operator {
    match op {
        FilterOperator::Eq => Operator::Eq,
        FilterOperator::Ne => Operator::Ne,
        FilterOperator::Gt => Operator::Gt,
        FilterOperator::Gte => Operator::Gte,
        FilterOperator::Lt => Operator::Lt,
        FilterOperator::Lte => Operator::Lte,
        FilterOperator::Match => Operator::Match,
        FilterOperator::Contains => Operator::Contains,
        FilterOperator::StartsWith => Operator::StartsWith,
        FilterOperator::In => Operator::In,
        FilterOperator::NotIn => Operator::NotIn,
        FilterOperator::ArrayContains => Operator::ArrayContains,
        FilterOperator::ArrayContainsAny => Operator::ArrayContainsAny,
    }
}

fn json_to_doc(v: &serde_json::Value) -> Result<FireLiteDoc, String> {
    let obj = v.as_object().ok_or("document must be object")?;
    let mut doc = FireLiteDoc::default();
    for (k, val) in obj {
        doc.insert(k.clone(), json_value_to_value(val)?);
    }
    Ok(doc)
}

fn json_to_vec(v: &serde_json::Value) -> Result<Vec<(String, Value)>, String> {
    let obj = v.as_object().ok_or("updates must be object")?;
    let mut out = Vec::new();
    for (k, val) in obj {
        out.push((k.clone(), json_value_to_value(val)?));
    }
    Ok(out)
}

fn json_value_to_value(v: &serde_json::Value) -> Result<Value, String> {
    Value::from_json(v.clone())
}

fn doc_to_json_value(id: &str, doc: &FireLiteDoc) -> Result<serde_json::Value, String> {
    // Ok(doc.to_json())
    let mut json = doc.to_json();
    if let Some(obj) = json.as_object_mut() {
        // Force the ID into the JSON response
        obj.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    }
    Ok(json)
}

fn projection_fields_to_json(id: &str, fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    map.insert("id".to_string(), serde_json::Value::String(id.to_string()));
    for (k, v) in fields {
        map.insert(k, value_to_json(&v)?);
    }
    Ok(serde_json::Value::Object(map))
}

fn value_to_json(v: &Value) -> Result<serde_json::Value, String> {
    match v {
        Value::Null | Value::ServerTimestamp => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int(i) => Ok(serde_json::Value::Number((*i).into())),
        Value::Float(f) => serde_json::Number::from_f64(*f).map(serde_json::Value::Number).ok_or("invalid float".into()),
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        // Value::Binary(bytes) => Ok(serde_json::Value::Array(bytes.iter().map(|b| serde_json::Value::Number((*b as u64).into())).collect())),
        Value::Binary(bytes) => {
            use base64::{Engine as _, engine::general_purpose};
            // This is 10x-50x faster to serialize and transfer than an array of numbers
            Ok(serde_json::Value::String(format!("__b64__:{}", general_purpose::STANDARD.encode(bytes))))
        }
        Value::Timestamp(micros) => Ok(serde_json::Value::Number((*micros).into())),
        Value::Reference { collection, doc_id } => {
            let mut map = serde_json::Map::new();
            map.insert("__ref__".to_string(), serde_json::Value::String(format!("{collection}/{doc_id}")));
            Ok(serde_json::Value::Object(map))
        }
        Value::Map(fields) => {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                map.insert(k.to_string(), value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        // ADD THIS ARM:
        Value::BlobLink { offset, len } => {
            let mut map = serde_json::Map::new();
            let mut meta = serde_json::Map::new();
            meta.insert("offset".to_string(), serde_json::json!(offset));
            meta.insert("len".to_string(), serde_json::json!(len));
            map.insert("__blob__".to_string(), serde_json::Value::Object(meta));
            Ok(serde_json::Value::Object(map))
        }
        Value::Array(values) => Ok(serde_json::Value::Array(values.iter().map(value_to_json).collect::<Result<Vec<_>, _>>()?)),
    }
}