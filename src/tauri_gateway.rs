use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
// use std::thread;
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
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<OrderByInput>,
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
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<OrderByInput>,
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
    SubscriptionAck { listener_id: String },
    Unsubscribed { listener_id: String },
    Stats { details: serde_json::Value },
    Collections { names: Vec<String> },
    Indexes { list: serde_json::Value },
    AuditLog { entries: Vec<crate::engine::AuditEntry> },
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
    db: Arc<FireLite>,
    subscriptions: Arc<Mutex<HashMap<String, SubscriptionEntry>>>,
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
            SubscriptionEntry {
                stop_tx,
                window_label: window.label().to_string(),
            },
        );

        let subscriptions = Arc::clone(&self.subscriptions);
        let db = Arc::clone(&self.db);
        let listener_id_for_thread = listener_id.clone();
        
        tokio::task::spawn_blocking(move || {
            let emit_snapshot = |win: &Window<R>| -> Result<(), String> {
                let rows = execute_query_input(&db, &query_template)?;
                let payload = SubscriptionPayload {
                    listener_id: listener_id_for_thread.clone(),
                    rows,
                };
                win.emit(&event_name, payload).map_err(|e: tauri::Error| e.to_string())
            };

            if emit_snapshot(&window).is_err() {
                subscriptions.lock().remove(&listener_id_for_thread);
                return;
            }

            loop {
                if stop_rx.try_recv().is_ok() { break; }
                match rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(_) => { 
                        while let Ok(_) = rx.try_recv() {} 
                        if emit_snapshot(&window).is_err() { break; } 
                    }
                    Err(RecvTimeoutError::Timeout) => continue,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
            subscriptions.lock().remove(&listener_id_for_thread);
        });

        Ok(())
    }
}

#[derive(Debug, Clone)]
struct QueryInput {
    collection: String,
    filters: Vec<FilterInput>,
    or_groups: Option<Vec<Vec<FilterInput>>>,
    order_by: Option<OrderByInput>,
    limit: Option<usize>,
    offset: Option<usize>,
    projection: Option<Vec<String>>,
    start_at: Option<Vec<serde_json::Value>>,
    start_after: Option<Vec<serde_json::Value>>,
    end_at: Option<Vec<serde_json::Value>>,
    end_before: Option<Vec<serde_json::Value>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "snake_case")]
struct SubscriptionPayload {
    listener_id: String,
    rows: Vec<serde_json::Value>,
}

// #[command]
// pub async fn firelite_exec<R: Runtime>(
//     window: Window<R>,
//     state: State<'_, FireLiteGateway>,
//     op: FireLiteOp,
// ) -> Result<FireLiteResponse, String> {
//     tokio::task::spawn_blocking(move || {
//         match op {
//             FireLiteOp::Get { collection, doc_id } => {
//                 let doc = state.db.get(&collection, &doc_id).map_err(|e| e.to_string())?;
//                 let data = doc.map(|d| doc_to_json_value(&d)).transpose()?;
//                 Ok(FireLiteResponse::Document { data })
//             }
//             FireLiteOp::Set { collection, doc_id, data } => {
//                 let doc = json_to_doc(&data)?;
//                 state.db.put(&collection, &doc_id, &doc).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::Patch { collection, doc_id, data } => {
//                 let updates = json_to_vec(&data)?;
//                 state.db.patch(&collection, &doc_id, updates).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::Delete { collection, doc_id } => {
//                 state.db.delete(&collection, &doc_id).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::CreateIndex { collection, field } => {
//                 state.db.create_index(&collection, &field).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::CreateFtsIndex { collection, field } => {
//                 state.db.create_fts_index(&collection, &field).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::CreateCompositeIndex { collection, fields } => {
//                 let parsed_fields = fields.into_iter().map(|f| (f.field, if f.desc { SortDirection::Desc } else { SortDirection::Asc })).collect();
//                 state.db.create_composite_index(&collection, parsed_fields);
//                 state.db.persist_index_defs().map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::Query { collection, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before } => {
//                 let rows = execute_query_input(&state.db, &QueryInput { 
//                     collection, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before 
//                 })?;
//                 Ok(FireLiteResponse::QueryResult { rows })
//             }
//             FireLiteOp::Batch { mutations } => {
//                 let mut batch = Vec::with_capacity(mutations.len());
//                 for item in mutations {
//                     match item.mutation {
//                         BatchMutationKind::Set => {
//                             let data = item.data.ok_or("missing data")?;
//                             batch.push(BatchMutation::Put { collection: item.collection, doc_id: item.doc_id, doc: json_to_doc(&data)? });
//                         }
//                         BatchMutationKind::Patch => {
//                             let data = item.data.ok_or("missing data")?;
//                             batch.push(BatchMutation::Patch { collection: item.collection, doc_id: item.doc_id, updates: json_to_vec(&data)? });
//                         }
//                         BatchMutationKind::Delete => {
//                             batch.push(BatchMutation::Delete { collection: item.collection, doc_id: item.doc_id });
//                         }
//                     }
//                 }
//                 state.db.write_batch(batch).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             // FIX: Included or_groups in pattern (Error E0027/E0425)
//             FireLiteOp::Aggregate { collection, filters, or_groups, kind, field } => {
//                 let mut query = Query::new(&collection);
//                 for filter in filters {
//                     query = query.where_filter(&filter.field, map_operator(&filter.op), json_value_to_value(&filter.value)?);
//                 }
//                 if let Some(groups) = or_groups {
//                     for group in groups {
//                         // FIX: Explicit Type for collect (Error E0282)
//                         let filters: Vec<crate::query::filter::Filter> = group.iter()
//                             .map(|f: &FilterInput| -> Result<crate::query::filter::Filter, String> { 
//                                 Ok(crate::query::filter::Filter { 
//                                     field: f.field.clone(), 
//                                     op: map_operator(&f.op), 
//                                     value: json_value_to_value(&f.value)? 
//                                 })
//                             })
//                             .collect::<Result<Vec<_>, String>>()?;
//                         query.or_groups.push(filters);
//                     }
//                 }
//                 use crate::query::query::AggregateOp;
//                 query = match kind {
//                     AggregateKind::Count => query.aggregate(AggregateOp::Count),
//                     AggregateKind::Sum => query.aggregate(AggregateOp::Sum(field.ok_or("missing field")?)),
//                     AggregateKind::Avg => query.aggregate(AggregateOp::Avg(field.ok_or("missing field")?)),
//                 };
//                 let result = state.db.execute_aggregation(query).map_err(|e| e.to_string())?;
//                 let val = *result.values().next().unwrap_or(&0.0);
//                 Ok(FireLiteResponse::AggregateResult { value: val })
//             }
//             FireLiteOp::Subscribe { listener_id, collection, filters, or_groups, order_by, limit, offset, projection, event_name, start_at, start_after, end_at, end_before } => {
//                 state.register_subscription(
//                     window,
//                     listener_id.clone(),
//                     QueryInput { collection, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before },
//                     event_name.unwrap_or_else(|| "firelite://snapshot".to_string()),
//                 )?;
//                 Ok(FireLiteResponse::SubscriptionAck { listener_id })
//             }
//             FireLiteOp::Unsubscribe { listener_id } => {
//                 state.unsubscribe(&listener_id);
//                 Ok(FireLiteResponse::Unsubscribed { listener_id })
//             }
//             FireLiteOp::GetStats => {
//                 let stats = state.db.get_stats();
//                 Ok(FireLiteResponse::Stats { details: serde_json::to_value(stats).unwrap() })
//             }
//             FireLiteOp::ListCollections => {
//                 let names = state.db.list_collections().map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Collections { names })
//             }
//             FireLiteOp::Compact => {
//                 state.db.compact().map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::Backup { path } => {
//                 state.db.backup(path).map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::ListIndexes { collection } => {
//                 let list = state.db.list_indexes(collection.as_deref());
//                 Ok(FireLiteResponse::Indexes { list: serde_json::to_value(list).unwrap() })
//             }
//             FireLiteOp::SnapshotIndices => {
//                 state.db.save_index_snapshots().map_err(|e| e.to_string())?;
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::GetAuditLog => {
//                 let entries = state.db.audit_entries();
//                 Ok(FireLiteResponse::AuditLog { entries })
//             }
//             FireLiteOp::SetDurability { mode } => {
//                 use crate::config::DurabilityMode;
//                 let d_mode = match mode {
//                     1 => DurabilityMode::Interval,
//                     2 => DurabilityMode::Manual,
//                     3 => DurabilityMode::OnCommit,
//                     _ => DurabilityMode::Always,
//                 };
                
//                 // FIX: Assumes engine.rs change (Error E0616/E0282)
//                 let shards = state.db.shards.read().unwrap();
//                 for shard in shards.values() {
//                     if let Ok(mut s) = shard.write() {
//                         s.set_durability_mode(d_mode);
//                     }
//                 }
//                 Ok(FireLiteResponse::Ok)
//             }
//             FireLiteOp::SetCompression { enabled, level: _ } => {
//                 let shards = state.db.shards.read().unwrap();
//                 for shard in shards.values() {
//                     if let Ok(mut _s) = shard.write() {
//                         // Logic here once setter is added to StorageEngine
//                     }
//                 }
//                 Ok(FireLiteResponse::Ok)
//             }
//         }
//     })
//     .await
//     .unwrap_or_else(|e| Err(format!("Tokio Task Error: {}", e))) 
// }

#[command]
pub async fn firelite_exec<R: Runtime>(
    window: Window<R>,
    state: State<'_, FireLiteGateway>,
    op: FireLiteOp,
) -> Result<FireLiteResponse, String> {
    // CLONE the gateway here. 
    // This is cheap because it only clones the Arcs (pointers).
    // This gives us an owned 'gateway' that can be moved into the 'static thread.
    let gateway = state.inner().clone();

    tokio::task::spawn_blocking(move || -> Result<FireLiteResponse, String> {
        // Use 'gateway' instead of 'state' throughout this block
        match op {
            FireLiteOp::Get { collection, doc_id } => {
                let doc = gateway.db.get(&collection, &doc_id).map_err(|e| e.to_string())?;
                let data = doc.map(|d| doc_to_json_value(&d)).transpose()?;
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
                gateway.db.create_composite_index(&collection, parsed_fields);
                gateway.db.persist_index_defs().map_err(|e| e.to_string())?;
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::Query { collection, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before } => {
                let rows = execute_query_input(&gateway.db, &QueryInput { 
                    collection, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before 
                })?;
                Ok(FireLiteResponse::QueryResult { rows })
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
            FireLiteOp::Aggregate { collection, filters, or_groups, kind, field } => {
                let mut query = Query::new(&collection);
                for filter in filters {
                    query = query.where_filter(&filter.field, map_operator(&filter.op), json_value_to_value(&filter.value)?);
                }
                if let Some(groups) = or_groups {
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
            FireLiteOp::Subscribe { listener_id, collection, filters, or_groups, order_by, limit, offset, projection, event_name, start_at, start_after, end_at, end_before } => {
                gateway.register_subscription(
                    window,
                    listener_id.clone(),
                    QueryInput { collection, filters, or_groups, order_by, limit, offset, projection, start_at, start_after, end_at, end_before },
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
                
                let shards = gateway.db.shards.read().unwrap();
                for shard in shards.values() {
                    if let Ok(mut s) = shard.write() {
                        s.set_durability_mode(d_mode);
                    }
                }
                Ok(FireLiteResponse::Ok)
            }
            FireLiteOp::SetCompression { enabled: _, level: _ } => {
                // Fixed the 'enabled' warning by prefixing with underscore
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


fn execute_query_input(db: &FireLite, input: &QueryInput) -> Result<Vec<serde_json::Value>, String> {
    let mut query = Query::new(&input.collection);

    for filter in &input.filters {
        query = query.where_filter(&filter.field, map_operator(&filter.op), json_value_to_value(&filter.value)?);
    }

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

    if let Some(order) = &input.order_by { query = query.order_by(&order.field, order.ascending); }
    if let Some(limit) = input.limit { query = query.limit(limit); }
    if let Some(offset) = input.offset { query = query.offset(offset); }

    if let Some(v) = &input.start_at { query.start_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.start_after { query.start_after = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_at { query.end_at = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }
    if let Some(v) = &input.end_before { query.end_before = Some(v.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>()?); }

    if let Some(projection) = &input.projection {
        if !projection.is_empty() {
            let rows = db.query_projected_zero_copy(query.clone(), projection).map_err(|e| e.to_string())?;
            return rows.into_iter().map(|(_, fields)| projection_fields_to_json(fields)).collect();
        }
    }

    let rows = db.query(query).map_err(|e| e.to_string())?;
    rows.into_iter().map(|(_, doc)| doc_to_json_value(&doc)).collect()
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
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() { Ok(Value::Int(i)) }
            else { Ok(Value::Float(n.as_f64().unwrap_or(0.0))) }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        serde_json::Value::Array(arr) => arr.iter().map(json_value_to_value).collect::<Result<Vec<_>, _>>().map(Value::Array),
        serde_json::Value::Object(obj) => {
            let mut map = Vec::new();
            for (k, v) in obj {
                map.push((Arc::from(k.as_str()), json_value_to_value(v)?));
            }
            Ok(Value::Map(map))
        }
    }
}

fn doc_to_json_value(doc: &FireLiteDoc) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in &doc.fields {
        map.insert(k.to_string(), value_to_json(v)?);
    }
    Ok(serde_json::Value::Object(map))
}

fn projection_fields_to_json(fields: Vec<(String, Value)>) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
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
        Value::Binary(bytes) => Ok(serde_json::Value::Array(bytes.iter().map(|b| serde_json::Value::Number((*b as u64).into())).collect())),
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
        Value::Array(values) => Ok(serde_json::Value::Array(values.iter().map(value_to_json).collect::<Result<Vec<_>, _>>()?)),
    }
}