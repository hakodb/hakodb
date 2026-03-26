use std::collections::HashMap;
use std::sync::mpsc::{self, RecvTimeoutError, Sender};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use parking_lot::Mutex;
use serde::{Deserialize, Serialize};
use tauri::{command, Runtime, State, Window};

use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::engine::{BatchMutation, FireLite};
use crate::index::composite::definition::SortDirection;
use crate::query::filter::Operator;
use crate::query::query::Query;

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "op", rename_all = "camelCase")]
pub enum FireLiteOp {
    Get {
        collection: String,
        doc_id: String,
    },
    Set {
        collection: String,
        doc_id: String,
        data: serde_json::Value,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
    CreateIndex {
        collection: String,
        field: String,
    },
    CreateFtsIndex {
        collection: String,
        field: String,
    },
    CreateCompositeIndex {
        collection: String,
        fields: Vec<CompositeFieldInput>,
    },
    Query {
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        or_groups: Option<Vec<Vec<FilterInput>>>,
        order_by: Option<OrderByInput>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        start_at: Option<Vec<serde_json::Value>>,    // Added for cursors
        start_after: Option<Vec<serde_json::Value>>, // Added for cursors
        end_at: Option<Vec<serde_json::Value>>,      // Added for cursors
        end_before: Option<Vec<serde_json::Value>>,  // Added for cursors
    },
    Batch {
        mutations: Vec<BatchInput>,
    },
    Aggregate {
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        kind: AggregateKind,
        field: Option<String>,
    },
    Subscribe {
        listener_id: String,
        collection: String,
        #[serde(default)]
        filters: Vec<FilterInput>,
        order_by: Option<OrderByInput>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        event_name: Option<String>,
    },
    Unsubscribe {
        listener_id: String,
    },
    Patch { collection: String, doc_id: String, data: serde_json::Value },
    Backup { path: String },
    Compact,
    GetStats,
    ListCollections,
    ListIndexes { collection: Option<String> },
    SnapshotIndices,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct FilterInput {
    pub field: String,
    pub op: FilterOperator,
    pub value: serde_json::Value,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderByInput {
    pub field: String,
    pub ascending: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BatchInput {
    pub mutation: BatchMutationKind,
    pub collection: String,
    pub doc_id: String,
    pub data: Option<serde_json::Value>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BatchMutationKind {
    Set,
    Delete,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum FilterOperator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Match,
    Contains,
    StartsWith,
    In,
    NotIn,
    ArrayContains,
    ArrayContainsAny,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum AggregateKind {
    Count,
    Sum,
    Avg,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
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

    pub fn db(&self) -> Arc<FireLite> {
        Arc::clone(&self.db)
    }

    pub fn cleanup_window_subscriptions(&self, window_label: &str) {
        let ids_to_remove: Vec<String> = self
            .subscriptions
            .lock()
            .iter()
            .filter_map(|(id, entry)| {
                if entry.window_label == window_label {
                    Some(id.clone())
                } else {
                    None
                }
            })
            .collect();

        for id in ids_to_remove {
            self.unsubscribe(&id);
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
        collection: String,
        filters: Vec<FilterInput>,
        order_by: Option<OrderByInput>,
        limit: Option<usize>,
        offset: Option<usize>,
        projection: Option<Vec<String>>,
        event_name: String,
    ) -> Result<(), String> {
        self.unsubscribe(&listener_id);

        let query_template = QueryInput {
            collection,
            filters,
            order_by,
            limit,
            offset,
            projection,
        };

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
        thread::spawn(move || {
            let emit_snapshot = |win: &Window<R>| -> Result<(), String> {
                let rows = execute_query_input(&db, &query_template)?;
                let payload = SubscriptionPayload {
                    listener_id: listener_id_for_thread.clone(),
                    rows,
                };
                win.emit(&event_name, payload).map_err(|e| e.to_string())
            };

            if emit_snapshot(&window).is_err() {
                subscriptions.lock().remove(&listener_id_for_thread);
                return;
            }

            loop {
                if stop_rx.try_recv().is_ok() {
                    break;
                }

                match rx.recv_timeout(Duration::from_millis(250)) {
                    Ok(_) => {
                        if emit_snapshot(&window).is_err() {
                            break;
                        }
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
    order_by: Option<OrderByInput>,
    limit: Option<usize>,
    offset: Option<usize>,
    projection: Option<Vec<String>>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
struct SubscriptionPayload {
    listener_id: String,
    rows: Vec<serde_json::Value>,
}

#[command]
pub fn firelite_exec<R: Runtime>(
    window: Window<R>,
    state: State<'_, FireLiteGateway>,
    op: FireLiteOp,
) -> Result<FireLiteResponse, String> {
    match op {
        FireLiteOp::Get { collection, doc_id } => {
            let doc = state
                .db
                .get(&collection, &doc_id)
                .map_err(|e| e.to_string())?;
            let data = doc.map(|d| doc_to_json_value(&d)).transpose()?;
            Ok(FireLiteResponse::Document { data })
        }
        FireLiteOp::Set {
            collection,
            doc_id,
            data,
        } => {
            let doc = json_to_doc(&data)?;
            state
                .db
                .put(&collection, &doc_id, &doc)
                .map_err(|e| e.to_string())?;
            Ok(FireLiteResponse::Ok)
        }
        FireLiteOp::Delete { collection, doc_id } => {
            state
                .db
                .delete(&collection, &doc_id)
                .map_err(|e| e.to_string())?;
            Ok(FireLiteResponse::Ok)
        }
        FireLiteOp::CreateIndex { collection, field } => {
            state
                .db
                .create_index(&collection, &field)
                .map_err(|e| e.to_string())?;
            Ok(FireLiteResponse::Ok)
        }
        FireLiteOp::CreateFtsIndex { collection, field } => {
            state
                .db
                .create_fts_index(&collection, &field)
                .map_err(|e| e.to_string())?;
            Ok(FireLiteResponse::Ok)
        }
        FireLiteOp::CreateCompositeIndex { collection, fields } => {
            let parsed_fields = fields
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
            state.db.create_composite_index(&collection, parsed_fields);
            Ok(FireLiteResponse::Ok)
        }
        FireLiteOp::Query {
            collection,
            filters,
            order_by,
            limit,
            offset,
            projection,
        } => {
            let rows = execute_query_input(
                &state.db,
                &QueryInput {
                    collection,
                    filters,
                    order_by,
                    limit,
                    offset,
                    projection,
                },
            )?;
            Ok(FireLiteResponse::QueryResult { rows })
        }
        FireLiteOp::Batch { mutations } => {
            let mut batch = Vec::with_capacity(mutations.len());
            for item in mutations {
                match item.mutation {
                    BatchMutationKind::Set => {
                        let data = item.data.ok_or_else(|| {
                            format!(
                                "set mutation for {}/{} is missing 'data'",
                                item.collection, item.doc_id
                            )
                        })?;
                        batch.push(BatchMutation::Put {
                            collection: item.collection,
                            doc_id: item.doc_id,
                            doc: json_to_doc(&data)?,
                        });
                    }
                    BatchMutationKind::Delete => {
                        batch.push(BatchMutation::Delete {
                            collection: item.collection,
                            doc_id: item.doc_id,
                        });
                    }
                }
            }
            state.db.write_batch(batch).map_err(|e| e.to_string())?;
            Ok(FireLiteResponse::Ok)
        }
        FireLiteOp::Aggregate {
            collection,
            filters,
            kind,
            field,
        } => {
            let mut query = Query::new(&collection);
            for filter in filters {
                query = query.where_filter(
                    &filter.field,
                    map_operator(&filter.op),
                    json_filter_value_to_value(&filter.value)?,
                );
            }

            use crate::query::query::AggregateOp;
            query = match &kind {
                AggregateKind::Count => query.aggregate(AggregateOp::Count),
                AggregateKind::Sum => {
                    let f = field
                        .clone()
                        .ok_or_else(|| "sum aggregate requires 'field'".to_string())?;
                    query.aggregate(AggregateOp::Sum(f))
                }
                AggregateKind::Avg => {
                    let f = field
                        .clone()
                        .ok_or_else(|| "avg aggregate requires 'field'".to_string())?;
                    query.aggregate(AggregateOp::Avg(f))
                }
            };

            let result = state
                .db
                .execute_aggregation(query)
                .map_err(|e| e.to_string())?;
            let value = match &kind {
                AggregateKind::Count => *result.get("count").unwrap_or(&0.0),
                AggregateKind::Sum => {
                    let field = field.clone().unwrap_or_default();
                    *result.get(&format!("sum_{}", field)).unwrap_or(&0.0)
                }
                AggregateKind::Avg => {
                    let field = field.clone().unwrap_or_default();
                    *result.get(&format!("avg_{}", field)).unwrap_or(&0.0)
                }
            };
            Ok(FireLiteResponse::AggregateResult { value })
        }
        FireLiteOp::Subscribe {
            listener_id,
            collection,
            filters,
            order_by,
            limit,
            offset,
            projection,
            event_name,
        } => {
            state.register_subscription(
                window,
                listener_id.clone(),
                collection,
                filters,
                order_by,
                limit,
                offset,
                projection,
                event_name.unwrap_or_else(|| "firelite://snapshot".to_string()),
            )?;
            Ok(FireLiteResponse::SubscriptionAck { listener_id })
        }
        FireLiteOp::Unsubscribe { listener_id } => {
            state.unsubscribe(&listener_id);
            Ok(FireLiteResponse::Unsubscribed { listener_id })
        }
    }
}

fn execute_query_input(
    db: &FireLite,
    input: &QueryInput,
) -> Result<Vec<serde_json::Value>, String> {
    let mut query = Query::new(&input.collection);

    for filter in &input.filters {
        query = query.where_filter(
            &filter.field,
            map_operator(&filter.op),
            json_value_to_value(&filter.value)?,
        );
    }

    if let Some(order) = &input.order_by {
        query = query.order_by(&order.field, order.ascending);
    }

    if let Some(limit) = input.limit {
        query = query.limit(limit);
    }
    if let Some(offset) = input.offset {
        query = query.offset(offset);
    }

    if let Some(projection) = &input.projection {
        if !projection.is_empty() {
            let rows = db
                .query_projected_zero_copy(query.clone(), projection)
                .map_err(|e| e.to_string())?;
            return rows
                .into_iter()
                .map(|(_, fields)| projection_fields_to_json(fields))
                .collect();
        }
    }

    let rows = db.query(query).map_err(|e| e.to_string())?;
    rows.into_iter()
        .map(|(_, doc)| doc_to_json_value(&doc))
        .collect()
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
    let obj = v
        .as_object()
        .ok_or_else(|| "document payload must be a JSON object".to_string())?;

    let mut doc = FireLiteDoc::default();
    for (k, val) in obj {
        doc.insert(k.clone(), json_value_to_value(val)?);
    }
    Ok(doc)
}

fn json_value_to_value(v: &serde_json::Value) -> Result<Value, String> {
    match v {
        serde_json::Value::Null => Ok(Value::Null),
        serde_json::Value::Bool(b) => Ok(Value::Bool(*b)),
        serde_json::Value::Number(n) => {
            if let Some(i) = n.as_i64() {
                Ok(Value::Int(i))
            } else if let Some(f) = n.as_f64() {
                Ok(Value::Float(f))
            } else {
                Err("unsupported number representation".to_string())
            }
        }
        serde_json::Value::String(s) => Ok(Value::String(s.clone())),
        serde_json::Value::Array(arr) => arr
            .iter()
            .map(json_value_to_value)
            .collect::<Result<Vec<_>, _>>()
            .map(Value::Array),
        serde_json::Value::Object(obj) => obj
            .iter()
            .map(|(k, v)| Ok((k.clone(), json_value_to_value(v)?)))
            .collect::<Result<Vec<_>, String>>()
            .map(Value::Map),
    }
}

fn json_filter_value_to_value(v: &serde_json::Value) -> Result<Value, String> {
    match v {
        serde_json::Value::Array(items) => {
            let mut out = Vec::with_capacity(items.len());
            for item in items {
                out.push(json_filter_value_to_value(item)?);
            }
            Ok(Value::Array(out))
        }
        _ => json_value_to_value(v),
    }
}

fn doc_to_json_value(doc: &FireLiteDoc) -> Result<serde_json::Value, String> {
    let mut map = serde_json::Map::new();
    for (k, v) in &doc.fields {
        map.insert(k.clone(), value_to_json(v)?);
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
        Value::Null => Ok(serde_json::Value::Null),
        Value::Bool(b) => Ok(serde_json::Value::Bool(*b)),
        Value::Int(i) => Ok(serde_json::Value::Number((*i).into())),
        Value::Float(f) => serde_json::Number::from_f64(*f)
            .map(serde_json::Value::Number)
            .ok_or_else(|| "invalid float value".to_string()),
        Value::String(s) => Ok(serde_json::Value::String(s.clone())),
        Value::Binary(bytes) => Ok(serde_json::Value::Array(
            bytes
                .iter()
                .map(|b| serde_json::Value::Number((*b as u64).into()))
                .collect(),
        )),
        Value::Reference { collection, doc_id } => {
            let mut map = serde_json::Map::new();
            map.insert(
                "__ref__".to_string(),
                serde_json::Value::String(format!("{collection}/{doc_id}")),
            );
            Ok(serde_json::Value::Object(map))
        }
        Value::Timestamp(micros) => Ok(serde_json::Value::Number((*micros).into())),
        Value::ServerTimestamp => Ok(serde_json::Value::Null),
        Value::Map(fields) => {
            let mut map = serde_json::Map::new();
            for (k, v) in fields {
                map.insert(k.clone(), value_to_json(v)?);
            }
            Ok(serde_json::Value::Object(map))
        }
        Value::Array(values) => Ok(serde_json::Value::Array(
            values
                .iter()
                .map(value_to_json)
                .collect::<Result<Vec<_>, _>>()?,
        )),
    }
}
