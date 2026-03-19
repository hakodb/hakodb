use std::io::Write;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use hashbrown::HashMap; 

use crate::config::{FireLiteConfig, DurabilityMode};
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value; 
use crate::error::{FireLiteError, Result};
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::planner::QueryPlanner;
use crate::query::query::Query;
use crate::storage::engine::{StorageEngine, StorageMutation};

#[derive(Debug, Clone)]
pub enum BatchMutation {
    Put {
        collection: String,
        doc_id: String,
        doc: FireLiteDoc,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
}

#[derive(Debug, Clone)]
pub enum ChangeKind {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub path: String,
    pub kind: ChangeKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AccessOp {
    Get,
    Put,
    Delete,
    Query,
    Batch,
}

#[derive(Debug, Clone)]
pub struct SecurityRule {
    pub collection_prefix: String,
    pub op: AccessOp,
    pub allow: bool,
}

#[derive(Debug, Clone)]
pub struct AuditEntry {
    pub op: AccessOp,
    pub collection: String,
    pub doc_id: Option<String>,
    pub ok: bool,
}

pub struct Transaction {
    mutations: Vec<BatchMutation>,
}

impl Transaction {
    pub fn put(&mut self, collection: &str, doc_id: &str, doc: FireLiteDoc) {
        self.mutations.push(BatchMutation::Put {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            doc,
        });
    }

    pub fn delete(&mut self, collection: &str, doc_id: &str) {
        self.mutations.push(BatchMutation::Delete {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        });
    }

    pub fn put_subdocument(
        &mut self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
        doc: FireLiteDoc,
    ) {
        self.put(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
            doc,
        );
    }

    pub fn commit(self, db: &FireLite) -> Result<()> {
        db.write_batch(self.mutations)
    }
}

pub struct SerializableTransaction {
    reads: HashMap<String, Option<u64>>,
    mutations: Vec<BatchMutation>,
}

impl SerializableTransaction {
    pub fn get(
        &mut self,
        db: &FireLite,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<FireLiteDoc>> {
        let key = doc_key(collection, doc_id);
        let doc = db.get(collection, doc_id)?;
        let version = db.current_version(&key);
        self.reads.insert(key, version);
        Ok(doc)
    }

    pub fn put(&mut self, collection: &str, doc_id: &str, doc: FireLiteDoc) {
        self.mutations.push(BatchMutation::Put {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            doc,
        });
    }

    pub fn delete(&mut self, collection: &str, doc_id: &str) {
        self.mutations.push(BatchMutation::Delete {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        });
    }

    pub fn commit(self, db: &FireLite) -> Result<()> {
        db.commit_serializable(self.reads, self.mutations)
    }
}

// New: Messages for the background index worker
enum IndexOp {
    Update {
        collection: String,
        puts: Vec<(String, FireLiteDoc)>,
        deletes: Vec<(String, FireLiteDoc)>,
    },
}

pub struct FireLite {
    storage: Arc<RwLock<StorageEngine>>,
    indexes: Arc<RwLock<IndexManager>>,
    executor: ParallelQueryExecutor,
    tx_lock: Mutex<()>,
    listeners: Mutex<HashMap<String, Vec<Sender<ChangeEvent>>>>,

    // --- NON-BLOCKING METADATA ---
    doc_versions: RwLock<HashMap<String, u64>>, // Mutex -> RwLock
    global_version: AtomicU64,                   // Mutex<u64> -> AtomicU64
    security_rules: RwLock<Vec<SecurityRule>>,   // Mutex -> RwLock
    
    // --- ASYNC AUDITING ---
    audit_tx: Sender<AuditEntry>,                // Non-blocking channel
    audit_data: Arc<RwLock<Vec<AuditEntry>>>,    // Shared for reading history

    index_tx: Sender<IndexOp>,
    maintenance_stop: Mutex<Option<Sender<()>>>,
    maintenance_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl FireLite {
    pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self> {
        std::fs::create_dir_all(path.as_ref())?;
        
        // 1. Setup Audit File
        let log_path = config.audit_log_path.clone().unwrap_or_else(|| {
            path.as_ref().join("audit.log").to_string_lossy().to_string()
        });
        let mut audit_file = if config.enable_audit_log {
            Some(std::fs::OpenOptions::new().create(true).append(true).open(log_path)?)
        } else {
            None
        };

        let storage = Arc::new(RwLock::new(StorageEngine::open(path, &config)?));
        let indexes = Arc::new(RwLock::new(IndexManager::default()));
        
        // Initialize Background Index Worker
        let (index_tx, index_rx) = channel::<IndexOp>();
        let indexes_for_worker = Arc::clone(&indexes);
        thread::spawn(move || {
            while let Ok(op) = index_rx.recv() {
                match op {
                    IndexOp::Update { collection, puts, deletes } => {
                        let mut idx = indexes_for_worker.write().unwrap();
                        if !puts.is_empty() {
                            // Convert Vec<(String, FireLiteDoc)> to required iterator format
                            let put_refs: Vec<(&str, &FireLiteDoc)> = puts.iter().map(|(id, doc)| (id.as_str(), doc)).collect();
                            idx.index_batch(&collection, put_refs);
                        }
                        if !deletes.is_empty() {
                            let del_refs: Vec<(&str, &FireLiteDoc)> = deletes.iter().map(|(id, doc)| (id.as_str(), doc)).collect();
                            idx.remove_batch(&collection, del_refs);
                        }
                    }
                }
            }
        });

        // 3. Initialize Background Audit Worker (FIXED INITIALIZATION)
        let (audit_tx, audit_rx) = channel::<AuditEntry>();
        let audit_data = Arc::new(RwLock::new(Vec::new()));
        let audit_data_clone = Arc::clone(&audit_data);
        
        thread::spawn(move || {
            while let Ok(entry) = audit_rx.recv() {
                if let Ok(mut history) = audit_data_clone.write() {
                    history.push(entry.clone());
                }
                if let Some(file) = audit_file.as_mut() {
                    let _ = writeln!(file, "op={:?} col={} doc={:?} ok={}", 
                        entry.op, entry.collection, entry.doc_id, entry.ok);
                }
            }
        });

        let db = Self {
            storage,
            indexes,
            executor: ParallelQueryExecutor::new(config.query_workers),
            tx_lock: Mutex::new(()),
            listeners: Mutex::new(HashMap::new()),
            doc_versions: RwLock::new(HashMap::new()),
            global_version: AtomicU64::new(1),
            security_rules: RwLock::new(Vec::new()),
            audit_tx,
            audit_data,
            index_tx,
            maintenance_stop: Mutex::new(None),
            maintenance_handle: Mutex::new(None),
        };

        db.rebuild_indexes_from_storage()?;

        // 4. Background Maintenance Thread
        let (stop_tx, stop_rx) = channel::<()>();
        let storage_bg = Arc::clone(&db.storage);
        let handle = thread::spawn(move || loop {
            if stop_rx.try_recv().is_ok() { break; }
            if let Ok(mut storage) = storage_bg.write() { // MUST BE .write()
                let _ = storage.run_background_maintenance();
            }
            thread::sleep(Duration::from_millis(500));
        });
        
        *db.maintenance_stop.lock().unwrap() = Some(stop_tx);
        *db.maintenance_handle.lock().unwrap() = Some(handle);

        Ok(db)
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction {
            mutations: Vec::new(),
        }
    }

    pub fn begin_serializable_transaction(&self) -> SerializableTransaction {
        SerializableTransaction {
            reads: HashMap::new(),
            mutations: Vec::new(),
        }
    }

    pub fn set_security_rules(&self, rules: Vec<SecurityRule>) {
        *self.security_rules.write().unwrap() = rules;
    }

    pub fn audit_entries(&self) -> Vec<AuditEntry> {
        // self.audit.lock().expect("audit lock poisoned").clone()
        self.audit_data.read().unwrap().clone()
    }

    pub fn create_composite_index(
        &self,
        collection: &str,
        fields: Vec<(String, SortDirection)>,
    ) -> u32 {
        self.indexes
            .write()
            .unwrap()
            .create_index(CompositeIndexDefinition::new(collection).with_fields(fields))
    }

    pub fn watch_collection(&self, collection: &str) -> Receiver<ChangeEvent> {
        let (tx, rx) = channel();
        self.listeners
            .lock()
            .expect("listeners lock poisoned")
            .entry(collection.to_string())
            .or_default()
            .push(tx);
        rx
    }

    fn notify_watchers(&self, collection: &str, event: ChangeEvent) {
        if let Some(list) = self
            .listeners
            .lock()
            .expect("listeners lock poisoned")
            .get_mut(collection)
        {
            list.retain(|sender| sender.send(event.clone()).is_ok());
        }
    }

    fn allowed(&self, collection: &str, op: AccessOp) -> bool {
        let rules = self.security_rules.read().unwrap(); // Read lock
        // let rules = self.security_rules.lock().expect("rules lock poisoned");
        if rules.is_empty() {
            return true;
        }
        let mut decision = true;
        for rule in rules
            .iter()
            .filter(|r| op == r.op && collection.starts_with(&r.collection_prefix))
        {
            decision = rule.allow;
        }
        decision
    }

    fn record_audit(&self, entry: AuditEntry) {
        let _ = self.audit_tx.send(entry);
    }

    fn current_version(&self, key: &str) -> Option<u64> {
        self.doc_versions.read().unwrap().get(key).cloned()
    }

    fn bump_versions_for_mutations(&self, mutations: &[BatchMutation]) {
        let mut versions = self.doc_versions.write().unwrap();
        for m in mutations {
            let key = match m {
                BatchMutation::Put { collection, doc_id, .. } => doc_key(collection, doc_id),
                BatchMutation::Delete { collection, doc_id } => doc_key(collection, doc_id),
            };
            // Atomic increment (No Mutex!)
            let new_v = self.global_version.fetch_add(1, Ordering::SeqCst);
            versions.insert(key, new_v);
        }
    }

    fn rebuild_indexes_from_storage(&self) -> Result<()> {
        let entries = self.storage.read().unwrap().scan_prefix("")?;

        let mut indexes = self.indexes.write().unwrap();
        let mut versions = self.doc_versions.write().unwrap(); // FIXED: Mutex -> RwLock
        
        for (key, bytes) in entries {
            if let Some((collection, doc_id)) = key.split_once(':') {
                if let Some(doc) = FireLiteDoc::decode(&bytes) {
                    indexes.index_document(collection, doc_id, &doc);
                    // FIXED: global_version is AtomicU64
                    let gv = self.global_version.fetch_add(1, Ordering::SeqCst);
                    versions.insert(key, gv);
                }
            }
        }
        Ok(())
    }

    fn commit_serializable(
        &self,
        reads: HashMap<String, Option<u64>>,
        mutations: Vec<BatchMutation>,
    ) -> Result<()> {
        let _tx_guard = self.tx_lock.lock().expect("transaction lock poisoned");

        for (key, expected) in reads {
            let actual = self.current_version(&key);
            if actual != expected {
                return Err(FireLiteError::Corrupt(format!(
                    "serializable transaction conflict on key '{}'",
                    key
                )));
            }
        }

        self.write_batch_internal(mutations)
    }

    fn write_batch_internal(&self, mut mutations: Vec<BatchMutation>) -> Result<()> {
        let mut storage_mutations = Vec::with_capacity(mutations.len());
        let mut change_events = Vec::new();
        
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;
        
        for m in &mut mutations {
            if let BatchMutation::Put { doc, .. } = m {
                // Look for ServerTimestamp placeholders and replace with real time
                for (_, val) in &mut doc.fields {
                    if matches!(val, Value::ServerTimestamp) {
                        *val = Value::Timestamp(now);
                    }
                }
            }
        }

        // Group for background indexing
        let mut puts_by_col: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
        let mut dels_by_col: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
        
        {
            // let mut storage = self.storage.lock().expect("storage lock poisoned");
            let mut storage = self.storage.write().unwrap();
    
            for mutation in &mutations {
                match mutation {
                    BatchMutation::Put { collection, doc_id, doc } => {
                        let key = doc_key(collection, doc_id);
                        storage_mutations.push(StorageMutation::Put {
                            key: key.clone(),
                            value: doc.encode(),
                        });
                        
                        puts_by_col.entry(collection.clone()).or_default()
                            .push((doc_id.clone(), doc.clone()));
    
                        change_events.push((collection.clone(), ChangeEvent { path: key, kind: ChangeKind::Put }));
                    }
    
                    BatchMutation::Delete { collection, doc_id } => {
                        let key = doc_key(collection, doc_id);
                        if let Some(bytes) = storage.get(&key)? {
                            if let Some(old_doc) = FireLiteDoc::decode(&bytes) {
                                dels_by_col.entry(collection.clone()).or_default()
                                    .push((doc_id.clone(), old_doc));
                            }
                        }
                        storage_mutations.push(StorageMutation::Delete { key: key.clone() });
                        change_events.push((collection.clone(), ChangeEvent { path: key, kind: ChangeKind::Delete }));
                    }
                }
            }
            storage.apply_batch(&storage_mutations)?;
        }

        // ASYNC INDEXING: Offload to worker thread
        for (collection, puts) in puts_by_col {
            let deletes = dels_by_col.remove(&collection).unwrap_or_default();
            let _ = self.index_tx.send(IndexOp::Update { collection, puts, deletes });
        }
        // Handle remaining deletes that didn't have associated puts in the same collection
        for (collection, deletes) in dels_by_col {
            let _ = self.index_tx.send(IndexOp::Update { collection, puts: Vec::new(), deletes });
        }
    
        self.bump_versions_for_mutations(&mutations);
        for (collection, event) in change_events {
            self.notify_watchers(&collection, event);
        }
    
        Ok(())
    }

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        let _tx_guard = self.tx_lock.lock().expect("transaction lock poisoned");
        if !mutations.iter().all(|m| {
            let collection = match m {
                BatchMutation::Put { collection, .. } => collection,
                BatchMutation::Delete { collection, .. } => collection,
            };
            self.allowed(collection, AccessOp::Batch)
        }) {
            self.record_audit(AuditEntry {
                op: AccessOp::Batch,
                collection: "<batch>".to_string(),
                doc_id: None,
                ok: false,
            });
            return Err(FireLiteError::Corrupt(
                "security policy denied batch".to_string(),
            ));
        }

        let result = self.write_batch_internal(mutations);
        self.record_audit(AuditEntry {
            op: AccessOp::Batch,
            collection: "<batch>".to_string(),
            doc_id: None,
            ok: result.is_ok(),
        });
        result
    }

    pub fn put(&self, collection: &str, doc_id: &str, doc: &FireLiteDoc) -> Result<()> {
        if !self.allowed(collection, AccessOp::Put) {
            self.record_audit(AuditEntry {
                op: AccessOp::Put,
                collection: collection.to_string(),
                doc_id: Some(doc_id.to_string()),
                ok: false,
            });
            return Err(FireLiteError::Corrupt(
                "security policy denied put".to_string(),
            ));
        }

        let result = self.write_batch(vec![BatchMutation::Put {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            doc: doc.clone(),
        }]);

        self.record_audit(AuditEntry {
            op: AccessOp::Put,
            collection: collection.to_string(),
            doc_id: Some(doc_id.to_string()),
            ok: result.is_ok(),
        });
        result
    }

    pub fn put_subdocument(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
        doc: &FireLiteDoc,
    ) -> Result<()> {
        self.put(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
            doc,
        )
    }

    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        if !self.allowed(collection, AccessOp::Get) {
            self.record_audit(AuditEntry {
                op: AccessOp::Get,
                collection: collection.to_string(),
                doc_id: Some(doc_id.to_string()),
                ok: false,
            });
            return Err(FireLiteError::Corrupt(
                "security policy denied get".to_string(),
            ));
        }

        let key = doc_key(collection, doc_id);
        let res = self
            .storage
            // .lock()
            // .expect("storage lock poisoned")
            .read()
            .unwrap()
            .get(&key)?
            .and_then(|v| FireLiteDoc::decode(&v));

        self.record_audit(AuditEntry {
            op: AccessOp::Get,
            collection: collection.to_string(),
            doc_id: Some(doc_id.to_string()),
            ok: true,
        });

        Ok(res)
    }

    pub fn get_subdocument(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
    ) -> Result<Option<FireLiteDoc>> {
        self.get(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
        )
    }

    pub fn delete(&self, collection: &str, doc_id: &str) -> Result<()> {
        if !self.allowed(collection, AccessOp::Delete) {
            self.record_audit(AuditEntry {
                op: AccessOp::Delete,
                collection: collection.to_string(),
                doc_id: Some(doc_id.to_string()),
                ok: false,
            });
            return Err(FireLiteError::Corrupt(
                "security policy denied delete".to_string(),
            ));
        }

        let result = self.write_batch(vec![BatchMutation::Delete {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        }]);

        self.record_audit(AuditEntry {
            op: AccessOp::Delete,
            collection: collection.to_string(),
            doc_id: Some(doc_id.to_string()),
            ok: result.is_ok(),
        });
        result
    }

    pub fn delete_subdocument(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
    ) -> Result<()> {
        self.delete(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
        )
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(FireLiteError::Corrupt(
                "security policy denied query".to_string(),
            ));
        }

        let result = {
            let (plan, _collection_rows) = {
                let storage = self.storage.read().unwrap();
                let indexes = self.indexes.read().unwrap();
                let rows = storage.count_prefix(&format!("{}:", query.collection));
                (QueryPlanner::plan(&query, &indexes, rows), rows)
            };

            let storage = self.storage.read().unwrap();
            let indexes = self.indexes.read().unwrap();
            
            // Execute and store the result
            self.executor.execute(&storage, &indexes, plan)
        };

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: result.is_ok(),
        });

        result
    }

    pub fn query_projected_zero_copy(
        &self,
        query: Query,
        fields: &[String],
    ) -> Result<Vec<(String, Vec<(String, crate::document::value::Value)>)>> {
        // FIXED: Changed .write() to .read() for better concurrency
        let storage = self.storage.read().unwrap();
        let docs = storage.scan_prefix(&format!("{}:", query.collection))?;
        let mut out = Vec::new();

        for (id, raw) in docs {
            if matches_filters_borrowed(&raw, &query.filters) {
                let projected = project_fields_borrowed(&raw, fields);
                let order_value = query
                    .order_by
                    .as_ref()
                    .and_then(|order| extract_field_value_borrowed(&raw, &order.field));
                out.push((id, projected, order_value));
            }
        }

        if let Some(order) = &query.order_by {
            out.sort_by(|(_, _, av), (_, _, bv)| format!("{:?}", av).cmp(&format!("{:?}", bv)));
            if !order.ascending {
                out.reverse();
            }
        }

        if let Some(limit) = query.limit {
            out.truncate(limit);
        }

        Ok(out
            .into_iter()
            .map(|(id, projected, _)| (id, projected))
            .collect())
    }

    pub fn query_subcollection(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        self.query(Query::new(&subcollection_prefix(
            collection,
            doc_id,
            subcollection,
        )))
    }

    pub fn compact(&self) -> Result<()> {
        self.storage
            .write()
            .unwrap()
            .compact()
    }

    pub fn flush(&self) -> Result<()> {
        self.storage.write().unwrap().flush_all()
    }

    pub fn set_durability_mode(&self, mode: DurabilityMode) {
        let mut storage = self.storage.write().unwrap();
        storage.set_durability_mode(mode);
    }

    pub fn execute_aggregation(&self, query: Query) -> Result<HashMap<String, f64>> {
        let storage = self.storage.read().map_err(|_| FireLiteError::Corrupt("storage lock poisoned".into()))?;
        let indexes = self.indexes.read().map_err(|_| FireLiteError::Corrupt("indexes lock poisoned".into()))?;
        
        let rows = storage.count_prefix(&format!("{}:", query.collection));
        let plan = QueryPlanner::plan(&query, &indexes, rows);
        
        // This now returns hashbrown::HashMap, matching the updated signature
        self.executor.execute_aggregation(&storage, &indexes, plan, &query.aggregations)
    }

        /// Online Backup: Flushes RAM to disk and copies files to a new location
    pub fn backup(&self, destination_path: impl AsRef<Path>) -> Result<()> {
        let mut storage = self.storage.write().unwrap();
        storage.checkpoint_inlined_data()?;
        storage.flush_all()?;

        std::fs::create_dir_all(destination_path.as_ref())?;

        // FIX: Use the new public getter instead of private field
        // let base = storage.base_dir().to_path_buf(); 
        for entry in std::fs::read_dir(storage.base_dir())? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();
            
            // Only backup data and logs, ignore audit.log (usually managed separately)
            if name_str.ends_with(".dat") || name_str == "wal.log" {
                let dest = destination_path.as_ref().join(file_name);
                std::fs::copy(entry.path(), dest)?;
            }
        }
        Ok(())
    }

    pub fn list_collections(&self) -> Result<Vec<String>> {
        // No more scanning the whole index! 
        // Just read the pre-calculated map.
        let storage = self.storage.read().unwrap();
        storage.list_collections()
    }

    pub fn get_stats(&self) -> HashMap<String, usize> {
        let storage = self.storage.read().unwrap();
        storage.collection_counts.clone()
    }

    pub fn patch(&self, collection: &str, doc_id: &str, updates: Vec<(String, Value)>) -> Result<()> {
        // 1. Read existing
        let mut doc = self.get(collection, doc_id)?
            .ok_or_else(|| FireLiteError::Corrupt("Document not found".into()))?;

        // 2. Apply updates
        for (k, v) in updates {
            doc.insert(k, v);
        }

        // 3. Write back
        self.put(collection, doc_id, &doc)
    }

    pub fn get_by_reference(&self, reference: &Value) -> Result<Option<FireLiteDoc>> {
        match reference {
            Value::Reference { collection, doc_id } => {
                // Reuse the existing security-checked 'get' method
                self.get(collection, doc_id)
            }
            _ => Err(FireLiteError::Corrupt("Provided value is not a Document Reference".into())),
        }
    }
}

fn doc_key(collection: &str, doc_id: &str) -> String {
    format!("{}:{}", collection, doc_id)
}

fn subcollection_prefix(collection: &str, doc_id: &str, subcollection: &str) -> String {
    format!("{}:{}/{}", collection, doc_id, subcollection)
}

fn matches_filters_borrowed(raw: &[u8], filters: &[crate::query::filter::Filter]) -> bool {
    if filters.is_empty() {
        return true;
    }

    let Some(view) = crate::document::firelite_doc::FireLiteDocView::new(raw) else {
        return false;
    };

    filters.iter().all(|f| {
        let mut matched = None;
        for (k, v) in view.iter() {
            if k == f.field {
                matched = v.to_owned_value();
                break;
            }
        }

        matched
            .as_ref()
            .map(|v| crate::query::filter::compare_values(v, &f.op, &f.value))
            .unwrap_or(false)
    })
}

fn extract_field_value_borrowed(raw: &[u8], field: &str) -> Option<crate::document::value::Value> {
    let view = crate::document::firelite_doc::FireLiteDocView::new(raw)?;
    for (k, v) in view.iter() {
        if k == field {
            return v.to_owned_value();
        }
    }
    None
}

fn project_fields_borrowed(
    raw: &[u8],
    fields: &[String],
) -> Vec<(String, crate::document::value::Value)> {
    let mut out = Vec::new();
    let Some(view) = crate::document::firelite_doc::FireLiteDocView::new(raw) else {
        return out;
    };

    for (k, v) in view.iter() {
        if fields.is_empty() || fields.iter().any(|f| f == k) {
            if let Some(value) = v.to_owned_value() {
                out.push((k.to_string(), value));
            }
        }
    }

    out
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::document::value::Value;

    use super::{AccessOp, BatchMutation, ChangeKind, FireLite, SecurityRule};
    use crate::config::FireLiteConfig;
    use crate::document::firelite_doc::FireLiteDoc;
    use crate::query::query::Query;

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn projected_query_applies_order_before_limit() {
        let path = temp_path("firelite-projected-order");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");

        for (id, score) in [("a", 3), ("b", 1), ("c", 2)] {
            let mut doc = FireLiteDoc::default();
            doc.insert("score", Value::Int(score));
            doc.insert("name", Value::String(id.to_string()));
            db.put("users", id, &doc).expect("put should succeed");
        }

        let query = Query::new("users").order_by("score", true).limit(2);
        let fields = vec!["name".to_string()];
        let rows = db
            .query_projected_zero_copy(query, &fields)
            .expect("projected query should succeed");

        assert_eq!(rows.len(), 2);
        assert!(rows[0].0.ends_with(":b"));
        assert!(rows[1].0.ends_with(":c"));

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }

    #[test]
    fn write_batch_commits_multiple_mutations() {
        let path = temp_path("firelite-batch");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");

        let mut user1 = FireLiteDoc::default();
        user1.insert("name", Value::String("alice".into()));

        let mut user2 = FireLiteDoc::default();
        user2.insert("name", Value::String("bob".into()));

        db.write_batch(vec![
            BatchMutation::Put {
                collection: "users".into(),
                doc_id: "1".into(),
                doc: user1,
            },
            BatchMutation::Put {
                collection: "users".into(),
                doc_id: "2".into(),
                doc: user2,
            },
        ])
        .expect("batch should succeed");

        assert!(db.get("users", "1").expect("get should succeed").is_some());
        assert!(db.get("users", "2").expect("get should succeed").is_some());

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }

    #[test]
    fn watch_stream_receives_changes() {
        let path = temp_path("firelite-watch");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");
        let rx = db.watch_collection("users");

        let mut user = FireLiteDoc::default();
        user.insert("name", Value::String("eve".into()));
        db.put("users", "7", &user).expect("put should succeed");

        let event = rx.recv().expect("watch should receive event");
        assert!(event.path.contains("users:7"));
        assert!(matches!(event.kind, ChangeKind::Put));

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }

    #[test]
    fn serializable_transaction_detects_conflict() {
        let path = temp_path("firelite-tx");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");

        let mut doc = FireLiteDoc::default();
        doc.insert("v", Value::Int(1));
        db.put("users", "1", &doc).expect("seed put");

        let mut tx = db.begin_serializable_transaction();
        let _ = tx.get(&db, "users", "1").expect("tx read");

        let mut outside = FireLiteDoc::default();
        outside.insert("v", Value::Int(2));
        db.put("users", "1", &outside).expect("outside write");

        let mut next = FireLiteDoc::default();
        next.insert("v", Value::Int(3));
        tx.put("users", "1", next);
        assert!(tx.commit(&db).is_err());

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }

    #[test]
    fn security_rule_can_deny_operation() {
        let path = temp_path("firelite-security");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");

        db.set_security_rules(vec![SecurityRule {
            collection_prefix: "users".to_string(),
            op: AccessOp::Delete,
            allow: false,
        }]);

        assert!(db.delete("users", "x").is_err());
        assert!(!db.audit_entries().is_empty());

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }
}

impl Drop for FireLite {
    fn drop(&mut self) {
        if let Some(tx) = self
            .maintenance_stop
            .lock()
            .expect("maintenance stop lock poisoned")
            .take()
        {
            let _ = tx.send(());
        }
        if let Some(handle) = self
            .maintenance_handle
            .lock()
            .expect("maintenance handle lock poisoned")
            .take()
        {
            let _ = handle.join();
        }
    }
}
