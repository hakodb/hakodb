use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use hashbrown::HashMap; 

use crate::config::FireLiteConfig;
use crate::document::firelite_doc::{FireLiteDoc, FireLiteDocView};
use crate::document::value::Value; 
use crate::error::{FireLiteError, Result};
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::planner::QueryPlanner;
use crate::query::query::Query;
use crate::storage::engine::{StorageEngine, StorageMutation};

// --- Data Types ---

#[derive(Debug, Clone)]
pub enum BatchMutation {
    Put { collection: String, doc_id: String, doc: FireLiteDoc },
    Delete { collection: String, doc_id: String },
}

#[derive(Debug, Clone)]
pub enum ChangeKind { Put, Delete }

#[derive(Debug, Clone)]
pub struct ChangeEvent { pub path: String, pub kind: ChangeKind }

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum AccessOp { Get, Put, Delete, Query, Batch }

#[derive(Debug, Clone)]
pub struct SecurityRule { pub collection_prefix: String, pub op: AccessOp, pub allow: bool }

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry { pub op: AccessOp, pub collection: String, pub doc_id: Option<String>, pub ok: bool }

pub struct Transaction { pub mutations: Vec<BatchMutation> }

impl Transaction {
    pub fn put(&mut self, collection: &str, doc_id: &str, doc: FireLiteDoc) {
        self.mutations.push(BatchMutation::Put { collection: collection.to_string(), doc_id: doc_id.to_string(), doc });
    }
    pub fn delete(&mut self, collection: &str, doc_id: &str) {
        self.mutations.push(BatchMutation::Delete { collection: collection.to_string(), doc_id: doc_id.to_string() });
    }
    pub fn commit(self, db: &FireLite) -> Result<()> { db.write_batch(self.mutations) }
}

pub struct SerializableTransaction {
    pub reads: HashMap<String, Option<u64>>,
    pub mutations: Vec<BatchMutation>,
}

impl SerializableTransaction {
    pub fn get(&mut self, db: &FireLite, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        let key = doc_key(collection, doc_id);
        let doc = db.get(collection, doc_id)?;
        let version = db.current_version(&key);
        self.reads.insert(key, version);
        Ok(doc)
    }
    pub fn put(&mut self, collection: &str, doc_id: &str, doc: FireLiteDoc) {
        self.mutations.push(BatchMutation::Put { collection: collection.to_string(), doc_id: doc_id.to_string(), doc });
    }
    pub fn delete(&mut self, collection: &str, doc_id: &str) {
        self.mutations.push(BatchMutation::Delete { collection: collection.to_string(), doc_id: doc_id.to_string() });
    }
    pub fn commit(self, db: &FireLite) -> Result<()> { db.commit_serializable(self.reads.clone(), self.mutations.clone()) }
}

enum IndexOp {
    Update { collection: String, puts: Vec<(String, FireLiteDoc)>, deletes: Vec<(String, FireLiteDoc)> },
}

pub struct FireLite {
    root_path: PathBuf,
    config: FireLiteConfig,
    shards: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>>, // The only storage
    
    indexes: Arc<RwLock<IndexManager>>,
    executor: ParallelQueryExecutor,
    tx_lock: Mutex<()>,
    listeners: Mutex<HashMap<String, Vec<Sender<ChangeEvent>>>>,
    doc_versions: RwLock<HashMap<String, u64>>, 
    global_version: AtomicU64,
    security_rules: RwLock<Vec<SecurityRule>>,
    audit_tx: Sender<AuditEntry>,
    audit_data: Arc<RwLock<Vec<AuditEntry>>>,
    index_tx: Sender<IndexOp>,
    maintenance_stop: Mutex<Option<Sender<()>>>,
    maintenance_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl FireLite {
    pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_path)?;
        
        let indexes = Arc::new(RwLock::new(IndexManager::default()));
        let (index_tx, index_rx) = channel::<IndexOp>();
        let (audit_tx, audit_rx) = channel::<AuditEntry>();
        let audit_data = Arc::new(RwLock::new(Vec::new()));

        // 1. Audit Worker Implementation
        let log_path = config.audit_log_path.clone().unwrap_or_else(|| root_path.join("audit.log").to_string_lossy().to_string());
        let mut audit_file = if config.enable_audit_log {
            Some(std::fs::OpenOptions::new().create(true).append(true).open(log_path)?)
        } else { None };

        let audit_data_clone = Arc::clone(&audit_data);
        thread::spawn(move || {
            while let Ok(entry) = audit_rx.recv() {
                if let Ok(mut history) = audit_data_clone.write() { history.push(entry.clone()); }
                if let Some(file) = audit_file.as_mut() {
                    let _ = writeln!(file, "[{:?}] op={:?} col={} doc={:?} ok={}", 
                        SystemTime::now(), entry.op, entry.collection, entry.doc_id.as_deref().unwrap_or("<none>"), entry.ok);
                }
            }
        });

        // 2. Index Worker Implementation
        let idx_clone = Arc::clone(&indexes);
        thread::spawn(move || {
            while let Ok(op) = index_rx.recv() {
                match op {
                    IndexOp::Update { collection, puts, deletes } => {
                        let mut mgr = idx_clone.write().unwrap();
                        let put_refs: Vec<(&str, &FireLiteDoc)> = puts.iter().map(|(id, d)| (id.as_str(), d)).collect();
                        mgr.index_batch(&collection, put_refs);
                        let del_refs: Vec<(&str, &FireLiteDoc)> = deletes.iter().map(|(id, d)| (id.as_str(), d)).collect();
                        mgr.remove_batch(&collection, del_refs);
                    }
                }
            }
        });

        let db = Self {
            root_path,
            config: config.clone(),
            shards: Arc::new(RwLock::new(HashMap::new())),
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

        db.recover_existing_shards()?;
        db.rebuild_indexes_from_shards()?;

        // 3. Maintenance Thread
        let (stop_tx, stop_rx) = channel::<()>();
        let shards_ptr = Arc::clone(&db.shards);
        let handle = thread::spawn(move || loop {
            if stop_rx.try_recv().is_ok() { break; }
            let active_shards: Vec<Arc<RwLock<StorageEngine>>> = shards_ptr.read().unwrap().values().cloned().collect();
            for s in active_shards { if let Ok(mut storage) = s.write() { let _ = storage.run_background_maintenance(); } }
            thread::sleep(Duration::from_secs(5));
        });
        
        *db.maintenance_stop.lock().unwrap() = Some(stop_tx);
        *db.maintenance_handle.lock().unwrap() = Some(handle);

        Ok(db)
    }

    fn recover_existing_shards(&self) -> Result<()> {
        let mut shards = self.shards.write().unwrap();
        if !self.root_path.exists() { return Ok(()); }
        for entry in std::fs::read_dir(&self.root_path)? {
            let entry = entry?;
            if entry.path().is_dir() {
                let col = entry.file_name().to_string_lossy().to_string();
                shards.insert(col, Arc::new(RwLock::new(StorageEngine::open(entry.path(), &self.config)?)));
            }
        }
        Ok(())
    }

    fn rebuild_indexes_from_shards(&self) -> Result<()> {
        let shards = self.shards.read().unwrap();
        let mut indexes = self.indexes.write().unwrap();
        let mut versions = self.doc_versions.write().unwrap();
        for (col, shard) in shards.iter() {
            let storage = shard.read().unwrap();
            for (key, bytes) in storage.scan_prefix("")? {
                if let Some((_, doc_id)) = key.split_once(':') {
                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                        indexes.index_document(col, doc_id, &doc);
                        versions.insert(key, self.global_version.fetch_add(1, Ordering::SeqCst));
                    }
                }
            }
        }
        Ok(())
    }

    fn get_shard(&self, collection: &str) -> Arc<RwLock<StorageEngine>> {
        if let Some(s) = self.shards.read().unwrap().get(collection) { return Arc::clone(s); }
        let mut shards = self.shards.write().unwrap();
        shards.entry(collection.to_string()).or_insert_with(|| {
            let path = self.root_path.join(collection);
            Arc::new(RwLock::new(StorageEngine::open(path, &self.config).expect("Shard fail")))
        }).clone()
    }

    // /// MASTER WRITE PATH: The public gateway that checks security and audit.
    // pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
    //     // Acquire lock once at the entry point
    //     let _guard = self.tx_lock.lock().unwrap();
        
    //     // Security check
    //     if !mutations.iter().all(|m| self.allowed(self.get_col(m), AccessOp::Batch)) {
    //         self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: false });
    //         return Err(FireLiteError::Corrupt("Denied".into()));
    //     }

    //     // Call the internal implementation that DOES NOT lock
    //     let res = self.write_batch_internal(mutations);
        
    //     self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: res.is_ok() });
    //     res
    // }

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        // 1. SECURITY & AUDIT (Read-only check, no lock needed)
        if !mutations.iter().all(|m| self.allowed(self.get_col(m), AccessOp::Batch)) {
            self.record_audit(AuditEntry { 
                op: AccessOp::Batch, 
                collection: "<sharded>".into(), 
                doc_id: None, 
                ok: false 
            });
            return Err(FireLiteError::Corrupt("Security Denied".into()));
        }

        // 2. Internal logic (Handles shard-specific locking and deterministic ordering)
        let res = self.write_batch_internal(mutations);
        
        // 3. Async Logging (Non-blocking)
        self.record_audit(AuditEntry { 
            op: AccessOp::Batch, 
            collection: "<sharded>".into(), 
            doc_id: None, 
            ok: res.is_ok() 
        });
        
        res
    }

    pub fn commit_serializable(&self, reads: HashMap<String, Option<u64>>, mutations: Vec<BatchMutation>) -> Result<()> {
        let _guard = self.tx_lock.lock().unwrap(); 

        for (key, expected) in reads { 
            if self.current_version(&key) != expected { 
                return Err(FireLiteError::Corrupt("Transaction Conflict".into())); 
            } 
        }
        
        // Delegate to the now-unlocked internal logic
        self.write_batch_internal(mutations)
    }

    /// INTERNAL LOGIC: Handles Sharding, Durability, and Async Indexing.
    fn write_batch_internal(&self, mut mutations: Vec<BatchMutation>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        
        let mut shard_groups: HashMap<String, Vec<StorageMutation>> = HashMap::new();
        let mut index_puts: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
        let mut index_dels: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
        let mut change_events: Vec<(String, ChangeEvent)> = Vec::new();

        for m in &mut mutations {
            match m {
                BatchMutation::Put { collection, doc_id, doc } => {
                    for (_, val) in &mut doc.fields { 
                        if matches!(val, Value::ServerTimestamp) { *val = Value::Timestamp(now); } 
                    }
                    let key = doc_key(collection, doc_id);
                    shard_groups.entry(collection.clone()).or_default().push(StorageMutation::Put {
                        key: key.clone(),
                        value: doc.encode(),
                    });
                    index_puts.entry(collection.clone()).or_default().push((doc_id.clone(), doc.clone()));
                    change_events.push((collection.clone(), ChangeEvent { path: key, kind: ChangeKind::Put }));
                }
                BatchMutation::Delete { collection, doc_id } => {
                    let key = doc_key(collection, doc_id);
                    let shard = self.get_shard(collection);
                    // Scope the read lock so it drops immediately
                    if let Some(bytes) = shard.read().unwrap().get(&key)? {
                        if let Some(old_doc) = FireLiteDoc::decode(&bytes) {
                            index_dels.entry(collection.clone()).or_default().push((doc_id.clone(), old_doc));
                        }
                    }
                    shard_groups.entry(collection.clone()).or_default().push(StorageMutation::Delete { key: key.clone() });
                    change_events.push((collection.clone(), ChangeEvent { path: key, kind: ChangeKind::Delete }));
                }
            }
        }

        // 1. APPLY TO SHARDS (DURABILITY)
        // for (col_name, ops) in shard_groups {
        //     let shard = self.get_shard(&col_name);
        //     shard.write().unwrap().apply_batch(&ops)?; 
        // }

        // 2. DETERMINISTIC LOCKING: Prevents Deadlocks between threads
        let mut sorted_shards: Vec<_> = shard_groups.keys().cloned().collect();
        sorted_shards.sort(); // Always lock in alphabetical order

        // 3. EXECUTION: Write to each shard folder
        for col_name in sorted_shards {
            if let Some(ops) = shard_groups.get(&col_name) {
                let shard = self.get_shard(&col_name);
                // Lock ONLY this shard. Thread B can simultaneously lock a different shard!
                let mut storage = shard.write().unwrap();
                storage.apply_batch(ops)?; 
            }
        }

        // 2. METADATA & ASYNC TASKS
        self.bump_versions_for_mutations(&mutations);
        
        for (col, puts) in index_puts {
            let deletes = index_dels.remove(&col).unwrap_or_default();
            let _ = self.index_tx.send(IndexOp::Update { collection: col, puts, deletes });
        }
        for (col, deletes) in index_dels {
            let _ = self.index_tx.send(IndexOp::Update { collection: col, puts: vec![], deletes });
        }
        for (col, event) in change_events {
            self.notify_watchers(&col, event);
        }
        Ok(())
    }

    // fn write_batch_internal(&self, mut mutations: Vec<BatchMutation>) -> Result<()> {
    //     let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        
    //     let mut storage_groups: HashMap<String, Vec<StorageMutation>> = HashMap::new();
    //     let mut index_puts: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
    //     let mut index_dels: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
    //     let mut change_events: Vec<(String, ChangeEvent)> = Vec::new();

    //     for m in &mut mutations {
    //         match m {
    //             BatchMutation::Put { collection, doc_id, doc } => {
    //                 for (_, val) in &mut doc.fields { 
    //                     if matches!(val, Value::ServerTimestamp) { *val = Value::Timestamp(now); } 
    //                 }
    //                 let key = doc_key(collection, doc_id);
    //                 storage_groups.entry(collection.clone()).or_default().push(StorageMutation::Put {
    //                     key: key.clone(),
    //                     value: doc.encode(),
    //                 });
    //                 index_puts.entry(collection.clone()).or_default().push((doc_id.clone(), doc.clone()));
    //                 change_events.push((collection.clone(), ChangeEvent { path: key, kind: ChangeKind::Put }));
    //             }
    //             BatchMutation::Delete { collection, doc_id } => {
    //                 let key = doc_key(collection, doc_id);
    //                 let shard = self.get_shard(collection); // FIX: Look in specific shard
    //                 if let Some(bytes) = shard.read().unwrap().get(&key)? {
    //                     if let Some(old_doc) = FireLiteDoc::decode(&bytes) {
    //                         index_dels.entry(collection.clone()).or_default().push((doc_id.clone(), old_doc));
    //                     }
    //                 }
    //                 storage_groups.entry(collection.clone()).or_default().push(StorageMutation::Delete { key: key.clone() });
    //                 change_events.push((collection.clone(), ChangeEvent { path: key, kind: ChangeKind::Delete }));
    //             }
    //         }
    //     }

    //     // Apply to physical shards
    //     for (col_name, ops) in storage_groups {
    //         let shard = self.get_shard(&col_name);
    //         shard.write().unwrap().apply_batch(&ops)?; // Targets collection folder
    //     }

    //     // Background workers and metadata updates...
    //     self.bump_versions_for_mutations(&mutations);
    //     // ... (Send to index_tx and notify_watchers)
    //     Ok(())
    // }

    pub fn put(&self, col: &str, id: &str, doc: &FireLiteDoc) -> Result<()> {
        self.write_batch(vec![BatchMutation::Put { collection: col.into(), doc_id: id.into(), doc: doc.clone() }])
    }

    // pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
    //     if !self.allowed(collection, AccessOp::Get) { return Err(FireLiteError::Corrupt("Denied".into())); }
    //     let shard = self.get_shard(collection);
    //     let key = doc_key(collection, doc_id);
    //     let bytes = shard.read().unwrap().get(&key)?;
    //     Ok(bytes.and_then(|b| FireLiteDoc::decode(&b)))
    // }
    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        if !self.allowed(collection, AccessOp::Get) { 
            self.record_audit(AuditEntry { op: AccessOp::Get, collection: collection.into(), doc_id: Some(doc_id.into()), ok: false });
            return Err(FireLiteError::Corrupt("Denied".into())); 
        }
        
        let shard = self.get_shard(collection);
        let key = doc_key(collection, doc_id);
        let res = shard.read().unwrap().get(&key)?.and_then(|b| FireLiteDoc::decode(&b));
        
        // AUDIT SUCCESS
        self.record_audit(AuditEntry { op: AccessOp::Get, collection: collection.into(), doc_id: Some(doc_id.into()), ok: true });
        Ok(res)
    }

    pub fn delete(&self, col: &str, id: &str) -> Result<()> {
        self.write_batch(vec![BatchMutation::Delete { collection: col.into(), doc_id: id.into() }])
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>> {
        if !self.allowed(&query.collection, AccessOp::Query) { 
            self.record_audit(AuditEntry { op: AccessOp::Query, collection: query.collection.clone(), doc_id: None, ok: false });
            return Err(FireLiteError::Corrupt("Denied".into())); 
        }
        
        let shard = self.get_shard(&query.collection);
        let storage = shard.read().unwrap();
        let indexes = self.indexes.read().unwrap();
        let rows = storage.count_prefix(&format!("{}:", query.collection));
        let plan = QueryPlanner::plan(&query, &indexes, rows);
        
        let res = self.executor.execute(&storage, &indexes, plan);
        
        // AUDIT RESULT
        self.record_audit(AuditEntry { op: AccessOp::Query, collection: query.collection.clone(), doc_id: None, ok: res.is_ok() });
        res
    }

    pub fn query_projected_zero_copy(&self, query: Query, fields: &[String]) -> Result<Vec<(String, Vec<(String, Value)>)>> {
        let shard = self.get_shard(&query.collection);
        let storage = shard.read().unwrap();
        let docs = storage.scan_prefix(&format!("{}:", query.collection))?;
        let mut out = Vec::new();
        for (id, raw) in docs {
            if matches_filters_borrowed(&raw, &query.filters) {
                let projected = project_fields_borrowed(&raw, fields);
                let order_val = query.order_by.as_ref().and_then(|o| extract_field_value_borrowed(&raw, &o.field));
                out.push((id, projected, order_val));
            }
        }
        if let Some(order) = &query.order_by {
            // Add this type hint to the closure parameter
            out.sort_by(|(_, _, av): &(String, Vec<(String, Value)>, Option<Value>), (_, _, bv)| {
                av.cmp(&bv)
            });
            if !order.ascending { out.reverse(); }
        }
        if let Some(limit) = query.limit { out.truncate(limit); }
        Ok(out.into_iter().map(|(id, proj, _)| (id, proj)).collect())
    }

    pub fn patch(&self, col: &str, id: &str, updates: Vec<(String, Value)>) -> Result<()> {
        if !self.allowed(col, AccessOp::Put) {
            self.record_audit(AuditEntry { op: AccessOp::Put, collection: col.into(), doc_id: Some(id.into()), ok: false });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }

        let shard = self.get_shard(col);
        let key = doc_key(col, id);
        let res = (|| -> Result<()> {
            let new_bytes = {
                let storage = shard.read().unwrap();
                let old = storage.get(&key)?.ok_or_else(|| FireLiteError::Corrupt("Not found".into()))?;
                FireLiteDoc::apply_patch_binary(&old, &updates).ok_or_else(|| FireLiteError::Corrupt("Patch fail".into()))?
            };
            shard.write().unwrap().put(key.clone(), &new_bytes)?;
            self.notify_watchers(col, ChangeEvent { path: key, kind: ChangeKind::Put });
            Ok(())
        })();

        // AUDIT RESULT
        self.record_audit(AuditEntry { op: AccessOp::Put, collection: col.into(), doc_id: Some(id.into()), ok: res.is_ok() });
        res
    }

    pub fn get_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        let shards = self.shards.read().unwrap();
        for (name, shard) in shards.iter() { stats.insert(name.clone(), shard.read().unwrap().count_prefix("")); }
        stats
    }

    pub fn backup(&self, dest: impl AsRef<Path>) -> Result<()> {
        self.flush()?;
        let shards = self.shards.write().unwrap();
        std::fs::create_dir_all(dest.as_ref())?;
        for (name, shard) in shards.iter() { 
            shard.write().unwrap().backup(dest.as_ref().join(name))?; 
        }
        Ok(())
    }

    fn allowed(&self, col: &str, op: AccessOp) -> bool {
        let rules = self.security_rules.read().unwrap();
        if rules.is_empty() { return true; }
        rules.iter().filter(|r| op == r.op && col.starts_with(&r.collection_prefix)).last().map(|r| r.allow).unwrap_or(true)
    }

    fn get_col<'a>(&self, m: &'a BatchMutation) -> &'a str {
        match m { BatchMutation::Put { collection, .. } => collection, BatchMutation::Delete { collection, .. } => collection }
    }

    pub fn current_version(&self, key: &str) -> Option<u64> { self.doc_versions.read().unwrap().get(key).cloned() }

    fn bump_versions_for_mutations(&self, mutations: &[BatchMutation]) {
        let mut versions = self.doc_versions.write().unwrap();
        for m in mutations {
            let key = match m { BatchMutation::Put { collection, doc_id, .. } => doc_key(collection, doc_id), BatchMutation::Delete { collection, doc_id } => doc_key(collection, doc_id) };
            versions.insert(key, self.global_version.fetch_add(1, Ordering::SeqCst));
        }
    }

    pub fn begin_serializable_transaction(&self) -> SerializableTransaction { SerializableTransaction { reads: HashMap::new(), mutations: Vec::new() } }
    pub fn create_composite_index(&self, col: &str, f: Vec<(String, SortDirection)>) -> u32 { self.indexes.write().unwrap().create_index(CompositeIndexDefinition::new(col).with_fields(f)) }
    pub fn watch_collection(&self, col: &str) -> Receiver<ChangeEvent> {
        let (tx, rx) = channel();
        self.listeners.lock().unwrap().entry(col.to_string()).or_default().push(tx);
        rx
    }
    fn notify_watchers(&self, col: &str, event: ChangeEvent) {
        if let Some(list) = self.listeners.lock().unwrap().get_mut(col) { list.retain(|s| s.send(event.clone()).is_ok()); }
    }
    pub fn compact(&self) -> Result<()> { for s in self.shards.read().unwrap().values() { s.write().unwrap().compact()?; } Ok(()) }
    pub fn flush(&self) -> Result<()> { for s in self.shards.read().unwrap().values() { s.write().unwrap().flush_all()?; } Ok(()) }
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let mut cols = Vec::new();
        if self.root_path.exists() { for e in std::fs::read_dir(&self.root_path)? { let e = e?; if e.path().is_dir() { cols.push(e.file_name().to_string_lossy().into()); } } }
        cols.sort(); Ok(cols)
    }
    pub fn get_by_reference(&self, reference: &Value) -> Result<Option<FireLiteDoc>> {
        match reference { Value::Reference { collection, doc_id } => self.get(collection, doc_id), _ => Err(FireLiteError::Corrupt("Not ref".into())) }
    }
    pub fn set_security_rules(&self, rules: Vec<SecurityRule>) { *self.security_rules.write().unwrap() = rules; }

    pub fn execute_aggregation(&self, query: Query) -> Result<HashMap<String, f64>> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry { op: AccessOp::Query, collection: query.collection.clone(), doc_id: None, ok: false });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }

        let shard = self.get_shard(&query.collection);
        let storage = shard.read().unwrap();
        let indexes = self.indexes.read().unwrap();
        let rows = storage.count_prefix(&format!("{}:", query.collection));
        let plan = QueryPlanner::plan(&query, &indexes, rows);
        
        let res = self.executor.execute_aggregation(&storage, &indexes, plan, &query.aggregations);
        
        self.record_audit(AuditEntry { op: AccessOp::Query, collection: query.collection.clone(), doc_id: None, ok: res.is_ok() });
        res
    }

    pub fn put_subdocument(&self, col: &str, id: &str, subcol: &str, subid: &str, doc: &FireLiteDoc) -> Result<()> {
        self.put(&subcollection_prefix(col, id, subcol), subid, doc)
    }

    pub fn audit_entries(&self) -> Vec<AuditEntry> {
        self.audit_data.read().unwrap().clone()
    }

    fn record_audit(&self, entry: AuditEntry) {
        let _ = self.audit_tx.send(entry);
    }
}


impl Drop for FireLite {
    fn drop(&mut self) {
        // 1. Signal background workers to stop immediately
        if let Some(tx) = self.maintenance_stop.lock().unwrap().take() {
            let _ = tx.send(());
        }

        // 2. Shut down shards in parallel (Optional but faster)
        // By taking the shards map, we ensure they start dropping.
        // StorageEngine's own Drop trait will handle the individual flushes.
        let mut shards = self.shards.write().unwrap();
        shards.clear(); 
        drop(shards);

        // 3. Finally, wait for the maintenance thread to exit
        // We do this LAST to ensure it doesn't block the shard cleanup
        if let Some(handle) = self.maintenance_handle.lock().unwrap().take() {
            // Use a timeout or just join. 
            // If it's stuck, it's usually because it's waiting on a shard lock 
            // that we just released above.
            let _ = handle.join();
        }
    }
}
// impl Drop for FireLite {
//     fn drop(&mut self) {
//         if let Some(tx) = self.maintenance_stop.lock().unwrap().take() { let _ = tx.send(()); }
//         if let Some(handle) = self.maintenance_handle.lock().unwrap().take() { let _ = handle.join(); }
//     }
// }

// --- Internal Helper Functions ---

fn doc_key(collection: &str, doc_id: &str) -> String { format!("{}:{}", collection, doc_id) }
fn subcollection_prefix(collection: &str, doc_id: &str, subcollection: &str) -> String { format!("{}:{}/{}", collection, doc_id, subcollection) }

fn extract_field_value_borrowed(raw: &[u8], field: &str) -> Option<Value> {
    let view = FireLiteDocView::new(raw)?;
    for (k, v) in view.iter() { if k == field { return v.to_owned_value(); } }
    None
}

fn matches_filters_borrowed(raw: &[u8], filters: &[crate::query::filter::Filter]) -> bool {
    if filters.is_empty() { return true; }
    let Some(view) = crate::document::firelite_doc::FireLiteDocView::new(raw) else { return false; };
    filters.iter().all(|f| {
        let mut matched = None;
        for (k, v) in view.iter() { if k == f.field { matched = v.to_owned_value(); break; } }
        matched.as_ref().map(|v| crate::query::filter::compare_values(v, &f.op, &f.value)).unwrap_or(false)
    })
}

fn project_fields_borrowed(raw: &[u8], fields: &[String]) -> Vec<(String, Value)> {
    let mut out = Vec::new();
    let Some(view) = crate::document::firelite_doc::FireLiteDocView::new(raw) else { return out; };
    for (k, v) in view.iter() {
        if fields.is_empty() || fields.iter().any(|f| f == k) {
            if let Some(value) = v.to_owned_value() { out.push((k.to_string(), value)); }
        }
    }
    out
}



