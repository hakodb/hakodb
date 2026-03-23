use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::mpsc::{channel, Receiver, Sender, SyncSender};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{SystemTime, UNIX_EPOCH, Duration};

use hashbrown::HashMap; 

use crate::config::FireLiteConfig;
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value; 
use crate::error::{FireLiteError, Result};
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::index::storage::index_storage::IndexStorage;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::planner::QueryPlanner;
use crate::query::query::Query;
use crate::storage::engine::{StorageEngine, StorageMutation, BlobWork};

use crate::util::lock::SafeLock; 

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
    
    index_storage: Arc<Mutex<IndexStorage>>,

    indexes: Arc<RwLock<IndexManager>>,
    executor: ParallelQueryExecutor,
    tx_lock: Mutex<()>,
    listeners: Mutex<HashMap<String, Vec<Sender<ChangeEvent>>>>,
    doc_versions: RwLock<HashMap<String, u64>>, 
    global_version: AtomicU64,
    security_rules: RwLock<Vec<SecurityRule>>,
    audit_data: Arc<RwLock<Vec<AuditEntry>>>,

    audit_tx: Sender<AuditEntry>,
    index_tx: Sender<IndexOp>,
    blob_tx: SyncSender<BlobWork>, 

    audit_stop: Mutex<Option<Sender<()>>>,
    audit_handle: Mutex<Option<thread::JoinHandle<()>>>,
    
    maintenance_stop: Mutex<Option<Sender<()>>>,
    maintenance_handle: Mutex<Option<thread::JoinHandle<()>>>,
}

impl FireLite {
    pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self> {
        let root_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_path)?;

        // 1. Initialize Global Shared State & Channels (ONCE)
        let indexes = Arc::new(RwLock::new(IndexManager::default()));
        
        let (index_tx, index_rx) = channel::<IndexOp>();
        let (audit_tx, audit_rx) = channel::<AuditEntry>();
        let (blob_tx, blob_rx) = std::sync::mpsc::sync_channel::<BlobWork>(5000);
        // let (blob_tx, blob_rx) = std::sync::mpsc::sync_channel::<BlobWork>(1000);

        let audit_data = Arc::new(RwLock::new(Vec::new()));
        let (audit_stop_tx, audit_stop_rx) = channel::<()>(); 

        // 2. Initialize Index Persistence
        let index_dir = root_path.join("_indices");
        let index_log_path = index_dir.join("index.log").to_string_lossy().to_string();
        let snapshot_dir = index_dir.join("snapshots").to_string_lossy().to_string();
        
        let index_storage = Arc::new(Mutex::new(
            IndexStorage::open(&index_log_path, &snapshot_dir)
                .map_err(|e| FireLiteError::Io(e))?
        ));

        // 3. Spawn Persistent Index Worker
        // This handles both memory updates AND physical logging
        let idx_clone = Arc::clone(&indexes);
        let storage_persist = Arc::clone(&index_storage);
        
        thread::spawn(move || {
            while let Ok(op) = index_rx.recv() {
                match op {
                    IndexOp::Update { collection, puts, deletes } => {

                        let mut mgr = match idx_clone.write() {
                            Ok(guard) => guard,
                            Err(_) => break, 
                        };
                        let mut persist = match storage_persist.lock() {
                            Ok(guard) => guard,
                            Err(_) => break,
                        };

                        for (id, doc) in puts {
                            mgr.index_document(&collection, &id, &doc);
                            
                            // 2. NEW: Update Secondary Indexes (Single Field)
                            if let Some(sec_map) = mgr.secondary.get_mut(&collection) {
                                for (field, index) in sec_map.iter_mut() {
                                    if let Some(val) = doc.get(field) {
                                        let key = crate::index::index_key::encode_scalar(val);
                                        // Use id.clone() here so 'id' stays alive for the next call
                                        index.insert(key, id.clone()); 
                                    }
                                }
                            }

                            // Log to Disk (Survivability)
                            let _ = persist.insert(1, doc.encode(), id);
                        }

                        for (id, doc) in deletes {
                            mgr.remove_document(&collection, &id, &doc);
                            // Log to Disk (Survivability)
                            let _ = persist.delete(1, doc.encode(), id);
                        }
                    }
                }
            }
        });

        // 4. Spawn Audit Worker Implementation
        let log_path = config.audit_log_path.clone().unwrap_or_else(|| {
            root_path.join("audit.log").to_string_lossy().to_string()
        });
        let mut audit_file = if config.enable_audit_log {
            Some(std::fs::OpenOptions::new().create(true).append(true).open(log_path)?)
        } else {
            None
        };

        let audit_data_clone = Arc::clone(&audit_data);
        let audit_handle_inner = thread::spawn(move || {
            loop {
                // Use recv_timeout so the thread can check the stop signal frequently
                match audit_rx.recv_timeout(Duration::from_millis(500)) {
                    Ok(entry) => {
                        if let Ok(mut history) = audit_data_clone.write() { history.push(entry.clone()); }
                        if let Some(file) = audit_file.as_mut() {
                            let _ = writeln!(file, "[{:?}] op={:?} col={} doc={:?} ok={}", 
                                SystemTime::now(), entry.op, entry.collection, entry.doc_id.as_deref().unwrap_or("<none>"), entry.ok);
                        }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                        // Check if we were told to stop
                        if audit_stop_rx.try_recv().is_ok() { break; }
                    }
                    Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                }
            }
        });

        // 5. Assemble the Engine Instance
        let db = Self {
            root_path: root_path.clone(),
            config: config.clone(),
            shards: Arc::new(RwLock::new(HashMap::new())),
            index_storage,
            indexes,
            executor: ParallelQueryExecutor::new(config.query_workers),
            tx_lock: Mutex::new(()),
            listeners: Mutex::new(HashMap::new()),
            doc_versions: RwLock::new(HashMap::new()),
            global_version: AtomicU64::new(1),
            security_rules: RwLock::new(Vec::new()),
            audit_tx,
            index_tx,
            blob_tx: blob_tx.clone(),
            audit_data,
            audit_stop: Mutex::new(Some(audit_stop_tx)), // <--- INITIALIZE
            audit_handle: Mutex::new(Some(audit_handle_inner)), // <--- INITIALIZE
            maintenance_stop: Mutex::new(None),
            maintenance_handle: Mutex::new(None),
        };

        // B. Blob Worker (The Janitor)
        let shards_ptr = Arc::clone(&db.shards);
        let root_path_clone = root_path.clone();
        let encryption_key = config.encryption_key.clone();
        
 
        let blob_rx = Arc::new(Mutex::new(blob_rx));

        // SPAWN MULTIPLE BLOB WORKERS (e.g., 4 workers)
        for _ in 0..4 {
            let rx = Arc::clone(&blob_rx);
            let shards_ptr = Arc::clone(&shards_ptr);
            let root_path_clone = root_path_clone.clone();
            let enc_key = encryption_key.clone();

            thread::spawn(move || {
                let enc_ctx = enc_key.map(|k| crate::storage::crypto::EncryptionContext::from_secret(&k));
                let mut file_handles: HashMap<String, std::fs::File> = HashMap::new();

                loop {
                    // 1. Get work (Locking the receiver is very fast)
                    let work = {
                        let lock = rx.lock().unwrap();
                        match lock.recv() {
                            Ok(w) => w,
                            Err(_) => break,
                        }
                    };

                    let BlobWork::Put { collection, key, data } = work;

                    // 2. Open shard-specific blob file
                    let file = file_handles.entry(collection.clone()).or_insert_with(|| {
                        let p = root_path_clone.join(&collection).join("blobs.dat");
                        std::fs::OpenOptions::new().create(true).read(true).append(true).open(p).expect("IO Fail")
                    });

                    // 3. Encrypt (Parallel across the 4 workers)
                    let payload = if let Some(ref enc) = enc_ctx {
                        enc.encrypt(&data).unwrap_or_else(|_| data.to_vec())
                    } else { data.to_vec() };

                    // 4. Write to Disk (Thread-safe positional append)
                    use std::io::{Write, Seek, SeekFrom};
                    let offset = file.seek(SeekFrom::End(0)).unwrap();
                    let len = payload.len() as u32;
                    file.write_all(&payload).unwrap();

                    // 5. Atomic Pointer Swap
                    let shards = shards_ptr.read().unwrap();
                    if let Some(shard_lock) = shards.get(&collection) {
                        if let Ok(mut shard) = shard_lock.write() {
                            shard.index.insert(key, crate::storage::engine::Pointer::Blob { offset, len });
                        }
                    }
                }
            });
        }

        // 6. Recovery & Sync
        db.recover_existing_shards()?;
        db.sync_indexes_with_persistence()?; 

        let (stop_tx, stop_rx) = channel::<()>();
        let shards_ptr = Arc::clone(&db.shards);
        let index_storage_ptr = Arc::clone(&db.index_storage);

        let handle = thread::spawn(move || loop {
            // Wait for 5 seconds OR a stop signal
            match stop_rx.recv_timeout(Duration::from_secs(5)) {
                // If we get a signal (Ok) or the sender dropped (Err Disconnected), exit NOW
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                
                // If 5 seconds passed without a signal, do the work
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let active_shards: Vec<_> = shards_ptr.read().unwrap().values().cloned().collect();
                    for s in active_shards { 
                        if let Ok(mut storage) = s.write() { let _ = storage.run_background_maintenance(); } 
                    }
                    
                    if let Ok(mut persist) = index_storage_ptr.lock() {
                        if let Ok(_) = persist.snapshot(1) { let _ = persist.reset_log(); }
                    }
                }
            }
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
                let mut storage = StorageEngine::open(entry.path(), &self.config)?;

                storage.blob_tx = Some(self.blob_tx.clone());

                shards.insert(col, Arc::new(RwLock::new(storage)));
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
            let mut storage = StorageEngine::open(path, &self.config).expect("Shard fail");
            
            storage.blob_tx = Some(self.blob_tx.clone());

            Arc::new(RwLock::new(storage))
        }).clone()
    }

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
                    for (_, v) in &mut doc.fields { 
                        if matches!(v, Value::ServerTimestamp) { *v = Value::Timestamp(now); } 
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

        // 2. DETERMINISTIC LOCKING: Prevents Deadlocks between threads
        let mut sorted_shards: Vec<_> = shard_groups.keys().cloned().collect();
        sorted_shards.sort(); // Always lock in alphabetical order


        let mut all_blob_work = Vec::new();

        // 3. EXECUTION: Write to each shard folder
        for col_name in sorted_shards {
            if let Some(ops) = shard_groups.get(&col_name) {
                let shard = self.get_shard(&col_name);
                // Lock ONLY this shard. Thread B can simultaneously lock a different shard!
                let mut storage = shard.write().unwrap();
                // storage.apply_batch(ops)?;
                let pending_blobs = storage.apply_batch(ops)?; 
                all_blob_work.extend(pending_blobs); 
            }
        }

        for work in all_blob_work {
            let _ = self.blob_tx.send(work);
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

    pub fn put(&self, col: &str, id: &str, doc: &FireLiteDoc) -> Result<()> {
        self.write_batch(vec![BatchMutation::Put { collection: col.into(), doc_id: id.into(), doc: doc.clone() }])
    }

    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        if !self.allowed(collection, AccessOp::Get) { 
            self.record_audit(AuditEntry { op: AccessOp::Get, collection: collection.into(), doc_id: Some(doc_id.into()), ok: false });
            return Err(FireLiteError::Corrupt("Denied".into())); 
        }
        
        let shard = self.get_shard(collection);
        let storage = shard.safe_read()?; 
        let res = storage.get(&doc_key(collection, doc_id))?.and_then(|b| FireLiteDoc::decode(&b));
        
        // AUDIT SUCCESS
        self.record_audit(AuditEntry { 
            op: AccessOp::Get, 
            collection: collection.into(), 
            doc_id: Some(doc_id.into()), 
            ok: true 
        });
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

        let shard_arc = self.get_shard(&query.collection);
        // let storage = shard_arc.read().unwrap();

        let indexes = self.indexes.read().unwrap();
        // let rows = storage.count_prefix(&format!("{}:", query.collection));
        let rows = shard_arc.read().unwrap().count_prefix(&format!("{}:", query.collection));
        
        // UPDATED: Pass self.config.query_workers
        let plan = QueryPlanner::plan(
            &query, 
            &indexes, 
            rows, 
            self.config.query_workers
        );
        
        // let res = self.executor.execute(&storage, &indexes, plan);
        let res = self.executor.execute(shard_arc, &indexes, plan);
        
        // AUDIT RESULT
        self.record_audit(AuditEntry { op: AccessOp::Query, collection: query.collection.clone(), doc_id: None, ok: res.is_ok() });
        res
    }

    pub fn query_projected_zero_copy(&self, query: Query, fields: &[String]) -> Result<Vec<(String, Vec<(String, Value)>)>> {
        // 1. Security Check
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry { op: AccessOp::Query, collection: query.collection.clone(), doc_id: None, ok: false });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }
        
        // 2. Prepare Query with Projection
        let mut q = query;
        q.projection = fields.to_vec();

        // 3. Acquire Locks
        let shard_arc = self.get_shard(&q.collection);
        
        // 4. Plan & Execute

        let indexes = self.indexes.read().unwrap();
        
        let rows = {
            let storage = shard_arc.read().unwrap();
            storage.count_prefix(&format!("{}:", q.collection))
        };

        let plan = QueryPlanner::plan(&q, &indexes, rows, self.config.query_workers);
        
        // let res = self.executor.execute_projected(&storage, &indexes, plan);
        let res = self.executor.execute_projected(shard_arc, &indexes, plan);
        
        // 5. Audit & Return
        self.record_audit(AuditEntry { 
            op: AccessOp::Query, 
            collection: q.collection.clone(), 
            doc_id: None, 
            ok: res.is_ok() 
        });
        
        res
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

        let shard_arc = self.get_shard(&query.collection);
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix(&format!("{}:", query.collection));
        
        // FIX: Add self.config.query_workers as the 4th argument
        let plan = QueryPlanner::plan(&query, &indexes, rows, self.config.query_workers);
        
        let res = self.executor.execute_aggregation(shard_arc, &indexes, plan, &query.aggregations);
        
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

    fn sync_indexes_with_persistence(&self) -> Result<()> {
        // 1. Load from Persistent Storage (Fast)
        {
            let mut mgr = self.indexes.write().unwrap();
            let mut persist = self.index_storage.lock().unwrap();
            mgr.composite = std::mem::take(&mut persist.manager);
        }

        // 2. Check if the index is actually empty
        // (This happens on a fresh install or if snapshots are missing)
        let is_empty = {
            let mgr = self.indexes.read().unwrap();
            // Check if there are any IDs in any collection in the B-Tree
            // Using an empty scan to check for any existence
            mgr.composite.get(1).map_or(true, |idx| idx.tree.is_empty())
        };

        if is_empty {
            // FALLBACK: If no snapshot was found, do the full scan once.
            // This resolves the "dead_code" warning for this method.
            self.rebuild_indexes_from_shards()?;
        } else {
            // CATCH-UP: Only scan shards for documents added since the last snapshot
            let shards = self.shards.read().unwrap();
            let mut mgr = self.indexes.write().unwrap();
            for (col_name, shard) in shards.iter() {
                let storage = shard.read().unwrap();
                let physical_count = storage.count_prefix("");
                let indexed_count = mgr.composite.exact_match_doc_ids(col_name, &[], &[]).map_or(0, |v| v.len());
                
                if physical_count > indexed_count {
                    // Shard has new data: scan and update index
                    for (key, bytes) in storage.scan_prefix("")? {
                        if let Some((_, doc_id)) = key.split_once(':') {
                            if let Some(doc) = FireLiteDoc::decode(&bytes) {
                                mgr.composite.index_document(col_name, doc_id, &doc);
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        // 1. Check if the index already exists to avoid double-work
        {
            let mgr = self.indexes.read().unwrap();
            if mgr.secondary.get(collection).map_or(false, |m| m.contains_key(field)) {
                return Ok(());
            }
        }

        // 2. Register the empty index in the manager
        {
            let mut mgr = self.indexes.write().unwrap();
            mgr.create_secondary_index(collection, field);
        }

        // 3. Spawn background thread for backfilling
        let shard = self.get_shard(collection);
        let idx_mgr = Arc::clone(&self.indexes);
        let col_name = collection.to_string();
        let field_name = field.to_string();

        thread::spawn(move || {
            // A. Scan the shard (Read lock is held only during the scan)
            let entries = {
                if let Ok(storage) = shard.read() {
                    storage.scan_prefix("").unwrap_or_default()
                } else {
                    return; 
                }
            };

            if entries.is_empty() { return; }

            // B. Process in chunks to keep the system responsive
            for chunk in entries.chunks(500) {
                // Acquire write lock only for the duration of this chunk
                let mut mgr = idx_mgr.write().unwrap();
                
                if let Some(sec_map) = mgr.secondary.get_mut(&col_name) {
                    if let Some(index) = sec_map.get_mut(&field_name) {
                        for (full_key, bytes) in chunk {
                            if let Some(doc) = FireLiteDoc::decode(bytes) {
                                if let Some(val) = doc.get(&field_name) {
                                    let idx_key = crate::index::index_key::encode_scalar(val);
                                    
                                    // Map "col:id" -> "id"
                                    if let Some((_, doc_id)) = full_key.split_once(':') {
                                        index.insert(idx_key, doc_id.to_string());
                                    }
                                }
                            }
                        }
                    }
                }
                // Lock 'mgr' is automatically dropped here when the scope ends,
                // allowing query threads to "sneak in" between chunks.
            }
        });

        Ok(())
    }

    pub fn create_fts_index(&self, collection: &str, field: &str) -> Result<()> {
        // 1. Register
        self.indexes.write().unwrap().create_fts_index(collection, field);

        // 2. Backfill from disk
        let shard = self.get_shard(collection);
        let storage = shard.read().unwrap();
        let entries = storage.scan_prefix(&format!("{}:", collection))?;

        let mut mgr = self.indexes.write().unwrap();
        if let Some(fields) = mgr.fts.get_mut(collection) {
            if let Some(index) = fields.get_mut(field) {
                for (full_key, bytes) in entries {
                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                        if let Some(Value::String(text)) = doc.get(field) {
                            if let Some((_, doc_id)) = full_key.split_once(':') {
                                index.insert(text, doc_id.to_string());
                            }
                        }
                    }
                }
            }
        }
        Ok(())
    }

    /// Explicitly trigger a snapshot (called by Maintenance Thread or FFI)
    pub fn save_index_snapshots(&self) -> Result<()> {
        let persist = self.index_storage.lock().unwrap();
        // Snapshot the primary composite index (ID 1)
        persist.snapshot(1).map_err(|e| FireLiteError::Io(e))?;
        Ok(())
    }

    
}


impl Drop for FireLite {
    fn drop(&mut self) {
        // 1. Signal workers to stop. They will wake up instantly due to recv_timeout.
        if let Some(tx) = self.maintenance_stop.lock().unwrap().take() {
            let _ = tx.send(());
        }
        if let Some(tx) = self.audit_stop.lock().unwrap().take() {
            let _: std::result::Result<(), std::sync::mpsc::SendError<()>> = tx.send(());
        }

        // 2. PARALLEL SHARD FLUSHING
        // We move the shards out of the map to ensure we own them during the flush
        let shards_to_flush: Vec<Arc<RwLock<StorageEngine>>> = {
            let mut shards_map = self.shards.write().unwrap();
            shards_map.drain().map(|(_, shard)| shard).collect()
        };

        let flush_handles: Vec<_> = shards_to_flush.into_iter().map(|shard| {
            thread::spawn(move || {
                if let Ok(mut storage) = shard.write() {
                    let _ = storage.checkpoint_inlined_data();
                    let _ = storage.flush_all();
                }
            })
        }).collect();

        // 3. Wait for shard flushes
        for h in flush_handles { let _ = h.join(); }

        // 4. Join the controller threads (Now near-instant because they woke up in step 1)
        if let Some(h) = self.maintenance_handle.lock().unwrap().take() { let _ = h.join(); }
        if let Some(h) = self.audit_handle.lock().unwrap().take() { let _ = h.join(); }
    }
}

// --- Internal Helper Functions ---

fn doc_key(collection: &str, doc_id: &str) -> String { format!("{}:{}", collection, doc_id) }
fn subcollection_prefix(collection: &str, doc_id: &str, subcollection: &str) -> String { format!("{}:{}/{}", collection, doc_id, subcollection) }

// fn extract_field_value_borrowed(raw: &[u8], field: &str) -> Option<Value> {
//     let view = FireLiteDocView::new(raw)?;
//     for (k, v) in view.iter() { if k == field { return v.to_owned_value(); } }
//     None
// }

// fn matches_filters_borrowed(raw: &[u8], filters: &[crate::query::filter::Filter]) -> bool {
//     if filters.is_empty() { return true; }
//     let Some(view) = crate::document::firelite_doc::FireLiteDocView::new(raw) else { return false; };
//     filters.iter().all(|f| {
//         let mut matched = None;
//         for (k, v) in view.iter() { if k == f.field { matched = v.to_owned_value(); break; } }
//         matched.as_ref().map(|v| crate::query::filter::compare_values(v, &f.op, &f.value)).unwrap_or(false)
//     })
// }

// fn project_fields_borrowed(raw: &[u8], fields: &[String]) -> Vec<(String, Value)> {
//     let mut out = Vec::new();
//     let Some(view) = crate::document::firelite_doc::FireLiteDocView::new(raw) else { return out; };
//     for (k, v) in view.iter() {
//         if fields.is_empty() || fields.iter().any(|f| f == k) {
//             if let Some(value) = v.to_owned_value() { out.push((k.to_string(), value)); }
//         }
//     }
//     out
// }



