use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender, SyncSender};
use std::sync::{Arc, Mutex, RwLock};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use hashbrown::HashMap;

use crate::config::FireLiteConfig;
use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::error::{FireLiteError, Result};
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::index::service::IndexingService;
use crate::index::storage::index_storage::IndexStorage;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::planner::QueryPlanner;
use crate::query::query::Query;
use crate::storage::engine::{BlobWork, StorageEngine, StorageMutation, Pointer};

use crate::util::lock::SafeLock;

use std::cell::RefCell;

thread_local! {
    // A reusable buffer for serialization to avoid allocations
    static WRITE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 * 1024));
    // A reusable buffer for building keys to avoid format!()
    static KEY_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(128));
}

// --- Data Types ---

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
    Patch { 
        collection: String, 
        doc_id: String, 
        updates: Vec<(String, Value)> 
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

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
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

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct AuditEntry {
    pub op: AccessOp,
    pub collection: String,
    pub doc_id: Option<String>,
    pub ok: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompositeIndexFieldInfo {
    pub field: String,
    pub direction: String,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct CompositeIndexInfo {
    pub id: u32,
    pub collection: String,
    pub fields: Vec<CompositeIndexFieldInfo>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexList {
    pub simple: HashMap<String, Vec<String>>,
    pub secondary: HashMap<String, Vec<String>>,
    pub fts: HashMap<String, Vec<String>>,
    pub composite: Vec<CompositeIndexInfo>,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedIndexState {
    secondary: HashMap<String, Vec<String>>,
    fts: HashMap<String, Vec<String>>,
    composite: Vec<PersistedCompositeIndex>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedCompositeIndex {
    id: u32, 
    collection: String,
    fields: Vec<(String, String)>,
}

pub struct Transaction {
    pub mutations: Vec<BatchMutation>,
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
    pub fn commit(self, db: &FireLite) -> Result<()> {
        db.write_batch(self.mutations)
    }
}

pub struct SerializableTransaction {
    pub reads: HashMap<String, Option<u64>>,
    pub mutations: Vec<BatchMutation>,
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
        db.commit_serializable(self.reads.clone(), self.mutations.clone())
    }
}

enum IndexOp {
    Update {
        collection: String,
        puts: Vec<(String, FireLiteDoc)>,
        deletes: Vec<(String, FireLiteDoc)>,
    },
}

pub struct FireLite {
    root_path: PathBuf,
    config: FireLiteConfig,
    pub(crate) shards: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>>, // The only storage

    index_storage: Arc<Mutex<IndexStorage>>,

    pub(crate) indexes: Arc<RwLock<IndexManager>>,
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

        // 1. Initialize Global Shared State & Channels
        let indexes = Arc::new(RwLock::new(IndexManager::default()));
        let (index_tx, index_rx) = channel::<IndexOp>();
        let (audit_tx, audit_rx) = channel::<AuditEntry>();
        let (blob_tx, blob_rx) = std::sync::mpsc::sync_channel::<BlobWork>(5000);

        let shared_blob_rx = Arc::new(Mutex::new(blob_rx));
        let audit_data = Arc::new(RwLock::new(Vec::new()));
        let (audit_stop_tx, audit_stop_rx) = channel::<()>();

        // 2. Initialize Index Persistence (Composite Index state)
        let index_dir = root_path.join("_indices");
        let index_log_path = index_dir.join("index.log").to_string_lossy().to_string();
        let snapshot_dir = index_dir.join("snapshots").to_string_lossy().to_string();

        let index_storage = Arc::new(Mutex::new(
            IndexStorage::open(&index_log_path, &snapshot_dir).map_err(|e| FireLiteError::Io(e))?,
        ));

        // 3. Spawn Persistent Index Worker
        let idx_clone = Arc::clone(&indexes);
        let storage_persist = Arc::clone(&index_storage);
        thread::spawn(move || {
            while let Ok(op) = index_rx.recv() {
                match op {
                    IndexOp::Update { collection, puts, deletes } => {
                        let mut mgr = match idx_clone.write() { Ok(g) => g, Err(_) => break };
                        let mut persist = match storage_persist.lock() { Ok(g) => g, Err(_) => break };

                        for (id, doc) in puts {
                            IndexingService::apply_put(&mut mgr, &collection, &id, &doc);
                            for idx in mgr.indexes_for_collection(&collection) {
                                if let Some(vals) = idx.document_values(&doc) {
                                    let key_bytes = crate::index::composite::key_encoder::encode_composite_key(&idx.definition, &vals, &id);
                                    let _ = persist.insert(idx.definition.id, key_bytes.to_vec(), id.clone());
                                }
                            }
                        }

                        for (id, doc) in deletes {
                            IndexingService::apply_delete(&mut mgr, &collection, &id, &doc);
                            for idx in mgr.indexes_for_collection(&collection) {
                                if let Some(vals) = idx.document_values(&doc) {
                                    let key_bytes = crate::index::composite::key_encoder::encode_composite_key(&idx.definition, &vals, &id);
                                    let _ = persist.delete(idx.definition.id, key_bytes.to_vec(), id.clone());
                                }
                            }
                        }
                    }
                }
            }
        });

        // 4. Spawn Audit Worker
        let log_path = config.audit_log_path.clone().unwrap_or_else(|| root_path.join("audit.log").to_string_lossy().to_string());
        let mut audit_file = if config.enable_audit_log { Some(std::fs::OpenOptions::new().create(true).append(true).open(log_path)?) } else { None };
        let audit_data_clone = Arc::clone(&audit_data);
        let audit_handle_inner = thread::spawn(move || loop {
            match audit_rx.recv_timeout(Duration::from_millis(500)) {
                Ok(entry) => {
                    if let Ok(mut history) = audit_data_clone.write() { history.push(entry.clone()); }
                    if let Some(file) = audit_file.as_mut() {
                        let _ = writeln!(file, "[{:?}] op={:?} col={} doc={:?} ok={}", SystemTime::now(), entry.op, entry.collection, entry.doc_id.as_deref().unwrap_or("<none>"), entry.ok);
                    }
                }
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => { if audit_stop_rx.try_recv().is_ok() { break; } }
                Err(_) => break,
            }
        });

        // 5. Assemble the Engine Instance
        let db = Self {
            root_path: root_path.clone(),
            config: config.clone(),
            shards: Arc::new(RwLock::new(HashMap::new())),
            index_storage: Arc::clone(&index_storage),
            indexes: Arc::clone(&indexes),
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
            audit_stop: Mutex::new(Some(audit_stop_tx)),
            audit_handle: Mutex::new(Some(audit_handle_inner)),
            maintenance_stop: Mutex::new(None),
            maintenance_handle: Mutex::new(None),
        };

        let _ = db.restore_index_defs();

        // 6. ORCHESTRATED BACKGROUND RECOVERY
        let shards_ptr = Arc::clone(&db.shards);
        let indexes_ptr = Arc::clone(&db.indexes);
        let persist_ptr = Arc::clone(&db.index_storage);
        let config_thread = config.clone();
        let blob_tx_thread = db.blob_tx.clone();
        let root_scan = root_path.clone();
        let snapshot_path = root_path.join("_indices").join("ram_indexes.bin");

        thread::spawn(move || {
            // --- STEP A: Try to load RAM Snapshot FIRST ---
            let mut snapshot_loaded = false;

            if snapshot_path.exists() {
                if let Ok(bytes) = std::fs::read(&snapshot_path) {
                    let mut mgr = indexes_ptr.write().unwrap();
                    // No version returned anymore
                    if mgr.import_state(&bytes).is_ok() {
                        snapshot_loaded = true;
                    }
                }
            }

            // --- STEP B: Discovery ---
            let mut discovered = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&root_scan) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if !name.starts_with('_') && name != "snapshots" {
                            discovered.push(name);
                        }
                    }
                }
            }

            // --- STEP C: Load Shards (and Rebuild if needed) ---
            for col_name in discovered {
                let path = root_scan.join(&col_name);
                if let Ok(mut storage) = StorageEngine::open(path, &config_thread, col_name.clone()) {
                    storage.blob_tx = Some(blob_tx_thread.clone());
                    
                    // REBUILD ONLY IF SNAPSHOT FAILED
                    if !snapshot_loaded {
                        if let Ok(data) = storage.scan_prefix("") {
                            let mut mgr = indexes_ptr.write().unwrap();
                            for (full_key, bytes) in data {
                                if let Some((_, doc_id)) = full_key.split_once(':') {
                                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                                        mgr.index_document(&col_name, doc_id, &doc);
                                    }
                                }
                            }
                        }
                    }

                    if let Ok(mut shards) = shards_ptr.write() {
                        shards.insert(col_name.clone(), Arc::new(RwLock::new(storage)));
                    }
                }
            }

            // --- STEP D: Load Persistent Composite Index Manager state ---
            {
                let log_path = {
                    let persist = persist_ptr.lock().unwrap();
                    persist.log_path().to_string() // Use the new getter
                };
                let mut mgr = indexes_ptr.write().unwrap();
                // mgr.composite = std::mem::take(&mut persist.manager);
                let _ = crate::index::storage::index_recovery::replay_log(&log_path, &mut mgr.composite);
            }
        });

        // 7. Blob Workers (4 Threads)
        let shards_ptr = Arc::clone(&db.shards);
        let encryption_key = config.encryption_key.clone();

        for _ in 0..4 {
            let rx = Arc::clone(&shared_blob_rx);
            let s_ptr = Arc::clone(&shards_ptr);
            let enc_key = encryption_key.clone();

            thread::spawn(move || {
                use std::io::{Seek, SeekFrom, Write};
                let enc_ctx = enc_key.map(|k| crate::storage::crypto::EncryptionContext::from_secret(&k));
                
                loop {
                    let work = {
                        let lock = match rx.lock() { Ok(g) => g, Err(_) => break };
                        match lock.recv() { Ok(w) => w, Err(_) => break }
                    };

                    let BlobWork::Put { collection, key, data } = work;
                    let payload = if let Some(ref enc) = enc_ctx {
                        enc.encrypt(&data).unwrap_or_else(|_| data.to_vec())
                    } else {
                        data.to_vec()
                    };

                    // FIX: Correctly extract the Arc<Mutex<File>> from the Shard
                    let file_mutex_opt = {
                        let shards = s_ptr.read().unwrap();
                        shards.get(&collection).and_then(|shard_arc| {
                            let shard_guard = shard_arc.read().ok()?;
                            shard_guard.blob_file.clone() // This is Option<Arc<Mutex<File>>>
                        })
                    };

                    if let Some(file_mutex) = file_mutex_opt {
                        let (offset, len) = {
                            let mut file = file_mutex.lock().unwrap();
                            let pos = file.seek(SeekFrom::End(0)).unwrap();
                            file.write_all(&payload).unwrap();
                            (pos, payload.len() as u32)
                        };

                        // Update index
                        let shards = s_ptr.read().unwrap();
                        if let Some(shard_arc) = shards.get(&collection) {
                            if let Ok(mut shard) = shard_arc.write() {
                                shard.index.insert(key, Pointer::Blob { offset, len });
                            }
                        }
                    }
                }
            });
        }

        // 8. Maintenance Thread
        let (stop_tx, stop_rx) = channel::<()>();
        let shards_ptr_m = Arc::clone(&db.shards);
        let index_storage_ptr_m = Arc::clone(&db.index_storage);

        let handle = thread::spawn(move || loop {
            match stop_rx.recv_timeout(Duration::from_secs(5)) {
                Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => break,
                Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {
                    let active_shards: Vec<_> = shards_ptr_m.read().unwrap().values().cloned().collect();
                    for s in active_shards {
                        if let Ok(mut storage) = s.write() { let _ = storage.run_background_maintenance(); }
                    }
                    if let Ok(mut persist) = index_storage_ptr_m.lock() {
                        if let Ok(_) = persist.snapshot(1) { let _ = persist.reset_log(); }
                    }
                }
            }
        });

        *db.maintenance_stop.lock().unwrap() = Some(stop_tx);
        *db.maintenance_handle.lock().unwrap() = Some(handle);

        Ok(db)
    }

    pub(crate) fn get_shard(&self, collection: &str) -> Arc<RwLock<StorageEngine>> {
        // Read lock check first (Fast Path)
        if let Some(s) = self.shards.read().unwrap().get(collection) {
            return Arc::clone(s);
        }

        // Write lock (Creation Path)
        let mut shards = self.shards.write().unwrap();
        shards.entry(collection.to_string()).or_insert_with(|| {
            let path = self.root_path.join(collection);
            let mut storage = StorageEngine::open(path, &self.config, collection.to_string())
                .expect("Failed to create collection shard");
            storage.blob_tx = Some(self.blob_tx.clone());
            Arc::new(RwLock::new(storage))
        }).clone()
    }

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        // 1. SECURITY & AUDIT (Read-only check, no lock needed)
        if !mutations
            .iter()
            .all(|m| self.allowed(self.get_col(m), AccessOp::Batch))
        {
            self.record_audit(AuditEntry {
                op: AccessOp::Batch,
                collection: "<sharded>".into(),
                doc_id: None,
                ok: false,
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
            ok: res.is_ok(),
        });

        res
    }

    pub fn commit_serializable(
        &self,
        reads: HashMap<String, Option<u64>>,
        mutations: Vec<BatchMutation>,
    ) -> Result<()> {
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
    fn write_batch_internal(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_micros() as i64;

        let mut shard_groups: HashMap<String, Vec<StorageMutation>> = HashMap::new();
        let mut index_puts: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
        let mut index_dels: HashMap<String, Vec<(String, FireLiteDoc)>> = HashMap::new();
        let mut unique_events: HashMap<String, (String, ChangeEvent)> = HashMap::new();
        
        // Track keys to update versions without re-iterating over mutations
        let mut affected_keys = Vec::with_capacity(mutations.len());

        for m in mutations { // Consumes mutations
            match m {
                BatchMutation::Put { collection, doc_id, mut doc } => {
                    for (_, v) in &mut doc.fields {
                        if matches!(v, Value::ServerTimestamp) {
                            *v = Value::Timestamp(now);
                        }
                    }

                    let key = fast_doc_key(&collection, &doc_id);
                    affected_keys.push(key.clone()); // Store for versioning

                    let doc_bytes = doc.encode(); 
                    shard_groups.entry(collection.clone()).or_default().push(
                        StorageMutation::Put { key: key.clone(), value: doc_bytes }
                    );

                    // MOVE doc_id and doc (Zero-Clone)
                    index_puts.entry(collection.clone()).or_default().push((doc_id, doc));

                    unique_events.insert(key.clone(), (
                        collection,
                        ChangeEvent { path: key, kind: ChangeKind::Put }
                    ));
                }

                BatchMutation::Delete { collection, doc_id } => {
                    let key = fast_doc_key(&collection, &doc_id);
                    affected_keys.push(key.clone());

                    let shard = self.get_shard(&collection);
                    if let Some(bytes) = shard.read().unwrap().get(&key)? {
                        if let Some(old_doc) = FireLiteDoc::decode(&bytes) {
                            index_dels.entry(collection.clone()).or_default().push((doc_id, old_doc));
                        }
                    }
                    
                    shard_groups.entry(collection.clone()).or_default().push(
                        StorageMutation::Delete { key: key.clone() }
                    );
                    
                    unique_events.insert(key.clone(), (
                        collection,
                        ChangeEvent { path: key, kind: ChangeKind::Delete }
                    ));
                }

                BatchMutation::Patch { collection, doc_id, updates } => {
                    let key = fast_doc_key(&collection, &doc_id);
                    affected_keys.push(key.clone());

                    let shard = self.get_shard(&collection);
                    let storage = shard.read().unwrap();
                    if let Some(old_bytes) = storage.get(&key)? {
                        if let Some(mut doc) = FireLiteDoc::decode(&old_bytes) {
                            let old_doc_for_index = doc.clone(); 
                            for (k, v) in updates {
                                doc.insert(k, v);
                            }
                            let new_bytes = doc.encode();

                            index_dels.entry(collection.clone()).or_default().push((doc_id.clone(), old_doc_for_index));
                            index_puts.entry(collection.clone()).or_default().push((doc_id, doc));

                            shard_groups.entry(collection.clone()).or_default().push(
                                StorageMutation::Put { key: key.clone(), value: new_bytes }
                            );

                            unique_events.insert(key.clone(), (
                                collection,
                                ChangeEvent { path: key, kind: ChangeKind::Put }
                            ));
                        }
                    }
                }
            }
        }

        // 2. DETERMINISTIC LOCKING (Alphabetical)
        let mut sorted_shards: Vec<_> = shard_groups.keys().cloned().collect();
        sorted_shards.sort();

        let mut all_blob_work = Vec::new();
        for col_name in sorted_shards {
            if let Some(ops) = shard_groups.get(&col_name) {
                let shard = self.get_shard(&col_name);
                let mut storage = shard.write().unwrap();
                let pending_blobs = storage.apply_batch(ops)?;
                all_blob_work.extend(pending_blobs);
            }
        }

        // Hand off blobs
        for work in all_blob_work {
            let _ = self.blob_tx.try_send(work);
        }

        // 3. Update metadata using our collected keys (NEW)
        self.bump_versions_by_keys(affected_keys);

        for (col, puts) in index_puts {
            let deletes = index_dels.remove(&col).unwrap_or_default();
            let _ = self.index_tx.send(IndexOp::Update {
                collection: col,
                puts,
                deletes,
            });
        }
        for (col, deletes) in index_dels {
            let _ = self.index_tx.send(IndexOp::Update {
                collection: col,
                puts: vec![],
                deletes,
            });
        }
        
        // for (col, event) in change_events {
        //     self.notify_watchers(&col, event);
        // }

        for (_, (col, event)) in unique_events {
            self.notify_watchers(&col, event);
        }

        Ok(())
    }

    pub fn put(&self, col: &str, id: &str, doc: &FireLiteDoc) -> Result<()> {
        self.write_batch(vec![BatchMutation::Put {
            collection: col.into(),
            doc_id: id.into(),
            doc: doc.clone(),
        }])
    }

    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        if !self.allowed(collection, AccessOp::Get) {
            self.record_audit(AuditEntry {
                op: AccessOp::Get,
                collection: collection.into(),
                doc_id: Some(doc_id.into()),
                ok: false,
            });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }

        let shard = self.get_shard(collection);
        let storage = shard.safe_read()?;
        let res = storage
            .get(&doc_key(collection, doc_id))?
            .and_then(|b| FireLiteDoc::decode(&b));

        // AUDIT SUCCESS
        self.record_audit(AuditEntry {
            op: AccessOp::Get,
            collection: collection.into(),
            doc_id: Some(doc_id.into()),
            ok: true,
        });
        Ok(res)
    }

    pub fn delete(&self, col: &str, id: &str) -> Result<()> {
        self.write_batch(vec![BatchMutation::Delete {
            collection: col.into(),
            doc_id: id.into(),
        }])
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection);
        // let storage = shard_arc.read().unwrap();

        let indexes = self.indexes.read().unwrap();
        // let rows = storage.count_prefix(&format!("{}:", query.collection));
        let rows = shard_arc
            .read()
            .unwrap()
            .count_prefix(&format!("{}:", query.collection));

        // UPDATED: Pass self.config.query_workers
        let plan = QueryPlanner::plan(&query, &indexes, rows, self.config.query_workers);

        // let res = self.executor.execute(&storage, &indexes, plan);
        let res = self.executor.execute(shard_arc, &indexes, plan);

        // AUDIT RESULT
        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: res.is_ok(),
        });
        res
    }

    pub fn query_projected_zero_copy(
        &self,
        query: Query,
        fields: &[String],
    ) -> Result<Vec<(String, Vec<(String, Value)>)>> {
        // 1. Security Check
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
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
            ok: res.is_ok(),
        });

        res
    }

    pub fn patch(&self, col: &str, id: &str, updates: Vec<(String, Value)>) -> Result<()> {
        if !self.allowed(col, AccessOp::Put) {
            self.record_audit(AuditEntry {
                op: AccessOp::Put,
                collection: col.into(),
                doc_id: Some(id.into()),
                ok: false,
            });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }

        let res = self.write_batch(vec![BatchMutation::Patch {
            collection: col.to_string(),
            doc_id: id.to_string(),
            updates,
        }]);

        // AUDIT RESULT
        self.record_audit(AuditEntry {
            op: AccessOp::Put,
            collection: col.into(),
            doc_id: Some(id.into()),
            ok: res.is_ok(),
        });
        res
    }

    pub fn get_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        let shards = self.shards.read().unwrap();
        for (name, shard) in shards.iter() {
            stats.insert(name.clone(), shard.read().unwrap().count_prefix(""));
        }
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
        if rules.is_empty() {
            return true;
        }
        rules
            .iter()
            .filter(|r| op == r.op && col.starts_with(&r.collection_prefix))
            .last()
            .map(|r| r.allow)
            .unwrap_or(true)
    }

    fn get_col<'a>(&self, m: &'a BatchMutation) -> &'a str {
        match m {
            BatchMutation::Put { collection, .. } => collection,
            BatchMutation::Delete { collection, .. } => collection,
            BatchMutation::Patch { collection, .. } => collection,
        }
    }

    pub fn current_version(&self, key: &str) -> Option<u64> {
        self.doc_versions.read().unwrap().get(key).cloned()
    }

    fn bump_versions_by_keys(&self, keys: Vec<String>) {
        let mut versions = self.doc_versions.write().unwrap();
        for key in keys {
            versions.insert(key, self.global_version.fetch_add(1, Ordering::SeqCst));
        }
    }

    // fn bump_versions_for_mutations(&self, mutations: &[BatchMutation]) {
    //     let mut versions = self.doc_versions.write().unwrap();
    //     for m in mutations {
    //         let key = match m {
    //             BatchMutation::Put {
    //                 collection, doc_id, ..
    //             } => doc_key(collection, doc_id),
    //             BatchMutation::Delete { collection, doc_id } => doc_key(collection, doc_id),
    //             BatchMutation::Patch { collection, doc_id, .. } => {
    //                 doc_key(collection, doc_id)
    //             }
    //         };
    //         versions.insert(key, self.global_version.fetch_add(1, Ordering::SeqCst));
    //     }
    // }

    pub fn begin_serializable_transaction(&self) -> SerializableTransaction {
        SerializableTransaction {
            reads: HashMap::new(),
            mutations: Vec::new(),
        }
    }

    pub fn watch_collection(&self, col: &str) -> Receiver<ChangeEvent> {
        let (tx, rx) = channel();
        self.listeners
            .lock()
            .unwrap()
            .entry(col.to_string())
            .or_default()
            .push(tx);
        rx
    }
    fn notify_watchers(&self, col: &str, event: ChangeEvent) {
        if let Some(list) = self.listeners.lock().unwrap().get_mut(col) {
            list.retain(|s| s.send(event.clone()).is_ok());
        }
    }
    pub fn compact(&self) -> Result<()> {
        for s in self.shards.read().unwrap().values() {
            s.write().unwrap().compact()?;
        }
        Ok(())
    }
    pub fn flush(&self) -> Result<()> {
        for s in self.shards.read().unwrap().values() {
            s.write().unwrap().flush_all()?;
        }
        Ok(())
    }

    pub fn list_collections(&self) -> Result<Vec<String>> {
        let mut cols = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.root_path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if !name.starts_with('_') && name != "snapshots" {
                            cols.push(name);
                        }
                    }
                }
            }
        }
        cols.sort();
        Ok(cols)
    }

    pub fn get_by_reference(&self, reference: &Value) -> Result<Option<FireLiteDoc>> {
        match reference {
            Value::Reference { collection, doc_id } => self.get(collection, doc_id),
            _ => Err(FireLiteError::Corrupt("Not ref".into())),
        }
    }
    pub fn set_security_rules(&self, rules: Vec<SecurityRule>) {
        *self.security_rules.write().unwrap() = rules;
    }

    pub fn execute_aggregation(&self, query: Query) -> Result<HashMap<String, f64>> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(FireLiteError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection);
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc
            .read()
            .unwrap()
            .count_prefix(&format!("{}:", query.collection));

        // FIX: Add self.config.query_workers as the 4th argument
        let plan = QueryPlanner::plan(&query, &indexes, rows, self.config.query_workers);

        let res = self
            .executor
            .execute_aggregation(shard_arc, &indexes, plan, &query.aggregations);

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: res.is_ok(),
        });
        res
    }

    pub fn put_subdocument(
        &self,
        col: &str,
        id: &str,
        subcol: &str,
        subid: &str,
        doc: &FireLiteDoc,
    ) -> Result<()> {
        self.put(&subcollection_prefix(col, id, subcol), subid, doc)
    }

    pub fn audit_entries(&self) -> Vec<AuditEntry> {
        self.audit_data.read().unwrap().clone()
    }

    fn record_audit(&self, entry: AuditEntry) {
        let _ = self.audit_tx.send(entry);
    }

    fn index_defs_path(&self) -> PathBuf {
        self.root_path.join("_indices").join("definitions.json")
    }

    // 1. The public version (used by create_index)
    pub(crate) fn persist_index_defs(&self) -> Result<()> {
        let mgr = self.indexes.read().unwrap();
        self.persist_index_defs_with_guard(&mgr)
    }

    // 2. The internal version (used by Drop or Startup)
    fn persist_index_defs_with_guard(&self, mgr: &crate::index::manager::IndexManager) -> Result<()> {
        let mut secondary: HashMap<String, Vec<String>> = HashMap::new();
        let mut fts: HashMap<String, Vec<String>> = HashMap::new();
        let mut composite: Vec<PersistedCompositeIndex> = Vec::new();

        for (col, fields_map) in &mgr.secondary {
            let mut list: Vec<String> = fields_map.keys().cloned().collect();
            list.sort();
            secondary.insert(col.clone(), list);
        }

        for (col, fields_map) in &mgr.fts {
            let mut list: Vec<String> = fields_map.keys().cloned().collect();
            list.sort();
            fts.insert(col.clone(), list);
        }

        for idx in mgr.composite.all_indexes() { 
            composite.push(PersistedCompositeIndex {
                id: idx.definition.id, // CRITICAL: Ensure 'id' is saved!
                collection: idx.definition.collection.clone(),
                fields: idx.definition.fields.iter().map(|f| {
                    (f.field.clone(), if matches!(f.direction, SortDirection::Desc) { "desc".to_string() } else { "asc".to_string() })
                }).collect(),
            });
        }

        let state = PersistedIndexState { secondary, fts, composite };
        let path = self.index_defs_path();
        if let Some(parent) = path.parent() { std::fs::create_dir_all(parent)?; }
        let json = serde_json::to_vec_pretty(&state).map_err(|e| FireLiteError::Corrupt(e.to_string()))?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn restore_index_defs(&self) -> Result<()> {
        let path = self.index_defs_path();
        if !path.exists() { return Ok(()); }
        
        let data = std::fs::read(path)?;
        let state: PersistedIndexState = serde_json::from_slice(&data).unwrap_or_default();

        let mut mgr = self.indexes.write().unwrap();

        // 1. Populate Secondary (Directly in manager, no public API call)
        for (collection, fields) in state.secondary {
            for field in fields {
                mgr.secondary.entry(collection.clone()).or_default()
                    .insert(field, crate::index::secondary_index::SecondaryIndex::default());
            }
        }

        // 2. Populate FTS (Directly in manager)
        for (collection, fields) in state.fts {
            for field in fields {
                mgr.fts.entry(collection.clone()).or_default()
                    .insert(field, crate::index::inverted_index::InvertedIndex::default());
            }
        }

        // 3. Populate Composite (Directly in manager)
        for def in state.composite {
            let fields: Vec<_> = def.fields.into_iter().map(|(field, dir)| {
                let direction = if dir.eq_ignore_ascii_case("desc") { 
                    SortDirection::Desc 
                } else { 
                    SortDirection::Asc 
                };
                crate::index::composite::definition::CompositeField { field, direction }
            }).collect();

            if !fields.is_empty() {
                mgr.composite.restore_index(crate::index::composite::definition::CompositeIndexDefinition {
                    id: def.id,
                    collection: def.collection,
                    fields,
                });
            }
        }
        
        // NO persist_index_defs() call here. We just loaded what was already on disk.
        Ok(())
    }

    pub fn list_indexes(&self, collection: Option<&str>) -> IndexList {
        let mgr = self.indexes.read().unwrap();

        let mut secondary: HashMap<String, Vec<String>> = HashMap::new();
        for (col, fields_map) in &mgr.secondary {
            // If a specific collection is requested, skip others
            if let Some(target) = collection {
                if target != col { continue; }
            }
            let mut fields: Vec<String> = fields_map.keys().cloned().collect();
            fields.sort();
            secondary.insert(col.clone(), fields);
        }

        let mut fts: HashMap<String, Vec<String>> = HashMap::new();
        for (col, fields_map) in &mgr.fts {
            if let Some(target) = collection {
                if target != col { continue; }
            }
            let mut fields: Vec<String> = fields_map.keys().cloned().collect();
            fields.sort();
            fts.insert(col.clone(), fields);
        }

        let mut composite = Vec::new();
        // Use the manager's own collection grouping instead of the catalog
        for idx in mgr.composite.all_indexes() {
            if let Some(target) = collection {
                if target != &idx.definition.collection { continue; }
            }
            composite.push(CompositeIndexInfo {
                id: idx.definition.id,
                collection: idx.definition.collection.clone(),
                fields: idx.definition.fields.iter().map(|f| CompositeIndexFieldInfo {
                    field: f.field.clone(),
                    direction: match f.direction {
                        SortDirection::Asc => "asc".to_string(),
                        SortDirection::Desc => "desc".to_string(),
                    },
                }).collect(),
            });
        }

        composite.sort_by(|a, b| a.collection.cmp(&b.collection).then_with(|| a.id.cmp(&b.id)));

        IndexList {
            simple: secondary.clone(),
            secondary,
            fts,
            composite,
        }
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        // 1. Check if it exists
        {
            let mgr = self.indexes.read().unwrap();
            if mgr
                .secondary
                .get(collection)
                .map_or(false, |m| m.contains_key(field))
            {
                return Ok(());
            }
        }

        // 2. Register the empty index
        {
            let mut mgr = self.indexes.write().unwrap();
            mgr.create_secondary_index(collection, field);
        }

        // 3. Prepare for background backfilling
        let shard_arc = self.get_shard(collection);
        let idx_mgr = Arc::clone(&self.indexes);
        let f_name = field.to_string();
        let col_name = collection.to_string();

        thread::spawn(move || {
            let pointers = {
                let storage = shard_arc.read().unwrap();
                storage.get_physical_index_snapshot()
            };

            // Process in chunks to avoid blocking
            for chunk in pointers.chunks(200) {
                let mut resolved_docs = Vec::new();
                {
                    let storage = shard_arc.read().unwrap();
                    for (key, ptr) in chunk {
                        if let Ok(Some(bytes)) = storage.read_pointer_internal(ptr, false) {
                            resolved_docs.push((key.clone(), bytes));
                        }
                    }
                }

                let mut mgr = idx_mgr.write().unwrap();
                let mut decoded_docs = Vec::new();
                for (full_key, bytes) in resolved_docs {
                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                        if let Some((_, doc_id)) = full_key.split_once(':') {
                            decoded_docs.push((doc_id.to_string(), doc));
                        }
                    }
                }
                IndexingService::backfill_secondary(
                    &mut mgr,
                    &col_name,
                    decoded_docs
                        .iter()
                        .map(|(doc_id, doc)| (doc_id.as_str(), doc))
                        .filter(|(_, doc)| doc.get(&f_name).is_some()),
                );
                thread::yield_now();
            }
        });

        let _ = self.persist_index_defs();
        Ok(())
    }

    pub fn create_fts_index(&self, collection: &str, field: &str) -> Result<()> {
        // 1. Register
        self.indexes
            .write()
            .unwrap()
            .create_fts_index(collection, field);

        // 3. Spawn background thread for backfilling
        let shard_arc = self.get_shard(collection);
        let idx_mgr = Arc::clone(&self.indexes);
        let f_name = field.to_string();
        let col_name = collection.to_string();

        thread::spawn(move || {
            // Step A: Lock, take a snapshot of the pointers, then release IMMEDIATELY
            let pointers = {
                let storage = shard_arc.read().unwrap();
                storage.get_physical_index_snapshot()
            };

            // Step B: Process documents in chunks
            for chunk in pointers.chunks(100) {
                let mut resolved_docs = Vec::new();

                // Briefly lock to read bytes, then release
                {
                    let storage = shard_arc.read().unwrap();
                    for (key, ptr) in chunk {
                        // Use the internal reader (uncached for indexing)
                        if let Ok(Some(bytes)) = storage.read_pointer_internal(ptr, false) {
                            resolved_docs.push((key.clone(), bytes));
                        }
                    }
                } // Lock released here!

                // Step C: Slow CPU work (Decoding/Indexing) happens while Shard is UNLOCKED
                let mut mgr = idx_mgr.write().unwrap();
                let mut decoded_docs = Vec::new();
                for (full_key, bytes) in resolved_docs {
                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                        if let Some((_, doc_id)) = full_key.split_once(':') {
                            decoded_docs.push((doc_id.to_string(), doc));
                        }
                    }
                }
                IndexingService::backfill_fts(
                    &mut mgr,
                    &col_name,
                    decoded_docs
                        .iter()
                        .map(|(doc_id, doc)| (doc_id.as_str(), doc))
                        .filter(|(_, doc)| matches!(doc.get(&f_name), Some(Value::String(_)))),
                );
                // Blob workers can now acquire the Write Lock here because we are between chunks
            }
        });

        let _ = self.persist_index_defs();
        Ok(())
    }

    pub fn create_composite_index(&self, col: &str, fields: Vec<(String, SortDirection)>) -> u32 {
        {
            let mgr = self.indexes.read().unwrap();
            for idx in mgr.indexes_for_collection(col) {
                let same = idx.definition.fields.len() == fields.len()
                    && idx
                        .definition
                        .fields
                        .iter()
                        .zip(fields.iter())
                        .all(|(a, b)| a.field == b.0 && a.direction == b.1);
                if same {
                    return idx.definition.id;
                }
            }
        }

        let def = CompositeIndexDefinition::new(col).with_fields(fields);
        let index_id = self.indexes.write().unwrap().create_index(def);

        let shard_arc = self.get_shard(col);
        let idx_mgr = Arc::clone(&self.indexes);
        let persist_ptr = Arc::clone(&self.index_storage); // <--- Required for persistence

        thread::spawn(move || {
            let pointers = {
                let storage = shard_arc.read().unwrap();
                storage.get_physical_index_snapshot()
            };

            for chunk in pointers.chunks(500) {
                let mut resolved_data = Vec::new();
                {
                    let storage = shard_arc.read().unwrap();
                    for (key, ptr) in chunk {
                        if let Ok(Some(bytes)) = storage.read_pointer_internal(ptr, false) {
                            resolved_data.push((key.clone(), bytes));
                        }
                    }
                }

                let mut mgr = idx_mgr.write().unwrap();
                let mut persist = persist_ptr.lock().unwrap();

                if let Some(composite_idx) = mgr.composite.get_mut(index_id) {
                    for (full_key, bytes) in resolved_data {
                        if let Some(doc) = FireLiteDoc::decode(&bytes) {
                            if let Some((_, doc_id)) = full_key.split_once(':') {
                                // 1. Insert into RAM
                                composite_idx.index_document(doc_id, &doc);

                                // 2. Insert LEAN KEY into index.log
                                if let Some(vals) = composite_idx.document_values(&doc) {
                                    let key_bytes =
                                        crate::index::composite::key_encoder::encode_composite_key(
                                            &composite_idx.definition,
                                            &vals,
                                            doc_id,
                                        );
                                    let _ = persist.insert(
                                        index_id,
                                        key_bytes.to_vec(),
                                        doc_id.to_string(),
                                    );
                                }
                            }
                        }
                    }
                }

                // Release locks to allow benchmark/queries to slip in!
                drop(persist);
                drop(mgr);
                thread::yield_now();
            }
        });

        let _ = self.persist_index_defs();
        index_id
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
        // 1. Signal workers to stop
        if let Some(tx) = self.maintenance_stop.lock().unwrap().take() { let _ = tx.send(()); }
        if let Some(tx) = self.audit_stop.lock().unwrap().take() { let _ = tx.send(()); }

        // 2. Save Index State
        let snapshot_path = self.root_path.join("_indices").join("ram_indexes.bin");
        
        if let Ok(mgr) = self.indexes.read() {
            // FIX: export_state now takes 0 arguments
            if let Ok(bytes) = mgr.export_state() {
                let _ = std::fs::write(snapshot_path, bytes);
            }
            // Save JSON definitions
            let _ = self.persist_index_defs_with_guard(&mgr); 
        }

        // 3. PARALLEL SHARD FLUSHING
        // Move shards out to ensure ownership during the flush
        let shards_to_flush: Vec<Arc<RwLock<StorageEngine>>> = {
            let mut shards_map = self.shards.write().unwrap();
            shards_map.drain().map(|(_, shard)| shard).collect()
        };

        let flush_handles: Vec<_> = shards_to_flush
            .into_iter()
            .map(|shard| {
                thread::spawn(move || {
                    if let Ok(mut storage) = shard.write() {
                        let _ = storage.checkpoint_inlined_data();
                        let _ = storage.flush_all();
                    }
                })
            })
            .collect();

        for h in flush_handles { let _ = h.join(); }

        // 4. JOIN CONTROLLER THREADS
        if let Some(h) = self.maintenance_handle.lock().unwrap().take() { let _ = h.join(); }
        if let Some(h) = self.audit_handle.lock().unwrap().take() { let _ = h.join(); }
    }
}

// --- Internal Helper Functions ---

fn doc_key(collection: &str, doc_id: &str) -> String {
    format!("{}:{}", collection, doc_id)
}

fn subcollection_prefix(collection: &str, doc_id: &str, subcollection: &str) -> String {
    format!("{}:{}/{}", collection, doc_id, subcollection)
}

// Helper to build a key without format!()
fn fast_doc_key(col: &str, id: &str) -> String {
    // KEY_BUFFER.with(|b| {
    //     let mut buffer = b.borrow_mut();
    //     buffer.clear();
    //     buffer.push_str(col);
    //     buffer.push(':');
    //     buffer.push_str(id);
    //     buffer.clone() // Still one clone here, but better than format!
    // })
    let mut s = String::with_capacity(col.len() + id.len() + 1);
    s.push_str(col);
    s.push(':');
    s.push_str(id);
    s
}
