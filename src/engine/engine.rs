use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock, Once};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};

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
use crate::storage::wal::WalOp;
use crate::storage::blob::{BlobManager, BlobWork};
use crate::storage::engine::{StorageEngine, Pointer};
// use crate::config::DurabilityMode;

use crate::util::lock::SafeLock;

use std::cell::RefCell;
use rayon::prelude::*;

static RAYON_INIT: Once = Once::new();

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

struct ShardWork {
    ops: Vec<WalOp>,
    keys: Vec<Arc<str>>,
    events: Vec<(String, ChangeEvent)>,
    index_puts: Vec<(String, FireLiteDoc)>,
    blob_queue_items: Vec<BlobWork>,
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

pub(crate) enum IndexOp {
    Update {
        collection: String,
        puts: Arc<Vec<(String, FireLiteDoc)>>, 
        deletes: Vec<(String, FireLiteDoc)>,
    },
}

pub struct FireLite {
    root_path: PathBuf,
    pub(crate) config: FireLiteConfig,
    pub(crate) shards: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>>, // The only storage
    index_storage: Arc<Mutex<IndexStorage>>,
    pub(crate) indexes: Arc<RwLock<IndexManager>>,
    executor: ParallelQueryExecutor,
    tx_lock: Mutex<()>,
    listeners: Mutex<HashMap<String, Vec<Sender<ChangeEvent>>>>,
    // doc_versions: RwLock<HashMap<String, u64>>,
    pub(crate) doc_versions: Vec<RwLock<HashMap<Arc<str>, u64>>>,
    global_version: AtomicU64,
    security_rules: RwLock<Vec<SecurityRule>>,
    audit_data: Arc<RwLock<Vec<AuditEntry>>>,
    audit_tx: Sender<AuditEntry>,
    #[allow(dead_code)]
    pub(crate) index_tx: Sender<IndexOp>,
    pub(crate) blob_tx: crossbeam_channel::Sender<BlobWork>,
    system_stop: Mutex<Option<Sender<()>>>,
    system_handle: Mutex<Option<thread::JoinHandle<()>>>,
    blob_stop_tx: Mutex<Option<Sender<()>>>, 
    blob_worker_handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) trigger_blob_flush: Arc<std::sync::atomic::AtomicBool>,
}

impl FireLite {
    pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self> {

        let query_threads = config.query_workers.max(1).min(8);
        RAYON_INIT.call_once(|| {
            rayon::ThreadPoolBuilder::new()
                .num_threads(query_threads)
                .thread_name(|i| format!("fl-query-{}", i))
                .build_global()
                .unwrap();
        });

        let root_path = path.as_ref().to_path_buf();
        std::fs::create_dir_all(&root_path)?;

        // 1. Initialize Channels
        let (index_tx, index_rx) = channel::<IndexOp>();
        let (audit_tx, audit_rx) = channel::<AuditEntry>();
        let (system_stop_tx, system_stop_rx) = channel::<()>();
        // let (transformation_tx, transformation_rx) = std::sync::mpsc::channel::<TransformTask>();
        let (btx, _brx) = crossbeam_channel::bounded::<BlobWork>(10000);
        let (stop_tx, stop_rx) = std::sync::mpsc::channel::<()>();

        // 2. Initialize Core State with EXPLICIT TYPES
        let indexes: Arc<RwLock<IndexManager>> = Arc::new(RwLock::new(IndexManager::default()));
        let shards: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>> = Arc::new(RwLock::new(HashMap::new()));
        
        let trigger_flush = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let trigger_for_system = Arc::clone(&trigger_flush);
        // let trigger_for_blobs = Arc::clone(&trigger_flush);

        // 3. Initialize Index Storage
        let index_dir = root_path.join("_indices");
        let index_log_path = index_dir.join("index.log").to_string_lossy().to_string();
        let snapshot_dir = index_dir.join("snapshots").to_string_lossy().to_string();
        let index_storage = Arc::new(Mutex::new(
            IndexStorage::open(&index_log_path, &snapshot_dir).map_err(|e| FireLiteError::Io(e))?,
        ));

        // --- WORKER 1: PERSISTENT INDEX WORKER ---
        let idx_clone = Arc::clone(&indexes);
        let storage_persist = Arc::clone(&index_storage);
        thread::spawn(move || {
            while let Ok(op) = index_rx.recv() {
                match op {
                    IndexOp::Update { collection, puts, deletes } => {
                        let mut mgr = match idx_clone.write() { Ok(g) => g, Err(_) => break };
                        let mut persist = match storage_persist.lock() { Ok(g) => g, Err(_) => break };
                        for (id, doc) in puts.iter() {
                            IndexingService::apply_put(&mut mgr, &collection, id, doc);
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

        // --- WORKER 2: SYSTEM WORKER (Audit + Maintenance) ---
        let log_path = config.audit_log_path.clone().unwrap_or_else(|| root_path.join("audit.log").to_string_lossy().to_string());
        let mut audit_file = if config.enable_audit_log { Some(std::fs::OpenOptions::new().create(true).append(true).open(log_path)?) } else { None };
        let audit_data = Arc::new(RwLock::new(Vec::new()));
        let audit_data_clone = Arc::clone(&audit_data);
        let shards_sys_clone = Arc::clone(&shards);
        let index_sys_ptr = Arc::clone(&index_storage);

        let system_handle_thread = thread::spawn(move || {
            let mut last_maint = Instant::now();
            loop {
                while let Ok(entry) = audit_rx.try_recv() {
                    if let Ok(mut history) = audit_data_clone.write() { history.push(entry.clone()); }
                    if let Some(file) = audit_file.as_mut() {
                        let _ = writeln!(file, "[{:?}] op={:?} col={} doc={:?} ok={}", SystemTime::now(), entry.op, entry.collection, entry.doc_id.as_deref().unwrap_or("<none>"), entry.ok);
                    }
                }
                if last_maint.elapsed() >= Duration::from_secs(5) {
                    let active_shards: Vec<Arc<RwLock<StorageEngine>>> = shards_sys_clone.read().unwrap().values().cloned().collect();
                    for s in active_shards {
                        if let Ok(storage) = s.try_read() {
                            if let Some(ref bm) = storage.blob_manager { let _ = bm.file().sync_data(); }
                        }
                        if let Ok(mut storage) = s.try_write() {
                            let _ = storage.run_background_maintenance(); 
                            storage.purge_old_tombstones(Duration::from_secs(86400));
                            
                            // 2. TRIGGER: If pending blob bytes > 16MB, touch the blob worker
                            if storage.total_pending_blob_bytes.load(Ordering::Relaxed) > 16 * 1024 * 1024 {
                                trigger_for_system.store(true, Ordering::Release);
                            }
                        }
                    }
                    if let Ok(mut persist) = index_sys_ptr.try_lock() {
                        if let Ok(_) = persist.snapshot(1) { let _ = persist.reset_log(); }
                    }
                    trigger_for_system.store(true, Ordering::Release);
                    last_maint = Instant::now();
                }
                if system_stop_rx.recv_timeout(Duration::from_millis(500)).is_ok() { break; }
            }
        });

        // --- WORKER 3: BLOB WORKER (IO Queue) ---
        let shards_blob_clone = Arc::clone(&shards);
        let trigger_for_blobs_w3 = Arc::clone(&trigger_flush);
        let blob_worker_handle = thread::spawn(move || {
            loop {
                let shutting_down = stop_rx.try_recv().is_ok();
                let triggered = trigger_for_blobs_w3.swap(false, Ordering::Acquire);
                
                let active_shards = { shards_blob_clone.read().unwrap().values().cloned().collect::<Vec<_>>() };
                let mut processed_any = false;

                for shard_arc in &active_shards {
                    let batch = {
                        if let Ok(mut shard) = shard_arc.try_write() {
                            let mut b = Vec::with_capacity(128);
                            while let Some(work) = shard.blob_flush_queue.pop_front() {
                                b.push(work);
                                if b.len() >= 128 { break; }
                            }
                            b
                        } else { Vec::new() }
                    };

                    if batch.is_empty() { continue; }
                    processed_any = true;

                    let bm = shard_arc.read().unwrap().blob_manager.clone().unwrap();
                    let mut completed_keys = Vec::with_capacity(batch.len());

                    for work in batch {
                        if let BlobWork::PutRaw { key, offset, data, skeleton, timestamp, len, ..} = work {
                            // 1. FAST PHYSICAL WRITE (OS Page Cache)
                            if bm.write_at(&data, offset).is_ok() {
                                completed_keys.push((key, skeleton, timestamp, len as usize));
                            }
                        }
                    }

                    // 2. ATOMIC META UPDATE (One lock per batch)
                    if !completed_keys.is_empty() {
                        if let Ok(mut shard) = shard_arc.write() {
                            for (key, skeleton, timestamp, len) in completed_keys {
                                shard.total_pending_blob_bytes.fetch_sub(len, Ordering::Relaxed);
                                
                                // Check if this is still the active version of the doc
                                if let Some(Pointer::BlobPending(active_doc)) = shard.index.get(&key) {
                                    if active_doc.get_logical_time() == timestamp {
                                        // Transition: Pending (Arc) -> Inlined (Bytes)
                                        // We use the skeleton bytes already calculated by main thread
                                        shard.update_index_entry(key, Some(Pointer::Inlined(skeleton)));
                                        // NOTE: We DO NOT write to WAL here. Main thread already did it.
                                    }
                                }
                            }
                        }
                    }
                    thread::yield_now(); 
                }

                if !processed_any && !triggered {
                    thread::sleep(Duration::from_millis(20));
                }
                if shutting_down && !processed_any { break; }
            }
        });

        let mut doc_versions = Vec::with_capacity(32);
        for _ in 0..32 {
            doc_versions.push(RwLock::new(HashMap::new()));
        }
        // 5. Assemble and Return
        let db = Self {
            root_path: root_path.clone(),
            config: config.clone(),
            shards,
            index_storage: Arc::clone(&index_storage),
            indexes,
            executor: ParallelQueryExecutor::new(config.query_workers),
            tx_lock: Mutex::new(()),
            listeners: Mutex::new(HashMap::new()),
            doc_versions,
            global_version: AtomicU64::new(1),
            security_rules: RwLock::new(Vec::new()),
            audit_tx,
            index_tx,
            blob_tx: btx.clone(),
            audit_data,
            system_stop: Mutex::new(Some(system_stop_tx)),
            system_handle: Mutex::new(Some(system_handle_thread)),
            trigger_blob_flush: trigger_flush,
            blob_stop_tx: Mutex::new(Some(stop_tx)),
            blob_worker_handle: Mutex::new(Some(blob_worker_handle)),
        };

        let _ = db.restore_index_defs();

        // 6. ORCHESTRATED BACKGROUND RECOVERY
        let shards_ptr = Arc::clone(&db.shards);
        let indexes_ptr = Arc::clone(&db.indexes);
        let persist_ptr = Arc::clone(&db.index_storage);
        let config_thread = config.clone();
        
        // Capture the new Crossbeam Sender
        let blob_tx_thread = db.blob_tx.clone(); 
        
        let root_scan = root_path.clone();
        let snapshot_path = root_path.join("_indices").join("ram_indexes.bin");

        thread::spawn(move || {
            // --- STEP A: Try to load RAM Snapshot FIRST ---
            let mut snapshot_loaded = false;
            if snapshot_path.exists() {
                if let Ok(bytes) = std::fs::read(&snapshot_path) {
                    let mut mgr = indexes_ptr.write().unwrap();
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
                        // Ignore system folders
                        if !name.starts_with('_') && name != "snapshots" {
                            discovered.push(name);
                        }
                    }
                }
            }

            // --- STEP C: Load Shards (and Rebuild if needed) ---
            for col_name in discovered {
                let path = root_scan.join(&col_name);
                // StorageEngine::open now takes the logical name as well
                if let Ok(mut storage) = StorageEngine::open(path, &config_thread, col_name.clone()) {
                    
                    // Use the Crossbeam Sender for background blob offloading
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
                    persist.log_path().to_string()
                };
                let mut mgr = indexes_ptr.write().unwrap();
                let _ = crate::index::storage::index_recovery::replay_log(&log_path, &mut mgr.composite);
            }
            
            // Note: This thread terminates naturally after recovery, 
            // keeping the idle thread count low.
        });

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

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        // 1. Security Check
        if !mutations.iter().all(|m| self.allowed(self.get_col(m), AccessOp::Batch)) {
            self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: false });
            return Err(FireLiteError::Corrupt("Security Denied".into()));
        }

        let res = self.write_batch_internal(mutations);

        // 2. Async Audit
        self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: res.is_ok() });
        res
    }

    fn write_batch_internal(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        let threshold = self.config.value_blob_threshold_bytes;

        let mut shard_map: HashMap<String, ShardWork> = HashMap::new();

        for m in mutations {
            // 1. Resolve basic info immediately
            let (col, doc_id, mut doc, is_delete) = match m {
                BatchMutation::Put { collection, doc_id, doc } => (collection, doc_id, doc, false),
                BatchMutation::Patch { collection, doc_id, updates } => {
                    let mut current_doc = self.get(&collection, &doc_id)?.unwrap_or_default();
                    for (k, v) in updates { current_doc.insert(k, v); }
                    (collection, doc_id, current_doc, false)
                }
                BatchMutation::Delete { collection, doc_id } => {
                    (collection, doc_id, FireLiteDoc::default(), true)
                }
            };

            // 2. COMPUTE KEY ONCE
            // let key = fast_doc_key(&col, &doc_id);
            let key_arc: Arc<str> = Arc::from(fast_doc_key(&col, &doc_id).as_str());

            let work = shard_map.entry(col.clone()).or_insert_with(|| ShardWork {
                ops: Vec::new(), keys: Vec::new(), events: Vec::new(), 
                index_puts: Vec::new(), blob_queue_items: Vec::new(),
            });

            if is_delete {
                let key_clone = key_arc.clone();
                work.ops.push(WalOp::Delete { key: key_arc.to_string(), timestamp: now });
                work.keys.push(key_arc);
                work.events.push((col, ChangeEvent { path: key_clone.to_string(), kind: ChangeKind::Delete }));
                continue;
            }

            doc._time = now;
            
            // 3. REUSE KEY for Blobs
            let blob_work = {
                let shard = self.get_shard(&col);
                let guard = shard.read().unwrap();
                guard.blob_manager.as_ref()
                    .map(|bm| bm.extract_blobs_raw(&col, &key_arc, &mut doc, threshold))
                    .unwrap_or_default()
            };

            // 4. USE BUFFERED ENCODING
            let skeleton_bytes = doc.encode_buffered();
            
            // REUSE KEY for WAL and Index
            work.ops.push(WalOp::PutInlined { key: key_arc.to_string(), value: skeleton_bytes });
            work.keys.push(key_arc); // Reuses the same String allocation
            work.index_puts.push((doc_id, doc));
            work.events.push((col, ChangeEvent { path: work.keys.last().unwrap().to_string(), kind: ChangeKind::Put }));
            
            for b in blob_work { work.blob_queue_items.push(b); }
        }

        // --- APPLY SHARD CHANGES ---
        for (col_name, work) in shard_map {
            let shard_arc = self.get_shard(&col_name);
            {
                let mut shard = shard_arc.write().unwrap();
                
                if !work.ops.is_empty() {
                    let tx_id = shard.next_tx_id;
                    shard.next_tx_id += 1;
                    shard.wal.append_batch_fast(tx_id, &work.ops, false)?;
                }

                for (doc_id, doc) in &work.index_puts {
                    let k = fast_doc_key(&col_name, &doc_id);
                    shard.update_index_entry(k, Some(Pointer::BlobPending(Arc::new(doc.clone()))));
                }
                
                // ... (Rest of the loop: queueing blobs, same as before) ...
                let mut b_bytes = 0;
                for b in work.blob_queue_items {
                    if let BlobWork::PutRaw { len, .. } = &b { b_bytes += *len as usize; }
                    shard.blob_flush_queue.push_back(b);
                }
                shard.total_pending_blob_bytes.fetch_add(b_bytes, Ordering::Relaxed);
            }
            
            self.trigger_blob_flush.store(true, Ordering::Release);
            
            // Notify Indexer (Worker 1)
            if !work.index_puts.is_empty() {
                let _ = self.index_tx.send(IndexOp::Update { 
                    collection: col_name, 
                    puts: Arc::new(work.index_puts), 
                    deletes: vec![] 
                });
            }
            
            self.bump_versions_by_keys(work.keys);
            for (c, e) in work.events { self.notify_watchers(&c, e); }
        }
        
        // 5. CONDITIONAL AUDIT (Zero overhead if disabled)
        if self.config.enable_audit_log {
            self.record_audit(AuditEntry { 
                op: AccessOp::Batch, 
                collection: "<sharded>".into(), 
                doc_id: None, 
                ok: true 
            });
        }

        Ok(())
    }

    // Fix signature for public helper
    pub fn process_doc_blobs(
        &self, 
        collection: &str,
        key: &str, // Added key
        doc: &mut FireLiteDoc, 
        blob_manager: &BlobManager,
        threshold: usize, 
    ) -> Vec<BlobWork> {
        blob_manager.extract_blobs(collection, key, doc, threshold)
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
        
        if let Some(mut doc) = res {
            self.resolve_doc(&mut doc, collection)?;
            return Ok(Some(doc));
        }

        Ok(None)
    }

    pub fn put(&self, col: &str, id: &str, doc: &FireLiteDoc) -> Result<()> {
        self.write_batch(vec![BatchMutation::Put {
            collection: col.into(),
            doc_id: id.into(),
            doc: doc.clone(),
        }])
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
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix(&format!("{}:", query.collection));

        let plan = QueryPlanner::plan(&query, &indexes, rows, self.config.query_workers);

        // SIMPLE CALL: No blob_file or encryption passed here!
        let results = self.executor.execute(shard_arc, &indexes, plan)?;

        // AUDIT RESULT
        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true, // prev ->results.is_ok()
        });

        // --- NEW: AUTO-RESOLVE BLOB LINKS FOR ALL RESULTS ---
        // for (_, doc) in &mut results {
        //     self.resolve_doc(doc, &query.collection)?;
        // }

        // res
        Ok(results)
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

        let mut q = query.clone();
        q.projection = fields.to_vec();

        let shard_arc = self.get_shard(&q.collection);
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix(&format!("{}:", q.collection));

        let plan = QueryPlanner::plan(&q, &indexes, rows, self.config.query_workers);

        // SIMPLE CALL: Worker handles blob resolution internally
        let results = self.executor.execute_projected(shard_arc, &indexes, plan)?;

        // 5. Audit & Return
        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: q.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(results)
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
            let s = shard.read().unwrap();
            // Count documents
            stats.insert(format!("{}_count", name), s.count_prefix(""));
            
            // NEW: Monitor the background queue size
            // This resolves the "never read" warning
            stats.insert(
                format!("{}_pending_blob_bytes", name), 
                s.total_pending_blob_bytes.load(Ordering::Relaxed)
            );
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
        // self.doc_versions.read().unwrap().get(key).cloned()
        let bucket = (fxhash::hash64(key) % 32) as usize;
        let shard = self.doc_versions[bucket].read().unwrap();
        shard.get(key).copied()
    }

    pub(crate) fn bump_versions_by_keys(&self, keys: Vec<Arc<str>>) {
        for key in keys {
            // Use a simple hash to pick a bucket
            let bucket = (fxhash::hash64(&key) % 32) as usize;
            let mut shard = self.doc_versions[bucket].write().unwrap();
            shard.insert(key, self.global_version.fetch_add(1, Ordering::Relaxed));
        }
    }

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

        // Capture encryption key for the thread
        let enc_key = self.config.encryption_key.clone();

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
                    if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
                        let _ = resolve_doc_static(&mut doc, &shard_arc, enc_key.as_deref());
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
        
        let enc_key = self.config.encryption_key.clone();

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
                        if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
                            let _ = resolve_doc_static(&mut doc, &shard_arc, enc_key.as_deref());
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

    /// Scans a document for BlobLinks and fetches the data from disk.
    /// This makes the "Skeleton" transparent to the user.
    pub fn resolve_document_blobs(&self, doc: &mut FireLiteDoc, collection: &str) -> Result<()> {
        self.resolve_doc(doc, collection)
    }

    // Helper for projected resolution
    fn resolve_single_value_blob(&self, collection: &str, offset: u64, len: u32) -> Result<Value> {
        let shard_arc = self.get_shard(collection);
        let blob_manager = {
            let shard = shard_arc.read().unwrap();
            shard
                .blob_manager
                .as_ref()
                .cloned()
                .ok_or_else(|| FireLiteError::StorageError("Blob manager missing".into()))?
        };
        let data = blob_manager.read_at(offset, len)?;

        if let Ok(s) = String::from_utf8(data.clone()) {
            Ok(Value::String(s))
        } else {
            Ok(Value::Binary(data))
        }
    }

    /// Internal helper: Resolves all Value::BlobLink fields in a document
    /// by reading from the collection's blob file.
    fn resolve_doc(&self, doc: &mut FireLiteDoc, collection: &str) -> Result<()> {
        // 1. Identify valid BlobLinks (ignore placeholders with offset u64::MAX)
        let needs_resolve = doc.fields.iter().any(|(_, v)| {
            matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX)
        });
        if !needs_resolve { return Ok(()); }

        let shard_arc = self.get_shard(collection);

        // 2. Snapshot the Shard state: Disk Manager + Memory Queue
        let (blob_manager, queue_snapshot) = {
            let shard = shard_arc.read().unwrap();
            
            // Build a temporary map of Offset -> Data from the RAM queue
            let mut in_memory = HashMap::new();
            for work in &shard.blob_flush_queue {
                if let BlobWork::PutRaw { offset, data, .. } = work {
                    in_memory.insert(*offset, Arc::clone(data));
                }
            }
            (shard.blob_manager.clone(), in_memory)
        };

        let bm = blob_manager.ok_or(FireLiteError::StorageError("No blob manager".into()))?;

        // 3. Filter fields that actually need resolving
        let blob_values: Vec<&mut Value> = doc.fields.iter_mut()
            .map(|(_, v)| v)
            .filter(|v| matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX))
            .collect();

        if blob_values.is_empty() { return Ok(()); }

        // 4. ADAPTIVE RESOLUTION (Memory-First Logic)
        if blob_values.len() <= 2 {
            // FAST PATH: Sequential resolution
            for val in blob_values {
                if let Value::BlobLink { offset, len } = *val {
                    // Priority 1: RAM Queue, Priority 2: Disk
                    let data = match queue_snapshot.get(&offset) {
                        Some(arc_bytes) => (**arc_bytes).clone(),
                        None => bm.read_at(offset, len)?,
                    };
                    *val = self.inflate_bytes(data);
                }
            }
        } else {
            // PARALLEL PATH: Using Rayon for multi-blob documents
            // We must wrap the snapshots in Arc to share across Rayon threads if necessary,
            // but here they are already Arcs or clones.
            let bm_ref = &bm;
            let queue_ref = &queue_snapshot;
            
            blob_values.into_par_iter().try_for_each(|val| -> Result<()> {
                if let Value::BlobLink { offset, len } = *val {
                    let data = match queue_ref.get(&offset) {
                        Some(arc_bytes) => (**arc_bytes).clone(),
                        None => bm_ref.read_at(offset, len)?,
                    };
                    *val = if let Ok(s) = String::from_utf8(data.clone()) {
                        Value::String(s)
                    } else {
                        Value::Binary(data)
                    };
                }
                Ok(())
            })?;
        }
        Ok(())
    }
    
    // Helper to reduce code duplication
    fn inflate_bytes(&self, data: Vec<u8>) -> Value {
        if let Ok(s) = String::from_utf8(data.clone()) {
            Value::String(s)
        } else {
            Value::Binary(data)
        }
    }

    pub fn vacuum(&self, collection: &str) -> Result<()> {
        let shard_arc = self.get_shard(collection);
        let mut shard = shard_arc.write().unwrap();
        
        let blob_path = shard.base_dir().join("blobs.dat");
        let temp_path = shard.base_dir().join("blobs.tmp");
        
        if !blob_path.exists() { return Ok(()); }

        let mut new_blob_file = std::fs::OpenOptions::new()
            .create(true).write(true).truncate(true).open(&temp_path)?;
        
        let mut new_offset = 0u64;
        let mut updates = Vec::new();

        // 1. Scan the index for all documents in this shard
        for (key, pointer) in &shard.index {
            // We need to read the document to find BlobLinks inside it
            if let Some(bytes) = shard.read_pointer(pointer)? {
                if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
                    let doc_changed = false;
                    
                    for (_, value) in &mut doc.fields {
                        if let Value::BlobLink { offset, len } = *value {
                            let data = self.resolve_single_value_blob(collection, offset, len)?;
                            let raw_bytes = match data {
                                Value::String(s) => s.into_bytes(),
                                Value::Binary(b) => b,
                                _ => continue,
                            };

                            new_blob_file.write_all(&raw_bytes)?;
                            *value = Value::BlobLink { offset: new_offset, len: raw_bytes.len() as u32 };
                            new_offset += raw_bytes.len() as u64;
                        }
                    }

                    if doc_changed {
                        updates.push((key.clone(), doc.encode()));
                    }
                }
            }
        }

        // 2. Commit the new blob file
        drop(new_blob_file);
        std::fs::rename(&temp_path, &blob_path)?;
        
        // Re-open the blob file handle in the shard
        // let new_file = std::fs::OpenOptions::new().read(true).append(true).open(&blob_path)?;
        let new_file = std::fs::OpenOptions::new().read(true).append(true).open(&blob_path)?;
        shard.blob_manager = Some(Arc::new(BlobManager::new(
            Arc::new(new_file),
            new_offset,
        )));

        // 3. Update the Skeletons in the Segment
        // (This triggers a standard storage Put for the updated skeletons)
        for (key, new_bytes) in updates {
            shard.put(key, &new_bytes)?;
        }

        Ok(())
    }

    pub fn db_name(&self) -> String {
        self.root_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "firelite_default".to_string())
    }

    fn record_audit(&self, entry: AuditEntry) {
        // Respect global config toggle
        if self.config.enable_audit_log {
            let _ = self.audit_tx.send(entry);
        }
    }

    pub fn flush_blobs(&self) {
        self.trigger_blob_flush.store(true, Ordering::Release);
    }

    pub fn get_collection_version(&self, collection: &str) -> i64 {
        let shard = self.get_shard(collection);
        let guard = shard.read().unwrap();
        
        // Strategy: Scan the RAM index for the highest timestamp.
        // Since this is in RAM, it's extremely fast.
        guard.index.values().map(|ptr| {
            match ptr {
                Pointer::Inlined(bytes) => {
                    // Extract _time from bytes [2..10] based on your VERSION 3 format
                    i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]))
                }
                Pointer::BlobPending(doc) => doc.get_logical_time(),
                Pointer::Deleted { timestamp } => *timestamp,
                _ => 0,
            }
        }).max().unwrap_or(0)
    }

    pub fn get_version_map(&self) -> std::collections::HashMap<String, i64> {
        let mut map = std::collections::HashMap::new();
        for col in self.list_collections().unwrap_or_default() {
            map.insert(col.clone(), self.get_collection_version(&col));
        }
        map
    }

}

impl Drop for FireLite {
    fn drop(&mut self) {
        // 1. Stop Audit/Maintenance
        if let Some(tx) = self.system_stop.lock().unwrap().take() { let _ = tx.send(()); }

        // 2. Shut down refinery/transformation worker
        // if let Some(tx) = self.transformation_tx.lock().unwrap().take() { drop(tx); }
        // if let Some(h) = self.transformation_handle.lock().unwrap().take() { let _ = h.join(); }

        // 3. FLUSH BLOB WORKER (The "Touch" Sequence)
        // Set the trigger to wake up the worker, then send the stop signal
        self.trigger_blob_flush.store(true, Ordering::Release);
        if let Some(tx) = self.blob_stop_tx.lock().unwrap().take() { let _ = tx.send(()); }
        if let Some(h) = self.blob_worker_handle.lock().unwrap().take() { let _ = h.join(); }

        // 4. SAVE RAM INDEXES
        let snapshot_path = self.root_path.join("_indices").join("ram_indexes.bin");
        if let Ok(mgr) = self.indexes.read() {
            if let Ok(bytes) = mgr.export_state() { let _ = std::fs::write(snapshot_path, bytes); }
            let _ = self.persist_index_defs_with_guard(&mgr); 
        }

        // 5. FINAL SHARD FLUSH
        let shards_to_flush: Vec<Arc<RwLock<StorageEngine>>> = {
            self.shards.read().unwrap().values().cloned().collect()
        };

        for shard in shards_to_flush {
            if let Ok(mut storage) = shard.write() {
                // Ensure the queue is fully drained one last time
                let _ = storage.drain_blob_queue(); 
                // Rewrite the WAL to ensure skeletons (not full blobs) are persisted
                let _ = storage.rewrite_wal_snapshot();
                let _ = storage.flush_all(); 
            }
        }

        if let Some(h) = self.system_handle.lock().unwrap().take() { let _ = h.join(); }
    }
}

// --- Internal Helper Functions ---
pub(crate) fn resolve_doc_static(
    doc: &mut FireLiteDoc, 
    shard_arc: &Arc<RwLock<StorageEngine>>, 
    _enc_secret: Option<&str>
) -> Result<()> {
    // 1. Placeholder check
    let needs_resolve = doc.fields.iter().any(|(_, v)| {
        matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX)
    });
    if !needs_resolve { return Ok(()); }

    // 2. RAM-First Access logic
    let (blob_manager, queue_snapshot) = {
        let shard = shard_arc.read().unwrap();
        let mut in_memory = HashMap::new();
        for work in &shard.blob_flush_queue {
            if let BlobWork::PutRaw { offset, data, .. } = work {
                in_memory.insert(*offset, Arc::clone(data));
            }
        }
        (shard.blob_manager.clone(), in_memory)
    };

    let bm = blob_manager.ok_or(FireLiteError::StorageError("No blob manager".into()))?;

    // 3. Resolve Fields
    let blob_values: Vec<&mut Value> = doc.fields.iter_mut()
        .map(|(_, v)| v)
        .filter(|v| matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX))
        .collect();

    for val in blob_values {
        if let Value::BlobLink { offset, len } = *val {
            // Priority: RAM Queue -> Disk
            let data = match queue_snapshot.get(&offset) {
                Some(bytes) => (**bytes).clone(),
                None => bm.read_at(offset, len)?,
            };

            *val = if let Ok(s) = String::from_utf8(data.clone()) {
                Value::String(s)
            } else {
                Value::Binary(data)
            };
        }
    }
    Ok(())
}

fn doc_key(collection: &str, doc_id: &str) -> String {
    format!("{}:{}", collection, doc_id)
}

fn subcollection_prefix(collection: &str, doc_id: &str, subcollection: &str) -> String {
    format!("{}:{}/{}", collection, doc_id, subcollection)
}

// Helper to build a key without format!()
fn fast_doc_key(col: &str, id: &str) -> String {
    let mut s = String::with_capacity(col.len() + id.len() + 1);
    s.push_str(col);
    s.push(':');
    s.push_str(id);
    s
}
