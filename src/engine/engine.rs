use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock, Once};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};

use hashbrown::{HashMap, HashSet};

use crate::config::{DurabilityMode, HakoConfig};
use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::error::{HakoError, Result};
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::index::service::IndexingService;
use crate::index::storage::index_storage::IndexStorage;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::plan_cache::PlanCache;
use crate::query::query::Query;
use crate::query::builder::Collection;
use crate::storage::wal::WalOp;
use crate::storage::crypto::EncryptionContext;
use crate::storage::blob::{BlobManager, BlobWork};
use crate::storage::engine::{StorageEngine, Pointer};
// use crate::config::DurabilityMode;

use crate::util::lock::SafeLock;

use std::cell::RefCell;
// use rayon::prelude::*;

static RAYON_INIT: Once = Once::new();

thread_local! {
    // A reusable buffer for serialization to avoid allocations
    static WRITE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 * 1024));
    // A reusable buffer for building keys to avoid format!()
    static KEY_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(128));
}

// --- Write-path phase accounting (ponytail) ---
// Always-on accumulators: a handful of Instant reads + Relaxed adds per
// batch (~0.1µs against 20µs+ batches). Read via `write_stats_report()`
// (resets), e.g. from the `hk_debug_write_stats` FFI hook or a test.
pub(crate) struct WritePhaseStats {
    pub batches: AtomicU64,
    pub mutations: AtomicU64,
    pub total_ns: AtomicU64,
    pub encode_ns: AtomicU64,
    pub wal_ns: AtomicU64,
    pub apply_ns: AtomicU64,
    pub index_send_ns: AtomicU64,
    pub versions_ns: AtomicU64,
    pub watchers_ns: AtomicU64,
    pub cache_ns: AtomicU64,
    pub blob_ns: AtomicU64,
    pub encdoc_ns: AtomicU64,
}
pub(crate) static WRITE_STATS: WritePhaseStats = WritePhaseStats {
    batches: AtomicU64::new(0),
    mutations: AtomicU64::new(0),
    total_ns: AtomicU64::new(0),
    encode_ns: AtomicU64::new(0),
    wal_ns: AtomicU64::new(0),
    apply_ns: AtomicU64::new(0),
    index_send_ns: AtomicU64::new(0),
    versions_ns: AtomicU64::new(0),
    watchers_ns: AtomicU64::new(0),
    cache_ns: AtomicU64::new(0),
    blob_ns: AtomicU64::new(0),
    encdoc_ns: AtomicU64::new(0),
};

/// Human-readable phase table + reset. All figures per batch unless noted.
pub fn write_stats_report() -> String {
    let b = WRITE_STATS.batches.load(Ordering::Relaxed).max(1);
    let m = WRITE_STATS.mutations.load(Ordering::Relaxed).max(1);
    let g = |v: &AtomicU64| v.load(Ordering::Relaxed);
    let mut s = format!(
        "write profile: {} batches, {} mutations ({:.1}/batch)\n",
        b, m, m as f64 / b as f64
    );
    for (name, ns) in [
        ("total  ", g(&WRITE_STATS.total_ns)),
        ("encode ", g(&WRITE_STATS.encode_ns)),
        ("wal    ", g(&WRITE_STATS.wal_ns)),
        // apply wall time nests the WAL call; net isolates index+lock work.
        ("apply* ", g(&WRITE_STATS.apply_ns).saturating_sub(g(&WRITE_STATS.wal_ns))),
        ("idxsend", g(&WRITE_STATS.index_send_ns)),
        ("version", g(&WRITE_STATS.versions_ns)),
        ("watch  ", g(&WRITE_STATS.watchers_ns)),
        ("cache  ", g(&WRITE_STATS.cache_ns)),
        ("blobex ", g(&WRITE_STATS.blob_ns)),
        ("encdoc ", g(&WRITE_STATS.encdoc_ns)),
    ] {
        s.push_str(&format!("  {} {:>10.1}us/batch {:>8.1}us/mutation\n",
            name, ns as f64 / 1000.0 / b as f64, ns as f64 / 1000.0 / m as f64));
    }
    for v in [&WRITE_STATS.batches, &WRITE_STATS.mutations, &WRITE_STATS.total_ns,
        &WRITE_STATS.encode_ns, &WRITE_STATS.wal_ns, &WRITE_STATS.apply_ns,
        &WRITE_STATS.index_send_ns, &WRITE_STATS.versions_ns,
        &WRITE_STATS.watchers_ns, &WRITE_STATS.cache_ns,
        &WRITE_STATS.blob_ns, &WRITE_STATS.encdoc_ns] {
        v.store(0, Ordering::Relaxed);
    }
    s
}

// --- Data Types ---

#[derive(Debug, Clone)]
pub enum BatchMutation {
    Put {
        collection: String,
        doc_id: String,
        doc: HakoDoc,
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
    // ponytail: shared id — every write fans out one event per mutation and
    // the old String clone per event was pure tax; subscribers bump or copy
    // out only what they forward.
    pub path: Arc<str>,
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

/// Collections that must never leave the device over sync (net or cloud),
/// in either direction. Sync-state, room registry, and the admin plane's
/// credential stores. (`__hako_security` is deliberately NOT here —
/// policy documents replicate by design.)
pub const SYNC_EXCLUDED_COLLECTIONS: &[&str] = &[
    "__hako_system",
    "__hako_rooms",
    "__users",
    "__groups",
];

/// True when `col` must be withheld from all sync tailers and catch-up.
/// Consulted on send AND receipt paths (see `sync_collections` and the
/// ingest choke point); the net_sync inbound apply uses the same
/// per-instance set seeded from this list.
pub fn is_sync_excluded(col: &str) -> bool {
    SYNC_EXCLUDED_COLLECTIONS.contains(&col)
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
    index_puts: Vec<(String, Arc<HakoDoc>)>, 
    index_deletes: Vec<(String, HakoDoc)>,
    blob_queue_items: Vec<BlobWork>,
}

impl Transaction {
    pub fn put(&mut self, collection: &str, doc_id: &str, doc: HakoDoc) {
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
    pub fn commit(self, db: &Hako) -> Result<Vec<String>> {
        Ok(db.write_batch(self.mutations)?)
    }
}

pub struct SerializableTransaction {
    pub reads: HashMap<String, Option<u64>>,
    pub mutations: Vec<BatchMutation>,
}

impl SerializableTransaction {
    pub fn get(
        &mut self,
        db: &Hako,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<HakoDoc>> {
        let key = doc_id.to_string();
        let doc = db.get(collection, doc_id)?;
        let version = db.current_version(&key);
        self.reads.insert(key, version);
        Ok(doc)
    }
    pub fn put(&mut self, collection: &str, doc_id: &str, doc: HakoDoc) {
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
    pub fn commit(&self, db: &Hako) -> Result<Vec<String>> {
        Ok(db.commit_serializable(self.reads.clone(), self.mutations.clone())?)
    }
}

pub(crate) enum IndexOp {
    Update {
        collection: String,
        // puts: Arc<Vec<(String, HakoDoc)>>, 
        puts: Arc<Vec<(String, Arc<HakoDoc>)>>, 
        deletes: Vec<(String, HakoDoc)>,
    },
}

/// Snapshot of background activity behind `Hako::await_quiescent`.
/// Fresh writes settle through four stages — open-time index recovery,
/// async index updates, blob persistence, periodic maintenance — and a
/// read benchmarked mid-flight measures contention, not the engine.
/// Poll this to see what is outstanding instead of guessing.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct QuiescenceStatus {
    /// Open-time index recovery finished (planner takes index paths).
    pub indexes_ready: bool,
    /// Index updates still queued behind the async index worker.
    pub pending_index_ops: usize,
    /// Index backfill threads still rebuilding over existing docs.
    pub index_backfills: usize,
    /// Blob bytes accepted from clients but not yet persisted.
    pub pending_blob_bytes: usize,
    /// Blob work items waiting for the worker.
    pub queued_blob_items: usize,
    /// The 5s system thread is inside checkpoint/purge/snapshot work.
    pub maintenance_running: bool,
}

impl QuiescenceStatus {
    /// Settled: nothing background outstanding.
    pub fn is_quiescent(&self) -> bool {
        self.indexes_ready
            && self.pending_index_ops == 0
            && self.index_backfills == 0
            && self.pending_blob_bytes == 0
            && self.queued_blob_items == 0
            && !self.maintenance_running
    }
}

/// Decrements the backfill counter on scope exit — including panics, so a
/// dying backfill thread can't wedge the quiescence verdict at nonzero.
struct BackfillGuard {
    counter: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for BackfillGuard {
    fn drop(&mut self) {
        self.counter.fetch_sub(1, Ordering::Relaxed);
    }
}

pub struct Hako {
    root_path: PathBuf,    pub(crate) config: HakoConfig,
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
    /// ponytail: lock-free gate for allowed(). The common case (no rules)
    /// skips the RwLock acquisition on every op; set alongside the rules.
    security_enabled: std::sync::atomic::AtomicBool,
    audit_data: Arc<RwLock<Vec<AuditEntry>>>,
    audit_tx: Sender<AuditEntry>,
    pub(crate) index_tx: Sender<IndexOp>,
    /// ponytail: ops sent to the async index worker but not yet applied.
    /// Quiescence reads this instead of channel len (std mpsc has none).
    pub(crate) index_inflight: Arc<std::sync::atomic::AtomicUsize>,
    /// ponytail: create_*_index backfill threads in flight (one per index
    /// build over existing docs). Quiescence covers these too — an index
    /// that exists-but-is-backfilling otherwise serves partial results
    /// with no signal at all.
    pub(crate) backfill_inflight: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) blob_tx: crossbeam_channel::Sender<BlobWork>,
    system_stop: Mutex<Option<Sender<()>>>,
    system_handle: Mutex<Option<thread::JoinHandle<()>>>,
    blob_stop_tx: Mutex<Option<Sender<()>>>, 
    blob_worker_handle: Mutex<Option<thread::JoinHandle<()>>>,
    pub(crate) trigger_blob_flush: Arc<std::sync::atomic::AtomicBool>,
    pub(crate) id_sequence: std::sync::atomic::AtomicU16,
    pub(crate) indexes_ready: Arc<std::sync::atomic::AtomicBool>,
    /// ponytail: true while the 5s system thread runs maintenance
    /// (checkpoint, compaction, tombstone purge, index snapshots) on any
    /// shard. Try-locks make it polite, but readers still share cache and
    /// IO with it — quiescence checks read this (see await_quiescent).
    pub(crate) maintenance_running: Arc<std::sync::atomic::AtomicBool>,
    plan_cache: PlanCache,
    /// ponytail: hot decoded-doc cache. Key is `collection\0doc_id` (ids are
    /// NUL-free by FFI construction). Value is the global doc version at
    /// decode time plus the post-`resolve_doc` document: repeat `get()`s of
    /// the same doc (tx read-modify-write loops, tight polling) skip storage
    /// lookup + full decode for one Arc clone. Versions only increase and
    /// every write bumps, so a version match is exact; writes also remove
    /// the key outright so stale entries can't linger.
    pub(crate) doc_cache: RwLock<HashMap<String, (u64, Arc<HakoDoc>)>>,
    /// Local-only replication scope ("the signal"): collections and keys
    /// whose writes must never leave this device. Advancing clocks is
    /// untouched (tombstones keep fresh timestamps, so the deleter never
    /// looks "behind" to a handshake); every sync outbound tailer and both
    /// handshake catch-up paths consult `is_local_only` and skip matches.
    /// Key format is `col\0id` (NUL separator — ids may contain anything).
    /// Persisted best-effort into `__hako_system/local_only` so a
    /// restart can't re-tail un-checkpointed local-only ops upstream.
    pub(crate) local_only_cols: RwLock<HashSet<String>>,
    pub(crate) local_only_keys: RwLock<HashSet<String>>,
}

impl Hako {
    pub fn open(path: impl AsRef<Path>, config: HakoConfig) -> Result<Self> {

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

        // check if index is ready
        let indexes_ready = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let indexes_ready_ptr = Arc::clone(&indexes_ready);

        // 2. Initialize Core State with EXPLICIT TYPES
        let indexes: Arc<RwLock<IndexManager>> = Arc::new(RwLock::new(IndexManager::default()));
        let shards: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>> = Arc::new(RwLock::new(HashMap::new()));
        
        let trigger_flush = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let trigger_for_system = Arc::clone(&trigger_flush);
        // let trigger_for_blobs = Arc::clone(&trigger_flush);
        let maintenance_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let maintenance_for_system = Arc::clone(&maintenance_flag);

        // 3. Initialize Index Storage
        let index_dir = root_path.join("_indices");
        let index_log_path = index_dir.join("index.log").to_string_lossy().to_string();
        let snapshot_dir = index_dir.join("snapshots").to_string_lossy().to_string();
        let index_storage = Arc::new(Mutex::new(
            IndexStorage::open(&index_log_path, &snapshot_dir).map_err(|e| HakoError::Io(e))?,
        ));

        // --- WORKER 1: PERSISTENT INDEX WORKER ---
        let index_inflight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let idx_clone = Arc::clone(&indexes);
        let index_inflight_worker = Arc::clone(&index_inflight);
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
                                if let Some(vals) = idx.document_values(&id, &doc) {
                                    let key_bytes = crate::index::composite::key_encoder::encode_composite_key(&idx.definition, &vals, &id);
                                    let _ = persist.insert(idx.definition.id, key_bytes.to_vec(), id.clone());
                                }
                            }
                        }
                        for (id, doc) in deletes {
                            IndexingService::apply_delete(&mut mgr, &collection, &id, &doc);
                            for idx in mgr.indexes_for_collection(&collection) {
                                if let Some(vals) = idx.document_values(&id, &doc) {
                                    let key_bytes = crate::index::composite::key_encoder::encode_composite_key(&idx.definition, &vals, &id);
                                    let _ = persist.delete(idx.definition.id, key_bytes.to_vec(), id.clone());
                                }
                            }
                        }
                        // Physically flush the Composite Index buffer to disk so queries find it instantly
                        let _ = persist.flush_log();
                    }

                }
                index_inflight_worker.fetch_sub(1, Ordering::Relaxed);
            }
        });

        // --- WORKER 2: SYSTEM WORKER (Audit + Maintenance) ---
        let log_path = config.audit_log_path.clone().unwrap_or_else(|| root_path.join("audit.log").to_string_lossy().to_string());
        let mut audit_file = if config.enable_audit_log { Some(std::fs::OpenOptions::new().create(true).append(true).open(log_path)?) } else { None };
        let audit_data = Arc::new(RwLock::new(Vec::new()));
        let audit_data_clone = Arc::clone(&audit_data);
        let shards_sys_clone = Arc::clone(&shards);
        let index_sys_ptr = Arc::clone(&index_storage);

        let indexes_sys_ptr = Arc::clone(&indexes);
        let root_path_sys = root_path.clone();
        // ponytail: copied, not shared — a benchmark holds maintenance for
        // a whole run; live-toggling mid-run would only add a race for no
        // benefit (the tick re-reads every 5s anyway if this ever needs it).
        let maintenance_on = config.background_maintenance;

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
                    // ponytail: holdable via background_maintenance=false
                    // (deterministic benchmarks, latency bounds). Skipping
                    // is always safe — writes/reads never depend on this
                    // block, files just grow until it runs again.
                    if maintenance_on {
                    // ponytail: visible to quiescence checks for the whole
                    // block (checkpoint, purge, snapshots) — set even though
                    // the inner locks are try_-based, because readers still
                    // share page cache and IO bandwidth with it.
                    maintenance_for_system.store(true, Ordering::Release);
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
                    
                    // Periodically snapshot RAM indexes (prevent loss on crash)
                    let snapshot_path = root_path_sys.join("_indices").join("ram_indexes.bin");
                    if let Ok(mgr) = indexes_sys_ptr.try_read() {
                        if let Ok(bytes) = mgr.export_state() { let _ = std::fs::write(snapshot_path, bytes); }
                    }

                    trigger_for_system.store(true, Ordering::Release);
                    last_maint = Instant::now();
                    maintenance_for_system.store(false, Ordering::Release);
                    } // end maintenance_on hold gate
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
                                        shard.update_index_entry(key, Some(Pointer::Inlined(Arc::new(skeleton))));
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
            security_enabled: std::sync::atomic::AtomicBool::new(false),
            audit_tx,
            index_tx,
            index_inflight,
            backfill_inflight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            blob_tx: btx.clone(),
            audit_data,
            system_stop: Mutex::new(Some(system_stop_tx)),
            system_handle: Mutex::new(Some(system_handle_thread)),
            trigger_blob_flush: trigger_flush,
            blob_stop_tx: Mutex::new(Some(stop_tx)),
            blob_worker_handle: Mutex::new(Some(blob_worker_handle)),
            id_sequence: std::sync::atomic::AtomicU16::new(0),
            indexes_ready,
            maintenance_running: maintenance_flag,
            plan_cache: PlanCache::default(),
            doc_cache: RwLock::new(HashMap::new()),
            local_only_cols: RwLock::new(HashSet::new()),
            local_only_keys: RwLock::new(HashSet::new()),
        };

        let _ = db.restore_index_defs();
        let _ = db.restore_local_only();

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
                let enc_context = config_thread.encryption_key.as_ref().and_then(|key| {
                    let apply = match &config_thread.encrypted_cols {
                        None => true,
                        Some(cols) => cols.is_empty() || cols.contains(&col_name),
                    };
                    if apply { Some(EncryptionContext::from_secret(key)) } else { None }
                });
                // StorageEngine::open now takes the logical name as well
                if let Ok(mut storage) = StorageEngine::open(path, &config_thread, col_name.clone(), enc_context) {
                    
                    // Use the Crossbeam Sender for background blob offloading
                    storage.blob_tx = Some(blob_tx_thread.clone());
                    
                    // REBUILD ONLY IF SNAPSHOT FAILED
                    if !snapshot_loaded {
                        if let Ok(data) = storage.scan_prefix("") {
                            let mut mgr = indexes_ptr.write().unwrap();
                            for (doc_id, bytes) in data {
                                if let Some(doc) = HakoDoc::decode(&bytes) {
                                    // doc_id is already the naked ID
                                    mgr.index_document(&col_name, &doc_id, &doc);
                                }
                            }
                        }
                    }

                    if let Ok(mut shards) = shards_ptr.write() {
                        // ponytail: only adopt the recovered shard if the user
                        // hasn't already created one for this collection.
                        // Unconditional insert lost in-memory writes that the
                        // user made between open() and recovery completion
                        // (recovery is async, so this race was always possible
                        // under Manual durability where writes never fsync).
                        shards.entry(col_name.clone()).or_insert_with(|| Arc::new(RwLock::new(storage)));
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
            
            // indexes are ready
            indexes_ready_ptr.store(true, std::sync::atomic::Ordering::Release);

            // Note: This thread terminates naturally after recovery, 
            // keeping the idle thread count low.
        });

        Ok(db)
    }

    // pub(crate) fn get_shard(&self, collection: &str) -> Arc<RwLock<StorageEngine>> {
    //     // Read lock check first (Fast Path)
    //     if let Some(s) = self.shards.read().unwrap().get(collection) {
    //         return Arc::clone(s);
    //     }


    //     // Write lock (Creation Path)
    //     let mut shards = self.shards.write().unwrap();
    //     shards.entry(collection.to_string()).or_insert_with(|| {
    //         let path = self.root_path.join(collection);
    //         let enc_context = self.get_encryption_for_col(collection);
    //         let mut storage = StorageEngine::open(path, &self.config, collection.to_string(), enc_context)
    //             .expect("Failed to create collection shard");
    //         storage.blob_tx = Some(self.blob_tx.clone());
    //         Arc::new(RwLock::new(storage))
    //     }).clone()
    // }
    pub(crate) fn get_shard(&self, collection: &str) -> Result<Arc<RwLock<StorageEngine>>> {
        // 1. Check with Read Lock (Fast Path)
        {
            let shards = self.shards.read().map_err(|_| HakoError::LockPoisoned("shards".into()))?;
            if let Some(s) = shards.get(collection) {
                return Ok(Arc::clone(s));
            }
        }

        // 2. Creation Path (Outside of write lock to prevent poisoning during IO)
        let path = self.root_path.join(collection);
        let encryption = self.get_encryption_for_col(collection);
        
        // This is where the security Err() bubbles up from Wal::replay
        let mut storage = StorageEngine::open(
            path, 
            &self.config, 
            collection.to_string(), 
            encryption
        )?; 
        
        storage.blob_tx = Some(self.blob_tx.clone());
        let shard_arc = Arc::new(RwLock::new(storage));

        // 3. Insert into map with Write Lock
        let mut shards = self.shards.write().map_err(|_| HakoError::LockPoisoned("shards".into()))?;
        Ok(shards.entry(collection.to_string()).or_insert(shard_arc).clone())
    }

    pub(crate) fn get_encryption_for_col(&self, collection: &str) -> Option<EncryptionContext> {
        let key = self.config.encryption_key.as_ref()?;
        
        let should_encrypt = match &self.config.encrypted_cols {
            // If No list is provided, encryption is global (default behavior)
            None => true,
            // If a list is provided, check if this collection is in it
            Some(cols) => {
                if cols.is_empty() {
                    true // Or false, depending on your preference. 
                         // Usually, an empty list means "all" in this context.
                } else {
                    cols.contains(&collection.to_string())
                }
            }
        };

        if should_encrypt {
            Some(EncryptionContext::from_secret(key))
        } else {
            None
        }
    }

    /// Cheap policy check for the sync guards: does THIS node encrypt this
    /// collection at rest? (No context is built — callers that need to
    /// encrypt/decrypt use `get_encryption_for_col`.)
    /// Only compiled when a sync transport exists to ask.
    #[cfg(any(feature = "net-sync", feature = "cloud-sync"))]
    pub(crate) fn is_collection_encrypted(&self, collection: &str) -> bool {
        self.get_encryption_for_col(collection).is_some()
    }

    pub fn commit_serializable(
        &self,
        reads: HashMap<String, Option<u64>>,
        mutations: Vec<BatchMutation>,
    ) -> Result<Vec<String>> {
        let _guard = self.tx_lock.lock().unwrap();

        for (key, expected) in reads {
            if self.current_version(&key) != expected {
                return Err(HakoError::Corrupt("Transaction Conflict".into()));
            }
        }

        // Delegate to the now-unlocked internal logic
        let res = self.write_batch_internal(mutations)?;
        Ok(res)
    }

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<Vec<String>> {
        // 1. Security Check
        if !mutations.iter().all(|m| self.allowed(self.get_col(m), AccessOp::Batch)) {
            self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: false });
            return Err(HakoError::Corrupt("Security Denied".into()));
        }

        let res = self.write_batch_internal(mutations);

        // 2. Async Audit
        self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: res.is_ok() });
        res
    }

    fn write_batch_internal(&self, mutations: Vec<BatchMutation>) -> Result<Vec<String>> {
        let t_total = Instant::now();
        WRITE_STATS.batches.fetch_add(1, Ordering::Relaxed);
        WRITE_STATS.mutations.fetch_add(mutations.len() as u64, Ordering::Relaxed);
        let now_nanos = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as i64;
        let now_micros = now_nanos / 1000; 

        let threshold = self.config.value_blob_threshold_bytes;
        let mut shard_map: HashMap<String, ShardWork> = HashMap::new();
        let mut assigned_ids = Vec::with_capacity(mutations.len());

        // Simple local tracker to avoid changing ShardWork
        // let mut deletes_for_indexer: Vec<(String, String, HakoDoc)> = Vec::new();

        let t_encode = Instant::now();
        // ponytail: last-collection cache — consecutive mutations usually hit
        // the same shard, so reuse its handle + blob manager instead of a map
        // lookup and two lock acquisitions per mutation.
        let mut last_shard: Option<(String, Arc<RwLock<StorageEngine>>, Option<Arc<BlobManager>>)> = None;
        for m in mutations {
            // 1. Resolve basic info immediately
            let (col, mut doc_id, mut doc, is_delete) = match m {
                BatchMutation::Put { collection, doc_id, doc } => (collection, doc_id, doc, false),
                BatchMutation::Patch { collection, doc_id, updates } => {
                    let shard_arc = self.get_shard(&collection)?;
                    let current_doc = {
                        let guard = shard_arc.read().unwrap();
                        guard.get(&doc_id)?
                            .and_then(|b| HakoDoc::decode(&b))
                            .unwrap_or_default()
                    };
                    let mut updated = current_doc;
                    for (k, v) in updates { updated.insert(k, v); }
                    (collection, doc_id, updated, false)
                }
                BatchMutation::Delete { collection, doc_id } => {
                    (collection, doc_id, HakoDoc::default(), true)
                }
            };

            if doc_id.is_empty() || doc_id == "" {
                doc_id = self.generate_sortable_id(now_nanos);
            }

            assigned_ids.push(doc_id.clone());

            // 2. COMPUTE KEY ONCE
            // ponytail: get_mut first — the old entry(col.clone()) allocated
            // a key String per mutation even on hit (20K allocs/batch on
            // single-collection batches).
            let work = match shard_map.get_mut(&col) {
                Some(w) => w,
                None => shard_map.entry(col.clone()).or_insert_with(|| ShardWork {
                    ops: Vec::new(), keys: Vec::new(), events: Vec::new(),
                    index_puts: Vec::new(), blob_queue_items: Vec::new(),
                    index_deletes: Vec::new()
                }),
            };

            let key_arc: Arc<str> = Arc::from(doc_id.as_str());
            
            if is_delete {
                // ponytail: doc_id moves into the WAL op (zero copy);
                // keys/events share the Arc (bumps, no String allocs).
                work.ops.push(WalOp::Delete { key: doc_id, timestamp: now_micros });
                work.keys.push(key_arc.clone());
                work.events.push((col, ChangeEvent { path: key_arc, kind: ChangeKind::Delete }));
                continue;
            }

            doc._time = now_micros;
            
            // 3. REUSE KEY for Blobs (shard handle cached per collection above)
            let t_blob = Instant::now();
            let same_col = matches!(&last_shard, Some((c, _, _)) if *c == col);
            if !same_col {
                let shard_arc = self.get_shard(&col)?;
                let bm = shard_arc.read().unwrap().blob_manager.clone();
                last_shard = Some((col.clone(), shard_arc, bm));
            }
            let (_, _, blob_mgr) = last_shard.as_ref().unwrap();
            let blob_work = blob_mgr.as_ref()
                .map(|bm| bm.extract_blobs_raw(&col, &key_arc, &mut doc, threshold))
                .unwrap_or_default();
            WRITE_STATS.blob_ns.fetch_add(t_blob.elapsed().as_nanos() as u64, Ordering::Relaxed);

            // 4. USE BUFFERED ENCODING
            let t_enc = Instant::now();
            let skeleton_bytes = doc.encode_buffered();
            WRITE_STATS.encdoc_ns.fetch_add(t_enc.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let doc_arc = Arc::new(doc);
            
            // REUSE KEY for WAL and Index
            work.ops.push(WalOp::PutInlined { key: key_arc.to_string(), value: skeleton_bytes });
            work.index_puts.push((doc_id, Arc::clone(&doc_arc))); 
            work.keys.push(key_arc.clone());
            // ponytail: event path bumps the same Arc instead of a fresh
            // String allocation per mutation.
            work.events.push((col, ChangeEvent {
                path: key_arc,
                kind: ChangeKind::Put
            }));
            
            for b in blob_work { work.blob_queue_items.push(b); }
        }
        WRITE_STATS.encode_ns.fetch_add(t_encode.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // --- APPLY SHARD CHANGES ---
        for (col_name, mut work) in shard_map {
            let shard_arc = self.get_shard(&col_name)?;
            // let index_entries = work.index_puts; 

            let t_apply = Instant::now();
            {
                let mut shard = shard_arc.write().unwrap();
            
                // OPTIMIZATION: Only run the delete-resolution scan if there are actually 
                // deletes in this specific shard's work.
                if work.ops.iter().any(|op| matches!(op, WalOp::Delete { .. })) {
                    for op in &work.ops {
                        if let WalOp::Delete { key, .. } = op {
                            if let Some(ptr) = shard.index.get(key) {
                                if let Ok(Some(bytes)) = shard.read_pointer_internal(ptr, false) {
                                    if let Some(old_doc) = HakoDoc::decode(&bytes) {
                                        work.index_deletes.push((key.clone(), old_doc));
                                    }
                                }
                            }
                        }
                    }
                }

                if !work.ops.is_empty() {
                    let tx_id = shard.next_tx_id;
                    shard.next_tx_id += 1;
                    let t_wal = Instant::now();
                    shard.wal.append_batch_fast(tx_id, &work.ops, false)?;
                    WRITE_STATS.wal_ns.fetch_add(t_wal.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }

                // Apply storage index changes.
                // ponytail: docs land Inlined with the bytes already encoded
                // for WAL (matched by bare key) — no BlobPending detour, no
                // per-read re-encode until background conversion. The WAL op
                // carries identical bytes, so durability is unchanged; the
                // blob worker's BlobPending-gated swap simply no-ops, and
                // pre-flush blob reads still resolve via the flush queue.
                // Falls back to BlobPending if the WAL op is ever absent.
                // ponytail: PutInlined ops and index_puts are pushed in
                // lockstep (one pair per put/patch; deletes touch ops only),
                // so zip positionally. The old code built a full HashMap of
                // every key first (SipHash ×2/row on big batches) — gone.
                // index_puts stays put for the indexer send below.
                let mut put_idx = 0;
                for op in &work.ops {
                    if let WalOp::PutInlined { value, .. } = op {
                        if let Some((doc_id, _)) = work.index_puts.get(put_idx) {
                            put_idx += 1;
                            let ptr = Pointer::Inlined(Arc::new(value.clone()));
                            shard.update_index_entry(doc_id.clone(), Some(ptr));
                        }
                    }
                }
                debug_assert!(work.index_puts.get(put_idx).is_none(), "ops/index_puts lockstep broke");
                for op in &work.ops {
                    if let WalOp::Delete { key, timestamp } = op {
                        shard.update_index_entry(key.clone(), Some(Pointer::Deleted { timestamp: *timestamp }));
                    }
                }
                
                let mut b_bytes = 0;
                for b in work.blob_queue_items {
                    if let BlobWork::PutRaw { len, .. } = &b { b_bytes += *len as usize; }
                    shard.blob_flush_queue.push_back(b);
                }
                shard.total_pending_blob_bytes.fetch_add(b_bytes, Ordering::Relaxed);
            }
            // apply wall time includes the WAL call above; report net below.
            WRITE_STATS.apply_ns.fetch_add(t_apply.elapsed().as_nanos() as u64, Ordering::Relaxed);
            
            self.trigger_blob_flush.store(true, Ordering::Release);

            // ponytail: drop hot-cache entries for written keys. The version
            // bump alone would invalidate them; this reclaims memory eagerly.
            // ponytail: skip the whole pass when the cache is empty (the
            // common batch/seed case) — 20K format!+remove for nothing.
            let t_cache = Instant::now();
            {
                let mut cache = self.doc_cache.write().unwrap();
                if !cache.is_empty() {
                    for (doc_id, _) in &work.index_puts {
                        cache.remove(&format!("{col_name}\0{doc_id}"));
                    }
                    for op in &work.ops {
                        if let WalOp::Delete { key, .. } = op {
                            cache.remove(&format!("{col_name}\0{key}"));
                        }
                    }
                }
            }
            WRITE_STATS.cache_ns.fetch_add(t_cache.elapsed().as_nanos() as u64, Ordering::Relaxed);
            
            // Notify Indexer (Worker 1)
            let t_idx = Instant::now();
            if !work.index_puts.is_empty() || !work.index_deletes.is_empty() {
                // ponytail: in-flight count for quiescence checks. Bump
                // before send; the worker drops it after applying. A failed
                // send (worker gone) refunds immediately so the counter
                // can't wedge the quiescence verdict.
                self.index_inflight.fetch_add(1, Ordering::Relaxed);
                if self.index_tx.send(IndexOp::Update {
                    collection: col_name,
                    puts: Arc::new(work.index_puts),
                    deletes: work.index_deletes
                }).is_err() {
                    self.index_inflight.fetch_sub(1, Ordering::Relaxed);
                }
            }
            WRITE_STATS.index_send_ns.fetch_add(t_idx.elapsed().as_nanos() as u64, Ordering::Relaxed);
            
            let t_ver = Instant::now();
            self.bump_versions_by_keys(work.keys);
            WRITE_STATS.versions_ns.fetch_add(t_ver.elapsed().as_nanos() as u64, Ordering::Relaxed);
            let t_watch = Instant::now();
            for (c, e) in work.events { self.notify_watchers(&c, e); }
            WRITE_STATS.watchers_ns.fetch_add(t_watch.elapsed().as_nanos() as u64, Ordering::Relaxed);
        }

        WRITE_STATS.total_ns.fetch_add(t_total.elapsed().as_nanos() as u64, Ordering::Relaxed);
        
        // 5. CONDITIONAL AUDIT (Zero overhead if disabled)
        if self.config.enable_audit_log {
            self.record_audit(AuditEntry { 
                op: AccessOp::Batch, 
                collection: "<sharded>".into(), 
                doc_id: None, 
                ok: true 
            });
        }

        Ok(assigned_ids)
    }

    // Fix signature for public helper
    pub fn process_doc_blobs(
        &self, 
        collection: &str,
        key: &str, // Added key
        doc: &mut HakoDoc, 
        blob_manager: &BlobManager,
        threshold: usize, 
    ) -> Vec<BlobWork> {
        blob_manager.extract_blobs(collection, key, doc, threshold)
    }

    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<HakoDoc>> {
        if !self.allowed(collection, AccessOp::Get) {
            // ponytail: gate entry construction (two String allocs) on the
            // toggle — record_audit already no-ops when disabled.
            if self.config.enable_audit_log {
                self.record_audit(AuditEntry {
                    op: AccessOp::Get,
                    collection: collection.into(),
                    doc_id: Some(doc_id.into()),
                    ok: false,
                });
            }
            return Err(HakoError::Corrupt("Denied".into()));
        }

        // ponytail: hot-cache probe. Hit skips storage lookup, full decode
        // and blob resolve for one deep clone (field Arcs are shared via
        // interning, so the clone is mostly the value Strings). Ordering is
        // linearizable: a hit linearizes at the version check, same as a
        // storage read racing a write today.
        // ponytail: one version lookup serves both the probe and the
        // populate below (the old code paid fxhash + sharded lock twice).
        let ver = self.current_version(doc_id);
        // ponytail: cache-key alloc only when a version exists to compare
        // against — unversioned docs paid a format! for a probe that could
        // never hit. Built once, shared by probe + populate below.
        let cache_key = ver.map(|_| format!("{collection}\0{doc_id}"));
        if let (Some(ver), Some(cache_key)) = (ver, &cache_key) {
            if let Some((v, cached)) = self.doc_cache.read().unwrap().get(cache_key) {
                if *v == ver {
                    return Ok(Some((**cached).clone()));
                }
            }
        }

        let shard = self.get_shard(collection)?;
        let storage = shard.safe_read()?;
        // ponytail: decode borrowed from the shared Arc — the old
        // storage.get() cloned the full bytes into a transient Vec just
        // to decode and drop them (one alloc + memcpy per get, found by
        // the alloc census). Identical output, zero copies.
        let res = storage
            .get_shared(doc_id)?
            .and_then(|b| HakoDoc::decode(&b));

        // AUDIT SUCCESS
        if self.config.enable_audit_log {
            self.record_audit(AuditEntry {
                op: AccessOp::Get,
                collection: collection.into(),
                doc_id: Some(doc_id.into()),
                ok: true,
            });
        }
        
        if let Some(mut doc) = res {
            self.resolve_doc(&mut doc, collection)?;
            // ponytail: populate post-resolve. Bounded at 8192 with single
            // arbitrary eviction when full.
            // ponytail: try_write — a contended cache must not serialize
            // readers. Skipped populate just means the next read re-decodes;
            // correctness is untouched (version check revalidates).
            if let (Some(ver), Some(cache_key)) = (ver, cache_key) {
                if let Ok(mut cache) = self.doc_cache.try_write() {
                    if cache.len() >= 8192 {
                        if let Some(k) = cache.keys().next().cloned() { cache.remove(&k); }
                    }
                    cache.insert(cache_key, (ver, Arc::new(doc.clone())));
                }
            }
            return Ok(Some(doc));
        }

        Ok(None)
    }

    /// Borrowed point read (v0.8.10+): pins the stored bytes and returns a
    /// `DocView` with lazy per-field access — no decode, no interning, no
    /// hot-cache interaction. The sqlite3 `SELECT` + typed-accessor analog:
    /// pull only the fields you touch. Deleted/missing reads None, same as
    /// [`Self::get`]; framing-invalid rows also read None (strict views).
    pub fn get_view(&self, collection: &str, doc_id: &str) -> Result<Option<crate::document::hako_doc::DocView>> {
        if !self.allowed(collection, AccessOp::Get) {
            if self.config.enable_audit_log {
                self.record_audit(AuditEntry {
                    op: AccessOp::Get,
                    collection: collection.into(),
                    doc_id: Some(doc_id.into()),
                    ok: false,
                });
            }
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let shard = self.get_shard(collection)?;
        let storage = shard.safe_read()?;
        let res = storage.get_shared(doc_id)?.and_then(|b| {
            crate::document::hako_doc::DocView::new(b)
        });

        if self.config.enable_audit_log {
            self.record_audit(AuditEntry {
                op: AccessOp::Get,
                collection: collection.into(),
                doc_id: Some(doc_id.into()),
                ok: true,
            });
        }

        Ok(res)
    }

    pub fn put(&self, col: &str, id: &str, doc: &HakoDoc) -> Result<String> {
        self.put_owned(col, id, doc.clone())
    }

    /// ponytail: owned-doc variant — skips the full deep clone that `put`
    /// pays (every String field). Use whenever the caller already owns the
    /// doc (FFI take-handles, Tauri JSON builds, subdocument assembly).
    pub fn put_owned(&self, col: &str, id: &str, doc: HakoDoc) -> Result<String> {
        let res = self.write_batch(vec![BatchMutation::Put {
            collection: col.into(),
            doc_id: id.into(),
            doc,
        }])?;
        Ok(res.into_iter().next().unwrap_or_default())
    }

    pub fn delete(&self, col: &str, id: &str) -> Result<String> {
        let res = self.write_batch(vec![BatchMutation::Delete {
            collection: col.into(),
            doc_id: id.into(),
        }])?;
        Ok(res.into_iter().next().unwrap_or_default())
    }

    /// The single shared gate for the local-only signal. Collection-level
    /// flag OR per-key mark. `key` may arrive bare (`id`) or namespaced
    /// (`col:id`) depending on the sync path — both forms are honored.
    pub fn is_local_only(&self, col: &str, key: &str) -> bool {
        if self.local_only_cols.read().unwrap().contains(col) {
            return true;
        }
        let keys = self.local_only_keys.read().unwrap();
        if keys.contains(&format!("{col}\0{key}")) {
            return true;
        }
        // ponytail: namespaced fallback — strip one leading `col:` only.
        if let Some(id) = key.strip_prefix(col).and_then(|s| s.strip_prefix(':')) {
            return keys.contains(&format!("{col}\0{id}"));
        }
        false
    }

    /// Mark a whole collection local-only (never emits, never accepts
    /// remote ops for it once sync filters check this — fully local).
    /// `false` rejoins: subsequent local writes replicate again, and any
    /// newer remote op applies per LWW (resurrect rule).
    pub fn set_collection_local(&self, col: &str, local: bool) {
        {
            let mut cols = self.local_only_cols.write().unwrap();
            if local {
                cols.insert(col.to_string());
            } else {
                cols.remove(col);
            }
        }
        self.persist_local_only();
    }

    pub fn is_collection_local(&self, col: &str) -> bool {
        self.local_only_cols.read().unwrap().contains(col)
    }

    /// Local-only single delete: marks the key, then runs the normal
    /// delete path so the tombstone keeps a fresh timestamp and the
    /// collection version advances (handshake-stability rule).
    pub fn delete_local(&self, col: &str, id: &str) -> Result<String> {
        self.local_only_keys.write().unwrap().insert(format!("{col}\0{id}"));
        self.persist_local_only();
        self.delete(col, id)
    }

    /// Opt a key back into replication. Only affects future ops; the
    /// existing local tombstone still stands until overwritten.
    pub fn replicate_key(&self, col: &str, id: &str) {
        self.local_only_keys.write().unwrap().remove(&format!("{col}\0{id}"));
        self.persist_local_only();
    }

    /// Opt a whole collection back into replication: clears the collection
    /// flag and every key mark under it. Tombstones themselves are untouched
    /// (still local history) — pair with `vacuum_collection` for restore.
    pub fn replicate_collection(&self, col: &str) {
        self.local_only_cols.write().unwrap().remove(col);
        {
            let mut keys = self.local_only_keys.write().unwrap();
            // ponytail: marks are `col\0id` — split on the separator instead
            // of prefix-matching, so col "ab" never eats col "abc" marks.
            keys.retain(|k| k.split_once('\0').map(|(c, _)| c != col).unwrap_or(true));
        }
        self.persist_local_only();
    }

    /// Vacuum a collection: purge its tombstones from the index. Emits no
    /// WAL op (never replicates) and drops the collection version to the
    /// newest live doc, so the next handshake pulls peers' current state.
    /// This is the restore half of rejoin: `vacuum_collection` +
    /// `replicate_collection` (or `set_collection_local(false)`) lets a
    /// reset peer pull back the room state without pushing anything out.
    /// Returns the number of tombstones purged.
    pub fn vacuum_collection(&self, col: &str) -> Result<usize> {
        let shard_arc = self.get_shard(col)?;
        let mut shard = shard_arc.write().unwrap();
        Ok(shard.purge_tombstones())
    }

    /// Local-only mass delete ("reset this query scope, don't propagate"):
    /// marks every matched key first (single persist), then batch-deletes.
    pub fn delete_where_local(&self, query: crate::query::query::Query) -> Result<usize> {
        let results = self.query(query.clone())?;
        if results.is_empty() { return Ok(0); }
        let count = results.len();
        {
            let mut marks = self.local_only_keys.write().unwrap();
            for (id, _) in &results {
                marks.insert(format!("{}\0{id}", query.collection));
            }
        }
        self.persist_local_only();
        let mutations = results.into_iter()
            .map(|(id, _)| BatchMutation::Delete {
                collection: query.collection.clone(),
                doc_id: id
            })
            .collect();
        self.write_batch(mutations)?;
        Ok(count)
    }

    /// Local-only batch delete by explicit ids (single mark persist +
    /// single batch — the CLI/SDK batch path).
    pub fn delete_ids_local(&self, col: &str, ids: &[String]) -> Result<usize> {
        if ids.is_empty() { return Ok(0); }
        {
            let mut marks = self.local_only_keys.write().unwrap();
            for id in ids {
                marks.insert(format!("{col}\0{id}"));
            }
        }
        self.persist_local_only();
        let mutations = ids.iter()
            .map(|id| BatchMutation::Delete {
                collection: col.to_string(),
                doc_id: id.clone()
            })
            .collect();
        self.write_batch(mutations)?;
        Ok(ids.len())
    }

    /// Best-effort durability for the marks. Lives in `__hako_system`,
    /// which net_sync already excludes from its tail and cloud_sync never
    /// routes (no room prefix) — the marker itself never replicates.
    fn persist_local_only(&self) {
        let cols: Vec<Value> = self.local_only_cols.read().unwrap().iter()
            .map(|c| Value::String(c.clone())).collect();
        let keys: Vec<Value> = self.local_only_keys.read().unwrap().iter()
            .map(|k| Value::String(k.clone())).collect();
        let mut doc = HakoDoc::default();
        doc.insert("cols", Value::Array(cols));
        doc.insert("keys", Value::Array(keys));
        let _ = self.put("__hako_system", "local_only", &doc);
    }

    fn restore_local_only(&self) -> Result<()> {
        let doc = match self.get("__hako_system", "local_only")? {
            Some(d) => d,
            None => return Ok(()),
        };
        if let Some(Value::Array(cols)) = doc.get("cols") {
            let mut set = self.local_only_cols.write().unwrap();
            for c in cols {
                if let Value::String(s) = c { set.insert(s.clone()); }
            }
        }
        if let Some(Value::Array(keys)) = doc.get("keys") {
            let mut set = self.local_only_keys.write().unwrap();
            for k in keys {
                if let Value::String(s) = k { set.insert(s.clone()); }
            }
        }
        Ok(())
    }

    pub fn collection(&self, name: &str) -> Collection<'_> {
        Collection::new(self, name)
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, HakoDoc)>> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready);

        // SIMPLE CALL: No blob_file or encryption passed here!
        let results = self.executor.execute(shard_arc, &indexes, (*plan).clone())?;

        // AUDIT RESULT
        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true, // prev ->results.is_ok()
        });

        Ok(results)
    }

    /// Raw scan: storage-encoded bytes instead of decoded docs. Same
    /// admission path as [`Self::query`] (security, plan cache, audit);
    /// execution stops after the index walk — see
    /// `ParallelQueryExecutor::execute_raw` for the contract (opaque bytes,
    /// index-satisfied filters/ordering required). Sets `query.raw` so the
    /// plan-cache key can't collide with a decoded query of the same shape.
    pub fn query_raw(&self, mut query: Query) -> Result<Vec<(String, std::sync::Arc<Vec<u8>>)>> {
        query.raw = true;
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready);

        let results = self.executor.execute_raw(shard_arc, &indexes, (*plan).clone())?;

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(results)
    }

    /// Zero-alloc scan walk: lends each matching row (`&str` id, `&[u8]`
    /// storage bytes) to `callback`; `false` stops early. Returns rows
    /// visited. See `ParallelQueryExecutor::execute_walk` for the contract
    /// (SortedKeys scans only for now; callback must not re-enter the
    /// engine — the storage read lock is held for the whole walk).
    pub fn walk<F>(&self, mut query: Query, callback: &mut F) -> Result<usize>
    where
        F: FnMut(&str, &[u8]) -> bool,
    {
        query.raw = true;
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready);

        let visited = self.executor.execute_walk(shard_arc, &indexes, (*plan).clone(), callback)?;

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(visited)
    }

    /// View walk: lends each matching row as a `DocView` (lazy per-field
    /// reads) instead of raw bytes. Same admission as [`Self::walk`]; see
    /// `ParallelQueryExecutor::execute_walk_view` for the contract.
    pub fn walk_view<F>(&self, mut query: Query, callback: &mut F) -> Result<usize>
    where
        F: FnMut(&str, &crate::document::hako_doc::DocView) -> bool,
    {
        query.raw = true;
        if !self.allowed(&query.collection, AccessOp::Query) {
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: false,
            });
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready);

        let visited = self.executor.execute_walk_view(shard_arc, &indexes, (*plan).clone(), callback)?;

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(visited)
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
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let mut q = query.clone();
        q.projection = fields.to_vec();

        let shard_arc = self.get_shard(&q.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready();
        let plan = self.plan_cache.get_or_compute(&q, &indexes, rows, self.config.query_workers, is_ready);

        // SIMPLE CALL: Worker handles blob resolution internally
        let results = self.executor.execute_projected(shard_arc, &indexes, (*plan).clone())?;

        // 5. Audit & Return
        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: q.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(results)
    }

    pub fn patch(&self, col: &str, id: &str, updates: Vec<(String, Value)>) -> Result<String> {
        if !self.allowed(col, AccessOp::Put) {
            self.record_audit(AuditEntry {
                op: AccessOp::Put,
                collection: col.into(),
                doc_id: Some(id.into()),
                ok: false,
            });
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let data = self.write_batch(vec![BatchMutation::Patch {
            collection: col.to_string(),
            doc_id: id.to_string(),
            updates,
        }]);

        // AUDIT RESULT
        let _ = self.record_audit(AuditEntry {
            op: AccessOp::Put,
            collection: col.into(),
            doc_id: Some(id.into()),
            ok: data.is_ok(),
        });

        Ok(data?.into_iter().next().unwrap_or_default()) 
    }

    pub fn delete_where(&self, query: crate::query::query::Query) -> Result<usize> {
        // 1. Find the matches
        let results = self.query(query.clone())?;
        if results.is_empty() { return Ok(0); }

        // 2. Map to delete mutations
        let count = results.len();
        let mutations = results.into_iter()
            .map(|(id, _)| BatchMutation::Delete { 
                collection: query.collection.clone(), 
                doc_id: id 
            })
            .collect();

        // 3. Execute batch
        self.write_batch(mutations)?;
        Ok(count)
    }

    pub fn patch_where(&self, query: crate::query::query::Query, updates: Vec<(String, Value)>) -> Result<usize> {
        let results = self.query(query.clone())?;
        if results.is_empty() { return Ok(0); }

        let count = results.len();
        let mutations = results.into_iter()
            .map(|(id, _)| BatchMutation::Patch { 
                collection: query.collection.clone(), 
                doc_id: id, 
                updates: updates.clone() 
            })
            .collect();

        self.write_batch(mutations)?;
        Ok(count)
    }

    pub fn get_stats(&self) -> HashMap<String, usize> {
        let mut stats = HashMap::new();
        let shards = self.shards.read().unwrap();
        for (name, shard) in shards.iter() {
            let s = shard.read().unwrap();
            // Count documents
            stats.insert(format!("{}_count", name), s.count_prefix(""));
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
        // ponytail: no-rules fast path without touching the lock.
        if !self.security_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            return true;
        }
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

    /// Watch support for out-of-tree gateways: prebuild a filter plan for
    /// a subscription once, then match per-event bytes against it. The match itself is
    /// zero-decode (borrows the stored bytes, no allocation), so the hot
    /// path costs the same as the former in-tree call — only the location
    /// moved, not the complexity.
    pub fn plan_for_watch(&self, query: &crate::query::query::Query) -> crate::query::plan::QueryPlan {        let indexes = self.indexes.read().unwrap();
        let is_ready = self.index_query_ready();
        crate::query::planner::QueryPlanner::plan(query, &indexes, 0, 1, is_ready)
    }

    /// Zero-decode view match for one watch event: true when the stored
    /// `bytes` for `doc_id` satisfy `plan` (built by `plan_for_watch`).
    /// Handles the exit case too — returns false when an update stops
    /// matching, so callers emit the removal.
    pub fn matches_watch(
        doc_id: &str,
        bytes: &[u8],
        plan: &crate::query::plan::QueryPlan,
    ) -> bool {
        crate::query::executor::worker::matches_filters_view(doc_id, bytes, plan)
    }

    /// Raw stored bytes for one document (watch event hydration without a
    /// full decoded point-get).
    pub fn get_raw_bytes(&self, collection: &str, doc_id: &str) -> Result<Option<Vec<u8>>> {
        let shard = self.get_shard(collection)?;
        let storage = shard.read().unwrap();
        storage.get(doc_id)
    }

    /// Set WAL durability on every shard (admin plane for out-of-tree
    /// gateways). Best-effort per shard, mirroring the former in-tree call.
    pub fn set_durability_mode_all(&self, mode: DurabilityMode) {
        for shard in self.shards.read().unwrap().values() {
            if let Ok(mut s) = shard.write() {
                s.set_durability_mode(mode);
            }
        }
    }

    pub(crate) fn notify_watchers(&self, col: &str, event: ChangeEvent) {
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

    /// True when `col` must be withheld from all sync paths: builtin
    /// `SYNC_EXCLUDED_COLLECTIONS` plus this deployment's
    /// `HakoConfig::sync_excluded` extras.
    pub fn is_sync_excluded_effective(&self, col: &str) -> bool {
        is_sync_excluded(col) || self.config.sync_excluded.iter().any(|c| c == col)
    }

    /// Sync enumeration: EVERY collection on disk (including `_`-hidden
    /// ones) minus the explicit excluded plane. Sync is opt-out, not
    /// opt-in — `list_collections()` stays the user-visible listing with
    /// `_` hiding, but sync must never depend on naming conventions.
    pub fn sync_collections(&self) -> Result<Vec<String>> {
        let mut cols = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.root_path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if name != "snapshots" && !self.is_sync_excluded_effective(&name) {
                            cols.push(name);
                        }
                    }
                }
            }
        }
        cols.sort();
        Ok(cols)
    }

    pub fn get_by_reference(&self, reference: &Value) -> Result<Option<HakoDoc>> {
        match reference {
            Value::Reference { collection, doc_id } => self.get(collection, doc_id),
            _ => Err(HakoError::Corrupt("Not ref".into())),
        }
    }
    
    pub fn set_security_rules(&self, rules: Vec<SecurityRule>) {
        self.security_enabled.store(!rules.is_empty(), std::sync::atomic::Ordering::Relaxed);
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
            return Err(HakoError::Corrupt("Denied".into()));
        }

        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc
            .read()
            .unwrap()
            .count_prefix("");

        let is_ready = self.index_query_ready();
        // FIX: Add self.config.query_workers as the 4th argument
        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready);

        let res = self
            .executor
            .execute_aggregation(shard_arc, &indexes, (*plan).clone(), &query.aggregations);

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
        doc: &HakoDoc,
    ) -> Result<String> {
        let res = self.put(&subcollection_prefix(col, id, subcol), subid, doc)?;
        Ok(res)
    }

    pub fn audit_entries(&self) -> Vec<AuditEntry> {
        self.audit_data.read().unwrap().clone()
    }

    fn index_defs_path(&self) -> PathBuf {
        self.root_path.join("_indices").join("definitions.json")
    }

    // 1. The public version (used by create_index). Public (not
    // pub(crate)) so out-of-tree gateways that create
    // indexes can persist defs without reimplementing the walk.
    pub fn persist_index_defs(&self) -> Result<()> {
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
        let json = serde_json::to_vec_pretty(&state).map_err(|e| HakoError::Corrupt(e.to_string()))?;
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
        let shard_arc = self.get_shard(collection)?;
        let idx_mgr = Arc::clone(&self.indexes);
        let f_name = field.to_string();
        let col_name = collection.to_string();
        let backfill_count = Arc::clone(&self.backfill_inflight);
        backfill_count.fetch_add(1, Ordering::Relaxed);

        thread::spawn(move || {
            let _guard = BackfillGuard { counter: backfill_count };
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
                    if let Some(doc) = HakoDoc::decode(&bytes) {
                        decoded_docs.push((full_key, doc));
                    }
                }
            IndexingService::backfill_secondary(
                &mut mgr,
                &col_name,
                decoded_docs
                    .iter()
                    .map(|(doc_id, doc)| (doc_id.as_str(), doc))
                    .filter(|(_, doc)| doc.get(&f_name).is_some()),
                    // .filter(|(id, doc)| {
                    //     f_name == "id" || f_name == "_time" || doc.get(&f_name).is_some()
                    // }),
            );
            thread::yield_now();
        }
    });

        let _ = self.persist_index_defs();
        self.plan_cache.invalidate();
        Ok(())
    }

    pub fn create_fts_index(&self, collection: &str, field: &str) -> Result<()> {
        // 1. Register
        self.indexes
            .write()
            .unwrap()
            .create_fts_index(collection, field);

        // 3. Spawn background thread for backfilling
        let shard_arc = self.get_shard(collection)?;
        let idx_mgr = Arc::clone(&self.indexes);
        let f_name = field.to_string();
        let col_name = collection.to_string();

        // Capture encryption key for the thread
        let enc_key = self.config.encryption_key.clone();
        let backfill_count = Arc::clone(&self.backfill_inflight);
        backfill_count.fetch_add(1, Ordering::Relaxed);

        thread::spawn(move || {
            // Step A: Lock, take a snapshot of the pointers, then release IMMEDIATELY
            let _guard = BackfillGuard { counter: backfill_count };
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
                    if let Some(mut doc) = HakoDoc::decode(&bytes) {
                        let _ = resolve_doc_static(&mut doc, &shard_arc, enc_key.as_deref());
                        decoded_docs.push((full_key, doc));
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
        self.plan_cache.invalidate();
        Ok(())
    }

    pub fn create_composite_index(&self, col: &str, fields: Vec<(String, SortDirection)>) -> Result<u32> {
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
                    return Ok(idx.definition.id);
                }
            }
        }

        let def = CompositeIndexDefinition::new(col).with_fields(fields);
        let index_id = self.indexes.write().unwrap().create_index(def);

        let shard_arc = self.get_shard(col)?;
        let idx_mgr = Arc::clone(&self.indexes);
        let persist_ptr = Arc::clone(&self.index_storage); // <--- Required for persistence
        
        let enc_key = self.config.encryption_key.clone();
        let backfill_count = Arc::clone(&self.backfill_inflight);
        backfill_count.fetch_add(1, Ordering::Relaxed);

        thread::spawn(move || {
            let _guard = BackfillGuard { counter: backfill_count };
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
                    for (doc_id, bytes) in resolved_data {
                        if let Some(mut doc) = HakoDoc::decode(&bytes) {
                            let _ = resolve_doc_static(&mut doc, &shard_arc, enc_key.as_deref());
                            composite_idx.index_document(&doc_id, &doc);

                            // 2. Insert LEAN KEY into index.log
                            if let Some(vals) = composite_idx.document_values(&doc_id, &doc) {
                                let key_bytes =
                                    crate::index::composite::key_encoder::encode_composite_key(
                                        &composite_idx.definition,
                                        &vals,
                                        &doc_id,
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

                // Release locks to allow benchmark/queries to slip in!
                drop(persist);
                drop(mgr);
                thread::yield_now();
            }
        });

        let _ = self.persist_index_defs();
        self.plan_cache.invalidate();
        Ok(index_id)
    }

    /// Explicitly trigger a snapshot (called by Maintenance Thread or FFI)
    pub fn save_index_snapshots(&self) -> Result<()> {
        let persist = self.index_storage.lock().unwrap();
        // Snapshot the primary composite index (ID 1)
        persist.snapshot(1).map_err(|e| HakoError::Io(e))?;
        Ok(())
    }

    pub fn list_storage_keys(&self, collection: &str) -> Result<Vec<String>> {
        let shard = self.get_shard(collection)?;
        let guard = shard.read().unwrap();
        let mut keys: Vec<String> = guard.index.iter()
            .filter(|(_, ptr)| !matches!(ptr, crate::storage::engine::Pointer::Deleted { .. }))
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();
        Ok(keys)
    }

    /// NEW: Diagnostic to see exactly what strings/values are inside a search index.
    pub fn inspect_index(&self, collection: &str, field: &str) -> Vec<String> {
        let mgr = self.indexes.read().unwrap();
        let mut entries = Vec::new();

        // Check Secondary Indexes
        if let Some(sec_map) = mgr.secondary.get(collection) {
            if let Some(idx) = sec_map.get(field) {
                for (key_bytes, ids) in idx.get_map() {
                    entries.push(format!("Value: {:?} -> IDs: {:?}", key_bytes, ids));
                }
            }
        }

        // Check Composite Indexes
        for idx in mgr.composite.indexes_for_collection(collection) {
            if idx.definition.fields.iter().any(|f| f.field == field) {
                for (key_bytes, id) in &idx.tree {
                    entries.push(format!("CompositeKey: {:?} -> ID: {}", key_bytes, id));
                }
            }
        }
        entries
    }

    pub fn resolve_document_blobs(&self, doc: &mut HakoDoc, collection: &str) -> Result<()> {
        self.resolve_doc(doc, collection)
    }

    // Helper for projected resolution
    fn resolve_single_value_blob(&self, collection: &str, offset: u64, len: u32) -> Result<Value> {
        let shard_arc = self.get_shard(collection)?;
        let blob_manager = {
            let shard = shard_arc.read().unwrap();
            shard
                .blob_manager
                .as_ref()
                .cloned()
                .ok_or_else(|| HakoError::StorageError("Blob manager missing".into()))?
        };
        let data = blob_manager.read_at(offset, len)?;

        if let Ok(s) = String::from_utf8(data.clone()) {
            Ok(Value::String(s))
        } else {
            Ok(Value::Binary(data))
        }
    }

    fn resolve_doc(&self, doc: &mut HakoDoc, collection: &str) -> Result<()> {
        // PASS 1: Collect mutable references once. 
        let blob_values: Vec<&mut Value> = doc.fields.iter_mut()
            .map(|(_, v)| v)
            .filter(|v| matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX))
            .collect();

        if blob_values.is_empty() { return Ok(()); }

        let shard_arc = self.get_shard(collection)?;

        // PASS 2: Snapshot Shard State
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

        let bm = blob_manager.ok_or(HakoError::StorageError("No blob manager".into()))?;

        for val in blob_values {
            if let Value::BlobLink { offset, len } = *val {
                let data = match queue_snapshot.get(&offset) {
                    Some(arc_bytes) => (**arc_bytes).clone(),
                    None => bm.read_at(offset, len)?,
                };
                *val = self.inflate_bytes(data);
            }
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
        let shard_arc = self.get_shard(collection)?;
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
                if let Some(mut doc) = HakoDoc::decode(&bytes) {
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
        let new_file = std::fs::OpenOptions::new().read(true).append(true).open(&blob_path)?;
        shard.blob_manager = Some(Arc::new(BlobManager::new(
            Arc::new(new_file),
            new_offset,
        )));

        // 3. Update the Skeletons in the Segment
        for (key, new_bytes) in updates {
            shard.put(key, &new_bytes)?;
        }

        Ok(())
    }

    pub fn db_name(&self) -> String {
        self.root_path
            .file_name()
            .map(|n| n.to_string_lossy().to_string())
            .unwrap_or_else(|| "hakodb_default".to_string())
    }

    /// True once the background open-recovery pass has rebuilt indexes.
    /// Queries issued before this silently plan `FullCollection` (the
    /// planner's `index_ready` gate) — poll after open/seed in benchmarks
    /// and tests before measuring. Mirrors `hk_engine_is_indexes_ready`.
    pub fn is_indexes_ready(&self) -> bool {
        self.indexes_ready.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Query planning may trust indexes only when open-recovery is done AND
    /// no runtime backfill is in flight. A new index registers before its
    /// backfill thread finishes; planning against the partial index returns
    /// wrong (empty/partial) results — fall back to `FullCollection`
    /// instead. The plan cache is invalidated at index creation, so no
    /// stale fast plan survives into the build window (a slow plan cached
    /// during the window self-heals at TTL).
    pub(crate) fn index_query_ready(&self) -> bool {
        self.indexes_ready.load(std::sync::atomic::Ordering::Acquire)
            && self.backfill_inflight.load(std::sync::atomic::Ordering::Acquire) == 0
    }

    /// Point sample of background activity. All fields best-effort (locks
    /// are try_-based so the check itself never stalls a writer).
    pub fn quiescence_status(&self) -> QuiescenceStatus {
        let mut pending_blob_bytes = 0usize;
        let mut queued_blob_items = 0usize;
        if let Ok(shards) = self.shards.read() {
            for s in shards.values() {
                if let Ok(storage) = s.try_read() {
                    pending_blob_bytes += storage.total_pending_blob_bytes.load(Ordering::Relaxed);
                    queued_blob_items += storage.blob_flush_queue.len();
                }
            }
        }
        QuiescenceStatus {
            indexes_ready: self.is_indexes_ready(),
            pending_index_ops: self.index_inflight.load(Ordering::Relaxed),
            index_backfills: self.backfill_inflight.load(Ordering::Relaxed),
            pending_blob_bytes,
            queued_blob_items,
            maintenance_running: self.maintenance_running.load(Ordering::Acquire),
        }
    }

    /// True when nothing background is outstanding (see
    /// `QuiescenceStatus::is_quiescent`). One sample — use
    /// `await_quiescent` for a settled verdict.
    pub fn is_quiescent(&self) -> bool {
        self.quiescence_status().is_quiescent()
    }

    /// Block until background work settles or `timeout` lapses. Requires
    /// TWO consecutive clear samples (5ms apart) so a millisecond gap
    /// between write batches doesn't read as settled. Returns true when
    /// settled. A continuously-written DB correctly never settles — poll
    /// `quiescence_status` to see what is outstanding instead.
    pub fn await_quiescent(&self, timeout: Duration) -> bool {
        let t0 = Instant::now();
        let mut clear_streak = 0u32;
        loop {
            if self.quiescence_status().is_quiescent() {
                clear_streak += 1;
                if clear_streak >= 2 {
                    return true;
                }
            } else {
                clear_streak = 0;
            }
            if t0.elapsed() >= timeout {
                return self.quiescence_status().is_quiescent();
            }
            thread::sleep(Duration::from_millis(5));
        }
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

    pub fn get_collection_version(&self, collection: &str) -> Result<i64> {
        let shard = self.get_shard(collection)?;
        let guard = shard.read().unwrap();
        
        // Strategy: Scan the RAM index for the highest timestamp.
        // Since this is in RAM, it's extremely fast.
        let max_ts = guard.index.values().map(|ptr| {
            match ptr {
                Pointer::Inlined(bytes) => {
                    // Extract _time from bytes [2..10] based on your VERSION 3 format
                    i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]))
                }
                Pointer::BlobPending(doc) => doc.get_logical_time(),
                Pointer::Deleted { timestamp } => *timestamp,
                _ => 0,
            }
        }).max().unwrap_or(0);

        Ok(max_ts)
    }

    pub fn get_version_map(&self) -> std::collections::HashMap<String, i64> {
        let mut map = std::collections::HashMap::new();
        // Sync enumeration (hidden included, excluded dropped) — the manual
        // `__hako_security` re-add this replaces is gone: enumeration
        // covers it now that sync no longer depends on `_` hiding.
        let cols = self.sync_collections().unwrap_or_default();
        for col in cols {
            // map.insert(col.clone(), self.get_collection_version(&col));
            if let Ok(version) = self.get_collection_version(&col) {
                map.insert(col, version);
            }
        }
        map
    }

    fn generate_sortable_id(&self, timestamp_nanos: i64) -> String {
        let seq = self.id_sequence.fetch_add(1, Ordering::Relaxed);
        let mut buf = vec![0u8; 20]; // Pre-allocate exactly once
        
        const HEX: &[u8] = b"0123456789abcdef";
        let mut t = timestamp_nanos as u64;
        let mut s = seq as u64;

        for i in (0..16).rev() {
            buf[i] = HEX[(t & 0xf) as usize];
            t >>= 4;
        }
        for i in (16..20).rev() {
            buf[i] = HEX[(s & 0xf) as usize];
            s >>= 4;
        }

        // Use from_utf8 to avoid an extra copy
        unsafe { String::from_utf8_unchecked(buf) }
    }

}

impl Drop for Hako {
    fn drop(&mut self) {
        // 1. Stop Audit/Maintenance
        if let Some(tx) = self.system_stop.lock().unwrap().take() { let _ = tx.send(()); }

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
            // self.shards.read().unwrap().values().cloned().collect()
            match self.shards.read() {
                Ok(guard) => guard.values().cloned().collect(),
                Err(_) => Vec::new(), // If poisoned, we can't safely flush
            }
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
    doc: &mut HakoDoc, 
    shard_arc: &Arc<RwLock<StorageEngine>>, 
    _enc_secret: Option<&str>
) -> Result<()> {
    // Single-pass collection
    let blob_values: Vec<&mut Value> = doc.fields.iter_mut()
        .map(|(_, v)| v)
        .filter(|v| matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX))
        .collect();

    if blob_values.is_empty() { return Ok(()); }

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

    let bm = blob_manager.ok_or(HakoError::StorageError("No blob manager".into()))?;

    for val in blob_values {
        if let Value::BlobLink { offset, len } = *val {
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

fn subcollection_prefix(_collection: &str, doc_id: &str, subcollection: &str) -> String {
format!("{}/{}", doc_id, subcollection)
}

#[cfg(test)]
mod profile_tests {
    use super::*;
    use crate::config::DurabilityMode;

    fn test_doc(i: usize) -> HakoDoc {
        let mut d = HakoDoc::default();
        d.insert("tenant", Value::String(format!("tenant-{}", i % 32)));
        d.insert("age", Value::Int(18 + (i % 70) as i64));
        d.insert("active", Value::Bool(i % 3 != 0));
        d.insert("score", Value::Float((i % 10000) as f64 / 7.0 + 0.5));
        d.insert("description", Value::String(format!("payload {i}")));
        d.insert("extra", Value::String("X".repeat(1024)));
        d
    }

    fn profile_puts(mode: DurabilityMode, n: usize) -> String {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-wprof-{nanos}"));
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = mode;
        let db = Hako::open(&dir, cfg).expect("open");
        // Reset first: open() itself writes (recovery/index setup paths).
        write_stats_report();
        // Warmup outside measurement: cold start (page faults, allocator,
        // background indexer/blob threads spinning up) dwarfs steady state.
        for i in 0..50 {
            db.put_owned("bench", &format!("w_{i}"), test_doc(i)).expect("put");
        }
        write_stats_report();
        for i in 0..n {
            db.put_owned("bench", &format!("p_{i}"), test_doc(i)).expect("put");
        }
        let rep = write_stats_report();
        std::fs::remove_dir_all(&dir).ok();
        rep
    }

    #[test]
    fn profile_write_phases_manual() {
        // Run single-threaded: counters are process-global.
        let rep = profile_puts(DurabilityMode::Manual, 200);
        eprintln!("\n[Manual 200x put_owned]\n{rep}");
    }

    #[test]
    fn profile_blob_extract_alone() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-wprof-blob-{nanos}"));
        let db = Hako::open(&dir, HakoConfig::default()).expect("open");
        // Warm up everything (locks, allocator, code pages).
        for i in 0..50 {
            db.put_owned("bench", &format!("w_{i}"), test_doc(i)).expect("put");
        }
        // (a) shard fetch + read lock alone.
        let t = Instant::now();
        for _ in 0..2000 {
            let shard = db.get_shard("bench").expect("shard");
            let guard = shard.read().unwrap();
            std::hint::black_box(guard.blob_manager.is_some());
        }
        let lock_us = t.elapsed().as_micros() as f64 / 2000.0;
        // (b) full blob block as in write_batch_internal.
        let mut doc = test_doc(999);
        let key_arc: Arc<str> = Arc::from("p_999");
        let t = Instant::now();
        for _ in 0..2000 {
            let shard = db.get_shard("bench").expect("shard");
            let guard = shard.read().unwrap();
            let work = guard.blob_manager.as_ref()
                .map(|bm| bm.extract_blobs_raw("bench", &key_arc, &mut doc, 16 * 1024))
                .unwrap_or_default();
            std::hint::black_box(work.len());
        }
        let full_us = t.elapsed().as_micros() as f64 / 2000.0;
        eprintln!("\nblob micro: lock+fetch {lock_us:.2}us/iter, full block {full_us:.2}us/iter");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn profile_write_phases_always() {
        let rep = profile_puts(DurabilityMode::Always, 50);
        eprintln!("\n[Always 50x put_owned]\n{rep}");
    }
}
