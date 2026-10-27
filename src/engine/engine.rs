use std::collections::VecDeque;
use std::io::Write;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::mpsc::{channel, Receiver, Sender};
use std::sync::{Arc, Mutex, RwLock, Once};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};

use hashbrown::{HashMap, HashSet};

use crate::cancel::CancelToken;
use crate::config::{DurabilityMode, HakoConfig};
use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::error::{HakoError, Result};
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::index::service::IndexingService;
use crate::index::storage::index_storage::IndexStorage;
use crate::index::traverse::TraverseSpec;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::plan_cache::PlanCache;
use crate::query::query::Query;
use crate::query::builder::Collection;
use crate::storage::wal::WalOp;
use crate::storage::crypto::EncryptionContext;
use crate::storage::blob::{BlobManager, BlobWork};
use crate::storage::engine::{StorageEngine, Pointer};

use crate::util::lock::SafeLock;

use std::cell::RefCell;

static RAYON_INIT: Once = Once::new();

thread_local! {
    // A reusable buffer for serialization to avoid allocations
    static WRITE_BUFFER: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(64 * 1024));
    // A reusable buffer for building keys to avoid format!()
    static KEY_BUFFER: RefCell<String> = RefCell::new(String::with_capacity(128));
}

// --- Write-path phase accounting (ponytail) ---
// Always-on accumulators: a handful of Instant reads + Relaxed adds per
// batch (~0.1Âµs against 20Âµs+ batches). Read via `write_stats_report()`
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
    // hakodb#71/F7 — operational counters on the same always-on hook.
    pub queries: AtomicU64,
    pub tx_conflicts: AtomicU64,
    pub tx_retries: AtomicU64,
    pub unique_violations: AtomicU64,
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
    queries: AtomicU64::new(0),
    tx_conflicts: AtomicU64::new(0),
    tx_retries: AtomicU64::new(0),
    unique_violations: AtomicU64::new(0),
};

/// Per-index scan counter (F7): how often the planner actually routed a
/// query through each index. Mutex is fine — one lock per indexed query,
/// against an actual index walk.
static INDEX_SCANS: std::sync::LazyLock<Mutex<std::collections::HashMap<String, u64>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

/// F7: count one admitted unique-index rejection (called from the ledger).
pub(crate) fn record_unique_violation() {
    WRITE_STATS.unique_violations.fetch_add(1, Ordering::Relaxed);
}

fn record_index_scan(label: &str) {
    if let Ok(mut m) = INDEX_SCANS.lock() {
        *m.entry(label.to_string()).or_insert(0) += 1;
    }
}

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
    // F7 operational counters ride the same hook (also reset above).
    let conflicts = WRITE_STATS.tx_conflicts.swap(0, Ordering::Relaxed);
    let retries = WRITE_STATS.tx_retries.swap(0, Ordering::Relaxed);
    let unique = WRITE_STATS.unique_violations.swap(0, Ordering::Relaxed);
    let queries = WRITE_STATS.queries.swap(0, Ordering::Relaxed);
    s.push_str(&format!(
        "ops: {queries} queries, {conflicts} tx conflicts, {retries} tx retries, {unique} unique violations\n"
    ));
    if let Ok(mut scans) = INDEX_SCANS.lock() {
        if !scans.is_empty() {
            let mut entries: Vec<(String, u64)> = scans.drain().collect();
            entries.sort_by(|a, b| b.1.cmp(&a.1).then_with(|| a.0.cmp(&b.0)));
            s.push_str("index scans:");
            for (label, n) in entries {
                s.push_str(&format!(" {label}={n}"));
            }
            s.push('\n');
        }
    }
    s
}

// --- Lock-wait accounting (ponytail) ---
// Time BLOCKED acquiring shard locks (wait, not hold): the contention
// signal for mixed read/write load. Write hold times are already split
// in WRITE_STATS (wal/apply/cache/...); read hold is decode-dominated
// and mostly outside the guard (borrowed Inlined bytes), so wait-only is
// the honest metric. Same always-on discipline as WRITE_STATS (a pair of
// Instant reads + Relaxed adds per acquisition). Read via
// `lock_stats_report()` (resets), e.g. from a test or a mixed-load probe.
// Sampling points (not exhaustive â€” the three that gate mixed r/w):
// write_batch apply (shard write), Hako::get (shard read), query scan
// (storage read in execute Phase 1).
pub(crate) struct LockPhaseStats {
    pub write_wait_ns: AtomicU64,
    pub write_acqs: AtomicU64,
    pub read_wait_ns: AtomicU64,
    pub read_acqs: AtomicU64,
}
pub(crate) static LOCK_STATS: LockPhaseStats = LockPhaseStats {
    write_wait_ns: AtomicU64::new(0),
    write_acqs: AtomicU64::new(0),
    read_wait_ns: AtomicU64::new(0),
    read_acqs: AtomicU64::new(0),
};

/// Budget of the query row cache (#142 Q1): 512 decoded link-free rows,
/// FIFO eviction. Queries populate in page bursts (`<= limit + offset` per
/// query, gated to pages of `<= 250` rows), so the resident set stays small
/// even under wide query mixes.
const ROW_CACHE_CAP: usize = 512;

/// Storage for [`ROW_CACHE_CAP`]: map for O(1) probes, `order` for FIFO
/// eviction (pop oldest, insert newest).
#[derive(Default)]
struct RowCache {
    map: HashMap<String, (u64, Arc<HakoDoc>)>,
    order: std::collections::VecDeque<String>,
}

thread_local! {
    /// Scratch buffer for the doc-cache key on point reads. Building
    /// `"{collection}\0{doc_id}"` with `format!` allocated + freed a String
    /// on every single get, hit or miss; a reused buffer removes that from
    /// the hottest read path.
    static CACHE_KEY_BUF: std::cell::RefCell<String> =
        std::cell::RefCell::new(String::with_capacity(96));
}

/// Human-readable lock-wait table + reset. `avg` per acquisition â€”
/// ~0 when free, >> 0 under contention.
pub fn lock_stats_report() -> String {
    let wacq = LOCK_STATS.write_acqs.load(Ordering::Relaxed);
    let racq = LOCK_STATS.read_acqs.load(Ordering::Relaxed);
    let ww = LOCK_STATS.write_wait_ns.load(Ordering::Relaxed);
    let rw = LOCK_STATS.read_wait_ns.load(Ordering::Relaxed);
    let s = format!(
        "lock profile: {wacq} write acqs, {racq} read acqs\n  write {wacq} total wait {:.2}us avg\n  read  {racq} total wait {:.2}us avg\n",
        ww as f64 / 1000.0 / wacq.max(1) as f64,
        rw as f64 / 1000.0 / racq.max(1) as f64,
    );
    for v in [&LOCK_STATS.write_wait_ns, &LOCK_STATS.write_acqs,
        &LOCK_STATS.read_wait_ns, &LOCK_STATS.read_acqs] {
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
    // ponytail: shared id â€” every write fans out one event per mutation and
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
/// credential stores. (`__hako_security` is deliberately NOT here â€”
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

/// Recursively sum file sizes under `dir` (issue #31 DropReport).
fn dir_size(dir: &Path) -> u64 {
    let mut total = 0u64;
    let Ok(entries) = std::fs::read_dir(dir) else { return 0 };
    for entry in entries.flatten() {
        let Ok(meta) = entry.metadata() else { continue };
        if meta.is_dir() {
            total += dir_size(&entry.path());
        } else {
            total += meta.len();
        }
    }
    total
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
    /// Uniqueness constraint (hakodb#71). Additive key: older JSON
    /// consumers ignore it; absent on legacy files → false.
    #[serde(default)]
    pub unique: bool,
    /// Sparse (missing fields skip the doc) vs dense (missing = NULL
    /// tuple). Inert for non-unique indexes; additive key as above.
    #[serde(default)]
    pub sparse: bool,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct IndexList {
    pub simple: HashMap<String, Vec<String>>,
    pub secondary: HashMap<String, Vec<String>>,
    pub fts: HashMap<String, Vec<String>>,
    pub composite: Vec<CompositeIndexInfo>,
    pub vector: HashMap<String, Vec<VectorIndexInfo>>,
    /// Tracked traverse-target collections (issue #53). Additive key with
    /// `#[serde(default)]` like vector (older JSON consumers ignore it);
    /// bincode planes treat def-sync as same-minor lockstep (see #51).
    #[serde(default)]
    pub traverse: Vec<String>,
}

/// One vector definition for list_indexes (additive key: older JSON
/// consumers ignore unknown top-level keys).
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorIndexInfo {
    pub field: String,
    pub dim: u32,
    pub metric: String,
    /// HNSW params (#141-1); `None` = derived defaults. Serialized to FFI
    /// consumers as null - additive keys, old consumers unaffected.
    pub m: Option<usize>,
    pub ef_construction: Option<usize>,
    pub ef_search: Option<usize>,
    /// Live stats (#141-7), serde-defaulted so older persisted defs load
    /// unchanged and JSON consumers get additive keys only.
    /// `indexed` = vectors currently in the HNSW graph; `complete` =
    /// backfill finished (ANN authoritative, same gate the query path
    /// uses); `mem_estimate` = rough resident-bytes bound (see
    /// `HnswIndex::bytes_estimate`).
    #[serde(default)]
    pub indexed: usize,
    #[serde(default)]
    pub complete: bool,
    #[serde(default)]
    pub mem_estimate: u64,
}

/// Analytics node key (#156 Phase E): (collection, doc id) - shared by
/// the whole-graph fns.
pub type GraphNode = (String, String);
/// Per-node edge map over `GraphNode`.
pub type GraphAdj = std::collections::HashMap<GraphNode, Vec<GraphNode>>;
/// One `graph_degree_centrality` row: (node, in-degree, out-degree).
pub type DegreeRow = (GraphNode, usize, usize);

/// One graph hit: collection + id (paths cross collections), BFS depth,
/// and the doc itself. Returned by `Hako::find_paths`.
#[derive(Debug, Clone)]
pub struct TraverseHit {
    pub collection: String,
    pub id: String,
    pub depth: usize,
    /// Accumulated path cost (BFS: hops as f64; Dijkstra: weight sum).
    pub cost: f64,
    /// Full discovery path from the start node to this hit, inclusive on
    /// both ends, as (collection, doc_id) pairs (#140-1). Single node =
    /// the start document itself.
    pub path: Vec<(String, String)>,
    /// Edge doc used at each hop, positionally aligned with the path's
    /// hops (#156 Phase F-1, #140-7): `via[i]` = `"edge_col/id"` connecting
    /// `path[i] -> path[i+1]`, `None` for inline-reference hops. Length ==
    /// hops == `path.len() - 1`.
    pub via: Vec<Option<String>>,
    pub doc: HakoDoc,
}

#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
struct PersistedIndexState {
    secondary: HashMap<String, Vec<String>>,
    fts: HashMap<String, Vec<String>>,
    composite: Vec<PersistedCompositeIndex>,
    /// `default` = pre-vector files still load (whole-struct fallback
    /// would otherwise wipe ALL defs on old files).
    #[serde(default)]
    vector: HashMap<String, Vec<PersistedVectorIndex>>,
    /// Tracked traverse targets (issue #53). Same default rule: pre-graph
    /// files load with tracking off.
    #[serde(default)]
    traverse: Vec<String>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedVectorIndex {
    field: String,
    dim: u32,
    metric: String,
    /// HNSW params (#141-1). Absent on pre-0.19 files → derived defaults.
    #[serde(default)]
    m: Option<usize>,
    #[serde(default)]
    ef_construction: Option<usize>,
    #[serde(default)]
    ef_search: Option<usize>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct PersistedCompositeIndex {
    id: u32, 
    collection: String,
    fields: Vec<(String, String)>,
    /// Unique-index constraint (hakodb#71). Additive with serde defaults:
    /// legacy defs.json files load as non-unique exactly like before.
    #[serde(default)]
    unique: bool,
    #[serde(default)]
    sparse: bool,
    /// Filtered-index predicate. Serialized as a full FilterGroup; older
    /// files default to None.
    #[serde(default)]
    filter: Option<crate::query::filter::FilterGroup>,
}

pub struct Transaction {
    pub mutations: Vec<BatchMutation>,
}

struct ShardWork {
    ops: Vec<WalOp>,
    keys: Vec<Arc<str>>,
    /// Keys whose final op in this batch is a delete. Their version entry is
    /// removed after the write so an absent read (None) stays comparable.
    deleted: HashSet<Arc<str>>,
    events: Vec<(String, ChangeEvent)>,
    index_puts: Vec<(String, Arc<HakoDoc>)>, 
    index_deletes: Vec<(String, HakoDoc)>,
    blob_queue_items: Vec<BlobWork>,
}

/// One unique index participating in write-batch admission (hakodb#71).
/// Defs are cloned once per collection per batch; per-doc work reuses
/// them. `index_id` mirrors `def.id` (the ledger keys by index id).
#[derive(Clone)]
struct GateDef {
    index_id: u32,
    def: crate::index::composite::definition::CompositeIndexDefinition,
}

/// Scope guard: on drop (batch abort â€” violation, WAL failure, panic)
/// replays the batch's ledger mutations in reverse, restoring exact
/// pre-batch claim state. Armed on construction, disarmed on success.
/// Owns the undo vec so the write loop can pass `&mut` freely.
///
/// ponytail: a WAL failure after partial applies removes ledgers for
/// rows that DID land â€” uniqueness for those rows under-enforces until
/// they rewrite or reload. A WAL write failure is already the
/// disk-full/corruption event path; acceptable trade, noted here.
struct LedgerRollbackGuard<'a> {
    ledger: &'a crate::index::composite::ledger::UniqueLedger,
    undo: Vec<crate::index::composite::ledger::LedgerUndo>,
    armed: bool,
}

impl<'a> LedgerRollbackGuard<'a> {
    fn new(ledger: &'a crate::index::composite::ledger::UniqueLedger) -> Self {
        Self {
            ledger,
            undo: Vec::new(),
            armed: true,
        }
    }
    fn disarm(mut self) {
        self.armed = false;
    }
}

impl Drop for LedgerRollbackGuard<'_> {
    fn drop(&mut self) {
        if self.armed && !self.undo.is_empty() {
            self.ledger.rollback(&self.undo);
        }
    }
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
        db.write_batch(self.mutations)
    }
}

/// Bounded retry policy for [`Hako::transaction`] (issue #71/F5 item 2).
#[derive(Debug, Clone, Copy)]
pub struct RetryPolicy {
    pub max_attempts: u32,
    pub base_delay: Duration,
    pub max_delay: Duration,
}

impl RetryPolicy {
    /// Exponential backoff, doubling from `base_delay` up to `max_delay`.
    pub fn exponential(max_attempts: u32, base_delay: Duration, max_delay: Duration) -> Self {
        Self {
            max_attempts,
            base_delay,
            max_delay,
        }
    }
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self::exponential(8, Duration::from_millis(1), Duration::from_millis(100))
    }
}

/// Serializable transaction read-set + mutations (issue #71/F5).
#[derive(Default)]
pub struct SerializableTransaction {
    pub reads: HashMap<String, Option<u64>>,
    pub mutations: Vec<BatchMutation>,
}

impl SerializableTransaction {
    /// The FIRST read of a key wins: a later re-read (e.g. the one inside
    /// `patch`) must not quietly refresh the read-set, or a transaction that
    /// already based a decision on the old version would commit over a
    /// concurrent write instead of conflicting.
    ///
    /// Issue #71/F5 item 3: registers the read-set from ONE atomic
    /// shard-locked read (doc + version together). The previous
    /// `db.get()` + `db.current_version()` pair left the window where a
    /// commit landing between the two calls recorded the NEW version for
    /// the OLD document â€” a silent lost-update read-modify-write.
    pub fn get(
        &mut self,
        db: &Hako,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<HakoDoc>> {
        let key = doc_id.to_string();
        match db.get_versioned(collection, doc_id)? {
            Some((doc, version)) => {
                self.reads.entry(key).or_insert(Some(version));
                Ok(Some(doc))
            }
            None => {
                self.reads.entry(key).or_insert(None);
                Ok(None)
            }
        }
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
    /// Issue #71/F5 item 5: patch inside the transaction. `get` first so the
    /// doc version joins the read-set (a concurrent commit conflicts), then
    /// queue the same `Patch` mutation `Hako::patch` uses — top-level field
    /// upsert semantics included.
    pub fn patch(
        &mut self,
        db: &Hako,
        collection: &str,
        doc_id: &str,
        updates: Vec<(String, Value)>,
    ) -> Result<()> {
        self.get(db, collection, doc_id)?;
        self.mutations.push(BatchMutation::Patch {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            updates,
        });
        Ok(())
    }
    /// Issue #71/F5 item 5: batched read-set registration via the item-3
    /// atomic read. One hash lookup per id, same shard lock each time.
    pub fn get_many(
        &mut self,
        db: &Hako,
        collection: &str,
        doc_ids: &[String],
    ) -> Result<Vec<Option<HakoDoc>>> {
        doc_ids
            .iter()
            .map(|id| self.get(db, collection, id))
            .collect()
    }
    pub fn commit(&self, db: &Hako) -> Result<Vec<String>> {
        db.commit_serializable(self.reads.clone(), self.mutations.clone())
    }
}

pub(crate) enum IndexOp {
    Update {
        collection: String,
        // puts: Arc<Vec<(String, HakoDoc)>>, 
        puts: Arc<Vec<(String, Arc<HakoDoc>)>>, 
        deletes: Vec<(String, HakoDoc)>,
        /// Per-collection count carried with the op: the query gate reads
        /// it without locking the worker or the global counter.
        pending: Arc<AtomicUsize>,
    },
}

/// Snapshot of background activity behind `Hako::await_quiescent`.
/// Fresh writes settle through four stages â€” open-time index recovery,
/// async index updates, blob persistence, periodic maintenance â€” and a
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

/// Decrements the backfill counter on scope exit â€” including panics, so a
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
    /// Live group-commit window ms (Interval mode). Initialized from the
    /// clamped open-time config; the runtime setter below keeps this and
    /// every shard WAL in lockstep (atomic, no lock on the read path).
    group_commit_interval_ms: AtomicU64,
    pub(crate) shards: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>>, // The only storage
    index_storage: Arc<Mutex<IndexStorage>>,
    pub(crate) indexes: Arc<RwLock<IndexManager>>,
    executor: ParallelQueryExecutor,
    tx_lock: Mutex<()>,
    listeners: Mutex<HashMap<String, Vec<Sender<ChangeEvent>>>>,
    // doc_versions: RwLock<HashMap<String, u64>>,
    pub(crate) doc_versions: Vec<RwLock<HashMap<Arc<str>, u64>>>,
    global_version: AtomicU64,
    /// Hybrid-clock watermark (issue #80): the highest doc timestamp this
    /// engine has ever emitted OR accepted from the wire. Local writes stamp
    /// at `max(now, watermark + 1)` so a wall-clock jump backward (NTP step,
    /// VM restore) can never emit a timestamp older than one already
    /// circulating â€” which would let a jumped peer's stale-but-future-
    /// stamped bootstrap re-offer silently resurrect over newer local
    /// writes, forever. Remote ingest also raises the watermark (see
    /// sync_core / cloud_sync apply paths).
    last_ts_watermark: std::sync::atomic::AtomicI64,
    security_rules: RwLock<Vec<SecurityRule>>,
    /// ponytail: lock-free gate for allowed(). The common case (no rules)
    /// skips the RwLock acquisition on every op; set alongside the rules.
    security_enabled: std::sync::atomic::AtomicBool,
    /// Read-only enforcement (hakocluster phase 3): when set, write_batch
    /// â€” the single admission gate for ALL local writes â€” refuses, while
    /// replicated ingest (apply_replicated_batch, bypasses write_batch by
    /// design) keeps converging. Reads never check this.
    read_only: std::sync::atomic::AtomicBool,
    audit_data: Arc<RwLock<Vec<AuditEntry>>>,
    audit_tx: Sender<AuditEntry>,
    pub(crate) index_tx: Sender<IndexOp>,
    /// ponytail: ops sent to the async index worker but not yet applied.
    /// Quiescence reads this instead of channel len (std mpsc has none).
    pub(crate) index_inflight: Arc<std::sync::atomic::AtomicUsize>,
    /// Per-collection pending IndexOps. A write in collection A must not
    /// force a full scan in collection B; the global counter remains for
    /// quiescence. Entries stay at zero after drain (bounded by collections)
    /// so a concurrent enqueue can never race a map removal.
    index_inflight_by_collection: Arc<RwLock<HashMap<String, Arc<AtomicUsize>>>>,
    /// ponytail: create_*_index backfill threads in flight (one per index
    /// build over existing docs). Quiescence covers these too â€” an index
    /// that exists-but-is-backfilling otherwise serves partial results
    /// with no signal at all.
    pub(crate) backfill_inflight: Arc<std::sync::atomic::AtomicUsize>,
    /// Collections whose RAM index was (re)built by a touch/load AFTER
    /// open, newer than any snapshot import. Recovery import skips them
    /// (import-then-touch and touch-then-import converge identically;
    /// without this the late import clobbers fresh fills with stale
    /// snapshot state â€” a load-bearing race on every lazy touch).
    index_touched: Arc<Mutex<std::collections::HashSet<String>>>,
    /// Per-collection lazy-load backfill progress (done, total). Present
    /// only while a `start_collection_load` thread runs; removed on
    /// completion or unload-abort. Poll via `collection_load_progress`.
    backfill_progress: Arc<Mutex<HashMap<String, (usize, usize)>>>,
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
    /// IO with it â€” quiescence checks read this (see await_quiescent).
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
    /// Query row cache (#142 Q1): decoded link-free rows the executor's
    /// fast path serves, keyed `col\0id` with the version at decode time.
    /// Separate from `doc_cache` (point reads own that) because queries
    /// populate in page bursts: the budget is 512 with FIFO eviction.
    /// Correctness rides the same contract as `doc_cache` — every write
    /// bumps the version and the probe requires an exact match — so dead
    /// entries (updated/deleted/dropped keys) are simply never matched
    /// again; they leave by FIFO without the write path paying an eager
    /// removal pass.
    row_cache: RwLock<RowCache>,
    /// Local-only replication scope ("the signal"): collections and keys
    /// whose writes must never leave this device. Advancing clocks is
    /// untouched (tombstones keep fresh timestamps, so the deleter never
    /// looks "behind" to a handshake); every sync outbound tailer and both
    /// handshake catch-up paths consult `is_local_only` and skip matches.
    /// Key format is `col\0id` (NUL separator â€” ids may contain anything).
    /// Persisted best-effort into `__hako_system/local_only` so a
    /// restart can't re-tail un-checkpointed local-only ops upstream.
    pub(crate) local_only_cols: RwLock<HashSet<String>>,
    pub(crate) local_only_keys: RwLock<HashSet<String>>,
    /// Unique-index claims ledger (hakodb#71). Synchronous admission for
    /// unique composite indexes â€” the async tree leaves a duplicate
    /// window, so writes consult this instead. Derived/rebuildable from
    /// storage (trees + recovery fold); WAL stays the durability source.
    pub(crate) unique_ledger: std::sync::Arc<crate::index::composite::ledger::UniqueLedger>,
    /// Cached unique-index defs per collection (hakodb#71). The gate used
    /// to take `indexes.read()` once per batch, which starves behind the
    /// indexer's writer lock (std RwLock is write-preferring): ~35us per
    /// 10-doc batch on the bench box. This cache is written only when
    /// composite defs change (create / drop_collection / restore), so the
    /// per-batch read stays uncontended.
    gate_cache: RwLock<HashMap<String, Arc<Vec<GateDef>>>>,
    /// Optional per-collection schemas (hakodb#71/F8). Empty by default;
    /// `schema_enabled` is the lock-free gate so schemaless deployments pay
    /// no RwLock on the write path. Persisted to `_indices/schemas.json`
    /// (defs, like indexes — never derived data).
    schemas: RwLock<HashMap<String, crate::schema::CollectionSchema>>,
    schema_enabled: std::sync::atomic::AtomicBool,
}

/// Relocate report: moved ids + ids missing at source.
#[derive(Debug, Clone, Default, serde::Serialize)]
pub struct RelocateReport {
    pub moved: Vec<String>,
    pub missing: Vec<String>,
}

/// Per-collection size/accounting snapshot (issue #32).
///
/// **Not a transaction**: a concurrent writer mutates segments and WAL
/// between reads, so fields may disagree by a few writes. For sizing,
/// disk-pressure alerting and ordering decisions only â€” never diff them
/// for correctness.
#[derive(Debug, Clone, serde::Serialize)]
pub struct CollectionStats {
    pub collection: String,
    /// Live (non-tombstoned) documents currently in the RAM index.
    pub doc_count: u64,
    /// Post-tombstone accounting: the shard's tracked inlined bytes.
    pub live_bytes: u64,
    /// On-disk total: every file under the shard directory (segments +
    /// WAL + blobs + any residue).
    pub total_bytes: u64,
    pub segment_count: u32,
    pub wal_bytes: u64,
    pub blob_bytes: u64,
    pub tombstone_count: u64,
    /// Lazy residency: parked on disk, absent from the shard map.
    pub unloaded: bool,
}

/// Result of [`Hako::explain_report`] (issue #71/F7). Plan-only: no query
/// execution, so `collection_rows` is the scan's upper bound and
/// `filter_count` the predicates applied after it.
#[derive(Debug, Clone, serde::Serialize)]
pub struct ExplainReport {
    pub collection: String,
    pub scan: String,
    pub index: Option<String>,
    pub collection_rows: usize,
    pub filter_count: usize,
    pub or_group_count: usize,
    pub elem_match_count: usize,
    pub filters_satisfied_by_index: bool,
    pub order_by_satisfied: bool,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub projection: Vec<String>,
    /// F6 / #22: true when the projected fields are served straight from
    /// the index (no document read) - the query is covering.
    pub covering: bool,
    /// Index definitions that exist on the collection (candidates the
    /// planner could have used — "why didn't my index run?").
    pub available_indexes: Vec<String>,
}

impl std::fmt::Display for ExplainReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "collection: {} ({} docs)", self.collection, self.collection_rows)?;
        match &self.index {
            Some(idx) => writeln!(f, "scan: {}  [index: {idx}]", self.scan)?,
            None => writeln!(f, "scan: {}  [no index]", self.scan)?,
        }
        writeln!(
            f,
            "filters: {} post-scan, {} or-groups, {} elemMatch (satisfied_by_index: {})",
            self.filter_count, self.or_group_count, self.elem_match_count,
            self.filters_satisfied_by_index
        )?;
        writeln!(f, "order satisfied by scan: {}", self.order_by_satisfied)?;
        if self.limit.is_some() || self.offset.is_some() {
            writeln!(f, "limit: {:?} offset: {:?}", self.limit, self.offset)?;
        }
        if !self.projection.is_empty() {
            writeln!(f, "projection: {}{}", self.projection.join(","),
                if self.covering { " (covering: index-only)" } else { "" })?;
        }
        if !self.available_indexes.is_empty() {
            writeln!(f, "available indexes: {}", self.available_indexes.join(", "))?;
        }
        Ok(())
    }
}

/// Result of [`Hako::drop_collection`].
#[derive(Debug, Clone, serde::Serialize)]
pub struct DropReport {
    /// Live docs that existed at drop time (each now tombstoned on the
    /// WAL, so sync peers converge to empty via the normal tail).
    pub docs_removed: u64,
    /// On-disk bytes the shard directory held when it was unlinked.
    pub bytes_reclaimed: u64,
    /// The dropped collection name(s) â€” always the requested one on
    /// success; empty when the collection did not exist (idempotent).
    pub collections_dropped: Vec<String>,
}

impl Hako {
    pub fn open(path: impl AsRef<Path>, config: HakoConfig) -> Result<Self> {

        let query_threads = config.query_workers.clamp(1, 8);
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
            IndexStorage::open(&index_log_path, &snapshot_dir).map_err(HakoError::Io)?,
        ));

        // --- WORKER 1: PERSISTENT INDEX WORKER ---
        let index_inflight = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let index_inflight_by_collection =
            Arc::new(RwLock::new(HashMap::<String, Arc<AtomicUsize>>::new()));
        let idx_clone = Arc::clone(&indexes);
        let index_inflight_worker = Arc::clone(&index_inflight);
        let storage_persist = Arc::clone(&index_storage);
        // P2: blob-resident embeddings resolve through the owning shard's
        // blob manager (clone is one Arc bump; resolution itself is gated
        // per-collection below).
        let shards_for_index = Arc::clone(&shards);
        thread::spawn(move || {
            while let Ok(op) = index_rx.recv() {
                match op {
                    IndexOp::Update { collection, puts, deletes, pending } => {
                        // P2: blob-resident embeddings must be durable
                        // BEFORE the indexer resolves them â€” blob puts
                        // land in an async flush queue, so resolving
                        // straight from the link would read unwritten
                        // regions (zeros/garbage indexed as vectors).
                        // Drain first, then resolve. Gated: only when a
                        // put actually carries a BlobLink in a vector
                        // field (lock-free scan, no guard held while
                        // draining â€” never nest index/shard locks).
                        let vec_probe: Vec<String> = idx_clone
                            .read()
                            .map(|m| {
                                m.vector
                                    .get(&collection)
                                    .map(|mm| mm.keys().cloned().collect())
                                    .unwrap_or_default()
                            })
                            .unwrap_or_default();
                        if !vec_probe.is_empty() {
                            let mut need_drain = false;
                            'scan: for (_, doc) in puts.iter() {
                                for f in &vec_probe {
                                    if matches!(doc.get(f), Some(Value::BlobLink { .. })) {
                                        need_drain = true;
                                        break 'scan;
                                    }
                                }
                            }
                            if need_drain {
                                if let Some(sh) = shards_for_index
                                    .read()
                                    .ok()
                                    .and_then(|s| s.get(&collection).cloned())
                                {
                                    if let Ok(mut g) = sh.write() {
                                        let _ = g.drain_blob_queue();
                                    }
                                }
                            }
                        }
                        let mut mgr = match idx_clone.write() { Ok(g) => g, Err(_) => break };
                        // P2 inflate-on-index: collections carrying vector
                        // defs index the RESOLVED doc so live writes agree
                        // with backfill (which resolves). Every other
                        // collection skips this entirely (no clone, no
                        // blob touch).
                        let vec_fields: Vec<String> = mgr
                            .vector
                            .get(&collection)
                            .map(|m| m.keys().cloned().collect())
                            .unwrap_or_default();
                        let blob_mgr = if vec_fields.is_empty() {
                            None
                        } else {
                            shards_for_index
                                .read()
                                .ok()
                                .and_then(|s| s.get(&collection).cloned())
                                .and_then(|sh| {
                                    sh.read().ok().and_then(|g| g.blob_manager.clone())
                                })
                        };
                        let mut persist = match storage_persist.lock() { Ok(g) => g, Err(_) => break };
                        for (id, doc) in puts.iter() {
                            // Resolved copy only when a vector field needs
                            // it; otherwise the original Arc (zero extra
                            // alloc on the hot path).
                            let resolved = IndexingService::resolve_vector_blobs(
                                doc,
                                &vec_fields,
                                blob_mgr.as_deref(),
                            );
                            let doc_ref: &HakoDoc = resolved.as_ref().unwrap_or(doc);
                            IndexingService::apply_put(&mut mgr, &collection, id, doc_ref);
                            for idx in mgr.indexes_for_collection(&collection) {
                                // Filtered defs only log/serve matching docs
                                // (hakodb#71) — the log must mirror the tree.
                                if !idx.definition.filter_passes(id, doc_ref) {
                                    continue;
                                }
                                if let Some(vals) = idx.document_values(id, doc_ref) {
                                    let key_bytes = crate::index::composite::key_encoder::encode_composite_key(&idx.definition, &vals, id);
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
                        pending.fetch_sub(1, Ordering::AcqRel);
                        index_inflight_worker.fetch_sub(1, Ordering::Relaxed);
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

        let indexes_sys_ptr = Arc::clone(&indexes);
        let root_path_sys = root_path.clone();
        // ponytail: copied, not shared â€” a benchmark holds maintenance for
        // a whole run; live-toggling mid-run would only add a race for no
        // benefit (the tick re-reads every 5s anyway if this ever needs it).
        let maintenance_on = config.background_maintenance;

        let system_handle_thread = thread::spawn(move || {
            let mut last_maint = Instant::now();
            // Query-aware shedding: consecutive ticks deferred while
            // queries fly (see below). Bounded so backlog drains under
            // sustained load instead of growing without limit.
            let mut deferred_ticks: u32 = 0;
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
                    // is always safe â€” writes/reads never depend on this
                    // block, files just grow until it runs again.
                    if maintenance_on {
                    // ponytail: visible to quiescence checks for the whole
                    // block (checkpoint, purge, snapshots) â€” set even though
                    // the inner locks are try_-based, because readers still
                    // share page cache and IO bandwidth with it.
                    maintenance_for_system.store(true, Ordering::Release);
                    // Query-aware shedding: queries in flight â†’ skip the
                    // NON-DURABILITY heavy work this tick (tier compaction,
                    // segment spill, tombstone purge, both snapshots) so
                    // queries never queue behind their I/O. WAL flush
                    // stays (durability is never deferred). At most 3
                    // consecutive sheds, then run regardless â€” backlog
                    // must drain under sustained load.
                    let shedding = crate::query::executor::executor::queries_inflight() > 0
                        && deferred_ticks < 3;
                    if shedding {
                        deferred_ticks += 1;
                    } else {
                        deferred_ticks = 0;
                    }
                    let active_shards: Vec<Arc<RwLock<StorageEngine>>> = shards_sys_clone.read().unwrap().values().cloned().collect();
                    for s in active_shards {
                        // Tier compaction runs lock-free (prepare/commit
                        // only) and only when not shedding â€” merge I/O
                        // never queues queries, guard held or not.
                        if !shedding {
                            let _ = StorageEngine::try_compact_tiers(&s);
                        }
                        if let Ok(storage) = s.try_read() {
                            if let Some(ref bm) = storage.blob_manager { let _ = bm.file().sync_data(); }
                        }
                        if let Ok(mut storage) = s.try_write() {
                            let _ = storage.run_background_maintenance(shedding);
                            if !shedding {
                                storage.purge_old_tombstones(Duration::from_secs(86400));
                            }

                            // 2. TRIGGER: If pending blob bytes > 16MB, touch the blob worker
                            if storage.total_pending_blob_bytes.load(Ordering::Relaxed) > 16 * 1024 * 1024 {
                                trigger_for_system.store(true, Ordering::Release);
                            }
                        }
                    }
                    if !shedding {
                        if let Ok(mut persist) = index_sys_ptr.try_lock() {
                            if persist.snapshot(1).is_ok() { let _ = persist.reset_log(); }
                        }

                        // Periodically snapshot RAM indexes (prevent loss on crash)
                        let _ = Self::write_index_snapshot(
                            &root_path_sys,
                            &shards_sys_clone,
                            &indexes_sys_ptr,
                        );
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
        let shards_blob_clone = Arc::clone(&shards);        let trigger_for_blobs_w3 = Arc::clone(&trigger_flush);
        let blob_worker_handle = thread::spawn(move || {
            // Stop is a one-shot message: once observed it must stay
            // observed, otherwise a busy shutdown iteration consumes the
            // signal and the worker idles forever (teardown hang).
            let mut stop_latched = false;
            // Meta-update completions waiting on a contended shard lock.
            // The worker NEVER blocks: a contended shard is skipped and
            // retried next tick (entries stay BlobPending, served from
            // their Arc â€” reads stay correct meanwhile). Bounded: each
            // item is carried at most until its shard frees.
            #[allow(clippy::type_complexity)]
            let mut carry: Vec<(Arc<RwLock<StorageEngine>>, String, Vec<u8>, i64, usize)> = Vec::new();
            loop {
                stop_latched = stop_latched || stop_rx.try_recv().is_ok();
                let shutting_down = stop_latched;
                let triggered = trigger_for_blobs_w3.swap(false, Ordering::Acquire);
                
                let active_shards = { shards_blob_clone.read().unwrap().values().cloned().collect::<Vec<_>>() };
                let mut processed_any = false;

                // 0. Retry carried-over meta updates first (one try
                // each; still-contended items ride to the next tick).
                if !carry.is_empty() {
                    let due = std::mem::take(&mut carry);
                    for (shard_arc, key, skeleton, timestamp, len) in due {
                        // ponytail: try on a clone so the Err arm can move
                        // the original back without fighting the guard borrow.
                        match Arc::clone(&shard_arc).try_write() {
                            Ok(mut shard) => {
                                Self::apply_blob_meta(&mut shard, key, skeleton, timestamp, len);
                                processed_any = true;
                            }
                            Err(_) => carry.push((shard_arc, key, skeleton, timestamp, len)),
                        }
                    }
                }

                for shard_arc in &active_shards {
                    // Clone the manager without holding any guard across
                    // IO: a contended shard is skipped whole, nothing is
                    // popped and nothing is lost.
                    let bm = match shard_arc.try_read() {
                        Ok(shard) => match shard.blob_manager.clone() {
                            Some(bm) => bm,
                            None => continue,
                        },
                        Err(_) => continue,
                    };
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

                    let mut completed_keys = Vec::with_capacity(batch.len());

                    for work in batch {
                        if let BlobWork::PutRaw { key, offset, data, skeleton, timestamp, len, ..} = work {
                            // 1. FAST PHYSICAL WRITE (OS Page Cache)
                            if bm.write_at(&data, offset).is_ok() {
                                completed_keys.push((key, skeleton, timestamp, len as usize));
                            }
                        }
                    }

                    // 2. ATOMIC META UPDATE (One lock per batch, never blocking)
                    if !completed_keys.is_empty() {
                        if let Ok(mut shard) = shard_arc.try_write() {
                            for (key, skeleton, timestamp, len) in completed_keys {
                                Self::apply_blob_meta(&mut shard, key, skeleton, timestamp, len);
                            }
                        } else {
                            for (key, skeleton, timestamp, len) in completed_keys {
                                carry.push((Arc::clone(shard_arc), key, skeleton, timestamp, len));
                            }
                        }
                    }
                    thread::yield_now(); 
                }

                if !processed_any && !triggered {
                    thread::sleep(Duration::from_millis(20));
                }
                // Exit only fully drained: a carried meta still owes its
                // index flip (Drop's own drain covers the queue itself).
                // At shutdown the system is quiescing, so a transiently
                // contended shard frees within ticks.
                if shutting_down && !processed_any && carry.is_empty() { break; }
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
            group_commit_interval_ms: AtomicU64::new(crate::config::clamp_group_commit_interval_ms(
                config.group_commit_interval_ms,
            )),
            shards,
            index_storage: Arc::clone(&index_storage),
            indexes,
            executor: ParallelQueryExecutor::new(config.query_workers),
            tx_lock: Mutex::new(()),
            listeners: Mutex::new(HashMap::new()),
            doc_versions,
            // ponytail: seed from wall-clock micros, not 1. Versions back
            // ETags and the doc_cache: restarting at 1 reissues small
            // versions that collide with pre-restart ETags held by polling
            // clients (daily reboots made this a real 304-stale hole, not
            // theoretical). Micros always advance across restarts, so fresh
            // versions can never equal pre-restart ones (same-process
            // uniqueness still comes from fetch_add). Caveat: a clock
            // stepped BACKWARD past the last run reopens the hole â€”
            // ETag users with strict needs should add a boot epoch.
            global_version: AtomicU64::new(crate::util::clock::now_micros().max(1) as u64),
            last_ts_watermark: std::sync::atomic::AtomicI64::new(crate::util::clock::now_micros()),
            security_rules: RwLock::new(Vec::new()),
            security_enabled: std::sync::atomic::AtomicBool::new(false),
            read_only: std::sync::atomic::AtomicBool::new(false),
            audit_tx,
            index_tx,
            index_inflight,
            index_inflight_by_collection,
            backfill_inflight: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            backfill_progress: Arc::new(Mutex::new(HashMap::new())),
            index_touched: Arc::new(Mutex::new(std::collections::HashSet::new())),
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
            row_cache: RwLock::new(RowCache::default()),
            local_only_cols: RwLock::new(HashSet::new()),
            local_only_keys: RwLock::new(HashSet::new()),
            unique_ledger: std::sync::Arc::new(crate::index::composite::ledger::UniqueLedger::default()),
            gate_cache: RwLock::new(HashMap::new()),
            schemas: RwLock::new(HashMap::new()),
            schema_enabled: std::sync::atomic::AtomicBool::new(false),
        };

        let _ = db.restore_index_defs();
        let _ = db.restore_schemas();
        let _ = db.restore_local_only();

        // 6. ORCHESTRATED BACKGROUND RECOVERY
        let shards_ptr = Arc::clone(&db.shards);
        let indexes_ptr = Arc::clone(&db.indexes);
        let persist_ptr = Arc::clone(&db.index_storage);
        let config_thread = config.clone();
        let backfill_ptr = Arc::clone(&db.backfill_inflight);
        let index_touched_ptr = Arc::clone(&db.index_touched);
        let ledger_ptr = Arc::clone(&db.unique_ledger);
        
        // Capture the new Crossbeam Sender
        let blob_tx_thread = db.blob_tx.clone(); 
        
        let root_scan = root_path.clone();
        let snapshot_path = root_path.join("_indices").join("ram_indexes.bin");

        thread::spawn(move || {
            // --- STEP A: Try to load RAM Snapshot FIRST ---
            let mut snapshot_loaded = false;
            // True when the snapshot already carried composite trees (new
            // format). Legacy files restore secondary/fts only; composites
            // backfill below (one-time upgrade path).
            let mut composite_restored = false;
            // WAL tails at snapshot time (None when the file predates
            // them): per-collection freshness for the STEP C decision.
            let mut snap_tails: Option<crate::index::manager::WalTails> = None;
            if snapshot_path.exists() {
                if let Ok(bytes) = std::fs::read(&snapshot_path) {
                    let mut mgr = indexes_ptr.write().unwrap();
                    // Collections touched after open are newer than any
                    // snapshot â€” leave their entries alone (both orders
                    // converge; otherwise a late import clobbers fresh
                    // fills with stale state).
                    let skip = index_touched_ptr.lock().map(|g| g.clone()).unwrap_or_default();
                    if let Ok((restored, tails)) = mgr.import_state(&bytes, &skip) {
                        snapshot_loaded = true;
                        composite_restored = restored;
                        snap_tails = tails;
                        // hakodb#71: unique-def trees restored from the
                        // snapshot refold into the claim ledger. Skipped
                        // collections (touched after open, fresher than
                        // the snapshot) fold from their STEP C rescan
                        // instead. Legacy dumps (restored=false) leave
                        // the fold to STEP E's backfill threads.
                        if restored {
                            let targets: Vec<u32> = mgr
                                .composite
                                .all_indexes()
                                .filter(|i| i.definition.unique
                                    && !skip.contains(&i.definition.collection))
                                .map(|i| i.definition.id)
                                .collect();
                            for uid in targets {
                                if let Some(idx) = mgr.composite.get(uid) {
                                    let _ = ledger_ptr.load_from_tree(uid, &idx.tree);
                                }
                            }
                        }
                    }
                }
            }

            // --- STEP B: Discovery ---
            let mut discovered = Vec::new();
            if let Ok(entries) = std::fs::read_dir(&root_scan) {
                for entry in entries.flatten() {
                    if entry.path().is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        // Ignore system folders â€” and the lazy archive
                        // group (no WAL replay, sort, index or composite
                        // backfill, no blob setup; the dir merely existing
                        // suffices until first touch).
                        if !name.starts_with('_') && name != "snapshots"
                            && !crate::config::lazy_pattern_match(&config_thread.lazy_collections, &name)
                        {
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
                    
                    // REBUILD ONLY IF STALE: a fresh snapshot (WAL tails
                    // equal) means the RAM entries are complete â€” skip the
                    // scan. Anything else (no snapshot, legacy file, crash
                    // after the last snapshot) clears and rebuilds, so
                    // post-snapshot deletes leave no ghosts.
                    //
                    // P2 touched-branch: collections with post-open writes
                    // ALWAYS rescan (fresh or not â€” a fresh snapshot plus
                    // RAM-buffered Manual writes would otherwise skip both
                    // import and rescan and lose old rows), but refill
                    // ADDITIVELY: wiping would drop the live entries the
                    // writes just built (disk is older by construction).
                    // Crash-window ghosts from additive refill stay
                    // invisible â€” every scan arm filters storage liveness.
                    let touched_now = index_touched_ptr
                        .lock()
                        .map(|g| g.contains(&col_name))
                        .unwrap_or(false);
                    let fresh = !touched_now && snap_tails.as_ref().and_then(|t| t.get(&col_name)).is_some_and(
                        |(d, f)| {
                            *d == storage.wal.data_len() && *f == storage.wal.file_len()
                        },
                    );
                    if !fresh {
                        if let Ok(data) = storage.scan_prefix("") {
                            let mut mgr = indexes_ptr.write().unwrap();
                            if !touched_now {
                                mgr.clear_collection(&col_name);
                            }
                            // ponytail: selective decode â€” only indexed
                            // fields materialize (same index state, no
                            // value allocs for the rest). Traverse forces
                            // full decode: refs live in any field, and any
                            // collection may point at tracked targets.
                            let wanted = Hako::indexed_fields_for(&mgr, &col_name);
                            let full = !mgr.traverse.is_empty();
                            if !wanted.is_empty() || full {
                                for (doc_id, bytes) in data {
                                    let doc = if full {
                                        HakoDoc::decode(&bytes)
                                    } else {
                                        HakoDoc::decode_projected(&bytes, &wanted)
                                    };
                                    if let Some(doc) = doc {
                                        // doc_id is already the naked ID
                                        mgr.index_document(&col_name, &doc_id, &doc);
                                    }
                                }
                                // hakodb#71: rescan rebuilt this collection's
                                // trees newest-of-all — fold unique defs into
                                // the claim ledger (write gate turns on).
                                let unique_ids: Vec<u32> = mgr
                                    .composite
                                    .indexes_for_collection(&col_name)
                                    .filter(|i| i.definition.unique)
                                    .map(|i| i.definition.id)
                                    .collect();
                                for uid in unique_ids {
                                    if let Some(idx) = mgr.composite.get(uid) {
                                        let _ = ledger_ptr.load_from_tree(uid, &idx.tree);
                                    }
                                }
                            }
                            // Vector graphs refilled above through the
                            // index_document hook â€” mark complete so ANN
                            // may serve this collection.
                            mgr.complete_vector(&col_name);
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

            // --- STEP C2: vector rebuild for collections whose rescan was
            // skipped (a fresh snapshot carries no HNSW bytes in P1) or
            // whose graphs are otherwise incomplete. Without this, graphs
            // stay incomplete forever and every KNN brute-forces. Runs
            // under backfill_inflight (queries fail-closed meanwhile â€”
            // same rule as creation). Collections rescanned in STEP C
            // already flipped `complete` above and are skipped here.
            {
                let targets: Vec<String> = match indexes_ptr.read() {
                    Ok(mgr) => mgr
                        .vector
                        .keys()
                        .filter(|col| {
                            mgr.hnsw.get(*col).is_none_or(|graphs| {
                                mgr.vector
                                    .get(*col)
                                    .is_some_and(|defs| {
                                        defs.keys().any(|f| {
                                            graphs.get(f).is_none_or(|g| !g.complete())
                                        })
                                    })
                            })
                        })
                        .cloned()
                        .collect(),
                    Err(_) => Vec::new(),
                };
                for col in targets {
                    let shard_opt = shards_ptr
                        .read()
                        .ok()
                        .and_then(|shards| shards.get(&col).cloned());
                    if let Some(shard_arc) = shard_opt {
                        Self::spawn_vector_backfill(
                            Arc::clone(&indexes_ptr),
                            Arc::clone(&backfill_ptr),
                            config_thread.encryption_key.clone(),
                            shard_arc,
                            col,
                        );
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

            // --- STEP E: legacy snapshot upgrade â€” secondary/fts came
            // from the file but composite trees did not (pre-fix format).
            // Backfill every registered composite from data (one-time;
            // quiescence covers it, queries sweep meanwhile, same as a
            // fresh create). Fresh-format snapshots skip this entirely.
            if snapshot_loaded && !composite_restored {
                let targets: Vec<(String, u32)> = {
                    match indexes_ptr.read() {
                        Ok(mgr) => mgr
                            .composite
                            .all_indexes()
                            .map(|idx| (idx.definition.collection.clone(), idx.definition.id))
                            .collect(),
                        Err(_) => Vec::new(),
                    }
                };
                for (col, id) in targets {
                    let shard_opt = shards_ptr
                        .read()
                        .ok()
                        .and_then(|shards| shards.get(&col).cloned());
                    if let Some(shard_arc) = shard_opt {
                        Self::spawn_composite_backfill(
                            Arc::clone(&indexes_ptr),
                            Arc::clone(&persist_ptr),
                            config_thread.encryption_key.clone(),
                            Arc::clone(&backfill_ptr),
                            shard_arc,
                            id,
                            Arc::clone(&ledger_ptr),
                        );
                    }
                }
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
    /// Blob meta-update for one physically-written item: backpressure
    /// accounting plus the Pending(Arc) -> Inlined(Bytes) transition when
    /// still current. WAL already holds the op (main thread wrote it).
    fn apply_blob_meta(
        shard: &mut StorageEngine,
        key: String,
        skeleton: Vec<u8>,
        timestamp: i64,
        len: usize,
    ) {
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

    /// Non-creating shard lookup (issue #6): read paths must not
    /// materialize a collection as a side effect â€” a denied or failed
    /// operation leaves `list_collections` untouched. Returns None when
    /// the shard is not currently loaded.
    pub(crate) fn try_get_shard(&self, collection: &str) -> Option<Arc<RwLock<StorageEngine>>> {
        self.shards
            .read()
            .ok()
            .and_then(|m| m.get(collection).cloned())
    }

    /// Read-path shard lookup that must NEVER register new collections
    /// (issue #6): returns an existing shard, opens one whose directory
    /// already exists (recovery/WAL replay â€” preserves touch-load for
    /// lazy and reopen-on-access for cold pages), and `None` otherwise.
    /// A name never written has no directory â‡’ no shard, no listing leak.
    pub(crate) fn try_get_shard_on_disk(
        &self,
        collection: &str,
    ) -> Result<Option<Arc<RwLock<StorageEngine>>>> {
        if let Some(s) = self.try_get_shard(collection) {
            return Ok(Some(s));
        }
        if self.root_path.join(collection).is_dir() {
            Ok(Some(self.get_shard(collection)?))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn get_shard(&self, collection: &str) -> Result<Arc<RwLock<StorageEngine>>> {
        // 1. Check with Read Lock (Fast Path)
        {
            let shards = self.shards.read().map_err(|_| HakoError::LockPoisoned("shards".into()))?;
            if let Some(s) = shards.get(collection) {
                return Ok(Arc::clone(s));
            }
        }

        // 2. Creation Path (Outside of write lock to prevent poisoning during IO)
        let (shard_arc, created) = self.open_shard_new(collection)?;
        // Touch-load indexing: a lazily-opened collection carries real data
        // the recovery skip never indexed â€” backfill RAM secondary/FTS (+
        // composite trees; set semantics, so a snapshot that already held
        // entries stays correct) and durable composite backfills. Without
        // this the touch degrades to FullCollection scans.
        if created && self.is_lazy_collection(collection) {
            self.backfill_lazy_collection(collection, &shard_arc);
        }
        Ok(shard_arc)
    }

    /// Open + insert without any indexing. Returns the authoritative Arc
    /// and whether this call created it (a racing thread may have won â€”
    /// its load path owns indexing then).
    fn open_shard_new(&self, collection: &str) -> Result<(Arc<RwLock<StorageEngine>>, bool)> {
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

        let mut shards = self.shards.write().map_err(|_| HakoError::LockPoisoned("shards".into()))?;
        if let Some(existing) = shards.get(collection) {
            return Ok((Arc::clone(existing), false));
        }
        shards.insert(collection.to_string(), Arc::clone(&shard_arc));
        Ok((shard_arc, true))
    }

    /// Explicit async load: opens the collection now, backfills RAM
    /// indexes in a background thread. Returns false when already
    /// loaded (no-op). Queries during the build safely sweep (the
    /// `index_query_ready` gate); poll `collection_load_progress` for
    /// (done, total). Unloading mid-build aborts the thread at the
    /// next chunk. Composites ride the existing durable spawner.
    pub fn start_collection_load(&self, collection: &str) -> Result<bool> {
        {
            let shards = self.shards.read().map_err(|_| HakoError::LockPoisoned("shards".into()))?;
            if shards.contains_key(collection) {
                return Ok(false);
            }
        }
        let (shard_arc, created) = self.open_shard_new(collection)?;
        if !created {
            return Ok(false);
        }
        Self::spawn_lazy_backfill(
            Arc::clone(&self.indexes),
            Arc::clone(&self.backfill_inflight),
            Arc::clone(&self.shards),
            Arc::clone(&self.backfill_progress),
            Arc::clone(&self.index_touched),
            collection.to_string(),
            shard_arc.clone(),
        );
        // Durable lean keys for this collection's registered composites
        // (same spawner the sync path uses).
        let ids: Vec<u32> = match self.indexes.read() {
            Ok(mgr) => mgr
                .composite
                .all_indexes()
                .filter(|idx| idx.definition.collection == collection)
                .map(|idx| idx.definition.id)
                .collect(),
            Err(_) => Vec::new(),
        };
        for id in ids {
            Self::spawn_composite_backfill(
                Arc::clone(&self.indexes),
                Arc::clone(&self.index_storage),
                self.config.encryption_key.clone(),
                Arc::clone(&self.backfill_inflight),
                shard_arc.clone(),
                id,
                Arc::clone(&self.unique_ledger),
            );
        }
        Ok(true)
    }

    /// Backfill progress (done, total), or None when no load thread runs
    /// for the collection (never started, finished, or aborted).
    pub fn collection_load_progress(&self, collection: &str) -> Option<(usize, usize)> {
        self.backfill_progress.lock().ok()?.get(collection).copied()
    }

    /// Background RAM-index build for one freshly opened collection.
    /// Chunked scan-resolve-decode over secondary/FTS (set semantics,
    /// snapshot-safe); composite trees ride along via backfill_composite
    /// while lean keys go through the dedicated spawner.
    fn spawn_lazy_backfill(
        idx_mgr: Arc<RwLock<IndexManager>>,
        backfill_count: Arc<AtomicUsize>,
        shards_map: Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>>,
        progress: Arc<Mutex<HashMap<String, (usize, usize)>>>,
        touched: Arc<Mutex<std::collections::HashSet<String>>>,
        collection: String,
        shard_arc: Arc<RwLock<StorageEngine>>,
    ) {
        backfill_count.fetch_add(1, Ordering::Relaxed);
        thread::spawn(move || {
            let _guard = BackfillGuard { counter: backfill_count };
            // Mark first: a concurrent recovery import skips marked
            // collections (same race as the sync touch path).
            if let Ok(mut t) = touched.lock() {
                t.insert(collection.clone());
            }
            let pointers = {
                let storage = shard_arc.read().unwrap();
                storage.get_physical_index_snapshot()
            };
            let total = pointers.len();
            // ponytail: register BEFORE the first chunk so pollers never
            // see a running-but-unregistered build.
            if let Ok(mut p) = progress.lock() {
                p.insert(collection.clone(), (0, total));
            }
            let mut done = 0usize;
            // Wanted set once up front (a concurrently created index
            // spawns its own backfill thread, so nothing is missed).
            let wanted: Vec<String> = idx_mgr
                .read()
                .map(|mgr| Self::indexed_fields_for(&mgr, &collection))
                .unwrap_or_default();
            for chunk in pointers.chunks(500) {
                // Unload-abort: the map no longer holds our shard.
                let still_mapped = shards_map
                    .read()
                    .map(|m| m.get(&collection).map(|s| Arc::ptr_eq(s, &shard_arc)).unwrap_or(false))
                    .unwrap_or(false);
                if !still_mapped {
                    break;
                }
                let mut resolved_docs = Vec::new();
                {
                    let storage = shard_arc.read().unwrap();
                    for (key, ptr) in chunk {
                        if let Ok(Some(bytes)) = storage.read_pointer_internal(ptr, false) {
                            resolved_docs.push((key.clone(), bytes));
                        }
                    }
                }
                let mut decoded_docs = Vec::new();
                for (doc_id, bytes) in &resolved_docs {
                    if let Some(doc) = HakoDoc::decode_projected(bytes, &wanted) {
                        decoded_docs.push((doc_id.as_str(), doc));
                    }
                }
                {
                    let mut mgr = idx_mgr.write().unwrap();
                    let refs: Vec<(&str, &HakoDoc)> =
                        decoded_docs.iter().map(|(id, d)| (*id, d)).collect();
                    IndexingService::backfill_secondary(&mut mgr, &collection, refs.iter().copied());
                    IndexingService::backfill_fts(&mut mgr, &collection, refs.iter().copied());
                    IndexingService::backfill_composite(&mut mgr, &collection, refs.iter().copied());
                }
                done += chunk.len();
                if let Ok(mut p) = progress.lock() {
                    p.insert(collection.clone(), (done.min(total), total));
                }
                thread::yield_now();
            }
            if let Ok(mut p) = progress.lock() {
                p.remove(&collection);
            }
        });
    }

    /// Full index load for one lazily-opened collection (see `get_shard`).
    /// Best-effort: the shard itself is usable regardless; a failed scan
    /// just leaves queries sweeping until the next touch.
    /// Union of registered index fields for `collection` (secondary +
    /// FTS + composite): the exact set a backfill must decode. Everything
    /// else is framing-skipped via `decode_projected` â€” same index state,
    /// no value allocs for unindexed fields. Empty = no indexes (decode
    /// path falls back to full, identical to today).
    pub(crate) fn indexed_fields_for(mgr: &IndexManager, collection: &str) -> Vec<String> {
        let mut out = Vec::new();
        if let Some(sec_map) = mgr.secondary.get(collection) {
            out.extend(sec_map.keys().cloned());
        }
        if let Some(fts_map) = mgr.fts.get(collection) {
            out.extend(fts_map.keys().cloned());
        }
        for idx in mgr.composite.all_indexes() {
            if idx.definition.collection == collection {
                out.extend(idx.definition.fields.iter().map(|f| f.field.clone()));
            }
        }
        // Vector fields must materialize too: recovery STEP C refills
        // HNSW graphs through index_document, and the create-time backfill
        // needs the embedding bytes (framing-skipped otherwise).
        if let Some(vec_map) = mgr.vector.get(collection) {
            out.extend(vec_map.keys().cloned());
        }
        out.sort();
        out.dedup();
        out
    }

    fn backfill_lazy_collection(&self, collection: &str, shard_arc: &Arc<RwLock<StorageEngine>>) {
        // Mark first: a concurrent recovery import skips marked
        // collections (its snapshot state is older by construction).
        if let Ok(mut touched) = self.index_touched.lock() {
            touched.insert(collection.to_string());
        }
        if let Ok(storage) = shard_arc.read() {
            // ponytail: same scan-decode-index shape as recovery STEP C
            // (encrypted bytes fail decode and are skipped there too).
            if let Ok(data) = storage.scan_prefix("") {
                if let Ok(mut mgr) = self.indexes.write() {
                    // Clear-then-fill: the snapshot may hold stale
                    // entries (post-snapshot deletes, or a restart
                    // mid-backfill) â€” refilling over them would leave
                    // ghosts, and there is no per-collection completion
                    // marker to trust instead. Unload/retouch cycles pay
                    // a full rebuild; explicit background load covers the
                    // latency for viewers.
                    if mgr.has_collection(collection) {
                        mgr.clear_collection(collection);
                    }
                    // Borrowed secondary fast path when it covers the
                    // whole job (secondary-only collection); otherwise
                    // the owned path below (identical entries).
                    if !IndexingService::backfill_secondary_borrowed(&mut mgr, collection, &data) {
                        let wanted = Self::indexed_fields_for(&mgr, collection);
                        // ponytail: no registered indexes = nothing to fill
                        // (composites ride the dedicated spawner below).
                        if wanted.is_empty() {
                            return;
                        }
                        for (doc_id, bytes) in &data {
                            if let Some(doc) = HakoDoc::decode_projected(bytes, &wanted) {
                                mgr.index_document(collection, doc_id, &doc);
                            }
                        }
                        // Owned refill flowed through the HNSW hook â€” the
                        // graphs are whole again (borrowed path above never
                        // runs when vector defs exist, see its guard).
                        mgr.complete_vector(collection);
                    }
                }
            }
        }
        // Durable lean keys for this collection's registered composites
        // (mirrors the legacy-upgrade STEP E spawner).
        let ids: Vec<u32> = match self.indexes.read() {
            Ok(mgr) => mgr
                .composite
                .all_indexes()
                .filter(|idx| idx.definition.collection == collection)
                .map(|idx| idx.definition.id)
                .collect(),
            Err(_) => Vec::new(),
        };
        for id in ids {
            Self::spawn_composite_backfill(
                Arc::clone(&self.indexes),
                Arc::clone(&self.index_storage),
                self.config.encryption_key.clone(),
                Arc::clone(&self.backfill_inflight),
                Arc::clone(shard_arc),
                id,
                Arc::clone(&self.unique_ledger),
            );
        }
    }

    /// Explicit load of one collection (archive use): opens + replays +
    /// backfills when needed, no-op when already loaded. Also opens plain
    /// (non-lazy) collections on demand.
    pub fn load_collection(&self, collection: &str) -> Result<()> {
        self.get_shard(collection)?;
        Ok(())
    }

    /// Explicit evict of one lazy collection (after archive use): flushes
    /// it like Drop, drops it from the map, evicts its cached docs and
    /// plans. Data stays on disk; next touch reloads. Refuses non-lazy
    /// collections so a hot store can't be evicted by accident.
    pub fn unload_collection(&self, collection: &str) -> Result<()> {
        if !self.is_lazy_collection(collection) {
            return Err(HakoError::Corrupt(format!("unload: `{collection}` not in lazy_collections")));
        }
        let removed = self
            .shards
            .write()
            .map_err(|_| HakoError::LockPoisoned("shards".into()))?
            .remove(collection);
        if let Some(shard_arc) = removed {
            if let Ok(mut storage) = shard_arc.write() {
                let _ = storage.drain_blob_queue();
                let _ = storage.rewrite_wal_snapshot();
                let _ = storage.flush_all();
            }
            if let Ok(mut cache) = self.doc_cache.write() {
                if !cache.is_empty() {
                    let prefix = format!("{collection}\0");
                    cache.retain(|k, _| !k.starts_with(&prefix));
                }
            }
            if let Ok(mut cache) = self.row_cache.write() {
                if !cache.map.is_empty() {
                    let prefix = format!("{collection}\0");
                    cache.map.retain(|k, _| !k.starts_with(&prefix));
                    cache.order.retain(|k| !k.starts_with(&prefix));
                }
            }
            self.plan_cache.invalidate();
        }
        Ok(())
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
    /// collection at rest? (No context is built â€” callers that need to
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
                // ponytail: typed + retryable (issue #71/F5) â€” conflicts are
                // a normal concurrency outcome, not `Corrupt`. SDK retry
                // loops classify via HakoError::kind()/hk_error_kind, never
                // by string-matching this message.
                WRITE_STATS.tx_conflicts.fetch_add(1, Ordering::Relaxed);
                return Err(HakoError::Conflict { key });
            }
        }

        // Delegate to the now-unlocked internal logic
        let res = self.write_batch_internal(mutations)?;
        Ok(res)
    }

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<Vec<String>> {
        // Read-only enforcement: one guard at the admission gate covers
        // put/put_owned/delete/patch/delete_where/tx (all route here).
        // Replicated ingest bypasses write_batch by design (see field).
        if self.read_only.load(Ordering::Relaxed) {
            self.record_audit(AuditEntry { op: AccessOp::Batch, collection: "<sharded>".into(), doc_id: None, ok: false });
            return Err(HakoError::Corrupt("ReadOnly".into()));
        }
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
        let mut now_micros = now_nanos / 1000;
        // Hybrid-clock guard (issue #80): never stamp below a timestamp
        // already emitted or accepted â€” a backward wall-clock step would
        // otherwise make every future local write lose LWW forever.
        let watermark = self.last_ts_watermark.load(std::sync::atomic::Ordering::Relaxed);
        if now_micros <= watermark {
            now_micros = watermark.saturating_add(1);
        }
        self.last_ts_watermark.store(now_micros, std::sync::atomic::Ordering::Relaxed);

        let threshold = self.config.value_blob_threshold_bytes;
        let mut shard_map: HashMap<String, ShardWork> = HashMap::new();
        let mut assigned_ids = Vec::with_capacity(mutations.len());

        // hakodb#71: unique-index admission ledger. All claims made while
        // preparing this batch undo in reverse on ANY early return
        // (violation, WAL error, panic) â€” the guard is the whole story.
        let mut gg_memo: HashMap<String, Vec<GateDef>> = HashMap::new();
        let mut gg_guard = LedgerRollbackGuard::new(&self.unique_ledger);

        // Simple local tracker to avoid changing ShardWork
        // let mut deletes_for_indexer: Vec<(String, String, HakoDoc)> = Vec::new();

        let t_encode = Instant::now();
        // ponytail: last-collection cache â€” consecutive mutations usually hit
        // the same shard, so reuse its handle + blob manager instead of a map
        // lookup and two lock acquisitions per mutation.
        #[allow(clippy::type_complexity)]
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

            if doc_id.is_empty() {
                doc_id = self.generate_sortable_id(now_nanos);
            }

            assigned_ids.push(doc_id.clone());

            // hakodb#71/F8: schema defaults + validation in the admission
            // gate (same place as F1), before any WAL append — a violating
            // batch member rejects the whole batch atomically.
            if !is_delete {
                self.apply_schema(&col, &mut doc)?;
            }

            // 2. COMPUTE KEY ONCE
            // ponytail: get_mut first â€” the old entry(col.clone()) allocated
            // a key String per mutation even on hit (20K allocs/batch on
            // single-collection batches).
            let work = match shard_map.get_mut(&col) {
                Some(w) => w,
                None => shard_map.entry(col.clone()).or_insert_with(|| ShardWork {
                    ops: Vec::new(), keys: Vec::new(), deleted: HashSet::new(),
                    events: Vec::new(), index_puts: Vec::new(),
                    blob_queue_items: Vec::new(), index_deletes: Vec::new()
                }),
            };

            let key_arc: Arc<str> = Arc::from(doc_id.as_str());
            
            if is_delete {
                // hakodb#71: release unique claims by doc id (before the
                // pushes below move doc_id/key_arc into the WAL/event
                // structures).
                self.unique_gate_release(
                    &col,
                    key_arc.as_ref(),
                    &mut gg_memo,
                    &mut gg_guard.undo,
                );
                // ponytail: doc_id moves into the WAL op (zero copy);
                // keys/events share the Arc (bumps, no String allocs).
                work.ops.push(WalOp::Delete { key: doc_id, timestamp: now_micros });
                work.keys.push(key_arc.clone());
                work.deleted.insert(key_arc.clone());
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

            // hakodb#71: unique-index admission. Violation (or
            // backfilling) returns here; the drop guard above undoes all
            // claims recorded so far in this batch.
            self.unique_gate_admit(
                &col,
                key_arc.as_ref(),
                &key_arc,
                &doc_arc,
                &mut gg_memo,
                &mut gg_guard.undo,
            )?;
            
            // REUSE KEY for WAL and Index
            work.ops.push(WalOp::PutInlined { key: key_arc.to_string(), value: skeleton_bytes });
            work.index_puts.push((doc_id, Arc::clone(&doc_arc))); 
            work.keys.push(key_arc.clone());
            work.deleted.remove(&key_arc);
            // ponytail: event path bumps the same Arc instead of a fresh
            // String allocation per mutation.
            work.events.push((col, ChangeEvent {
                path: key_arc,
                kind: ChangeKind::Put
            }));
            
            for b in blob_work { work.blob_queue_items.push(b); }
        }
        WRITE_STATS.encode_ns.fetch_add(t_encode.elapsed().as_nanos() as u64, Ordering::Relaxed);

        // Versions are keyed by bare doc id, so a delete in one collection
        // must not erase a version for the same id that stays live in
        // another collection in this batch (relocate is the common case).
        let live_keys: HashSet<Arc<str>> = shard_map
            .values()
            .flat_map(|w| {
                w.keys
                    .iter()
                    .filter(|k| !w.deleted.contains(*k))
                    .cloned()
            })
            .collect();

        // --- APPLY SHARD CHANGES ---
        for (col_name, mut work) in shard_map {
            // P2: mark first â€” a concurrent recovery rescan must ADD to
            // (not wipe+rebuild) collections with post-open writes; disk
            // state is older by construction (Manual WAL may still be
            // RAM-buffered). Same `touched` set the snapshot import
            // already honors; without the mark a late rescan silently
            // drops freshly indexed rows from every RAM family.
            if !work.ops.is_empty() {
                if let Ok(mut touched) = self.index_touched.lock() {
                    touched.insert(col_name.clone());
                }
            }
            let shard_arc = self.get_shard(&col_name)?;
            // let index_entries = work.index_puts; 

            let t_apply = Instant::now();
            {
                // ponytail: lock-wait sample â€” time blocked on the shard
                // write lock (the mixed r/w contention signal).
                let t_lock = Instant::now();
                let mut shard = shard_arc.write().unwrap();
                LOCK_STATS.write_wait_ns.fetch_add(t_lock.elapsed().as_nanos() as u64, Ordering::Relaxed);
                LOCK_STATS.write_acqs.fetch_add(1, Ordering::Relaxed);
            
                // OPTIMIZATION: Only run the delete-resolution scan if there are actually 
                // deletes in this specific shard's work.
                if work.ops.iter().any(|op| matches!(op, WalOp::Delete { .. })) {
                    for op in &work.ops {
                        if let WalOp::Delete { key, .. } = op {
                            // W2 (#143): a decoded copy of the pre-delete doc
                            // may already be resident from a get/query. The
                            // version bump happens below in this same commit,
                            // so the point-read probe contract (exact version
                            // match) holds: hit = clone the resident doc,
                            // miss = the old storage read + decode.
                            let cached = self
                                .current_version(key)
                                .and_then(|ver| self.probe_doc_cache(&col_name, key, ver));
                            if let Some(cached) = cached {
                                work.index_deletes.push((key.clone(), (*cached).clone()));
                            } else if let Some(ptr) = shard.index.get(key) {
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
                    // Any error here unwinds write_batch_internal; the
                    // outer rollback guard (declared before the loop)
                    // drops last, after both shard locks release, and
                    // reverses the batch's ledger claims (hakodb#71).
                    shard.wal.append_batch_fast(tx_id, &work.ops, false)?;
                    WRITE_STATS.wal_ns.fetch_add(t_wal.elapsed().as_nanos() as u64, Ordering::Relaxed);
                }

                // Apply storage index changes.
                // ponytail: docs land Inlined with the bytes already encoded
                // for WAL (matched by bare key) â€” no BlobPending detour, no
                // per-read re-encode until background conversion. The WAL op
                // carries identical bytes, so durability is unchanged; the
                // blob worker's BlobPending-gated swap simply no-ops, and
                // pre-flush blob reads still resolve via the flush queue.
                // Falls back to BlobPending if the WAL op is ever absent.
                // ponytail: PutInlined ops and index_puts are pushed in
                // lockstep (one pair per put/patch; deletes touch ops only),
                // so zip positionally. The old code built a full HashMap of
                // every key first (SipHash Ã—2/row on big batches) â€” gone.
                // index_puts stays put for the indexer send below.
                let mut put_idx = 0;
                // ponytail (#151): batched sorted-view maintenance; flushed
                // below, still inside the shard write lock.
                let mut pending_sorted = Vec::new();
                for op in &work.ops {
                    if let WalOp::PutInlined { value, .. } = op {
                        if let Some((doc_id, _)) = work.index_puts.get(put_idx) {
                            put_idx += 1;
                            let ptr = Pointer::Inlined(Arc::new(value.clone()));
                            shard.update_index_entry_deferred(doc_id.clone(), Some(ptr), &mut pending_sorted);
                        }
                    }
                }
                debug_assert!(work.index_puts.get(put_idx).is_none(), "ops/index_puts lockstep broke");
                for op in &work.ops {
                    if let WalOp::Delete { key, timestamp } = op {
                        shard.update_index_entry_deferred(key.clone(), Some(Pointer::Deleted { timestamp: *timestamp }), &mut pending_sorted);
                    }
                }
                shard.flush_pending_sorted(&mut pending_sorted);
                
                let mut b_bytes = 0;
                for b in work.blob_queue_items {
                    if let BlobWork::PutRaw { len, .. } = &b { b_bytes += *len as usize; }
                    shard.blob_flush_queue.push_back(b);
                }
                shard.total_pending_blob_bytes.fetch_add(b_bytes, Ordering::Relaxed);

                // Issue #71/F5: version bump inside the shard write lock â€”
                // this is the linearization point the versioned read
                // (`get_versioned`) serializes against. The old post-lock
                // bump left `get()->current_version()` windows where the new
                // version paired with the old document (silent lost update).
                let t_ver = Instant::now();
                self.bump_versions_by_keys(work.keys.clone());
                self.clear_versions_by_keys(&work.deleted, &live_keys);
                WRITE_STATS.versions_ns.fetch_add(t_ver.elapsed().as_nanos() as u64, Ordering::Relaxed);
            }
            // apply wall time includes the WAL call above; report net below.
            WRITE_STATS.apply_ns.fetch_add(t_apply.elapsed().as_nanos() as u64, Ordering::Relaxed);
            
            self.trigger_blob_flush.store(true, Ordering::Release);

            // ponytail: drop hot-cache entries for written keys. The version
            // bump alone would invalidate them; this reclaims memory eagerly.
            // ponytail: skip the whole pass when the cache is empty (the
            // common batch/seed case) â€” 20K format!+remove for nothing.
            let t_cache = Instant::now();
            {
                let mut cache = self.doc_cache.write().unwrap();
                if !cache.is_empty() {
                    // W1 (#143): one scratch-buffer prefix per batch — the old
                    // format!-per-key paid an alloc + free for every written
                    // id under the write lock (1.6 us/mut on the warm-delete
                    // phase). No behavior change: same keys removed.
                    CACHE_KEY_BUF.with(|b| {
                        let mut b = b.borrow_mut();
                        b.clear();
                        b.push_str(&col_name);
                        b.push('\0');
                        let prefix = b.len();
                        for (doc_id, _) in &work.index_puts {
                            b.truncate(prefix);
                            b.push_str(doc_id);
                            cache.remove(b.as_str());
                        }
                        for op in &work.ops {
                            if let WalOp::Delete { key, .. } = op {
                                b.truncate(prefix);
                                b.push_str(key);
                                cache.remove(b.as_str());
                            }
                        }
                    });
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
                self.enqueue_index_update(
                    col_name,
                    Arc::new(work.index_puts),
                    work.index_deletes,
                );
            }
            WRITE_STATS.index_send_ns.fetch_add(t_idx.elapsed().as_nanos() as u64, Ordering::Relaxed);
            
            // Issue #71/F5: the version bump moved INSIDE the shard write
            // lock (linearization point for the versioned read). No second
            // bump here â€” this comment is the tombstone for the moved code.
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

        gg_guard.disarm();
        Ok(assigned_ids)
    }

    // ================= hakodb#71: unique composite index gate ============

    /// Snapshot the unique-index definitions of `collection` for gate
    /// admission. Called once per (collection, batch) and memoized; the
    /// read-lock cost on uncontended RwLock is ~ns and every other write
    /// path already pays it for touched-marking, so no separate cache
    /// layer is added.
    fn unique_gate_defs(
        &self,
        col: &str,
        memo: &mut HashMap<String, Vec<GateDef>>,
    ) -> Vec<GateDef> {
        if let Some(cached) = memo.get(col) {
            return cached.clone();
        }
        let defs: Vec<GateDef> = {
            let cache = self.gate_cache.read().unwrap();
            match cache.get(col) {
                // Clone of Arc, then of the (usually empty) Vec: an empty
                // defs list is the common case, no lock on `indexes`.
                Some(cached) => (**cached).clone(),
                None => {
                    drop(cache);
                    let defs: Vec<GateDef> = {
                        let mgr = self.indexes.read().unwrap();
                        mgr.composite
                            .indexes_for_collection(col)
                            .filter(|i| i.definition.unique)
                            .map(|i| GateDef {
                                index_id: i.definition.id,
                                def: i.definition.clone(),
                            })
                            .collect()
                    };
                    self.gate_cache
                        .write()
                        .unwrap()
                        .insert(col.to_string(), Arc::new(defs.clone()));
                    defs
                }
            }
        };
        memo.insert(col.to_string(), defs.clone());
        defs
    }


    /// Admit one document to every unique index of its collection.
    /// Claims are recorded in `undo`; any error (violation or
    /// backfilling) aborts the whole batch (the caller's rollback guard
    /// undoes prior claims). Filtered/sparse/structural skips leave the
    /// doc unclaimed for that index â€” the tree mirrors the same rules,
    /// so a later matching write still sees old tuples... unless the doc
    /// was claimed under different rules â€” those tuples were claimed by
    /// the SAME doc id, and `claim` releases the doc's previous claim
    /// atomically (by_doc lookup), so no stale ownership survives.
    fn unique_gate_admit(
        &self,
        col: &str,
        doc_id: &str,
        doc_id_arc: &Arc<str>,
        doc: &HakoDoc,
        memo: &mut HashMap<String, Vec<GateDef>>,
        undo: &mut Vec<crate::index::composite::ledger::LedgerUndo>,
    ) -> Result<()> {
        let defs = self.unique_gate_defs(col, memo);
        // ponytail: fast path â€” no unique index on the collection pays
        // exactly one small-Vec clone above and nothing else.
        if defs.is_empty() {
            return Ok(());
        }
        for d in &defs {
            if !self.unique_ledger.is_ready(d.index_id) {
                return Err(HakoError::IndexBackfilling {
                    collection: col.to_string(),
                    index: d.index_id.to_string(),
                });
            }
            if !d.def.filter_passes(doc_id, doc) {
                continue;
            }
            let Some(values) = d.def.document_values_of(doc_id, doc) else { continue };
            let prefix = crate::index::composite::key_encoder::encode_composite_prefix(
                &d.def, &values,
            );
            self.unique_ledger
                .claim(d.index_id, prefix, doc_id_arc, undo)
                // Fill caller-actionable context (fkip-234 report): the
                // ledger knows only the index id; the gate owns the
                // collection and a readable tuple summary.
                .map_err(|e| match e {
                    HakoError::UniqueViolation { key, .. } => {
                        HakoError::UniqueViolation {
                            collection: col.to_string(),
                            index: d.index_id.to_string(),
                            key,
                        }
                    }
                    HakoError::IndexBackfilling { index, .. } => {
                        HakoError::IndexBackfilling {
                            collection: col.to_string(),
                            index,
                        }
                    }
                    other => other,
                })?;
        }
        Ok(())
    }

    /// Release the claims a deleted doc held across every unique index
    /// of its collection (delete path; O(1) via the by-doc map).
    fn unique_gate_release(
        &self,
        col: &str,
        doc_id: &str,
        memo: &mut HashMap<String, Vec<GateDef>>,
        undo: &mut Vec<crate::index::composite::ledger::LedgerUndo>,
    ) {
        let defs = self.unique_gate_defs(col, memo);
        if defs.is_empty() {
            return;
        }
        for d in &defs {
            self.unique_ledger.release_doc(d.index_id, doc_id, undo);
        }
    }

    /// Move documents between collections (archive/restore) as paired
    /// operations through existing machinery â€” never memcpy. Per doc,
    /// in order: timestamp-preserving put at dest FIRST (duplicate-
    /// transient), then a fresh-timestamp tombstone at src. Stored
    /// bytes stay identical across moves (only live versions/caches
    /// advance); retry with the same ids is idempotent. An existing
    /// dest doc with the same id is overwritten (operator-explicit).
    /// Refuses the whole call when either side is sync-excluded or
    /// local-only, or when src == dst. Needs no new wire kinds:
    /// watchers see Put (dst) + Delete (src), sync converges via WAL.
    pub fn relocate_docs(&self, src: &str, dst: &str, ids: &[String]) -> Result<RelocateReport> {
        if src == dst {
            return Err(HakoError::Corrupt("relocate: src == dst".into()));
        }
        if self.read_only.load(Ordering::Relaxed) {
            return Err(HakoError::Corrupt("read-only".into()));
        }
        for col in [src, dst] {
            if self.is_sync_withheld(col) || self.is_collection_local(col) {
                return Err(HakoError::Corrupt(format!("relocate: excluded collection `{col}`")));
            }
        }
        if !self.allowed(src, AccessOp::Delete) || !self.allowed(dst, AccessOp::Put) {
            return Err(HakoError::Corrupt("Denied".into()));
        }
        if !self.allowed(src, AccessOp::Get) {
            return Err(HakoError::Corrupt("Denied".into()));
        }
        let threshold = self.config.value_blob_threshold_bytes;
        let mut report = RelocateReport::default();
        for id in ids {
            let Some(mut doc) = self.get(src, id)? else {
                report.missing.push(id.clone());
                continue;
            };
            // Dest put, timestamp preserved (no restamp â€” LWW identity
            // is the move's whole point). Mirrors the write-batch put
            // prep; blob offsets re-reserve in the DEST file.
            let key_arc: Arc<str> = Arc::from(id.as_str());
            let dst_arc = self.get_shard(dst)?;
            let blob_work = dst_arc
                .read()
                .unwrap()
                .blob_manager
                .clone()
                .map(|bm| self.process_doc_blobs(dst, id, &mut doc, &bm, threshold))
                .unwrap_or_default();
            let skeleton = doc.encode_buffered();
            let doc_arc = Arc::new(doc);
            // hakodb#71: unique admission at the dest gate. The src
            // delete below routes through write_batch (its release hook
            // runs); the put must claim before the WAL lands, same as
            // the batch path. On violation: rollback, report, stop.
            let mut rl_undo: Vec<crate::index::composite::ledger::LedgerUndo> = Vec::new();
            let mut rl_memo: HashMap<String, Vec<GateDef>> = HashMap::new();
            if let Err(e) = self.unique_gate_admit(
                dst,
                key_arc.as_ref(),
                &key_arc,
                &doc_arc,
                &mut rl_memo,
                &mut rl_undo,
            ) {
                self.unique_ledger.rollback(&rl_undo);
                return Err(e);
            }
            {
                let mut shard = dst_arc.write().unwrap();
                let tx_id = shard.next_tx_id;
                shard.next_tx_id += 1;
                shard.wal.append_batch_fast(
                    tx_id,
                    &[WalOp::PutInlined { key: id.clone(), value: skeleton.clone() }],
                    false,
                )?;
                shard.update_index_entry(id.clone(), Some(Pointer::Inlined(Arc::new(skeleton))));
                let mut b_bytes = 0usize;
                for b in blob_work {
                    if let BlobWork::PutRaw { len, .. } = &b {
                        b_bytes += *len as usize;
                    }
                    shard.blob_flush_queue.push_back(b);
                }
                shard.total_pending_blob_bytes.fetch_add(b_bytes, Ordering::Relaxed);
                // F5 linearization: bump inside the shard lock like all
                // other write paths.
                self.bump_versions_by_keys(vec![key_arc.clone()]);
            }
            // Indexer gets the put (secondary/FTS/composite would
            // otherwise miss the moved doc until backfill). Same
            // in-flight accounting as the write path (refund on dead
            // worker so quiescence never wedges). Marks touched like a
            // write: recovery must add, not wipe, the dest's live rows.
            if let Ok(mut touched) = self.index_touched.lock() {
                touched.insert(dst.to_string());
            }
            self.enqueue_index_update(
                dst.to_string(),
                Arc::new(vec![(id.clone(), Arc::clone(&doc_arc))]),
                vec![],
            );
            {
                // W1 (#143): one scratch-buffer key serves both cache removes
                // (rename is cold, but the pattern is free and the two
                // format!s here went away with it).
                CACHE_KEY_BUF.with(|b| {
                    let mut b = b.borrow_mut();
                    b.clear();
                    b.push_str(dst);
                    b.push('\0');
                    b.push_str(id);
                    {
                        let mut cache = self.doc_cache.write().unwrap();
                        if !cache.is_empty() {
                            cache.remove(b.as_str());
                        }
                    }
                    let mut rcache = self.row_cache.write().unwrap();
                    if rcache.map.remove(b.as_str()).is_some() {
                        let stale = b.as_str();
                        if let Some(pos) = rcache.order.iter().position(|k| *k == stale) {
                            rcache.order.remove(pos);
                        }
                    }
                });
            }
            if self.config.enable_audit_log {
                self.record_audit(AuditEntry {
                    op: AccessOp::Put,
                    collection: dst.into(),
                    doc_id: Some(id.clone()),
                    ok: true,
                });
            }
            self.notify_watchers(
                dst,
                ChangeEvent { path: key_arc.clone(), kind: ChangeKind::Put },
            );
            // Src tombstone with FRESH timestamp via the normal delete
            // path (WAL + index + versions + cache + watchers + audit).
            self.delete(src, id)?;
            // The delete drops the bare-key version; the doc is still live
            // at dst, so restore a version for the moved identity.
            self.bump_versions_by_keys(vec![key_arc.clone()]);
            self.trigger_blob_flush.store(true, Ordering::Release);
            report.moved.push(id.clone());
        }
        Ok(report)
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
        // Hot-cache fast path: clone straight out of the resident Arc, without
        // the Arc clone/drop and the extra frame that `get_arc` costs per hit
        // (this is the pre-0.17 shape; the regression was the indirection).
        // Miss delegates to `get_arc`, which repeats the admission check and
        // the probe: keep this hit branch in sync with `get_arc`'s.
        if !self.allowed(collection, AccessOp::Get) {
            return Err(self.deny(AccessOp::Get, collection, Some(doc_id)));
        }
        if let Some(ver) = self.current_version(doc_id) {
            if let Some(cached) = self.probe_doc_cache(collection, doc_id, ver) {
                return Ok(Some((*cached).clone()));
            }
        }
        Ok(self.get_arc(collection, doc_id)?.map(|doc| (*doc).clone()))
    }

    /// Reusable-key cache probe: point reads must not pay a `format!` alloc
    /// for the key on every hit.
    #[inline]
    fn probe_doc_cache(&self, collection: &str, doc_id: &str, ver: u64) -> Option<Arc<HakoDoc>> {
        CACHE_KEY_BUF.with(|b| {
            let mut b = b.borrow_mut();
            b.clear();
            b.push_str(collection);
            b.push('\0');
            b.push_str(doc_id);
            self.doc_cache
                .read()
                .unwrap()
                .get(b.as_str())
                .filter(|(v, _)| *v == ver)
                .map(|(_, d)| Arc::clone(d))
        })
    }

    /// Query row cache probe (#142 Q1): same key format and version
    /// contract as [`Self::probe_doc_cache`], hit or miss both cheap.
    #[inline]
    pub(crate) fn probe_row_cache(&self, collection: &str, doc_id: &str, ver: u64) -> Option<Arc<HakoDoc>> {
        CACHE_KEY_BUF.with(|b| {
            let mut b = b.borrow_mut();
            b.clear();
            b.push_str(collection);
            b.push('\0');
            b.push_str(doc_id);
            self.row_cache
                .read()
                .unwrap()
                .map
                .get(b.as_str())
                .filter(|(v, _)| *v == ver)
                .map(|(_, d)| Arc::clone(d))
        })
    }

    /// Populate the query row cache. Clones `doc` once for the resident
    /// copy; eviction is FIFO at the 512 cap. `try_write` so a contended
    /// cache never serializes query readers — a skipped insert just means
    /// the next query re-decodes.
    pub(crate) fn row_cache_insert(&self, collection: &str, doc_id: &str, ver: u64, doc: &HakoDoc) {
        if let Ok(mut cache) = self.row_cache.try_write() {
            let key = format!("{collection}\0{doc_id}");
            match cache.map.get_mut(&key) {
                // Refresh in place: the order slot is already queued, and a
                // duplicate FIFO entry would evict the fresh value.
                Some(slot) => *slot = (ver, Arc::new(doc.clone())),
                None => {
                    if cache.map.len() >= ROW_CACHE_CAP {
                        if let Some(oldest) = cache.order.pop_front() {
                            cache.map.remove(&oldest);
                        }
                    }
                    cache.order.push_back(key.clone());
                    cache.map.insert(key, (ver, Arc::new(doc.clone())));
                }
            }
        }
    }

    /// Shared-cache point read: same admission, version check, decode and
    /// blob resolution as [`Self::get`], but a cache hit returns the resident
    /// `Arc` instead of deep-cloning the document. Read-only callers that
    /// only inspect or serialize the doc should prefer this.
    pub fn get_arc(&self, collection: &str, doc_id: &str) -> Result<Option<Arc<HakoDoc>>> {
        if !self.allowed(collection, AccessOp::Get) {
            return Err(self.deny(AccessOp::Get, collection, Some(doc_id)));
        }

        // ponytail: hot-cache probe. Hit skips storage lookup, full decode
        // and blob resolve for one deep clone (field Arcs are shared via
        // interning, so the clone is mostly the value Strings). Ordering is
        // linearizable: a hit linearizes at the version check, same as a
        // storage read racing a write today.
        // ponytail: one version lookup serves both the probe and the
        // populate below (the old code paid fxhash + sharded lock twice).
        let ver = self.current_version(doc_id);
        if let Some(ver) = ver {
            if let Some(cached) = self.probe_doc_cache(collection, doc_id, ver) {
                return Ok(Some(cached));
            }
        }
        // Owned key only for the populate below: a cache hit never allocates.
        let cache_key = ver.map(|_| format!("{collection}\0{doc_id}"));

        let Some(shard) = self.try_get_shard_on_disk(collection)? else { return Ok(None) };
        // ponytail: lock-wait sample â€” readers blocked here are queued
        // behind the writer's flush (the mixed r/w contention signal).
        let t_lock = Instant::now();
        let storage = shard.safe_read()?;
        LOCK_STATS.read_wait_ns.fetch_add(t_lock.elapsed().as_nanos() as u64, Ordering::Relaxed);
        LOCK_STATS.read_acqs.fetch_add(1, Ordering::Relaxed);
        // ponytail: decode borrowed from the shared Arc â€” the old
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
            let doc = Arc::new(doc);
            // ponytail: populate post-resolve. Bounded at 8192 with single
            // arbitrary eviction when full.
            // ponytail: try_write â€” a contended cache must not serialize
            // readers. Skipped populate just means the next read re-decodes;
            // correctness is untouched (version check revalidates).
            if let (Some(ver), Some(cache_key)) = (ver, cache_key) {
                if let Ok(mut cache) = self.doc_cache.try_write() {
                    if cache.len() >= 8192 {
                        if let Some(k) = cache.keys().next().cloned() { cache.remove(&k); }
                    }
                    cache.insert(cache_key, (ver, Arc::clone(&doc)));
                }
            }
            return Ok(Some(doc));
        }

        Ok(None)
    }

    /// Borrowed point read (v0.8.10+): pins the stored bytes and returns a
    /// `DocView` with lazy per-field access â€” no decode, no interning, no
    /// hot-cache interaction. The sqlite3 `SELECT` + typed-accessor analog:
    /// pull only the fields you touch. Deleted/missing reads None, same as
    /// [`Self::get`]; framing-invalid rows also read None (strict views).
    pub fn get_view(&self, collection: &str, doc_id: &str) -> Result<Option<crate::document::hako_doc::DocView>> {
        if !self.allowed(collection, AccessOp::Get) {
            return Err(self.deny(AccessOp::Get, collection, Some(doc_id)));
        }

        let shard = self.get_shard(collection)?;
        // ponytail: lock-wait sample (same signal as get()).
        let t_lock = Instant::now();
        let storage = shard.safe_read()?;
        LOCK_STATS.read_wait_ns.fetch_add(t_lock.elapsed().as_nanos() as u64, Ordering::Relaxed);
        LOCK_STATS.read_acqs.fetch_add(1, Ordering::Relaxed);
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

    /// Cheap existence check: pins the stored row and validates its framing,
    /// with no owned decode and no hot-cache interaction. Returns false for a
    /// missing document or a collection that does not exist; unlike
    /// [`Self::get_view`], it never materializes a collection.
    pub fn exists(&self, collection: &str, doc_id: &str) -> Result<bool> {
        if !self.allowed(collection, AccessOp::Get) {
            return Err(self.deny(AccessOp::Get, collection, Some(doc_id)));
        }

        let Some(shard) = self.try_get_shard_on_disk(collection)? else {
            return Ok(false);
        };
        // ponytail: same read-lock sample as get(); no decode, no cache.
        let t_lock = Instant::now();
        let storage = shard.safe_read()?;
        LOCK_STATS.read_wait_ns.fetch_add(t_lock.elapsed().as_nanos() as u64, Ordering::Relaxed);
        LOCK_STATS.read_acqs.fetch_add(1, Ordering::Relaxed);
        let found = storage
            .get_shared(doc_id)?
            .is_some_and(|b| crate::document::hako_doc::DocView::new(b).is_some());

        if self.config.enable_audit_log {
            self.record_audit(AuditEntry {
                op: AccessOp::Get,
                collection: collection.into(),
                doc_id: Some(doc_id.into()),
                ok: true,
            });
        }

        Ok(found)
    }

    /// Point read that emits JSON straight from the pinned bytes: no owned
    /// `HakoDoc`, no per-field String/Map allocs. Returns `(logical_time,
    /// json)` so response paths can keep the same ETag fallback as `get`.
    ///
    /// Blob-backed fields are the one exception: `DocView` cannot inflate
    /// them, so those documents fall back to decode + resolve to keep the
    /// output byte-identical to `get`. Blob-free reads stay zero-decode.
    pub fn get_json(&self, collection: &str, doc_id: &str) -> Result<Option<(i64, Vec<u8>)>> {
        let Some(view) = self.get_view(collection, doc_id)? else {
            return Ok(None);
        };
        let time = view.time();
        Ok(self.json_from_view(&view, collection)?.map(|json| (time, json)))
    }

    /// Serialize one pinned row, falling back to owned decode only when the
    /// row has blob links. Shared by point and query JSON paths so both keep
    /// `get`'s resolved-blob byte contract.
    fn json_from_view(
        &self,
        view: &crate::document::hako_doc::DocView,
        collection: &str,
    ) -> Result<Option<Vec<u8>>> {
        if view.has_blob_links() {
            // shortcut: blob docs pay one decode; upgrade to a blob-aware
            // view writer if large-blob JSON reads become hot.
            let Some(mut doc) = view.to_owned_doc() else {
                return Ok(None);
            };
            self.resolve_document_blobs(&mut doc, collection)?;
            return Ok(Some(doc.to_json_bytes()));
        }
        let mut out = Vec::with_capacity(view.as_bytes().len() + 16);
        if view.write_json(&mut out).is_none() {
            return Ok(None);
        }
        Ok(Some(out))
    }

    pub fn put(&self, col: &str, id: &str, doc: &HakoDoc) -> Result<String> {
        self.put_owned(col, id, doc.clone())
    }

    /// ponytail: owned-doc variant â€” skips the full deep clone that `put`
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
    /// (`col:id`) depending on the sync path â€” both forms are honored.
    pub fn is_local_only(&self, col: &str, key: &str) -> bool {
        if self.local_only_cols.read().unwrap().contains(col) {
            return true;
        }
        let keys = self.local_only_keys.read().unwrap();
        if keys.contains(&format!("{col}\0{key}")) {
            return true;
        }
        // ponytail: namespaced fallback â€” strip one leading `col:` only.
        if let Some(id) = key.strip_prefix(col).and_then(|s| s.strip_prefix(':')) {
            return keys.contains(&format!("{col}\0{id}"));
        }
        false
    }

    /// Mark a whole collection local-only (never emits, never accepts
    /// remote ops for it once sync filters check this â€” fully local).
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
    /// (still local history) â€” pair with `vacuum_collection` for restore.
    pub fn replicate_collection(&self, col: &str) {
        self.local_only_cols.write().unwrap().remove(col);
        {
            let mut keys = self.local_only_keys.write().unwrap();
            // ponytail: marks are `col\0id` â€” split on the separator instead
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

    /// Drop a collection entirely (issue #31): one batched delete emits
    /// per-doc tombstones (watchers + sync tail see the change through the
    /// normal write path), then the shard is flushed, evicted from memory,
    /// and its directory unlinked â€” memory first, unlink last, so no
    /// reader ever reaches for a disappearing directory. Index definitions
    /// (secondary/fts/composite/vector) are removed and re-persisted.
    ///
    /// Gates (fail-closed, same family as relocate_docs): sync-excluded
    /// plane, local-only collections, read-only engines, and
    /// lazy-unloaded (archived) collections â€” the last refuses with
    /// "load it first" because dropping parked data needs it resident for
    /// the tombstone pass.
    ///
    /// Dropping a nonexistent collection is a no-op (empty report,
    /// idempotent). Peers that still hold the data converge to empty via
    /// the emitted deletes; cross-instance drop is NOT atomic â€” drop on
    /// the writer first.
    pub fn drop_collection(&self, collection: &str) -> Result<DropReport> {
        if self.read_only.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(HakoError::Corrupt("ReadOnly".into()));
        }
        if self.is_sync_excluded_effective(collection) {
            return Err(HakoError::Corrupt(format!(
                "drop: `{collection}` is on the sync-excluded plane (system store)"
            )));
        }
        if self.is_collection_local(collection) {
            return Err(HakoError::Corrupt(format!(
                "drop: `{collection}` is marked local-only"
            )));
        }
        if self.is_lazy_unloaded(collection) {
            return Err(HakoError::Corrupt(format!(
                "drop: `{collection}` is an unloaded lazy collection â€” load it first"
            )));
        }

        // Nothing loaded under this name -> nothing to drop (idempotent).
        let Some(shard_arc) = self
            .shards
            .read()
            .map_err(|_| HakoError::LockPoisoned("shards".into()))?
            .get(collection)
            .cloned()
        else {
            return Ok(DropReport {
                docs_removed: 0,
                bytes_reclaimed: 0,
                collections_dropped: Vec::new(),
            });
        };

        // 1. Tombstone every live doc through the normal write path: one
        //    batch per 1000 ids (max_batch_ops convention). This is what
        //    makes the drop visible â€” watchers fire, sync peers receive
        //    deletes, versions advance â€” instead of silent unlinking.
        let mut ids: Vec<String> = Vec::new();
        {
            let storage = shard_arc.read().unwrap();
            ids.extend(storage.index.keys().cloned());
        }
        let docs_removed = ids.len() as u64;
        for chunk in ids.chunks(1000) {
            let mutations: Vec<BatchMutation> = chunk
                .iter()
                .map(|id| BatchMutation::Delete {
                    collection: collection.to_string(),
                    doc_id: id.clone(),
                })
                .collect();
            self.write_batch(mutations)?;
        }

        // 2. Evict from the shard map (memory first â€” the reverse order
        //    risks a reader opening a shard whose directory is about to
        //    disappear), then flush the final state so the WAL on disk
        //    holds the complete tombstone record even for the unlink.
        let evicted = self
            .shards
            .write()
            .map_err(|_| HakoError::LockPoisoned("shards".into()))?
            .remove(collection);
        if let Some(shard) = evicted {
            if let Ok(mut storage) = shard.write() {
                let _ = storage.drain_blob_queue();
                let _ = storage.flush_all();
            }
        }

        // 3. Index definitions: remove every family's RAM registration,
        //    re-persist definitions.json without them, and drop the RAM
        //    structures + doc cache + plan cache entries.
        {
            // Unique-ledger claims for this collection go with the defs
            // (hakodb#71): collect the ids before dropping.
            let unique_ids: Vec<u32> = {
                let mgr = self.indexes.read().unwrap();
                mgr.composite
                    .indexes_for_collection(collection)
                    .filter(|i| i.definition.unique)
                    .map(|i| i.definition.id)
                    .collect()
            };
            let mut mgr = self.indexes.write().unwrap();
            mgr.drop_collection_defs(collection);
            for id in unique_ids {
                self.unique_ledger.reset(id);
            }
        }
        self.gate_cache.write().unwrap().clear();
        self.persist_index_defs()?;
        // F8: a dropped collection's schema goes with its defs.
        {
            let mut schemas = self.schemas.write().unwrap();
            if schemas.remove(collection).is_some() {
                self.schema_enabled
                    .store(!schemas.is_empty(), std::sync::atomic::Ordering::Relaxed);
            }
        }
        let _ = self.persist_schemas();
        if let Ok(mut cache) = self.doc_cache.write() {
            if !cache.is_empty() {
                let prefix = format!("{collection}\0");
                cache.retain(|k, _| !k.starts_with(&prefix));
            }
        }
        if let Ok(mut cache) = self.row_cache.write() {
            if !cache.map.is_empty() {
                let prefix = format!("{collection}\0");
                cache.map.retain(|k, _| !k.starts_with(&prefix));
                cache.order.retain(|k| !k.starts_with(&prefix));
            }
        }
        self.plan_cache.invalidate();

        // 4. Unlink last: the directory only disappears once nothing in
        //    the engine references it anymore.
        let dir = self.root_path.join(collection);
        let bytes_reclaimed = dir_size(&dir);
        std::fs::remove_dir_all(&dir)
            .map_err(|e| HakoError::Io(std::io::Error::new(e.kind(), format!("drop `{collection}`: {e}"))))?;

        self.record_audit(AuditEntry {
            op: AccessOp::Batch,
            collection: collection.to_string(),
            doc_id: None,
            ok: true,
        });

        Ok(DropReport {
            docs_removed,
            bytes_reclaimed,
            collections_dropped: vec![collection.to_string()],
        })
    }

    /// Rename a collection (issue #132): replay its live docs onto `to`
    /// through the normal write path, delete them from `from`, then drop
    /// the old directory. Index definitions, the schema, and local-only
    /// marks move with it.
    ///
    /// Sync semantics: this is NOT a metadata rename. Peers receive Put
    /// ops for `to` and Delete ops for `from`, so they converge without a
    /// full re-sync. The cost is a full rewrite of the collection (bytes,
    /// versions, index rebuilds) â€” treat rename as writer-side
    /// maintenance, not a hot path.
    ///
    /// Gates: read-only engine, empty/path-like/reserved names, missing
    /// `from`, existing `to`, sync-excluded plane, unloaded lazy
    /// collection. Refusals leave both names untouched.
    ///
    /// Not atomic: a crash mid-way leaves a partial `to` with `from`
    /// intact (or half-deleted). Drop the leftover name and retry.
    /// Returns the number of docs moved.
    pub fn rename_collection(&self, from: &str, to: &str) -> Result<u64> {
        if self.read_only.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(HakoError::Corrupt("ReadOnly".into()));
        }
        let bad_name = |name: &str| {
            name.is_empty()
                || name.starts_with('_')
                || name == "snapshots"
                || name.contains(['/', '\\', '\0'])
                || name == "."
                || name == ".."
        };
        if from == to {
            return Err(HakoError::InvalidInput(
                "rename: `from` and `to` are the same collection".into(),
            ));
        }
        if bad_name(from) || bad_name(to) {
            return Err(HakoError::InvalidInput(format!(
                "rename: invalid collection name (`{from}` -> `{to}`)"
            )));
        }
        if self.is_sync_excluded_effective(from) || self.is_sync_excluded_effective(to) {
            return Err(HakoError::Corrupt(
                "rename: sync-excluded collections cannot be renamed".into(),
            ));
        }
        if self.is_lazy_unloaded(from) || self.is_lazy_unloaded(to) {
            return Err(HakoError::Corrupt(format!(
                "rename: `{}` is an unloaded lazy collection â€” load it first",
                if self.is_lazy_unloaded(from) { from } else { to }
            )));
        }
        let from_dir = self.root_path.join(from);
        let to_dir = self.root_path.join(to);
        if !from_dir.is_dir() {
            return Err(HakoError::InvalidInput(format!(
                "rename: `{from}` does not exist"
            )));
        }
        if to_dir.exists()
            || self
                .shards
                .read()
                .map(|m| m.contains_key(to))
                .unwrap_or(false)
        {
            return Err(HakoError::InvalidInput(format!(
                "rename: `{to}` already exists"
            )));
        }

        // Snapshot the metadata that must follow the data.
        let (secondary, fts, vector, composite, traverse) = {
            let mgr = self.indexes.read().unwrap();
            let secondary: Vec<String> = mgr
                .secondary
                .get(from)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            let fts: Vec<String> = mgr
                .fts
                .get(from)
                .map(|m| m.keys().cloned().collect())
                .unwrap_or_default();
            let vector: Vec<(String, u32, crate::index::vector::Metric)> = mgr
                .vector
                .get(from)
                .map(|m| {
                    m.iter()
                        .map(|(f, d)| (f.clone(), d.dim, d.metric))
                        .collect()
                })
                .unwrap_or_default();
            let composite = mgr
                .indexes_for_collection(from)
                .map(|i| i.definition.clone())
                .collect::<Vec<_>>();
            let traverse = mgr.traverse.is_tracked(from);
            (secondary, fts, vector, composite, traverse)
        };
        let schema = self.schema(from);
        let was_local = self.is_collection_local(from);

        let ids: Vec<String> = {
            let shard = self.get_shard(from)?;
            let storage = shard.read().unwrap();
            storage.index.keys().cloned().collect()
        };

        // Local-only signal first, mirrored onto `to` and kept on `from`
        // until the deletes land: neither the copied docs nor the old
        // tombstones may replicate.
        {
            let mut keys = self.local_only_keys.write().unwrap();
            let prefix = format!("{from}\0");
            let moved: Vec<String> = keys
                .iter()
                .filter(|k| k.starts_with(&prefix))
                .map(|k| format!("{to}\0{}", &k[prefix.len()..]))
                .collect();
            keys.extend(moved);
        }
        if was_local {
            self.set_collection_local(to, true);
        }
        self.persist_local_only();

        for chunk in ids.chunks(1000) {
            let mut mutations = Vec::with_capacity(chunk.len());
            for id in chunk {
                if let Some(doc) = self.get(from, id)? {
                    mutations.push(BatchMutation::Put {
                        collection: to.to_string(),
                        doc_id: id.clone(),
                        doc,
                    });
                }
            }
            if !mutations.is_empty() {
                self.write_batch(mutations)?;
            }
        }

        for chunk in ids.chunks(1000) {
            let mutations: Vec<BatchMutation> = chunk
                .iter()
                .map(|id| BatchMutation::Delete {
                    collection: from.to_string(),
                    doc_id: id.clone(),
                })
                .collect();
            self.write_batch(mutations)?;
        }

        // Index defs rebuild on the new name (derived RAM graphs follow).
        for field in secondary {
            self.create_index(to, &field)?;
        }
        for field in fts {
            self.create_fts_index(to, &field)?;
        }
        for (field, dim, metric) in vector {
            self.create_vector_index(to, &field, dim, metric)?;
        }
        for def in composite {
            let fields = def
                .fields
                .iter()
                .map(|f| (f.field.clone(), f.direction.clone()))
                .collect();
            self.create_composite_index_ex(to, fields, def.unique, def.sparse, def.filter.clone())?;
        }
        if traverse {
            self.create_traverse_index(to)?;
        }
        if let Some(schema) = schema {
            self.set_schema(to, schema)?;
        }

        // Old marks and the local flag go; `from` is empty by now, so the
        // normal drop cleanup (dir + defs + schema) is safe.
        {
            let prefix = format!("{from}\0");
            self.local_only_keys
                .write()
                .unwrap()
                .retain(|k| !k.starts_with(&prefix));
        }
        self.set_collection_local(from, false);
        self.drop_collection(from)?;

        Ok(ids.len() as u64)
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
    /// single batch â€” the CLI/SDK batch path).
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
    /// routes (no room prefix) â€” the marker itself never replicates.
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

    /// hakodb#71 Release C — F4 aggregation pipeline. `Match` stages run
    /// through the normal query executor (planner/F2/F3 included); the
    /// remaining stages stream over the materialised rows. A pipeline
    /// whose first stage is not `Match` seeds from a full collection scan.
    pub fn run_pipeline(
        &self,
        pipeline: crate::query::pipeline::Pipeline,
    ) -> Result<Vec<(String, HakoDoc)>> {
        use crate::query::pipeline::{self, Stage};
        pipeline::validate(&pipeline)?;
        let mut rows: Option<Vec<(String, HakoDoc)>> = None;
        for stage in &pipeline.stages {
            match stage {
                Stage::Match(q) => {
                    // A Match stage replaces the row set (each one runs the
                    // full query machinery on its own collection).
                    rows = Some(self.query(q.clone())?);
                }
                other => {
                    if rows.is_none() {
                        rows = Some(self.query(Query::new(&pipeline.collection))?);
                    }
                    let current = rows.take().unwrap_or_default();
                    rows = Some(pipeline::run_stages(current, std::slice::from_ref(other))?);
                }
            }
        }
        Ok(rows.unwrap_or_default())
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, HakoDoc)>> {
        self.query_with_cancel(query, None)
    }

    fn query_with_cancel(
        &self,
        query: Query,
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<(String, HakoDoc)>> {
        Ok(self
            .query_rows(query, cancel)?
            .into_iter()
            .map(|(id, row)| (id, row.into_doc()))
            .collect())
    }

    /// [`Self::query`] handing out shared `Arc`s instead of owned docs:
    /// rows served from the query row cache are returned zero-copy (one
    /// refcount bump, #142 Q3); freshly decoded rows are wrapped once.
    /// Mutation goes through copy-on-write at the FFI boundary (`DocRef`),
    /// so a cache entry is never mutated through a handle.
    pub fn query_arc(&self, query: Query) -> Result<Vec<(String, std::sync::Arc<HakoDoc>)>> {
        Ok(self
            .query_rows(query, None)?
            .into_iter()
            .map(|(id, row)| (id, row.into_arc()))
            .collect())
    }

    /// [`Self::query_arc`] keeping the executor's `RowRef` distinction (#161):
    /// only rows served from the query row cache are `Shared` (the entire
    /// source of the zero-copy win); freshly decoded rows stay owned, so the
    /// FFI wraps them as `DocRef::Owned` and large scans pay zero per-row
    /// `Arc::new`/drop churn — the std scan-regression root cause.
    pub(crate) fn query_row_refs(
        &self,
        query: Query,
    ) -> Result<Vec<(String, crate::query::executor::executor::RowRef)>> {
        self.query_rows(query, None)
    }

    /// The single query funnel (plan cache, traverse branch, audit): returns
    /// the executor's `RowRef` rows so the owned and arc wrappers above share
    /// one implementation.
    fn query_rows(
        &self,
        query: Query,
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<(String, crate::query::executor::executor::RowRef)>> {
        if let Some(token) = cancel {
            token.check()?;
        }
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(self.deny(AccessOp::Query, &query.collection, None));
        }

        let Some(shard_arc) = self.try_get_shard_on_disk(&query.collection)? else { return Ok(Vec::new()) };
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready, vec_ready);
        WRITE_STATS.queries.fetch_add(1, Ordering::Relaxed);
        if let Some(label) = plan.scan.index_label() {
            record_index_scan(&label);
        }

        // Graph traversal (issue #53): cross-collection BFS can't ride
        // the single-shard executor â€” dedicated path below. Normal scans
        // are untouched (perf: zero hot-path change).
        if matches!(plan.scan, crate::query::plan::ScanType::Traverse { .. }) {
            let results = self.execute_traverse_query(&plan, cancel)?;
            self.record_audit(AuditEntry {
                op: AccessOp::Query,
                collection: query.collection.clone(),
                doc_id: None,
                ok: true,
            });
            return Ok(results
                .into_iter()
                .map(|(id, doc)| (id, crate::query::executor::executor::RowRef::Owned(doc)))
                .collect());
        }

        // SIMPLE CALL: No blob_file or encryption passed here!
        let results = self
            .executor
            .execute_with_cancel(shard_arc, &indexes, (*plan).clone(), cancel, Some(self))?;

        // AUDIT RESULT
        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true, // prev ->results.is_ok()
        });

        Ok(results)
    }

    /// EXPLAIN: return the plan `query()` would execute, without running it.
    /// Same admission gate (a denied caller learns nothing about indexes),
    /// same plan-cache path (what you see is what the next call runs).
    /// For SDKs, gateway debugging, and tests that pin routing behavior.
    pub fn explain(&self, query: Query) -> Result<crate::query::plan::QueryPlan> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(HakoError::Corrupt("Denied".into()));
        }
        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");
        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();
        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready, vec_ready);
        Ok((*plan).clone())
    }

    /// F7: human-readable plan report for one query. Plan-only (nothing is
    /// executed); `collection_rows` is the scan's upper bound and
    /// `filter_count` the predicates applied after the scan, so "why is my
    /// index not used?" is answerable without instrumenting the app.
    pub fn explain_report(&self, query: Query) -> Result<ExplainReport> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(HakoError::Corrupt("Denied".into()));
        }
        let shard_arc = self.get_shard(&query.collection)?;
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");
        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();
        let plan = self.plan_cache.get_or_compute(
            &query,
            &indexes,
            rows,
            self.config.query_workers,
            is_ready,
            vec_ready,
        );
        let mut available = Vec::new();
        if let Some(fields) = indexes.secondary.get(&plan.collection) {
            for field in fields.keys() {
                available.push(format!("secondary_index({field})"));
            }
        }
        if let Some(fields) = indexes.fts.get(&plan.collection) {
            for field in fields.keys() {
                available.push(format!("inverted_index({field})"));
            }
        }
        if let Some(fields) = indexes.vector.get(&plan.collection) {
            for field in fields.keys() {
                available.push(format!("vector_knn({field})"));
            }
        }
        for idx in indexes.composite.indexes_for_collection(&plan.collection) {
            let fields: Vec<&str> =
                idx.definition.fields.iter().map(|f| f.field.as_str()).collect();
            available.push(format!("composite_index({})", fields.join(",")));
        }
        available.sort();
        drop(indexes);
        Ok(ExplainReport {
            collection: plan.collection.clone(),
            scan: plan.scan.describe(),
            index: plan.scan.index_label(),
            collection_rows: rows,
            filter_count: plan.filters.len(),
            or_group_count: plan.or_groups.len(),
            elem_match_count: plan.elem_match.len(),
            filters_satisfied_by_index: plan.filters_satisfied_by_index,
            order_by_satisfied: plan.order_by_satisfied,
            limit: plan.limit,
            offset: plan.offset,
            projection: plan.projection.clone(),
            covering: plan.covering_projection().is_some(),
            available_indexes: available,
        })
    }

    /// Traverse-query path (issue #53): BFS via `find_paths`, hits
    /// filtered to the queried collection (cross-collection hits ride
    /// `find_paths` â€” pipeline rows are single-collection), `extra`
    /// verified per hit (missing-field rejects, VectorKnn rule), then
    /// order/offset/limit mirroring the legacy manual sort.
    fn execute_traverse_query(
        &self,
        plan: &crate::query::plan::QueryPlan,
        cancel: Option<&CancelToken>,
    ) -> Result<Vec<(String, HakoDoc)>> {
        use crate::query::filter::compare_values;
        use crate::query::plan::ScanType;
        let (start_col, start_id, spec, extra) = match &plan.scan {
            ScanType::Traverse { start_col, start_id, spec, extra } => {
                (start_col.clone(), start_id.clone(), spec.clone(), extra.clone())
            }
            _ => {
                return Err(HakoError::Corrupt("traverse executor reached without a Traverse plan".into()))
            }
        };
        let mut spec = spec;
        // scan_limit (limit+offset, planner-set) bounds the BFS fetch;
        // fall back to the spec default when the planner left it open.
        if let Some(fetch) = plan.scan_limit {
            spec.limit = fetch;
        }
        let mut results: Vec<(String, HakoDoc)> = Vec::new();
        for hit in self.find_paths(&start_col, &start_id, &spec)? {
            if let Some(token) = cancel {
                token.check()?;
            }
            if hit.collection != plan.collection {
                continue;
            }
            let mut pass = true;
            for f in extra.iter() {
                match hit.doc.get(&f.field) {
                    Some(v) => {
                        if !compare_values(v, &f.op, &f.value) {
                            pass = false;
                            break;
                        }
                    }
                    None => {
                        pass = false;
                        break;
                    }
                }
            }
            if !pass {
                continue;
            }
            results.push((hit.id, hit.doc));
        }
        if !plan.order_by.is_empty() {
            results.sort_by(|(id_a, doc_a), (id_b, doc_b)| {
                for order in &plan.order_by {
                    let cmp = match order.field.as_str() {
                        "id" => id_a.cmp(id_b),
                        "_time" => doc_a._time.cmp(&doc_b._time),
                        _ => {
                            let av = doc_a.get(&order.field);
                            let bv = doc_b.get(&order.field);
                            av.cmp(&bv)
                        }
                    };
                    if cmp != std::cmp::Ordering::Equal {
                        return if order.ascending { cmp } else { cmp.reverse() };
                    }
                }
                std::cmp::Ordering::Equal
            });
        }
        let offset = plan.offset.unwrap_or(0);
        if offset > 0 {
            results = results.into_iter().skip(offset).collect();
        }
        if let Some(limit) = plan.limit {
            results.truncate(limit);
        }
        Ok(results)
    }

    /// Raw scan: storage-encoded bytes instead of decoded docs. Same
    /// admission path as [`Self::query`] (security, plan cache, audit);
    /// execution stops after the index walk â€” see
    /// `ParallelQueryExecutor::execute_raw` for the contract (opaque bytes,
    /// index-satisfied filters/ordering required). Sets `query.raw` so the
    /// plan-cache key can't collide with a decoded query of the same shape.
    pub fn query_raw(&self, mut query: Query) -> Result<Vec<(String, std::sync::Arc<Vec<u8>>)>> {
        query.raw = true;
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(self.deny(AccessOp::Query, &query.collection, None));
        }

        let Some(shard_arc) = self.try_get_shard_on_disk(&query.collection)? else { return Ok(Vec::new()) };
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready, vec_ready);

        let results = self.executor.execute_raw(shard_arc, &indexes, (*plan).clone())?;

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(results)
    }

    /// JSON rows from the raw fast path when the query allows it, else from
    /// the decoded query. Raw rows are serialized from pinned bytes; rows
    /// with blob links fall back to decode + resolve so bytes match
    /// `query` + `to_json_bytes`. Projections/aggregations and queries the
    /// raw executor cannot satisfy take the decoded path (same semantics).
    pub fn query_json(&self, query: Query) -> Result<Vec<(String, Vec<u8>)>> {
        if query.projection.is_empty() && query.aggregations.is_empty() {
            match self.query_raw(query.clone()) {
                Ok(rows) => {
                    let mut out = Vec::with_capacity(rows.len());
                    for (id, bytes) in rows {
                        let Some(view) = crate::document::hako_doc::DocView::new(bytes) else {
                            continue;
                        };
                        let json = if query.defer_blobs {
                            view.to_json_bytes()
                        } else {
                            self.json_from_view(&view, &query.collection)?
                        };
                        if let Some(json) = json {
                            out.push((id, json));
                        }
                    }
                    return Ok(out);
                }
                Err(HakoError::QueryError(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let mut q = query;
        q.raw = false;
        Ok(self
            .query(q)?
            .into_iter()
            .map(|(id, doc)| (id, doc.to_json_bytes()))
            .collect())
    }

    /// [`Self::query_json`] writing the JSON array straight into `out`.
    /// Same rows and same bytes, but without the per-row `Vec` that path
    /// allocates and then copies: 20 rows used to pay 20 allocs + 20 frees
    /// + 20 memcpys per page for bytes that are written exactly once.
    pub fn query_json_into(&self, query: Query, out: &mut Vec<u8>) -> Result<usize> {
        if query.projection.is_empty() && query.aggregations.is_empty() {
            match self.query_raw(query.clone()) {
                Ok(rows) => {
                    out.push(b'[');
                    let mut n = 0usize;
                    for (_id, bytes) in rows {
                        let Some(view) = crate::document::hako_doc::DocView::new(bytes) else {
                            continue;
                        };
                        if n > 0 {
                            out.push(b',');
                        }
                        let mark = out.len();
                        let wrote = if query.defer_blobs || !view.has_blob_links() {
                            view.write_json(out).is_some()
                        } else {
                            // rare: blob links need a decode + resolve.
                            match self.json_from_view(&view, &query.collection)? {
                                Some(json) => {
                                    out.extend_from_slice(&json);
                                    true
                                }
                                None => false,
                            }
                        };
                        if !wrote {
                            out.truncate(mark);
                            if n > 0 {
                                out.pop();
                            }
                            continue;
                        }
                        n += 1;
                    }
                    out.push(b']');
                    return Ok(n);
                }
                Err(HakoError::QueryError(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let mut q = query;
        q.raw = false;
        out.push(b'[');
        let mut n = 0usize;
        for (_id, doc) in self.query(q)? {
            if n > 0 {
                out.push(b',');
            }
            doc.write_json(out);
            n += 1;
        }
        out.push(b']');
        Ok(n)
    }

    /// Callback over [`Self::query_json`] rows. The rows are materialized
    /// first (this is not the zero-copy `walk_view`); `false` stops early.
    pub fn walk_json<F>(&self, query: Query, callback: &mut F) -> Result<usize>
    where
        F: FnMut(&str, &[u8]) -> bool,
    {
        let rows = self.query_json(query)?;
        let mut visited = 0;
        for (id, json) in rows {
            visited += 1;
            if !callback(&id, &json) {
                break;
            }
        }
        Ok(visited)
    }

    /// Zero-alloc scan walk: lends each matching row (`&str` id, `&[u8]`
    /// storage bytes) to `callback`; `false` stops early. Returns rows
    /// visited. See `ParallelQueryExecutor::execute_walk` for the contract
    /// (SortedKeys scans only for now; callback must not re-enter the
    /// engine â€” the storage read lock is held for the whole walk).
    pub fn walk<F>(&self, mut query: Query, callback: &mut F) -> Result<usize>
    where
        F: FnMut(&str, &[u8]) -> bool,
    {
        query.raw = true;
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(self.deny(AccessOp::Query, &query.collection, None));
        }

        let Some(shard_arc) = self.try_get_shard_on_disk(&query.collection)? else { return Ok(0) };
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready, vec_ready);

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
            return Err(self.deny(AccessOp::Query, &query.collection, None));
        }

        let Some(shard_arc) = self.try_get_shard_on_disk(&query.collection)? else { return Ok(0) };
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();

        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready, vec_ready);

        let visited = self.executor.execute_walk_view(shard_arc, &indexes, (*plan).clone(), callback)?;

        self.record_audit(AuditEntry {
            op: AccessOp::Query,
            collection: query.collection.clone(),
            doc_id: None,
            ok: true,
        });

        Ok(visited)
    }

    #[allow(clippy::type_complexity)]
    pub fn query_projected_zero_copy(
        &self,
        query: Query,
        fields: &[String],
    ) -> Result<Vec<(String, Vec<(String, Value)>)>> {
        // 1. Security Check
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(self.deny(AccessOp::Query, &query.collection, None));
        }

        let mut q = query.clone();
        q.projection = fields.to_vec();

        let Some(shard_arc) = self.try_get_shard_on_disk(&q.collection)? else { return Ok(Vec::new()) };
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc.read().unwrap().count_prefix("");

        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();
        let plan = self.plan_cache.get_or_compute(&q, &indexes, rows, self.config.query_workers, is_ready, vec_ready);

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
            return Err(self.deny(AccessOp::Put, col, Some(id)));
        }

        let data = self.write_batch(vec![BatchMutation::Patch {
            collection: col.to_string(),
            doc_id: id.to_string(),
            updates,
        }]);

        // AUDIT RESULT
        self.record_audit(AuditEntry {
            op: AccessOp::Put,
            collection: col.into(),
            doc_id: Some(id.into()),
            ok: data.is_ok(),
        });

        Ok(data?.into_iter().next().unwrap_or_default()) 
    }

    pub fn delete_where(&self, query: crate::query::query::Query) -> Result<usize> {
        self.delete_where_impl(query, None)
    }

    /// [`Self::delete_where`] with cooperative cancellation.
    ///
    /// The token is checked during the matching scan and once more before
    /// the delete batch commits. Cancelling before that final batch means
    /// no document is deleted; the whole batch is atomic, so there is no
    /// partially deleted match set.
    pub fn delete_where_with_cancel(
        &self,
        query: crate::query::query::Query,
        cancel: &CancelToken,
    ) -> Result<usize> {
        self.delete_where_impl(query, Some(cancel))
    }

    fn delete_where_impl(
        &self,
        query: crate::query::query::Query,
        cancel: Option<&CancelToken>,
    ) -> Result<usize> {
        // 1. Find the matches (cancel checks ride the scan).
        let results = self.query_with_cancel(query.clone(), cancel)?;
        if let Some(token) = cancel {
            token.check()?;
        }
        if results.is_empty() { return Ok(0); }

        // 2. Map to delete mutations
        let count = results.len();
        let mutations = results.into_iter()
            .map(|(id, _)| BatchMutation::Delete { 
                collection: query.collection.clone(), 
                doc_id: id 
            })
            .collect();

        // 3. Execute batch; cancelled before commit means zero writes.
        if let Some(token) = cancel {
            token.check()?;
        }
        self.write_batch(mutations)?;
        Ok(count)
    }

    pub fn patch_where(&self, query: crate::query::query::Query, updates: Vec<(String, Value)>) -> Result<usize> {
        self.patch_where_impl(query, updates, None)
    }

    /// [`Self::patch_where`] with cooperative cancellation.
    ///
    /// The token is checked during the matching scan and once more before
    /// the patch batch commits. Cancelling before that final batch means
    /// no document is modified; the batch itself is atomic.
    pub fn patch_where_with_cancel(
        &self,
        query: crate::query::query::Query,
        updates: Vec<(String, Value)>,
        cancel: &CancelToken,
    ) -> Result<usize> {
        self.patch_where_impl(query, updates, Some(cancel))
    }

    fn patch_where_impl(
        &self,
        query: crate::query::query::Query,
        updates: Vec<(String, Value)>,
        cancel: Option<&CancelToken>,
    ) -> Result<usize> {
        let results = self.query_with_cancel(query.clone(), cancel)?;
        if let Some(token) = cancel {
            token.check()?;
        }
        if results.is_empty() { return Ok(0); }

        let count = results.len();
        let mutations = results.into_iter()
            .map(|(id, _)| BatchMutation::Patch { 
                collection: query.collection.clone(), 
                doc_id: id, 
                updates: updates.clone() 
            })
            .collect();

        if let Some(token) = cancel {
            token.check()?;
        }
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
        drop(shards);
        // FS-aware tail: unopened (lazy-skipped or never-touched)
        // collections are absent from the map, but their bytes are real â€”
        // copy them with the same *.dat + wal.log filter the per-shard
        // backup uses (their WAL is untouched, so no flush is owed).
        let loaded: std::collections::HashSet<String> =
            self.shards.read().unwrap().keys().cloned().collect();
        if let Ok(entries) = std::fs::read_dir(&self.root_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if name == "_indices" || name == "snapshots" || loaded.contains(&name) {
                    continue;
                }
                if entry.path().is_dir() {
                    copy_collection_dir(&entry.path(), &dest.as_ref().join(&name))?;
                }
            }
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
            .rfind(|r| op == r.op && col.starts_with(&r.collection_prefix))
            .map(|r| r.allow)
            .unwrap_or(true)
    }

    /// Denial constructor for the admission gates: audits the rejection
    /// (entry allocs gated on the toggle, same as the inlined sites did)
    /// and returns the uniform `"Denied"` error. One copy instead of ~12.
    #[inline]
    fn deny(&self, op: AccessOp, collection: &str, doc_id: Option<&str>) -> HakoError {
        if self.config.enable_audit_log {
            self.record_audit(AuditEntry {
                op,
                collection: collection.into(),
                doc_id: doc_id.map(Into::into),
                ok: false,
            });
        }
        HakoError::Corrupt("Denied".into())
    }

    fn get_col<'a>(&self, m: &'a BatchMutation) -> &'a str {
        match m {
            BatchMutation::Put { collection, .. } => collection,
            BatchMutation::Delete { collection, .. } => collection,
            BatchMutation::Patch { collection, .. } => collection,
        }
    }

    ///
    /// Issue #71/F5 item 3: the doc and its version are captured inside one
    /// shard lock acquisition. The old pair (`get()` + `current_version()`)
    /// is two calls â€” a commit landing between them recorded the NEW
    /// version against the OLD document, silently reopening the
    /// read-modify-write lost-update window this closes.
    ///
    /// (`bump_versions_by_keys` runs inside the same batch's shard write
    /// lock â€” moved there as part of this â€” so the version read here has a
    /// single serializable point per shard.)
    pub fn get_versioned(
        &self,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<(HakoDoc, u64)>> {
        if !self.allowed(collection, AccessOp::Get) {
            return Err(self.deny(AccessOp::Get, collection, Some(doc_id)));
        }
        let Some(shard) = self.try_get_shard_on_disk(collection)? else {
            return Ok(None);
        };
        let shard = shard.safe_read()?;
        let doc = shard
            .get_shared(doc_id)?
            .and_then(|b| HakoDoc::decode(&b));
        let Some(doc) = doc else { return Ok(None) };
        let version = self.current_version(doc_id);
        Ok(Some((doc, version.unwrap_or(0))))
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

    /// Drop version entries for keys whose final op was a delete. A
    /// tombstoned doc is absent again, so `get_versioned` reports None; the
    /// commit check must compare against None too, not a historical version.
    fn clear_versions_by_keys(
        &self,
        keys: &HashSet<Arc<str>>,
        keep: &HashSet<Arc<str>>,
    ) {
        for key in keys {
            if keep.contains(key) {
                continue;
            }
            let bucket = (fxhash::hash64(&key) % 32) as usize;
            if let Ok(mut shard) = self.doc_versions[bucket].write() {
                shard.remove(key.as_ref());
            }
        }
    }

    pub fn begin_serializable_transaction(&self) -> SerializableTransaction {
        SerializableTransaction {
            reads: HashMap::new(),
            mutations: Vec::new(),
        }
    }

    /// Issue #71/F5 item 2: run a serializable read-modify-write with a
    /// bounded retry loop. Retries only on `is_retryable()` errors (read-set
    /// conflicts); the closure runs against a FRESH transaction each
    /// attempt, so a retry re-reads and re-applies the whole body.
    ///
    /// `ponytail:` fixed exponential backoff, no jitter — a handful of
    /// writers under the global tx lock serialize anyway; add jitter if the
    /// retry counter shows a thundering herd on a contending workload.
    pub fn transaction<T>(
        &self,
        policy: RetryPolicy,
        mut body: impl FnMut(&mut SerializableTransaction) -> Result<T>,
    ) -> Result<T> {
        let attempts = policy.max_attempts.max(1);
        let mut backoff = policy.base_delay;
        for attempt in 1..=attempts {
            let mut tx = self.begin_serializable_transaction();
            let out = body(&mut tx)?;
            match tx.commit(self) {
                Ok(_) => return Ok(out),
                Err(e) if e.is_retryable() && attempt < attempts => {
                    WRITE_STATS.tx_retries.fetch_add(1, Ordering::Relaxed);
                    std::thread::sleep(backoff);
                    backoff = (backoff * 2).min(policy.max_delay);
                }
                Err(e) => return Err(e),
            }
        }
        unreachable!("retry loop returns on the last attempt")
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
    /// path costs the same as the former in-tree call â€” only the location
    /// moved, not the complexity.
    pub fn plan_for_watch(&self, query: &crate::query::query::Query) -> crate::query::plan::QueryPlan {        let indexes = self.indexes.read().unwrap();
        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();
        crate::query::planner::QueryPlanner::plan(query, &indexes, 0, 1, is_ready, vec_ready)
    }

    /// Zero-decode view match for one watch event: true when the stored
    /// `bytes` for `doc_id` satisfy `plan` (built by `plan_for_watch`).
    /// Handles the exit case too â€” returns false when an update stops
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
        let Some(shard) = self.try_get_shard_on_disk(collection)? else { return Ok(None) };
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
        self.compact_impl(None)
    }

    /// [`Self::compact`] with cooperative cancellation.
    ///
    /// Shards merged before cancellation stay merged; the remaining shards
    /// are left untouched. Compaction is idempotent, so a cancelled pass
    /// loses no data and can simply be retried.
    pub fn compact_with_cancel(&self, cancel: &CancelToken) -> Result<()> {
        self.compact_impl(Some(cancel))
    }

    fn compact_impl(&self, cancel: Option<&CancelToken>) -> Result<()> {
        for s in self.shards.read().unwrap().values() {
            if let Some(token) = cancel {
                token.check()?;
            }
            s.write().unwrap().compact_with_cancel(cancel)?;
        }
        Ok(())
    }

    /// [`Self::compact`] restricted to one collection.
    ///
    /// A no-op when the collection is absent or not currently loaded (a lazy
    /// collection that was never touched): whole-database compaction has the
    /// same resident-shard scope. Compaction is idempotent, so repeating it
    /// is safe.
    pub fn compact_collection(&self, collection: &str) -> Result<()> {
        self.compact_collection_impl(collection, None)
    }

    /// [`Self::compact_collection`] with cooperative cancellation.
    ///
    /// A cancelled pass leaves the collection merged up to the last completed
    /// rewrite and loses no data, same as [`Self::compact_with_cancel`].
    pub fn compact_collection_with_cancel(
        &self,
        collection: &str,
        cancel: &CancelToken,
    ) -> Result<()> {
        self.compact_collection_impl(collection, Some(cancel))
    }

    fn compact_collection_impl(
        &self,
        collection: &str,
        cancel: Option<&CancelToken>,
    ) -> Result<()> {
        let Some(shard) = self.try_get_shard(collection) else {
            return Ok(());
        };
        if let Some(token) = cancel {
            token.check()?;
        }
        shard.write().unwrap().compact_with_cancel(cancel)?;
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

    /// True when `col` matches `HakoConfig::lazy_collections` (exact name
    /// or trailing-`*` prefix). Static config truth, no locks.
    pub fn is_lazy_collection(&self, col: &str) -> bool {
        crate::config::lazy_pattern_match(&self.config.lazy_collections, col)
    }

    /// True when `col` is lazy AND not currently loaded (absent from the
    /// shard map). The dynamic half of the sync gate: archived data never
    /// meshes until touched, then normal rules apply.
    /// Account one collection (issue #32): doc/tombstone counts and live
    /// bytes from the RAM index, WAL/segment/blob/total bytes from disk.
    /// See [`CollectionStats`] for the consistency caveat. A collection
    /// that was never created returns zeroed stats with `unloaded`
    /// reflecting lazy residency.
    pub fn collection_stats(&self, collection: &str) -> Result<CollectionStats> {
        let unloaded = self.is_lazy_unloaded(collection);
        let Some(shard_arc) = self.try_get_shard(collection) else {
            let dir = self.root_path.join(collection);
            return Ok(CollectionStats {
                collection: collection.to_string(),
                doc_count: 0,
                live_bytes: 0,
                total_bytes: dir_size(&dir),
                segment_count: 0,
                wal_bytes: 0,
                blob_bytes: 0,
                tombstone_count: 0,
                unloaded,
            });
        };
        let storage = shard_arc.read().unwrap();
        let mut doc_count = 0u64;
        let mut tombstone_count = 0u64;
        for ptr in storage.index.values() {
            match ptr {
                Pointer::Deleted { .. } => tombstone_count += 1,
                _ => doc_count += 1,
            }
        }
        let dir = self.root_path.join(collection);
        let total_bytes = dir_size(&dir);
        let wal_bytes = storage.wal.file_len();
        let blob_bytes = std::fs::metadata(dir.join("blobs.dat")).map(|m| m.len()).unwrap_or(0);
        Ok(CollectionStats {
            collection: collection.to_string(),
            doc_count,
            live_bytes: storage.inlined_bytes as u64,
            total_bytes,
            segment_count: storage.segment_count(),
            wal_bytes,
            blob_bytes,
            tombstone_count,
            unloaded,
        })
    }

    pub fn is_lazy_unloaded(&self, col: &str) -> bool {
        self.is_lazy_collection(col)
            && !self
                .shards
                .read()
                .map(|m| m.contains_key(col))
                .unwrap_or(true)
    }

    /// The one sync-withholding gate every transport, tailer, handshake
    /// and enumerator consults: static exclusions plus lazy-unloaded.
    /// (Handshake constructors keep seeding their static `excluded` sets
    /// from `is_sync_excluded_effective`; per-message checks use this.)
    pub fn is_sync_withheld(&self, col: &str) -> bool {
        self.is_sync_excluded_effective(col) || self.is_lazy_unloaded(col)
    }

    /// Archive-group collections present on disk but not loaded â€” the
    /// stats surface for "marked unloaded" (sync/backup consult the gate
    /// directly, not this listing).
    pub fn unloaded_lazy_collections(&self) -> Vec<String> {
        let mut out = Vec::new();
        let loaded = self.shards.read().map(|m| m.keys().cloned().collect::<std::collections::HashSet<_>>()).unwrap_or_default();
        if let Ok(entries) = std::fs::read_dir(&self.root_path) {
            for entry in entries.flatten() {
                let name = entry.file_name().to_string_lossy().to_string();
                if entry.path().is_dir()
                    && !loaded.contains(&name)
                    && self.is_lazy_collection(&name)
                {
                    out.push(name);
                }
            }
        }
        out.sort();
        out
    }

    /// Sync enumeration: EVERY collection on disk (including `_`-hidden
    /// ones) minus the withheld plane (static exclusions + lazy-unloaded).
    /// Sync is opt-out, not opt-in â€” `list_collections()` stays the
    /// user-visible listing with `_` hiding, but sync must never depend
    /// on naming conventions.
    pub fn sync_collections(&self) -> Result<Vec<String>> {
        let mut cols = Vec::new();
        if let Ok(entries) = std::fs::read_dir(&self.root_path) {
            for entry in entries.flatten() {
                if let Ok(meta) = entry.metadata() {
                    if meta.is_dir() {
                        let name = entry.file_name().to_string_lossy().to_string();
                        if name != "snapshots" && !self.is_sync_withheld(&name) {
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

    /// Read-only enforcement switch (hakocluster: replicas + failover).
    /// Raise the hybrid-clock watermark to a timestamp observed on the
    /// wire (issue #80). Called by the ingest paths for LWW-ACCEPTED ops
    /// only, so dropped stale ops can't drag the stamp forward. The chase
    /// clamps at a generous skew ceiling: a doc stamped beyond it still
    /// applies (content wins), but an absurd/attacker timestamp can't pin
    /// the watermark at i64::MAX â€” that would tie every later stamp and
    /// tombstone comparison into permanent drops.
    pub(crate) fn observe_remote_ts(&self, ts: i64) {
        const SKEW_CEILING_MICROS: i64 = 100 * 365 * 24 * 60 * 60 * 1_000_000; // 100y
        let ceiling = crate::util::clock::now_micros() + SKEW_CEILING_MICROS;
        let effective = ts.min(ceiling);
        let mut cur = self.last_ts_watermark.load(std::sync::atomic::Ordering::Relaxed);
        while effective > cur {
            match self.last_ts_watermark.compare_exchange_weak(
                cur,
                effective,
                std::sync::atomic::Ordering::Relaxed,
                std::sync::atomic::Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }

    /// Refuses local writes at the write_batch gate; replicated ingest
    /// and all reads are unaffected. Reversible.
    pub fn set_read_only(&self, ro: bool) {
        self.read_only.store(ro, std::sync::atomic::Ordering::Relaxed);
    }

    /// Read-only state (see [`Self::set_read_only`]).
    pub fn is_read_only(&self) -> bool {
        self.read_only.load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Live group-commit window in ms (Interval mode). Reads the atomic,
    /// so it reflects runtime changes (see setter), not just open-time
    /// config (observability for fleet stagger checks, e.g. hakocluster
    /// PerInstance verification).
    pub fn group_commit_interval_ms(&self) -> u64 {
        self.group_commit_interval_ms.load(Ordering::Relaxed)
    }

    /// Set the group-commit window live on every shard (admin plane for
    /// out-of-tree gateways, mirroring [`Self::set_durability_mode_all`]).
    /// Clamped like open-time config (1..=30000); arming/disarming
    /// strict-time follows automatically. Best-effort per shard.
    pub fn set_group_commit_interval_ms_all(&self, ms: u64) {
        let ms = crate::config::clamp_group_commit_interval_ms(ms);
        self.group_commit_interval_ms.store(ms, Ordering::Relaxed);
        for shard in self.shards.read().unwrap().values() {
            if let Ok(mut s) = shard.write() {
                s.set_group_commit_interval_ms(ms);
            }
        }
    }

    pub fn execute_aggregation(&self, query: Query) -> Result<HashMap<String, f64>> {
        if !self.allowed(&query.collection, AccessOp::Query) {
            return Err(self.deny(AccessOp::Query, &query.collection, None));
        }

        let Some(shard_arc) = self.try_get_shard_on_disk(&query.collection)? else { return Ok(HashMap::new()) };
        let indexes = self.indexes.read().unwrap();
        let rows = shard_arc
            .read()
            .unwrap()
            .count_prefix("");

        let is_ready = self.index_query_ready(&query.collection);
        let vec_ready = self.vector_query_ready();
        // FIX: Add self.config.query_workers as the 4th argument
        let plan = self.plan_cache.get_or_compute(&query, &indexes, rows, self.config.query_workers, is_ready, vec_ready);

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
                unique: idx.definition.unique,
                sparse: idx.definition.sparse,
                filter: idx.definition.filter.clone(),
            });
        }

        let mut vector: HashMap<String, Vec<PersistedVectorIndex>> = HashMap::new();
        for (col, fields_map) in &mgr.vector {
            let mut list: Vec<PersistedVectorIndex> = fields_map
                .iter()
                .map(|(f, d)| PersistedVectorIndex {
                    field: f.clone(),
                    dim: d.dim,
                    metric: d.metric.name().to_string(),
                    m: d.m,
                    ef_construction: d.ef_construction,
                    ef_search: d.ef_search,
                })
                .collect();
            list.sort_by(|a, b| a.field.cmp(&b.field));
            vector.insert(col.clone(), list);
        }

        let mut traverse: Vec<String> = mgr.traverse.tracked_cols();
        traverse.sort();

        let state = PersistedIndexState { secondary, fts, composite, vector, traverse };
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

        // 2b. Populate vector defs. Invalid entries (bad dim/metric from
        // a newer writer) are skipped, never fatal to open. Graphs are
        // ensured empty here; STEP C(2) fills them (rescan hook or
        // rebuild) â€” never trust a snapshot for derived ANN state.
        for (collection, defs) in state.vector {
            for def in defs {
                let metric = match def.metric.as_str() {
                    "l2" => crate::index::vector::Metric::L2,
                    "inner_product" => crate::index::vector::Metric::InnerProduct,
                    _ => crate::index::vector::Metric::Cosine,
                };
                let params = crate::index::vector::HnswParams {
                    m: def.m,
                    ef_construction: def.ef_construction,
                    ef_search: def.ef_search,
                };
                let _ = mgr.create_vector_index(&collection, &def.field, def.dim, metric, params);
                mgr.ensure_vector_graph(&collection, &def.field);
            }
        }

        // 2c. Populate traverse targets (issue #53). Backlink edges are
        // derived and rebuild in STEP C through the hook â€” only the
        // tracked set persists.
        for collection in state.traverse {
            mgr.traverse.track(&collection);
        }

        // 3. Populate Composite (Directly in manager)
        for def in state.composite {
            let fields: Vec<(String, SortDirection)> = def.fields.into_iter().map(|(field, dir)| {
                let direction = if dir.eq_ignore_ascii_case("desc") { 
                    SortDirection::Desc 
                } else { 
                    SortDirection::Asc 
                };
                (field, direction)
            }).collect();

            if !fields.is_empty() {
                let mut definition = crate::index::composite::definition::CompositeIndexDefinition::new(&def.collection)
                    .with_fields(fields)
                    .with_constraints(def.unique, def.sparse, def.filter);
                definition.id = def.id;
                mgr.composite.restore_index(definition);
            }
        }
        
        drop(mgr);
        self.gate_cache.write().unwrap().clear();

        // NO persist_index_defs() call here. We just loaded what was already on disk.
        Ok(())
    }

    // --- Per-collection schemas (hakodb#71/F8) ------------------------

    fn schemas_path(&self) -> PathBuf {
        self.root_path.join("_indices").join("schemas.json")
    }

    /// Declare (or replace) a collection schema. Validates the declaration
    /// itself; existing documents are NOT re-validated (enforcement starts
    /// with the next write — documented, same as unique indexes backfilled
    /// over historical duplicates). Empty `fields` clears the schema.
    pub fn set_schema(
        &self,
        collection: &str,
        schema: crate::schema::CollectionSchema,
    ) -> Result<()> {
        schema.validate()?;
        {
            let mut guard = self
                .schemas
                .write()
                .map_err(|_| HakoError::LockPoisoned("schemas".into()))?;
            if schema.fields.is_empty() {
                guard.remove(collection);
            } else {
                guard.insert(collection.to_string(), schema);
            }
            self.schema_enabled
                .store(!guard.is_empty(), std::sync::atomic::Ordering::Relaxed);
        }
        self.persist_schemas()
    }

    pub fn schema(&self, collection: &str) -> Option<crate::schema::CollectionSchema> {
        self.schemas.read().ok()?.get(collection).cloned()
    }

    pub fn clear_schema(&self, collection: &str) -> Result<()> {
        self.set_schema(collection, crate::schema::CollectionSchema::default())
    }

    pub fn list_schemas(&self) -> HashMap<String, crate::schema::CollectionSchema> {
        self.schemas.read().map(|g| g.clone()).unwrap_or_default()
    }

    fn persist_schemas(&self) -> Result<()> {
        let path = self.schemas_path();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let map = self.list_schemas();
        let json = serde_json::to_vec_pretty(&map)
            .map_err(|e| HakoError::Corrupt(e.to_string()))?;
        std::fs::write(path, json)?;
        Ok(())
    }

    fn restore_schemas(&self) -> Result<()> {
        let path = self.schemas_path();
        if !path.exists() {
            return Ok(());
        }
        let data = std::fs::read(path)?;
        // A malformed file must not brick the store: schemas are defs, and
        // silently dropping them would be worse — surface as Corrupt and
        // let the caller decide (open() logs it best-effort).
        let map: HashMap<String, crate::schema::CollectionSchema> = serde_json::from_slice(&data)
            .map_err(|e| HakoError::Corrupt(format!("schemas.json: {e}")))?;
        let enabled = !map.is_empty();
        *self
            .schemas
            .write()
            .map_err(|_| HakoError::LockPoisoned("schemas".into()))? = map;
        self.schema_enabled
            .store(enabled, std::sync::atomic::Ordering::Relaxed);
        Ok(())
    }

    /// Admission-gate hook: apply defaults + validate declared fields.
    /// No-op (no lock) when no schema exists anywhere.
    fn apply_schema(&self, collection: &str, doc: &mut HakoDoc) -> Result<()> {
        if !self.schema_enabled.load(std::sync::atomic::Ordering::Relaxed) {
            return Ok(());
        }
        let schema = self
            .schemas
            .read()
            .map_err(|_| HakoError::LockPoisoned("schemas".into()))?
            .get(collection)
            .cloned();
        match schema {
            Some(s) => s.apply(collection, doc),
            None => Ok(()),
        }
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
                if target != idx.definition.collection { continue; }
            }
            composite.push(CompositeIndexInfo {
                id: idx.definition.id,
                collection: idx.definition.collection.clone(),
                unique: idx.definition.unique,
                sparse: idx.definition.sparse,
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

        let mut vector: HashMap<String, Vec<VectorIndexInfo>> = HashMap::new();
        for (col, fields_map) in &mgr.vector {
            if let Some(target) = collection {
                if target != col { continue; }
            }
            let mut infos: Vec<VectorIndexInfo> = fields_map
                .iter()
                .map(|(f, d)| {
                    let graph = mgr.hnsw.get(col).and_then(|m| m.get(f));
                    VectorIndexInfo {
                        field: f.clone(),
                        dim: d.dim,
                        metric: d.metric.name().to_string(),
                        m: d.m,
                        ef_construction: d.ef_construction,
                        ef_search: d.ef_search,
                        indexed: graph.map(|g| g.len()).unwrap_or(0),
                        complete: graph.is_some_and(|g| g.complete() && !g.is_empty()),
                        mem_estimate: graph.map(|g| g.bytes_estimate()).unwrap_or(0),
                    }
                })
                .collect();
            infos.sort_by(|a, b| a.field.cmp(&b.field));
            vector.insert(col.clone(), infos);
        }

        let mut traverse: Vec<String> = mgr
            .traverse
            .tracked_cols()
            .into_iter()
            .filter(|c| collection.is_none_or(|t| t == c))
            .collect();
        traverse.sort();

        IndexList {
            simple: secondary.clone(),
            secondary,
            fts,
            composite,
            vector,
            traverse,
        }
    }

    /// Drop one index definition. Idempotent: an absent target is a no-op.
    ///
    /// Definitions are local metadata. On a synced deployment, dropping on
    /// one peer is not enough: another peer's additive def sync can
    /// recreate it, so drop on every member (def tombstones are a separate
    /// follow-up, hakodb#115).
    pub fn drop_index(&self, collection: &str, field: &str) -> Result<()> {
        self.ensure_index_mutation_allowed()?;
        let removed = self
            .indexes
            .write()
            .unwrap()
            .drop_secondary_index(collection, field);
        if removed {
            self.finish_index_drop()?;
        }
        Ok(())
    }

    pub fn drop_fts_index(&self, collection: &str, field: &str) -> Result<()> {
        self.ensure_index_mutation_allowed()?;
        let removed = self
            .indexes
            .write()
            .unwrap()
            .drop_fts_index(collection, field);
        if removed {
            self.finish_index_drop()?;
        }
        Ok(())
    }

    pub fn drop_vector_index(&self, collection: &str, field: &str) -> Result<()> {
        self.ensure_index_mutation_allowed()?;
        let removed = self
            .indexes
            .write()
            .unwrap()
            .drop_vector_index(collection, field);
        if removed {
            self.finish_index_drop()?;
        }
        Ok(())
    }

    /// Drop a composite index by id. The collection must match the index's
    /// owning collection; a mismatched id returns `InvalidInput` instead of
    /// silently deleting another collection's index. An absent id is a
    /// no-op (idempotent), matching every other drop.
    pub fn drop_composite_index(&self, collection: &str, id: u32) -> Result<()> {
        self.ensure_index_mutation_allowed()?;
        let unique = {
            let mgr = self.indexes.read().unwrap();
            match mgr.composite.get(id) {
                Some(idx) if idx.definition.collection == collection => Some(idx.definition.unique),
                Some(_) => {
                    return Err(HakoError::InvalidInput(format!(
                        "composite index {id} does not belong to `{collection}`"
                    )))
                }
                None => None,
            }
        };
        if let Some(unique) = unique {
            self.indexes.write().unwrap().composite.drop_index(id);
            if unique {
                self.unique_ledger.reset(id);
            }
            self.finish_index_drop()?;
        }
        Ok(())
    }

    pub fn drop_traverse_index(&self, collection: &str) -> Result<()> {
        self.ensure_index_mutation_allowed()?;
        let removed = self
            .indexes
            .write()
            .unwrap()
            .drop_traverse_index(collection);
        if removed {
            self.finish_index_drop()?;
        }
        Ok(())
    }

    /// Drop an edge index: its traverse tracking plus the `_from`/`_to`
    /// secondary indexes that `create_edge_index` owns.
    pub fn drop_edge_index(&self, collection: &str) -> Result<()> {
        self.ensure_index_mutation_allowed()?;
        let removed = {
            let mut mgr = self.indexes.write().unwrap();
            let from = mgr.drop_secondary_index(collection, "_from");
            let to = mgr.drop_secondary_index(collection, "_to");
            let tracked = mgr.drop_traverse_index(collection);
            from || to || tracked
        };
        if removed {
            self.finish_index_drop()?;
        }
        Ok(())
    }

    fn ensure_index_mutation_allowed(&self) -> Result<()> {
        if self.read_only.load(std::sync::atomic::Ordering::Relaxed) {
            return Err(HakoError::Corrupt("ReadOnly".into()));
        }
        Ok(())
    }

    fn finish_index_drop(&self) -> Result<()> {
        self.gate_cache.write().unwrap().clear();
        self.plan_cache.invalidate();
        self.persist_index_defs()
    }

    pub fn create_index(&self, collection: &str, field: &str) -> Result<()> {
        // 1. Check if it exists
        {
            let mgr = self.indexes.read().unwrap();
            if mgr
                .secondary
                .get(collection)
                .is_some_and(|m| m.contains_key(field))
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
            // ponytail: selective decode â€” a new index only needs the
            // registered fields (same entries, no value allocs for rest).
            let wanted: Vec<String> = idx_mgr
                .read()
                .map(|mgr| Self::indexed_fields_for(&mgr, &col_name))
                .unwrap_or_default();
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
                    if let Some(doc) = HakoDoc::decode_projected(&bytes, &wanted) {
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
            // ponytail: selective decode (see create_index above).
            let wanted: Vec<String> = idx_mgr
                .read()
                .map(|mgr| Self::indexed_fields_for(&mgr, &col_name))
                .unwrap_or_default();
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
                    if let Some(mut doc) = HakoDoc::decode_projected(&bytes, &wanted) {
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

    /// Register a vector index on `field`. Registers the def, ensures an
    /// (empty, incomplete) HNSW graph, and spawns the backfill that fills
    /// it â€” queries fail-closed to exact until `complete` flips.
    /// Re-registering overwrites dim/metric and rebuilds the graph.
    pub fn create_vector_index(
        &self,
        collection: &str,
        field: &str,
        dim: u32,
        metric: crate::index::vector::Metric,
    ) -> Result<()> {
        self.create_vector_index_with_params(collection, field, dim, metric, Default::default())
    }

    /// Vector index with explicit HNSW parameters (#141-1): `None` fields
    /// ride the per-dim derived defaults. Parameters persist in the index
    /// definitions (they sync like the rest of the def) and survive reopen.
    pub fn create_vector_index_with_params(
        &self,
        collection: &str,
        field: &str,
        dim: u32,
        metric: crate::index::vector::Metric,
        params: crate::index::vector::HnswParams,
    ) -> Result<()> {
        self.indexes
            .write()
            .unwrap()
            .create_vector_index(collection, field, dim, metric, params)?;
        {
            if let Ok(mut mgr) = self.indexes.write() {
                mgr.ensure_vector_graph(collection, field);
            }
        }

        let shard_arc = self.get_shard(collection)?;
        Self::spawn_vector_backfill(
            Arc::clone(&self.indexes),
            Arc::clone(&self.backfill_inflight),
            self.config.encryption_key.clone(),
            shard_arc,
            collection.to_string(),
        );

        let _ = self.persist_index_defs();
        self.plan_cache.invalidate();
        Ok(())
    }

    /// Shared vector backfill spawner: full HNSW (re)build for one
    /// collection. Used at explicit creation AND at open for collections
    /// whose rescan was skipped (fresh snapshot carries no HNSW bytes in
    /// P1). Same chunks/yield discipline as the FTS spawner; runs under
    /// `backfill_inflight` so queries fail-closed meanwhile.
    fn spawn_vector_backfill(
        idx_mgr: Arc<RwLock<IndexManager>>,
        backfill_count: Arc<AtomicUsize>,
        enc_key: Option<String>,
        shard_arc: Arc<RwLock<StorageEngine>>,
        col_name: String,
    ) {
        backfill_count.fetch_add(1, Ordering::Relaxed);
        thread::spawn(move || {
            let _guard = BackfillGuard { counter: backfill_count };
            // Graphs may predate this pass (re-registration) â€” reset to
            // incomplete so a concurrent query can't ANN-serve a half
            // graph; `complete` flips only after the full pass below.
            {
                if let Ok(mut mgr) = idx_mgr.write() {
                    if let Some(graphs) = mgr.hnsw.get_mut(&col_name) {
                        for graph in graphs.values_mut() {
                            graph.clear();
                        }
                    }
                    for field in mgr
                        .vector
                        .get(&col_name)
                        .map(|m| m.keys().cloned().collect::<Vec<_>>())
                        .unwrap_or_default()
                    {
                        mgr.ensure_vector_graph(&col_name, &field);
                    }
                }
            }
            let pointers = {
                shard_arc.read().unwrap().get_physical_index_snapshot()
            };
            let wanted: Vec<String> = idx_mgr
                .read()
                .map(|mgr| Self::indexed_fields_for(&mgr, &col_name))
                .unwrap_or_default();
            for chunk in pointers.chunks(100) {
                let mut resolved_docs = Vec::new();
                {
                    let storage = shard_arc.read().unwrap();
                    for (key, ptr) in chunk {
                        // Ghost guard: deleted docs must not enter the
                        // graph (liveness filters them at query, but they
                        // would still warp routing + waste RAM).
                        if matches!(ptr, Pointer::Deleted { .. }) {
                            continue;
                        }
                        if let Ok(Some(bytes)) = storage.read_pointer_internal(ptr, false) {
                            resolved_docs.push((key.clone(), bytes));
                        }
                    }
                }
                let mut mgr = idx_mgr.write().unwrap();
                for (doc_id, bytes) in resolved_docs {
                    if let Some(mut doc) = HakoDoc::decode_projected(&bytes, &wanted) {
                        let _ = resolve_doc_static(&mut doc, &shard_arc, enc_key.as_deref());
                        mgr.index_vector(&col_name, &doc_id, &doc);
                    }
                }
            }
            if let Ok(mut mgr) = idx_mgr.write() {
                mgr.complete_vector(&col_name);
            }
        });
    }

    /// Nearest-first KNN over a vector index (P0 exact brute-force). k
    /// rides the query limit. Dim mismatch against the definition is a
    /// hard error (never silent truncation); querying an unindexed field
    /// returns empty (fail-closed, like the planner gate).
    pub fn find_near(
        &self,
        collection: &str,
        field: &str,
        query: &[f32],
        k: usize,
    ) -> Result<Vec<(String, HakoDoc)>> {
        let dim = {
            let mgr = self.indexes.read().unwrap();
            match mgr.vector.get(collection).and_then(|m| m.get(field)) {
                Some(def) => def.dim as usize,
                None => return Ok(Vec::new()),
            }
        };
        if query.len() != dim {
            return Err(HakoError::Corrupt(format!(
                "find_near dim mismatch: query {} vs index {dim} on {collection}.{field}",
                query.len()
            )));
        }
        let bytes = crate::index::vector::encode_f32s(query);
        let q = Query::new(collection)
            .where_filter(field, crate::query::filter::Operator::Near, Value::Binary(bytes))
            .limit(k);
        self.query(q)
    }

    /// Scored nearest-neighbor search (#141-3): the same candidates as
    /// [`Self::find_near`], each rescored exactly against the stored vector
    /// — the same `distance()` the FFI rescore uses, O(k·D) after the ANN
    /// pass. Score = distance (cosine distance or squared L2), **lower is
    /// closer**. `max_distance` keeps only hits at or under the bound
    /// (#141-11); `None` returns everything the search found. Unindexed
    /// field → empty vec (mirrors `find_near`'s fail-closed behavior).
    pub fn find_near_scored(
        &self,
        collection: &str,
        field: &str,
        query: &[f32],
        k: usize,
        max_distance: Option<f64>,
    ) -> Result<Vec<(String, f64, HakoDoc)>> {
        let Some(metric) = self.vector_metric(collection, field) else {
            return Ok(Vec::new());
        };
        let rows = self.find_near(collection, field, query, k)?;
        let mut out = Vec::with_capacity(rows.len());
        for (id, doc) in rows {
            let score = match doc.get(field) {
                Some(Value::Binary(b)) => crate::index::vector::decode_f32s(b)
                    .ok()
                    .filter(|stored| stored.len() == query.len())
                    .map_or(f64::INFINITY, |stored| {
                        crate::index::vector::distance(metric, query, &stored)
                    }),
                _ => f64::INFINITY,
            };
            if let Some(max) = max_distance {
                if score > max {
                    continue;
                }
            }
            out.push((id, score, doc));
        }
        Ok(out)
    }

    /// Batched nearest-neighbor search (#141-5): one readiness/wait cycle
    /// for many query vectors. Per-query results match `find_near` 1:1.
    pub fn find_near_batch(
        &self,
        collection: &str,
        field: &str,
        queries: &[&[f32]],
        k: usize,
    ) -> Result<Vec<Vec<(String, HakoDoc)>>> {
        queries
            .iter()
            .map(|q| self.find_near(collection, field, q, k))
            .collect()
    }

    /// Operational visibility for ANN state: true once the HNSW graph for
    /// (collection, field) completed a full pass and may serve queries.
    /// Tests poll this (with quiescence) to distinguish the ANN path from
    /// the exact fallback; operators can gate dashboards on it.
    pub fn vector_index_complete(&self, collection: &str, field: &str) -> bool {
        self.indexes.read().is_ok_and(|mgr| {
            mgr.hnsw
                .get(collection)
                .and_then(|m| m.get(field))
                .is_some_and(|g| g.complete() && !g.is_empty())
        })
    }

    /// Metric of a registered vector index, for FFI score recompute
    /// (`hk_engine_find_near`): the query pipeline drops scores, so the
    /// FFI layer recomputes the exact distance per hit from the stored
    /// document vector â€” same `distance()` the rescore uses, O(kÂ·D).
    pub fn vector_metric(
        &self,
        collection: &str,
        field: &str,
    ) -> Option<crate::index::vector::Metric> {
        self.indexes.read().ok().and_then(|mgr| {
            mgr.vector
                .get(collection)
                .and_then(|m| m.get(field))
                .map(|def| def.metric)
        })
    }

    /// Register an edge collection (issue #53, Phase B): tracks the
    /// collection for inbound lookup AND indexes `_from`/`_to` for edge
    /// lookup â€” all three ride existing machinery (traverse defs, plain
    /// secondary indexes), so sync + persistence need no new shapes.
    /// Idempotent like every other create.
    pub fn create_edge_index(&self, edge_collection: &str) -> Result<()> {
        self.create_traverse_index(edge_collection)?;
        self.create_index(edge_collection, "_from")?;
        self.create_index(edge_collection, "_to")?;
        Ok(())
    }

    /// Opt a collection in as a traversal target (issue #53, Phase A):
    /// inbound edges to its docs are tracked from here on (live writes
    /// via the hook, existing docs via the backfill below). Idempotent
    /// (secondary norm â€” re-create is a no-op, never a backfill storm).
    /// Forward traversal needs no state and works regardless; incoming
    /// into an untracked collection is a loud error at query time.
    pub fn create_traverse_index(&self, collection: &str) -> Result<()> {
        {
            let mgr = self.indexes.read().unwrap();
            if mgr.traverse.is_tracked(collection) {
                return Ok(());
            }
        }
        {
            self.indexes.write().unwrap().traverse.track(collection);
        }

        // Backfill existing docs (mirror create_fts): snapshot pointers,
        // full-decode (refs live in any field), hook-feed in chunks.
        let shard_arc = self.get_shard(collection)?;
        let idx_mgr = Arc::clone(&self.indexes);
        let col_name = collection.to_string();
        let enc_key = self.config.encryption_key.clone();
        let backfill_count = Arc::clone(&self.backfill_inflight);
        backfill_count.fetch_add(1, Ordering::Relaxed);
        thread::spawn(move || {
            let _guard = BackfillGuard { counter: backfill_count };
            let pointers = {
                let storage = shard_arc.read().unwrap();
                storage.get_physical_index_snapshot()
            };
            for chunk in pointers.chunks(100) {
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
                for (full_key, bytes) in resolved_docs {
                    if let Some(mut doc) = HakoDoc::decode(&bytes) {
                        let _ = resolve_doc_static(&mut doc, &shard_arc, enc_key.as_deref());
                        mgr.traverse.index_doc(&col_name, &full_key, &doc);
                    }
                }
            }
        });

        let _ = self.persist_index_defs();
        self.plan_cache.invalidate();
        Ok(())
    }

    /// One expansion step shared by BFS and Dijkstra (issue #53): live
    /// outgoing refs (weight 1.0), edge-collection hops in `via_edge`
    /// (weight from the edge doc), verified backlinks (weight 1.0), and
    /// edge-collection reverse hops. Sorted for determinism; NOT deduped
    /// (visited/dist maps own that â€” Dijkstra must see every route).
    /// Stale entries (overwrites, secondary over-approximation) are
    /// re-verified against storage before following.
    fn expand_neighbors(
        &self,
        col: &str,
        doc_id: &str,
        spec: &TraverseSpec,
    ) -> Vec<(String, String, f64, Option<String>)> {
        // 4th element (#156 Phase F-1): the edge doc that connects the hop,
        // `"edge_col/id"`, `None` for inline-reference hops.
        let mut next: Vec<(String, String, f64, Option<String>)> = Vec::new();
        if spec.direction.uses_outgoing() {
            if let Ok(Some(doc)) = self.get(col, doc_id) {
                for (field, value) in doc.fields.iter() {
                    if let Some(f) = spec.edge_field.as_deref() {
                        if &**field != f {
                            continue;
                        }
                    }
                    if let Value::Reference { collection: tgt, doc_id: tid } = value {
                        next.push((tgt.clone(), tid.clone(), 1.0, None));
                    }
                }
            }
            if let Some(edge_col) = spec.via_edge.as_deref() {
                next.extend(self.edge_step(edge_col, col, doc_id, true, spec));
            }
        }
        if spec.direction.uses_incoming() {
            let edges: Vec<(String, String, String)> = self
                .indexes
                .read()
                .unwrap()
                .traverse
                .inbound(col, doc_id)
                .map(|v| {
                    v.iter()
                        .filter(|e| spec.edge_field.as_deref().is_none_or(|f| e.field == f))
                        .map(|e| (e.src_col.clone(), e.src_id.clone(), e.field.clone()))
                        .collect()
                })
                .unwrap_or_default();
            for (scol, sid, sfield) in edges {
                let live = self.get(&scol, &sid).ok().flatten().is_some_and(|d| {
                    d.get(&sfield).is_some_and(|v| {
                        matches!(v, Value::Reference { collection: c, doc_id: t } if c == col && t == doc_id)
                    })
                });
                if live {
                    next.push((scol, sid, 1.0, None));
                }
            }
            if let Some(edge_col) = spec.via_edge.as_deref() {
                next.extend(self.edge_step(edge_col, col, doc_id, false, spec));
            }
        }
        next.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then_with(|| a.1.cmp(&b.1))
                .then_with(|| a.3.cmp(&b.3))
        });
        next
    }

    /// One edge-collection hop (Phase B): edge docs in `edge_col` whose
    /// `match_field` (`_from` outgoing / `_to` incoming) references the
    /// anchor; the neighbor is the other end, weighted by `weight_field`
    /// (missing/non-numeric/non-finite â†’ 1.0, negative â†’ clamped 0 â€”
    /// documented, never silent-skip). Secondary lookup + verify, same
    /// tolerance as backlinks.
    /// One edge-collection hop (Phase B / #156 Phase F-1): edge docs in
    /// `edge_col` whose `match_field` (`_from` outgoing / `_to` incoming)
    /// references the anchor; the neighbor is the other end, weighted by
    /// `spec.weight_field` (missing/non-numeric/non-finite → 1.0, negative
    /// → clamped 0 — documented, never silent-skip). Secondary lookup +
    /// verify, same tolerance as backlinks. F-1: edge docs must satisfy
    /// every set field predicate (`edge_type` / `edge_match`); the
    /// connecting edge doc's `"edge_col/id"` rides the 4th element for
    /// result projection (`via`).
    fn edge_step(
        &self,
        edge_col: &str,
        anchor_col: &str,
        anchor_id: &str,
        forward: bool,
        spec: &TraverseSpec,
    ) -> Vec<(String, String, f64, Option<String>)> {
        let (match_field, other_field) = if forward { ("_from", "_to") } else { ("_to", "_from") };
        let key = crate::index::index_key::encode_scalar(&Value::Reference {
            collection: anchor_col.to_string(),
            doc_id: anchor_id.to_string(),
        });
        let ids: Vec<String> = self
            .indexes
            .read()
            .ok()
            .and_then(|mgr| mgr.lookup_secondary(edge_col, match_field, &key))
            .map(|v| v.into_iter().map(|s| s.to_string()).collect())
            .unwrap_or_default();
        let mut out = Vec::new();
        for eid in ids {
            let Ok(Some(edoc)) = self.get(edge_col, &eid) else {
                continue;
            };
            let linked = edoc.get(match_field).is_some_and(|v| {
                matches!(v, Value::Reference { collection: c, doc_id: t } if c == anchor_col && t == anchor_id)
            });
            if !linked {
                continue;
            }
            if !Self::edge_doc_matches(spec, &edoc) {
                continue;
            }
            if let Some(Value::Reference { collection: oc, doc_id: oid }) = edoc.get(other_field) {
                let w = spec
                    .weight_field
                    .as_deref()
                    .and_then(|wf| edoc.get(wf))
                    .and_then(|v| match v {
                        Value::Int(i) => Some(*i as f64),
                        Value::Float(f) => Some(*f),
                        _ => None,
                    })
                    .filter(|w| w.is_finite())
                    .map(|w| w.max(0.0))
                    .unwrap_or(1.0);
                out.push((oc.clone(), oid.clone(), w, Some(format!("{edge_col}/{eid}"))));
            }
        }
        out
    }

    /// F-1 edge predicates (#140-7): every set predicate must hold. One
    /// compare per predicate — the edge doc is already decoded for the
    /// liveness verify, so this costs nothing extra. Inline-ref hops have
    /// no edge doc: they never reach here (exempt by construction).
    fn edge_doc_matches(spec: &TraverseSpec, edoc: &HakoDoc) -> bool {
        [&spec.edge_type, &spec.edge_match]
            .into_iter()
            .all(|p| match p {
                Some((field, want)) => edoc.get(field.as_str()) == Some(want),
                None => true,
            })
    }

    /// BFS traversal from one anchor (issue #53, Phase A). Outgoing
    /// follows live ref fields; incoming consults the backlink map
    /// (tracked targets only) with stale edges re-verified against
    /// storage. Missing anchor = empty (fail-closed); incoming into an
    /// untracked collection = hard error (loud, not silent); depth/limit
    /// validated, never clamped. Level order, sorted within a level
    /// (deterministic across runs); stops at the first `limit` hits.
    pub fn find_paths(
        &self,
        collection: &str,
        id: &str,
        spec: &TraverseSpec,
    ) -> Result<Vec<TraverseHit>> {
        spec.validate()?;
        self.ensure_incoming_state(collection, spec)?;
        if self.get(collection, id)?.is_none() {
            return Ok(Vec::new());
        }
        // Weighted walks ride Dijkstra (Phase B); unweighted stay on the
        // proven BFS below (untouched â€” its tests pin the behavior).
        if spec.weight_field.is_some() {
            return self.find_paths_weighted(collection, id, spec);
        }
        let start = (collection.to_string(), id.to_string());
        let mut visited: HashSet<(String, String)> = HashSet::new();
        visited.insert(start.clone());
        // First-visit parent map (#140-1): BFS discovery order IS the
        // shortest-hop path, so walking parents back reconstructs it.
        let mut parent: HashMap<(String, String), (String, String)> = HashMap::new();
        // G1 (#168): the discovery edge rides per node only when the via
        // projection is requested — otherwise the walk pays no extra map
        // writes and hits carry an empty `via`.
        let mut parent_edge: HashMap<(String, String), Option<String>> = HashMap::new();
        let want_via = spec.include_via;
        let mut queue: VecDeque<((String, String), usize)> = VecDeque::new();
        queue.push_back((start.clone(), 0));
        let mut out = Vec::new();
        'bfs: while let Some(((col, doc_id), depth)) = queue.pop_front() {
            if depth >= spec.max_depth {
                continue;
            }
            // Filtered traversal (#156 Phase C): only matching nodes are
            // visited or expanded — non-matching neighbors are pruned (the
            // walk cannot route through filtered territory). The start
            // node is always expanded, so only neighbors get filtered here.
            let next: Vec<(String, String, Option<String>)> = self
                .expand_neighbors(&col, &doc_id, spec)
                .into_iter()
                .filter(|(ncol, nid, _, _)| self.node_matches(spec, ncol, nid))
                .map(|(a, b, _, e)| (a, b, e))
                .collect();
            for (ncol, nid, edge) in next {
                if !visited.insert((ncol.clone(), nid.clone())) {
                    continue;
                }
                parent.insert((ncol.clone(), nid.clone()), (col.clone(), doc_id.clone()));
                if want_via {
                    parent_edge.insert((ncol.clone(), nid.clone()), edge);
                }
                let nd = depth + 1;
                if nd < spec.max_depth {
                    queue.push_back(((ncol.clone(), nid.clone()), nd));
                }
                if out.len() >= spec.limit {
                    break 'bfs;
                }
                if let Ok(Some(doc)) = self.get(&ncol, &nid) {
                    let path = Self::reconstruct_traverse_path(&parent, &start, (&ncol, &nid));
                    let via = if want_via {
                        Self::reconstruct_traverse_via(&parent, &parent_edge, &start, (&ncol, &nid))
                    } else {
                        Vec::new()
                    };
                    out.push(TraverseHit { collection: ncol, id: nid, depth: nd, cost: nd as f64, path, via, doc });
                }
            }
        }
        Ok(out)
    }

    /// Walk the first-visit parent chain back to the start (#140-1).
    /// `parent` covers every node the walk discovered, so the chain always
    /// reaches `start`; the `None` arm is defensive only.
    fn reconstruct_traverse_path(
        parent: &HashMap<(String, String), (String, String)>,
        start: &(String, String),
        node: (&str, &str),
    ) -> Vec<(String, String)> {
        let mut chain = vec![(node.0.to_string(), node.1.to_string())];
        let mut cur = (node.0.to_string(), node.1.to_string());
        while cur != *start {
            match parent.get(&cur) {
                Some(p) => {
                    chain.push(p.clone());
                    cur = p.clone();
                }
                None => break,
            }
        }
        chain.reverse();
        chain
    }

    /// Walk the discovery edge chain back to the start (#156 Phase F-1).
    /// `parent[node]` guides the walk cur-to-start; `parent_edge[node]` =
    /// the edge doc that connected the parent to it, recorded at first
    /// visit. Collected cur-to-start, then reversed so `via[i]` matches
    /// hop `path[i] -> path[i+1]`. Both maps cover every discovered node;
    /// the `None` arm is defensive only (mirrors the path twin).
    fn reconstruct_traverse_via(
        parent: &HashMap<(String, String), (String, String)>,
        parent_edge: &HashMap<(String, String), Option<String>>,
        start: &(String, String),
        node: (&str, &str),
    ) -> Vec<Option<String>> {
        let mut chain = Vec::new();
        let mut cur = (node.0.to_string(), node.1.to_string());
        while cur != *start {
            chain.push(parent_edge.get(&cur).cloned().unwrap_or(None));
            match parent.get(&cur) {
                Some(p) => cur = p.clone(),
                None => break,
            }
        }
        chain.reverse();
        chain
    }

    /// Dijkstra over the same expansion as BFS (issue #53, Phase B):
    /// cost-ordered cheapest paths, same depth cap / limit / liveness /
    /// determinism rules. Twin of the BFS loop above (kept separate so
    /// the proven path stays byte-identical â€” see expand_neighbors note
    /// if they ever diverge).
    fn find_paths_weighted(
        &self,
        collection: &str,
        id: &str,
        spec: &TraverseSpec,
    ) -> Result<Vec<TraverseHit>> {
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let mut dist: HashMap<(String, String), f64> = HashMap::new();
        #[allow(clippy::type_complexity)]
        let mut heap: BinaryHeap<(Reverse<(u64, String, String)>, usize)> = BinaryHeap::new();
        let start = (collection.to_string(), id.to_string());
        let mut parent: HashMap<(String, String), (String, String)> = HashMap::new();
        // G1 (#168): discovery edge recorded on relax only when via is wanted.
        let want_via = spec.include_via;
        let mut parent_edge: HashMap<(String, String), Option<String>> = HashMap::new();
        dist.insert(start.clone(), 0.0);
        heap.push((Reverse((0, collection.to_string(), id.to_string())), 0));
        let mut out = Vec::new();
        while let Some((Reverse((cbits, col, doc_id)), depth)) = heap.pop() {
            let key = (col.clone(), doc_id.clone());
            if cbits > dist.get(&key).copied().unwrap_or(f64::INFINITY).to_bits() {
                continue;
            }
            if depth >= 1 {
                if out.len() >= spec.limit {
                    break;
                }
                if let Ok(Some(doc)) = self.get(&col, &doc_id) {
                    let path = Self::reconstruct_traverse_path(&parent, &start, (&col, &doc_id));
                    let via = if want_via {
                        Self::reconstruct_traverse_via(&parent, &parent_edge, &start, (&col, &doc_id))
                    } else {
                        Vec::new()
                    };
                    out.push(TraverseHit {
                        collection: col.clone(),
                        id: doc_id.clone(),
                        depth,
                        cost: f64::from_bits(cbits),
                        path,
                        via,
                        doc,
                    });
                }
            }
            if depth >= spec.max_depth {
                continue;
            }
            for (ncol, nid, w, edge) in self.expand_neighbors(&col, &doc_id, spec) {
                // Filtered traversal (#156 Phase C): prune non-matching
                // neighbors before relaxing (same semantics as the BFS).
                if !self.node_matches(spec, &ncol, &nid) {
                    continue;
                }
                let nc = f64::from_bits(cbits) + w;
                let nkey = (ncol.clone(), nid.clone());
                if nc < dist.get(&nkey).copied().unwrap_or(f64::INFINITY) {
                    // Relaxation improved (#140-1): this node's best-known
                    // path now comes through the current node.
                    parent.insert(nkey.clone(), (col.clone(), doc_id.clone()));
                    if want_via {
                        parent_edge.insert(nkey.clone(), edge);
                    }
                    dist.insert(nkey, nc);
                    heap.push((Reverse((nc.to_bits(), ncol, nid)), depth + 1));
                }
            }
        }
        Ok(out)
    }

    /// Incoming traversal needs reverse state: tracked anchor (backlinks)
    /// OR a registered edge collection (secondary `_from`/`_to`). Neither =
    /// loud error (a silent empty walk hides a forgotten create). Shared by
    /// `find_paths` and `find_simple_paths` (#156 Phase C).
    fn ensure_incoming_state(
        &self,
        collection: &str,
        spec: &TraverseSpec,
    ) -> Result<()> {
        if !spec.direction.uses_incoming() {
            return Ok(());
        }
        let (tracked, via_ok) = self
            .indexes
            .read()
            .map(|mgr| {
                let tracked = mgr.traverse.is_tracked(collection);
                let via_ok = spec.via_edge.as_deref().is_some_and(|e| {
                    mgr.secondary.get(e).is_some_and(|m| {
                        m.contains_key("_from") && m.contains_key("_to")
                    })
                });
                (tracked, via_ok)
            })
            .unwrap_or((false, false));
        if !tracked && !via_ok {
            return Err(HakoError::Corrupt(format!(
                "traverse incoming needs reverse state: create_traverse_index({collection}) or create_edge_index(<edges>) first"
            )));
        }
        Ok(())
    }

    /// Bounded simple-path enumeration (#156 Phase C): best-first (UCS)
    /// over distinct simple paths — no node repeats within one path. Every
    /// popped state is a result; results come out cost-ordered (hop count
    /// without `weight_field`, weight sum with) until `spec.limit` serves.
    /// Routes, not node hits: two routes to the same node are two results,
    /// and shared prefixes are emitted as separate results (a→b and a→b→c).
    /// Tie-break is the total heap order (cost bits, endpoint, whole path)
    /// — deterministic. Filtered specs prune exactly like `find_paths`.
    /// Memory is O(states) = O(limit × fan-out), each state carrying its
    /// path — capped by `limit`, not by graph size.
    pub fn find_simple_paths(
        &self,
        collection: &str,
        id: &str,
        spec: &TraverseSpec,
    ) -> Result<Vec<TraverseHit>> {
        spec.validate()?;
        self.ensure_incoming_state(collection, spec)?;
        if self.get(collection, id)?.is_none() {
            return Ok(Vec::new());
        }
        use std::cmp::Reverse;
        use std::collections::BinaryHeap;
        let start = (collection.to_string(), id.to_string());
        // (cost bits, endpoint, whole path, whole via) — total order =
        // determinism; `via` (F-1 #140-7) rides the tail so identical node
        // paths reached over different edge docs stay ordered.
        type SimpleState = (u64, String, String, Vec<(String, String)>, Vec<Option<String>>);
        let mut heap: BinaryHeap<Reverse<SimpleState>> = BinaryHeap::new();
        heap.push(Reverse((
            0,
            start.0.clone(),
            start.1.clone(),
            vec![start.clone()],
            Vec::new(),
        )));
        let mut out = Vec::new();
        while let Some(Reverse((cbits, ecol, eid, path, via))) = heap.pop() {
            if out.len() >= spec.limit {
                break;
            }
            let depth = path.len() - 1;
            if let Ok(Some(doc)) = self.get(&ecol, &eid) {
                out.push(TraverseHit {
                    collection: ecol.clone(),
                    id: eid.clone(),
                    depth,
                    cost: f64::from_bits(cbits),
                    path: path.clone(),
                    via: via.clone(),
                    doc,
                });
            }
            if depth >= spec.max_depth {
                continue;
            }
            for (ncol, nid, w, edge) in self.expand_neighbors(&ecol, &eid, spec) {
                // Simple + filtered: skip repeats within the path, prune
                // non-matching nodes (same semantics as the walks above).
                if !self.node_matches(spec, &ncol, &nid) {
                    continue;
                }
                if path.iter().any(|(c, i)| c == &ncol && i == &nid) {
                    continue;
                }
                let mut np = path.clone();
                np.push((ncol.clone(), nid.clone()));
                // G1 (#168): via rides states only when requested; empty
                // `Vec` clones stay allocation-free otherwise.
                let mut nv = if spec.include_via { via.clone() } else { Vec::new() };
                if spec.include_via {
                    nv.push(edge);
                }
                heap.push(Reverse((
                    (f64::from_bits(cbits) + w).to_bits(),
                    ncol.clone(),
                    nid.clone(),
                    np,
                    nv,
                )));
            }
        }
        Ok(out)
    }

    /// Filtered-traversal predicate (#156 Phase C): `None` matches all;
    /// `Some((field, value))` requires the node's field to equal `value`
    /// (`Value::PartialEq` is cross-type numeric aware). One `get` per
    /// candidate neighbor — the walk already pays that shape elsewhere.
    fn node_matches(&self, spec: &TraverseSpec, col: &str, id: &str) -> bool {
        match &spec.node_match {
            None => true,
            Some((field, want)) => self
                .get(col, id)
                .ok()
                .flatten()
                .is_some_and(|d| d.get(field.as_str()) == Some(want)),
        }
    }

    // ===== Graph analytics (#156 Phase E): pure functions over one
    // ===== collection's node set + `expand_neighbors` edges (inline refs
    // ===== and `via_edge` collections both route through it; incoming
    // ===== directions honor tracking like the walks do). Cross-collection
    // ===== targets are skipped — the graph is the spec'd collection,
    // ===== outbound edges to other collections don't join it (whole-data
    // ===== analytics stay out of scope). `node_match` prunes nodes and
    // ===== edges with the Phase C prune semantics. max_depth/limit don't
    // ===== apply to whole-graph algorithms.

    /// Live doc ids of a collection from its shard index (tombstones
    /// skip). A shard that never opened = empty node set (fail-closed —
    /// opening would create the shard, an unwanted side effect on
    /// analytics of unknown collections).
    fn graph_node_ids(&self, collection: &str) -> Vec<String> {
        let shard = match self.shards.read() {
            Ok(s) => s,
            Err(_) => return Vec::new(),
        };
        match shard.get(collection) {
            None => Vec::new(),
            Some(arc) => match arc.read() {
                Ok(st) => st.scan_prefix_keys(""),
                Err(_) => Vec::new(),
            },
        }
    }

    /// Node set + per-node edges (deduped, sorted — deterministic across
    /// runs). Only the matching nodes carry edges when `node_match` is
    /// set; every algorithm over an empty set yields an empty result.
    fn graph_adjacency(
        &self,
        collection: &str,
        spec: &TraverseSpec,
    ) -> Result<(Vec<GraphNode>, GraphAdj)> {
        let mut nodes: Vec<(String, String)> = self
            .graph_node_ids(collection)
            .into_iter()
            .map(|id| (collection.to_string(), id))
            .filter(|(c, id)| self.node_matches(spec, c, id))
            .collect();
        nodes.sort();
        let mut adj: std::collections::HashMap<(String, String), Vec<(String, String)>> =
            std::collections::HashMap::new();
        for key in &nodes {
            let mut targets: Vec<(String, String)> = self
                .expand_neighbors(&key.0, &key.1, spec)
                .into_iter()
                // Same-collection only + filter prunes the target side.
                .filter(|(ncol, nid, _, _)| {
                    ncol == &key.0 && self.node_matches(spec, ncol, nid)
                })
                .map(|(a, b, _, _)| (a, b))
                .collect();
            targets.sort();
            targets.dedup();
            adj.insert(key.clone(), targets);
        }
        Ok((nodes, adj))
    }

    /// Weak connected components (#156 Phase E): union-find over the
    /// adjacency; members sort ascending, components order by their
    /// smallest member. Direction-agnostic for the node set (both
    /// endpoints of an edge live in the collection).
    pub fn graph_components(
        &self,
        collection: &str,
        spec: &TraverseSpec,
    ) -> Result<Vec<Vec<(String, String)>>> {
        let (nodes, adj) = self.graph_adjacency(collection, spec)?;
        let pos: std::collections::HashMap<&(String, String), usize> =
            nodes.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let mut parent: Vec<usize> = (0..nodes.len()).collect();
        fn find(parent: &[usize], mut x: usize) -> usize {
            while parent[x] != x {
                x = parent[x];
            }
            x
        }
        for (src, targets) in &adj {
            if let Some(&sp) = pos.get(src) {
                for t in targets {
                    if let Some(&tp) = pos.get(t) {
                        let ra = find(&parent, sp);
                        let rb = find(&parent, tp);
                        if ra != rb {
                            parent[ra.min(rb)] = ra.max(rb);
                            // ponytail: no union-by-rank — embedded
                            // scale, path-halving is ceremony here.
                        }
                    }
                }
            }
        }
        let mut buckets: std::collections::HashMap<usize, Vec<(String, String)>> =
            std::collections::HashMap::new();
        for (i, key) in nodes.into_iter().enumerate() {
            buckets.entry(find(&parent, i)).or_default().push(key);
        }
        let mut out: Vec<Vec<(String, String)>> = buckets.into_values().collect();
        out.sort();
        Ok(out)
    }

    /// One directed cycle (#156 Phase E), or `None` if the graph is a
    /// DAG in the spec's direction. DFS color scan — visits every
    /// node/edge once, no exponential cycle enumeration; the reported
    /// path is the stack slice from the revisited node.
    pub fn graph_find_cycle(
        &self,
        collection: &str,
        spec: &TraverseSpec,
    ) -> Result<Option<Vec<(String, String)>>> {
        let (nodes, adj) = self.graph_adjacency(collection, spec)?;
        let pos: std::collections::HashMap<&(String, String), usize> =
            nodes.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let children: Vec<Vec<usize>> = nodes
            .iter()
            .map(|k| {
                adj.get(k)
                    .map(|ts| ts.iter().filter_map(|t| pos.get(t).copied()).collect())
                    .unwrap_or_default()
            })
            .collect();
        // 0 white, 1 grey (on stack), 2 black (closed).
        let mut color = vec![0u8; nodes.len()];
        let mut stack: Vec<(usize, usize)> = Vec::new();
        for s in 0..nodes.len() {
            if color[s] != 0 {
                continue;
            }
            color[s] = 1;
            stack.push((s, 0));
            while let Some(&(n, i)) = stack.last() {
                let ts = &children[n];
                if i >= ts.len() {
                    color[n] = 2;
                    stack.pop();
                    continue;
                }
                stack.last_mut().expect("non-empty").1 = i + 1;
                let c = ts[i];
                match color[c] {
                    0 => {
                        color[c] = 1;
                        stack.push((c, 0));
                    }
                    1 => {
                        // Back edge into the grey stack: the cycle is the
                        // stack slice from the revisited node to the top.
                        let from = stack.iter().position(|&(v, _)| v == c).expect("grey on stack");
                        return Ok(Some(
                            stack[from..].iter().map(|&(v, _)| nodes[v].clone()).collect(),
                        ));
                    }
                    _ => {}
                }
            }
        }
        Ok(None)
    }

    /// Topological order (#156 Phase E), Kahn-style: `None` when the
    /// graph closed a cycle (use `graph_find_cycle` for the path).
    /// Deterministic: the frontier pops the lexicographically smallest
    /// zero-indegree node first (nodes come sorted; position order is
    /// the same order).
    pub fn graph_topo_sort(
        &self,
        collection: &str,
        spec: &TraverseSpec,
    ) -> Result<Option<Vec<(String, String)>>> {
        let (nodes, adj) = self.graph_adjacency(collection, spec)?;
        let pos: std::collections::HashMap<&(String, String), usize> =
            nodes.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let children: Vec<Vec<usize>> = nodes
            .iter()
            .map(|k| {
                adj.get(k)
                    .map(|ts| ts.iter().filter_map(|t| pos.get(t).copied()).collect())
                    .unwrap_or_default()
            })
            .collect();
        let mut indeg = vec![0usize; nodes.len()];
        for ts in &children {
            for &c in ts {
                indeg[c] += 1;
            }
        }
        // Position order == lexicographic (col, id) order.
        let mut frontier: std::collections::BTreeSet<usize> =
            (0..nodes.len()).filter(|&i| indeg[i] == 0).collect();
        let mut out = Vec::with_capacity(nodes.len());
        while let Some(&n) = frontier.iter().next() {
            frontier.remove(&n);
            out.push(nodes[n].clone());
            for &c in &children[n] {
                indeg[c] -= 1;
                if indeg[c] == 0 {
                    frontier.insert(c);
                }
            }
        }
        if out.len() == nodes.len() {
            Ok(Some(out))
        } else {
            Ok(None)
        }
    }

    /// Degree centrality (#156 Phase E): (node, in-degree, out-degree)
    /// per collection node, sorted by total degree desc then node asc.
    pub fn graph_degree_centrality(
        &self,
        collection: &str,
        spec: &TraverseSpec,
    ) -> Result<Vec<DegreeRow>> {
        let (nodes, adj) = self.graph_adjacency(collection, spec)?;
        let pos: std::collections::HashMap<&(String, String), usize> =
            nodes.iter().enumerate().map(|(i, k)| (k, i)).collect();
        let mut indeg = vec![0usize; nodes.len()];
        for targets in adj.values() {
            for t in targets {
                if let Some(&p) = pos.get(t) {
                    indeg[p] += 1;
                }
            }
        }
        let mut rows: Vec<((String, String), usize, usize)> = nodes
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), indeg[i], adj.get(k).map_or(0, Vec::len)))
            .collect();
        rows.sort_by(|a, b| {
            (a.1 + a.2)
                .cmp(&(b.1 + b.2))
                .reverse()
                .then_with(|| a.0.cmp(&b.0))
        });
        Ok(rows)
    }

    /// Viz export (#156 Phase E): self-contained JSON —
    /// `{"nodes":[{"collection","id","doc"}],"edges":[{"from","to"}]}`.
    /// Nodes = live matching docs (doc JSON re-encodes resolved values);
    /// edges = the deduped adjacency. Same rules as the analytics fns.
    pub fn export_graph_json(&self, collection: &str, spec: &TraverseSpec) -> Result<String> {
        let (nodes, adj) = self.graph_adjacency(collection, spec)?;
        let nodes_json: Vec<serde_json::Value> = nodes
            .iter()
            .map(|(c, id)| {
                let mut mdoc = serde_json::Map::new();
                if let Ok(Some(d)) = self.get(c, id) {
                    for (f, v) in d.fields.iter() {
                        mdoc.insert(f.to_string(), v.to_json());
                    }
                }
                serde_json::json!({ "collection": c, "id": id, "doc": mdoc })
            })
            .collect();
        let mut edges: Vec<serde_json::Value> = adj
            .iter()
            .flat_map(|(from, ts)| {
                ts.iter().map(move |to| {
                    serde_json::json!({
                        "from": format!("{}/{}", from.0, from.1),
                        "to": format!("{}/{}", to.0, to.1)
                    })
                })
            })
            .collect();
        // Deterministic docs: adj is a HashMap, iteration order varies per
        // process — sort the edge list by (from, to).
        edges.sort_by(|a, b| {
            (a["from"].as_str(), a["to"].as_str()).cmp(&(b["from"].as_str(), b["to"].as_str()))
        });
        Ok(serde_json::to_string(&serde_json::json!({
            "nodes": nodes_json,
            "edges": edges
        }))
        .unwrap_or_else(|_| "{}".to_string()))
    }

    /// Shared composite backfill spawner: used at explicit creation AND
    /// at open when a legacy snapshot restored secondary/fts but no
    /// composite trees (pre-fix files). Same chunks/yield discipline
    /// either way; quiescence covers it via backfill_inflight.
    fn spawn_composite_backfill(
        idx_mgr: Arc<RwLock<IndexManager>>,
        persist_ptr: Arc<Mutex<IndexStorage>>,
        enc_key: Option<String>,
        backfill_count: Arc<AtomicUsize>,
        shard_arc: Arc<RwLock<StorageEngine>>,
        index_id: u32,
        unique_ledger: std::sync::Arc<crate::index::composite::ledger::UniqueLedger>,
    ) {
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

                            // 2. Insert LEAN KEY into index.log. Filtered
                            // defs (hakodb#71) only log matching docs —
                            // the log must mirror the tree.
                            if composite_idx.definition.filter_passes(&doc_id, &doc) {
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
                }

                // Release locks to allow benchmark/queries to slip in!
                drop(persist);
                drop(mgr);
                thread::yield_now();
            }

            // hakodb#71: a unique index built here (legacy snapshot
            // upgrade, lazy touch-load) folds its sorted tree into the
            // claim ledger on completion, turning the admission gate
            // on. Non-unique trees skip the fold entirely.
            let is_unique = idx_mgr.read().ok().and_then(|mgr| {
                mgr.composite.get(index_id).map(|i| i.definition.unique)
            });
            if let Some(true) = is_unique {
                if let Ok(mgr) = idx_mgr.read() {
                    if let Some(idx) = mgr.composite.get(index_id) {
                        let _ = unique_ledger.load_from_tree(index_id, &idx.tree);
                    }
                }
            }
        });
    }

    pub fn create_composite_index(&self, col: &str, fields: Vec<(String, SortDirection)>) -> Result<u32> {
        self.create_composite_index_ex(col, fields, false, true, None)
    }

    /// Composite index creation with uniqueness constraints (hakodb#71).
    /// Dedupe compares fields AND constraints: a field-identical but
    /// constraint-different def creates a SEPARATE index (never silently
    /// reuse an id without the constraints being asked for). Unique
    /// indexes backfill synchronously â€” tree + claim ledger populate, an
    /// adjacent-prefix duplicate scan runs over the sorted tree, and the
    /// admission gate turns on the moment this returns. Historical
    /// duplicates reject the whole creation (def + ledger rolled back):
    /// loud failure, never a panic. Filter is deduped by its JSON form
    /// (cold path, one call per create).
    pub fn create_composite_index_ex(
        &self,
        col: &str,
        fields: Vec<(String, SortDirection)>,
        unique: bool,
        sparse: bool,
        filter: Option<crate::query::filter::FilterGroup>,
    ) -> Result<u32> {
        // Dotted-path validation (hakodb#71 F2): bounded nesting, no
        // empty segments. The fetch hook itself accepts any depth —
        // this refuses to BUILD an index on unreasonable paths.
        for (field, _) in &fields {
            if field.contains("..") || field.starts_with('.') || field.ends_with('.') {
                return Err(HakoError::InvalidInput(format!(
                    "invalid indexed field `{field}`: empty dotted segment"
                )));
            }
            if field.split('.').count() > 16 {
                return Err(HakoError::InvalidInput(format!(
                    "indexed field `{field}` exceeds 16 path segments"
                )));
            }
        }
        {
            let mgr = self.indexes.read().unwrap();
            for idx in mgr.indexes_for_collection(col) {
                let d = &idx.definition;
                let fields_same = d.fields.len() == fields.len()
                    && d.fields
                        .iter()
                        .zip(fields.iter())
                        .all(|(a, b)| a.field == b.0 && a.direction == b.1);
                let filter_same = match (&d.filter, &filter) {
                    (None, None) => true,
                    (Some(a), Some(b)) => {
                        serde_json::to_string(a).ok() == serde_json::to_string(b).ok()
                    }
                    _ => false,
                };
                // sparse is inert for non-unique indexes; compare the
                // canonical "dense" bit so legacy defs dedupe cleanly.
                if fields_same
                    && d.unique == unique
                    && (d.unique && !d.sparse) == (unique && !sparse)
                    && filter_same
                {
                    return Ok(d.id);
                }
            }
        }

        let def = CompositeIndexDefinition::new(col)
            .with_fields(fields)
            .with_constraints(unique, sparse, filter);
        let index_id = self.indexes.write().unwrap().create_index(def);
        // Publish the new def to the admission gate before backfill: a
        // batch landing mid-backfill must see `!ledger.is_ready` and
        // reject instead of writing past a unique constraint.
        self.gate_cache.write().unwrap().clear();

        let shard_arc = self.get_shard(col)?;
        if !unique {
            Self::spawn_composite_backfill(
                Arc::clone(&self.indexes),
                Arc::clone(&self.index_storage),
                self.config.encryption_key.clone(),
                Arc::clone(&self.backfill_inflight),
                shard_arc,
                index_id,
                Arc::clone(&self.unique_ledger),
            );
        } else {
            // Synchronous unique backfill (hakodb#71): blocks the caller
            // for one scan â€” operator-driven, once per index lifetime.
            if let Err(e) = self.backfill_unique_sync(&shard_arc, index_id) {
                // Roll the not-ready def back: tree registration gone,
                // ledger never went live, defs re-persisted without it.
                self.indexes.write().unwrap().composite.drop_index(index_id);
                self.gate_cache.write().unwrap().clear();
                self.unique_ledger.reset(index_id);
                let _ = self.persist_index_defs();
                self.plan_cache.invalidate();
                return Err(e);
            }
        }

        let _ = self.persist_index_defs();
        self.plan_cache.invalidate();
        Ok(index_id)
    }

    /// One-shot scan feeding BOTH the composite tree and the unique
    /// claim ledger (hakodb#71). Mirrors spawn_composite_backfill's
    /// resolve/chunk/yield structure, minus the thread. Finishes inside
    /// the backfill_inflight window so quiescence covers it.
    /// ponytail: sync instead of entity-backed async because the
    /// admission gate needs a COMPLETE claim set before it may fire;
    /// an async variant would need ready-signalling plumbing (worker
    /// â†’ engine) â€” revisit if creations over multi-GB collections get
    /// slow to a fault.
    fn backfill_unique_sync(
        &self,
        shard_arc: &Arc<RwLock<StorageEngine>>,
        index_id: u32,
    ) -> Result<()> {
        self.backfill_inflight.fetch_add(1, Ordering::Relaxed);
        let enc_key = self.config.encryption_key.clone();
        {
            let _guard = BackfillGuard { counter: Arc::clone(&self.backfill_inflight) };
            let pointers = shard_arc.read().unwrap().get_physical_index_snapshot();
            for chunk in pointers.chunks(500) {
                let mut resolved = Vec::new();
                {
                    let storage = shard_arc.read().unwrap();
                    for (key, ptr) in chunk {
                        if let Ok(Some(bytes)) = storage.read_pointer_internal(ptr, false) {
                            resolved.push((key.clone(), bytes));
                        }
                    }
                }

                {
                    let mut mgr = self.indexes.write().unwrap();
                    let mut persist = self.index_storage.lock().unwrap();
                    if let Some(idx) = mgr.composite.get_mut(index_id) {
                        for (doc_id, bytes) in resolved {
                            if let Some(mut doc) = HakoDoc::decode(&bytes) {
                                let _ =
                                    resolve_doc_static(&mut doc, shard_arc, enc_key.as_deref());
                                idx.index_document(&doc_id, &doc);
                                // Lean key into index.log (same as the
                                // async backfill — recovery parity).
                                // Filtered defs only log matching docs.
                                if idx.definition.filter_passes(&doc_id, &doc) {
                                    if let Some(vals) = idx.document_values(&doc_id, &doc) {
                                        let key_bytes =
                                            crate::index::composite::key_encoder::encode_composite_key(
                                                &idx.definition, &vals, &doc_id,
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
                    } else {
                        // Def vanished mid-backfill (concurrent drop):
                        // nothing left to enforce, bail quietly.
                        return Ok(());
                    }
                    thread::yield_now();
                }
            }
        }

        // Fold the sorted tree into the claim ledger. Adjacent equal
        // prefixes = historical duplicates â†’ dense error, no claims
        // registered (load_from_tree keeps partial state private).
        let mgr = self.indexes.read().unwrap();
        match mgr.composite.get(index_id) {
            Some(idx) => self
                .unique_ledger
                .load_from_tree(index_id, &idx.tree)
                .map(|_| ())
                .map_err(|e| match e {
                    HakoError::UniqueViolation { collection: _, index, key } => {
                        HakoError::UniqueViolation {
                            collection: idx.definition.collection.clone(),
                            index,
                            key,
                        }
                    }
                    other => other,
                }),
            None => Ok(()),
        }
    }

    /// Explicitly trigger a snapshot (called by Maintenance Thread or FFI)
    /// Snapshot the RAM index state (secondary/fts/composite) plus per-
    /// collection WAL tails, atomically (tmp + rename + fsync) so a power
    /// loss never leaves a half-written file (import falls back to full
    /// rescan on anything unreadable).
    pub fn export_index_snapshot(&self) -> Result<()> {
        Self::write_index_snapshot(&self.root_path, &self.shards, &self.indexes)
    }

    #[allow(clippy::type_complexity)]
    fn write_index_snapshot(
        root_path: &std::path::Path,
        shards: &Arc<RwLock<HashMap<String, Arc<RwLock<StorageEngine>>>>>,
        indexes: &Arc<RwLock<IndexManager>>,
    ) -> Result<()> {
        use crate::index::manager::WalTails;
        let tails: WalTails = shards
            .read()
            .map(|m| {
                m.iter()
                    .map(|(name, shard)| {
                        let (data_len, file_len) = shard
                            .read()
                            .map(|s| (s.wal.data_len(), s.wal.file_len()))
                            .unwrap_or((0, 0));
                        (name.clone(), (data_len, file_len))
                    })
                    .collect()
            })
            .unwrap_or_default();
        let bytes = indexes
            .read()
            .map_err(|_| HakoError::LockPoisoned("indexes".into()))?
            .export_state_with_tails(&tails)?;
        let dir = root_path.join("_indices");
        let _ = std::fs::create_dir_all(&dir);
        let tmp = dir.join("ram_indexes.bin.tmp");
        let dst = dir.join("ram_indexes.bin");
        std::fs::write(&tmp, &bytes)?;
        // ponytail: best-effort durability only â€” a failed sync still
        // leaves the previous good snapshot (rename is atomic).
        if let Ok(f) = std::fs::OpenOptions::new().read(true).open(&tmp) {
            let _ = f.sync_all();
        }
        std::fs::rename(&tmp, &dst)?;
        Ok(())
    }

    pub fn save_index_snapshots(&self) -> Result<()> {
        let persist = self.index_storage.lock().unwrap();
        // Snapshot the primary composite index (ID 1)
        persist.snapshot(1).map_err(HakoError::Io)?;
        Ok(())
    }

    pub fn list_storage_keys(&self, collection: &str) -> Result<Vec<String>> {
        let Some(shard) = self.try_get_shard_on_disk(collection)? else { return Ok(Vec::new()) };
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

    fn resolve_doc(&self, doc: &mut HakoDoc, collection: &str) -> Result<()> {
        // Single acquisition: snapshot queue + manager under one read
        // guard, then resolve without holding any lock. Re-acquiring
        // inside resolve (nested read behind a queued writer) is a
        // self-deadlock under writer-preference RwLocks â€” never do it.
        let shard_arc = self.get_shard(collection)?;
        let (blob_manager, queue_snapshot) = {
            let shard = shard_arc.read().map_err(|_| HakoError::LockPoisoned("shard".into()))?;
            Self::snapshot_blob_state(&shard)
        };
        Self::apply_blob_snapshot(self, doc, blob_manager, queue_snapshot)
    }

    /// Snapshot the blob state needed for resolve: manager handle plus
    /// not-yet-flushed queue bytes by offset. Call under a read guard;
    /// the returned Arcs outlive it.
    #[allow(clippy::type_complexity)]
    fn snapshot_blob_state(
        shard: &StorageEngine,
    ) -> (Option<Arc<BlobManager>>, HashMap<u64, Arc<Vec<u8>>>) {
        let mut in_memory = HashMap::new();
        for work in &shard.blob_flush_queue {
            if let BlobWork::PutRaw { offset, data, .. } = work {
                in_memory.insert(*offset, Arc::clone(data));
            }
        }
        (shard.blob_manager.clone(), in_memory)
    }

    /// Pure resolve step: no locks taken. Missing-file bytes are an
    /// error (same as before); absent links stay untouched.
    fn apply_blob_snapshot(
        &self,
        doc: &mut HakoDoc,
        blob_manager: Option<Arc<BlobManager>>,
        queue_snapshot: HashMap<u64, Arc<Vec<u8>>>,
    ) -> Result<()> {
        // PASS 1: Collect mutable references once.
        let blob_values: Vec<&mut Value> = doc.fields.iter_mut()
            .map(|(_, v)| v)
            .filter(|v| matches!(v, Value::BlobLink { offset, .. } if *offset != u64::MAX))
            .collect();

        if blob_values.is_empty() { return Ok(()); }

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
        self.vacuum_impl(collection, None)
    }

    /// [`Self::vacuum`] with cooperative cancellation.
    ///
    /// Cancellation before the final rename removes the temp blob file and
    /// leaves the existing `blobs.dat` and every document untouched.
    pub fn vacuum_with_cancel(&self, collection: &str, cancel: &CancelToken) -> Result<()> {
        self.vacuum_impl(collection, Some(cancel))
    }

    fn vacuum_impl(&self, collection: &str, cancel: Option<&CancelToken>) -> Result<()> {
        if let Some(token) = cancel {
            token.check()?;
        }
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
            if let Some(token) = cancel {
                if token.is_cancelled() {
                    drop(new_blob_file);
                    let _ = std::fs::remove_file(&temp_path);
                    return Err(HakoError::Cancelled);
                }
            }
            // We need to read the document to find BlobLinks inside it
            if let Some(bytes) = shard.read_pointer(pointer)? {
                if let Some(mut doc) = HakoDoc::decode(&bytes) {
                    let mut doc_changed = false;
                    
                    for (_, value) in &mut doc.fields {
                        if let Value::BlobLink { offset, len } = *value {
                            // Read through the guard we already hold: going
                            // back through `resolve_single_value_blob` would
                            // re-acquire the shard write lock and deadlock.
                            let raw_bytes = shard
                                .blob_manager
                                .as_ref()
                                .ok_or_else(|| {
                                    HakoError::StorageError("Blob manager missing".into())
                                })?
                                .read_at(offset, len)?;

                            new_blob_file.write_all(&raw_bytes)?;
                            *value = Value::BlobLink { offset: new_offset, len: raw_bytes.len() as u32 };
                            new_offset += raw_bytes.len() as u64;
                            doc_changed = true;
                        }
                    }

                    if doc_changed {
                        updates.push((key.clone(), doc.encode()));
                    }
                }
            }
        }

        // 2. Commit the new blob file
        if let Some(token) = cancel {
            if token.is_cancelled() {
                drop(new_blob_file);
                let _ = std::fs::remove_file(&temp_path);
                return Err(HakoError::Cancelled);
            }
        }
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
    /// planner's `index_ready` gate) â€” poll after open/seed in benchmarks
    /// and tests before measuring. Mirrors `hk_engine_is_indexes_ready`.
    pub fn is_indexes_ready(&self) -> bool {
        self.indexes_ready.load(std::sync::atomic::Ordering::Acquire)
    }

    /// Query planning may trust indexes only when open-recovery is done AND
    /// no runtime backfill is in flight. A new index registers before its
    /// backfill thread finishes; planning against the partial index returns
    /// wrong (empty/partial) results â€” fall back to `FullCollection`
    /// instead. The plan cache is invalidated at index creation, so no
    /// stale fast plan survives into the build window (a slow plan cached
    /// during the window self-heals at TTL).
    pub(crate) fn index_query_ready(&self, collection: &str) -> bool {
        self.indexes_ready.load(std::sync::atomic::Ordering::Acquire)
            && self.backfill_inflight.load(std::sync::atomic::Ordering::Acquire) == 0
            // Pending IndexOps (hakodb#71 BUG-2, report from box fkip-234):
            // a write returns Ok after enqueueing its index update; a query
            // that lets the planner trust a composite/secondary tree before
            // the worker applied the ops loses read-your-writes. Scope the
            // fallback to the written collection: a pending write in `hot`
            // must not force `cold` onto FullCollection.
            && self
                .index_inflight_by_collection
                .read()
                .map(|m| {
                    m.get(collection)
                        .is_none_or(|p| p.load(Ordering::Acquire) == 0)
                })
                .unwrap_or(false)
    }

    /// Queue one index update and account it globally (quiescence) and per
    /// collection (planner gate). The op carries the per-collection counter
    /// so the worker never needs a map lock; a failed send refunds both.
    pub(crate) fn enqueue_index_update(
        &self,
        collection: String,
        puts: Arc<Vec<(String, Arc<HakoDoc>)>>,
        deletes: Vec<(String, HakoDoc)>,
    ) -> bool {
        if puts.is_empty() && deletes.is_empty() {
            return true;
        }
        let pending = {
            let mut map = self.index_inflight_by_collection.write().unwrap();
            Arc::clone(
                map.entry(collection.clone())
                    .or_insert_with(|| Arc::new(AtomicUsize::new(0))),
            )
        };
        pending.fetch_add(1, Ordering::AcqRel);
        self.index_inflight.fetch_add(1, Ordering::AcqRel);
        if self
            .index_tx
            .send(IndexOp::Update {
                collection,
                puts,
                deletes,
                pending: Arc::clone(&pending),
            })
            .is_err()
        {
            pending.fetch_sub(1, Ordering::AcqRel);
            self.index_inflight.fetch_sub(1, Ordering::AcqRel);
            return false;
        }
        true
    }

    /// Vector planning keeps the backfill gate but drops the pending IndexOps
    /// one: ANN runs against in-memory handles that the executor live-filters
    /// (deleted ids stay invisible), so tree lag only lowers recall — never
    /// serves ghost rows. FullCollection cannot score Near at all; falling
    /// back to it during a write burst would rather serve empty results.
    pub(crate) fn vector_query_ready(&self) -> bool {
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
    /// `QuiescenceStatus::is_quiescent`). One sample â€” use
    /// `await_quiescent` for a settled verdict.
    pub fn is_quiescent(&self) -> bool {
        self.quiescence_status().is_quiescent()
    }

    /// Block until background work settles or `timeout` lapses. Requires
    /// TWO consecutive clear samples (5ms apart) so a millisecond gap
    /// between write batches doesn't read as settled. Returns true when
    /// settled. A continuously-written DB correctly never settles â€” poll
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
        // Sync enumeration (hidden included, excluded dropped) â€” the manual
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

        // 4. SAVE RAM INDEXES (+ WAL tails, atomically)
        let _ = Self::write_index_snapshot(&self.root_path, &self.shards, &self.indexes);
        if let Ok(mgr) = self.indexes.read() {
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

/// Plain recursive copy of one unopened collection dir, mirroring the
/// `StorageEngine::backup` file filter (data segments + WAL only).
fn copy_collection_dir(src: &std::path::Path, dst: &std::path::Path) -> Result<()> {
    std::fs::create_dir_all(dst)?;
    for entry in std::fs::read_dir(src)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            copy_collection_dir(&entry.path(), &dst.join(entry.file_name()))?;
        } else {
            let n = entry.file_name().to_string_lossy().into_owned();
            if n.ends_with(".dat") || n == "wal.log" {
                std::fs::copy(entry.path(), dst.join(entry.file_name()))?;
            }
        }
    }
    Ok(())
}

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
        d.insert("active", Value::Bool(!i.is_multiple_of(3)));
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

    /// Version seed: micros-scale (never the old restart-at-1), strictly
    /// increasing per mutation. A restart can never reissue a version an
    /// ETag holder already saw (the 304-stale hole that killed naive
    /// counter ETags under daily reboots).
    #[test]
    fn versions_seeded_from_wall_clock() {        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-verseed-{nanos}"));
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        db.put_owned("c", "k", test_doc(1)).expect("put");
        let v1 = db.current_version("k").expect("version after write");
        // Micros since 2026 â‰ˆ 1.78e15: proves the wall-clock seed (the old
        // code issued 1 here). Deterministic: no timing involved.
        assert!(v1 > 1_000_000_000_000_000, "seeded, not restarted-at-1: {v1}");
        db.put_owned("c", "k", test_doc(2)).expect("put");
        let v2 = db.current_version("k").expect("version after rewrite");
        assert!(v2 > v1, "strictly increasing per mutation");
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Runtime group-commit setter: default from open config, live after
    /// set, clamped like open (1..=30000). The getter reads the live
    /// value, so fleet stagger checks observe runtime changes.
    #[test]
    fn group_commit_interval_runtime_setter() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-gciset-{nanos}"));
        let db = Hako::open(&dir, HakoConfig::default()).expect("open");
        assert_eq!(
            db.group_commit_interval_ms(),
            crate::config::DEFAULT_GROUP_COMMIT_INTERVAL_MS
        );
        db.set_group_commit_interval_ms_all(50);
        assert_eq!(db.group_commit_interval_ms(), 50);
        db.set_group_commit_interval_ms_all(0);
        assert_eq!(db.group_commit_interval_ms(), 1);
        db.set_group_commit_interval_ms_all(999_999);
        assert_eq!(db.group_commit_interval_ms(), 30_000);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Relocate: paired put-preserving-timestamp + fresh tombstone.
    /// Roundtrip keeps stored bytes identical, versions advance,
    /// retry is idempotent, refusals are explicit.
    #[test]
    fn relocate_docs_roundtrip() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-reloc-{nanos}"));
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        let mut d = HakoDoc::default();
        d.insert("v", Value::String("one".into()));
        // Big value forces the blob path (re-extract at dest).
        d.insert("big", Value::String("B".repeat(20_000)));
        db.put_owned("src", "k1", d).expect("seed");
        db.put_owned("src", "k2", test_doc(2)).expect("seed");

        let rep = db
            .relocate_docs("src", "dst", &["k1".to_string(), "ghost".to_string()])
            .expect("relocate");
        assert_eq!(rep.moved, vec!["k1".to_string()]);
        assert_eq!(rep.missing, vec!["ghost".to_string()]);
        // Src tombstoned, dst identical (fields + _time preserved).
        assert!(db.get("src", "k1").expect("get").is_none());
        let back = db.get("dst", "k1").expect("get").expect("moved");
        assert_eq!(back.get("v"), Some(&Value::String("one".into())));
        assert_eq!(back.get("big").map(|v| match v {
            Value::String(s) => s.len(),
            _ => 0,
        }), Some(20_000));
        let orig_time = back._time;
        // Versions advanced on the moved identity.
        assert!(db.current_version("k1").is_some());
        // Idempotent retry: already moved + still missing.
        let rep2 = db
            .relocate_docs("src", "dst", &["k1".to_string(), "ghost".to_string()])
            .expect("retry");
        assert!(rep2.moved.is_empty());
        assert_eq!(rep2.missing.len(), 2);
        // Restore: identical bytes again (same _time).
        let rep3 = db.relocate_docs("dst", "src", &["k1".to_string()]).expect("restore");
        assert_eq!(rep3.moved, vec!["k1".to_string()]);
        let home = db.get("src", "k1").expect("get").expect("restored");
        assert_eq!(home._time, orig_time);
        assert!(db.get("dst", "k1").expect("get").is_none());
        // Explicit refusals.
        assert!(db.relocate_docs("src", "src", &["k1".to_string()]).is_err());
        assert!(db.relocate_docs("__users", "dst", &["k1".to_string()]).is_err());
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Selective decode keeps indexed entries identical while skipping
    /// value allocs for the rest (backfill paths feed it only registered
    /// fields â€” see indexed_fields_for).
    #[test]
    fn decode_projected_matches_full_on_wanted() {
        let mut d = HakoDoc::default();
        d.insert("a", Value::String("x".into()));
        d.insert("b", Value::Int(7));
        d.insert("c", Value::Bool(true));
        let bytes = d.encode_buffered();
        let sub = HakoDoc::decode_projected(&bytes, &["b".to_string()]).expect("decode");
        assert_eq!(sub.get("b"), Some(&Value::Int(7)));
        assert!(sub.get("a").is_none());
        assert!(sub.get("c").is_none());
        assert_eq!(sub._time, d._time);
        // Empty projection = full decode (safe fallback everywhere).
        let full = HakoDoc::decode_projected(&bytes, &[]).expect("decode");
        assert_eq!(full.get("a"), Some(&Value::String("x".into())));
        assert_eq!(full.get("c"), Some(&Value::Bool(true)));
        // Same strictness as full decode on corrupt input.
        let mut bad = bytes.clone();
        bad.truncate(bad.len() - 2);
        assert!(HakoDoc::decode(&bad).is_none());
        assert!(HakoDoc::decode_projected(&bad, &["b".to_string()]).is_none());
    }

    /// Background lazy load: start_collection_load opens now and
    /// backfills async with pollable progress; queries sweep meanwhile
    /// and index correctly after. Unloaded again via unload_collection.
    #[test]
    fn bg_lazy_load_progress_and_correctness() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-bgload-{nanos}"));
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            for i in 0..8_000u32 {
                let mut d = HakoDoc::default();
                d.insert("kind", Value::String(format!("k{}", i % 8)));
                d.insert("v", Value::Int(i as i64));
                db.put_owned("arc", &format!("d{i:05}"), d).expect("put");
            }
            db.create_index("arc", "kind").expect("secondary");
            assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
        }
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.lazy_collections = vec!["arc*".to_string()];
        let db = Hako::open(&dir, cfg).expect("open");
        assert!(db.is_lazy_unloaded("arc"));
        assert!(db.collection_load_progress("arc").is_none());
        assert!(db.collection_load_progress("nope").is_none());
        assert!(db.start_collection_load("arc").expect("start"));
        // Second start while loading-or-loaded is a no-op false.
        let _ = db.start_collection_load("arc").expect("second");
        // Either we observe progress mid-build, or the build already
        // finished on a fast box â€” both converge below.
        let t0 = std::time::Instant::now();
        let mut saw_progress = false;
        while t0.elapsed() < std::time::Duration::from_secs(15) {
            if let Some((done, total)) = db.collection_load_progress("arc") {
                saw_progress = true;
                assert!(done <= total);
                if done == total {
                    break;
                }
            } else if db.await_quiescent(std::time::Duration::from_secs(1)) {
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
        assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
        assert!(db.collection_load_progress("arc").is_none());
        assert!(!db.is_lazy_unloaded("arc"));
        // Index correct post-build (no FullCollection degradation).
        let enc = crate::index::index_key::encode_scalar(&Value::String("k3".into()));
        let hits = db.indexes.read().unwrap()
            .lookup_secondary("arc", "kind", &enc)
            .unwrap_or_default();
        assert_eq!(hits.len(), 1000);
        let _ = saw_progress;
        db.unload_collection("arc").expect("unload");
        assert!(db.is_lazy_unloaded("arc"));
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Borrowed secondary backfill on wide docs: same entries as the
    /// owned path (single hit per unique value), exercising the fast
    /// path that skips full decode.
    #[test]
    fn borrowed_backfill_matches_owned_on_wide_docs() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-spike-{nanos}"));
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            for i in 0..15_000u32 {
                let mut d = HakoDoc::default();
                d.insert("no", Value::String(format!("W{i:06}")));
                for f in 0..30 {
                    d.insert(
                        format!("pad{f:02}"),
                        Value::String(format!("padding value {i} field {f} xxxxxxxxxx")),
                    );
                }
                db.put_owned("big", &format!("w{i:05}"), d).expect("put");
            }
            db.create_index("big", "no").expect("secondary");
            assert!(db.await_quiescent(std::time::Duration::from_secs(120)));
        }
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.lazy_collections = vec!["big*".to_string()];
        let db = Hako::open(&dir, cfg).expect("open");
        assert!(db.is_lazy_unloaded("big"));
        assert!(db.get("big", "w00000").expect("get").is_some());
        assert!(db.await_quiescent(std::time::Duration::from_secs(120)));
        let enc =
            crate::index::index_key::encode_scalar(&Value::String("W000001".into()));
        let hits = db
            .indexes
            .read()
            .unwrap()
            .lookup_secondary("big", "no", &enc)
            .unwrap_or_default();
        assert_eq!(hits.len(), 1);
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Borrowed path maps virtual id/_time from doc_id/_time, never from
    /// a literal same-named field (owned parity).
    #[test]
    fn borrowed_backfill_virtual_id_wins_over_literal() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-virt-{nanos}"));
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            let mut d = HakoDoc::default();
            d.insert("id", Value::String("WRONG".into()));
            d.insert("v", Value::Int(1));
            db.put_owned("c", "real-id", d).expect("put");
            db.create_index("c", "id").expect("secondary");
            assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
        }
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.lazy_collections = vec!["c*".to_string()];
        let db = Hako::open(&dir, cfg).expect("open");
        assert!(db.get("c", "real-id").expect("get").is_some());
        assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
        let enc = crate::index::index_key::encode_scalar(&Value::String("real-id".into()));
        let hits = db
            .indexes
            .read()
            .unwrap()
            .lookup_secondary("c", "id", &enc)
            .unwrap_or_default();
        assert_eq!(hits.len(), 1);
        let enc_wrong =
            crate::index::index_key::encode_scalar(&Value::String("WRONG".into()));
        assert!(db
            .indexes
            .read()
            .unwrap()
            .lookup_secondary("c", "id", &enc_wrong)
            .unwrap_or_default()
            .is_empty());
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Crash-after-snapshot: WAL moves past the last RAM snapshot
    /// without the index worker (simulated via a raw StorageEngine that
    /// bypasses RAM state, exactly like a kill -9). Reopen must rescan
    /// the stale collection: new docs indexed, deleted docs gone (no
    /// ghosts), byte-identical data.
    #[test]
    fn crash_stale_snapshot_rescans_clean() {
        use crate::storage::engine::StorageEngine;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-crash-{nanos}"));
        let mk = |kind: &str| {
            let mut d = HakoDoc::default();
            d.insert("kind", Value::String(kind.into()));
            d
        };
        // Phase 1: live DB, indexed, clean shutdown (fresh snapshot).
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            db.put_owned("c", "a1", mk("x")).expect("put");
            db.put_owned("c", "a2", mk("y")).expect("put");
            db.create_index("c", "kind").expect("secondary");
            assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
        }
        // Phase 2: raw engine moves WAL past the snapshot (no RAM index
        // involved) â€” then drops (WAL flushed, snapshot untouched).
        {
            let cfg = HakoConfig::default();
            let mut eng =
                StorageEngine::open(dir.join("c"), &cfg, "c".to_string(), None).expect("open");
            eng.put("a3".to_string(), &mk("x").encode_buffered())
                .expect("put");
            eng.delete("a1").expect("delete");
        }
        // Phase 3: reopen detects staleness via WAL tails and rescans.
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
            let enc = crate::index::index_key::encode_scalar(&Value::String("x".into()));
            let hits = db
                .indexes
                .read()
                .unwrap()
                .lookup_secondary("c", "kind", &enc)
                .unwrap_or_default();
            // a3 joined, a1's ghost gone â€” exactly one live "x".
            assert_eq!(hits.len(), 1);
            assert!(db.get("c", "a1").expect("get").is_none());
            assert!(db.get("c", "a3").expect("get").is_some());
            drop(db);
        }
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Early termination (issue #22): unordered bounded queries return
    /// the scan-order prefix â€” identical rows to full scan + truncate.
    #[test]
    fn early_termination_matches_full_scan() {
        use crate::query::filter::Operator;
        use crate::query::query::Query;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-early-{nanos}"));
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            for i in 0..5_000u32 {
                let mut d = HakoDoc::default();
                d.insert("g", Value::Int((i % 10) as i64));
                db.put_owned("t", &format!("d{i:05}"), d).expect("put");
            }
            assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
        }
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
        let full: Vec<String> = db
            .query(Query::new("t").where_filter("g", Operator::Gte, Value::Int(0)).limit(5000))
            .expect("query")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert!(full.len() > 100);
        // Bounded page equals the full-scan prefix (limit and limit+offset).
        let page: Vec<String> = db
            .query(Query::new("t").where_filter("g", Operator::Gte, Value::Int(0)).limit(20))
            .expect("query")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(page, full[..20].to_vec());
        let page_off: Vec<String> = db
            .query(
                Query::new("t")
                    .where_filter("g", Operator::Gte, Value::Int(0))
                    .offset(50)
                    .limit(20),
            )
            .expect("query")
            .into_iter()
            .map(|(id, _)| id)
            .collect();
        assert_eq!(page_off, full[50..70].to_vec());
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Borrowed secondary backfill inserts the same entries as the
    /// owned path (unit level; end-to-end covered by the lazy tests).
    #[test]
    fn borrowed_backfill_unit() {
        use crate::index::service::IndexingService;
        let mut mgr = crate::index::manager::IndexManager::default();
        mgr.create_secondary_index("c", "kind");
        let mut d1 = HakoDoc::default();
        d1.insert("kind", Value::String("x".into()));
        let mut d2 = HakoDoc::default();
        d2.insert("kind", Value::String("y".into()));
        let docs = vec![
            ("a1".to_string(), d1.encode_buffered()),
            ("a2".to_string(), d2.encode_buffered()),
        ];
        assert!(IndexingService::backfill_secondary_borrowed(&mut mgr, "c", &docs));
        let enc = crate::index::index_key::encode_scalar(&Value::String("x".into()));
        let hits = mgr.lookup_secondary("c", "kind", &enc).unwrap_or_default();
        assert_eq!(hits.len(), 1);
    }

    /// Relocate refuses every sync-excluded side (config, builtin,
    /// whole-collection local-only, unloaded-lazy) in lockstep with what
    /// sync withholds â€” a move must never launder data onto the mesh.
    #[test]
    fn relocate_refuses_excluded_sides() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-relocgate-{nanos}"));
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            let mut d = HakoDoc::default();
            d.insert("v", Value::String("x".into()));
            for (c, id) in [("hot", "h1"), ("nope", "n1"), ("cold", "c1")] {
                db.put_owned(c, id, d.clone()).expect("seed");
            }
        }
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.sync_excluded = vec!["nope".to_string()];
        cfg.lazy_collections = vec!["cold*".to_string()];
        let db = Hako::open(&dir, cfg).expect("open");
        db.set_collection_local("loc", true);
        // Every excluded side refused, both directions.
        assert!(db.relocate_docs("nope", "hot", &["n1".to_string()]).is_err());
        assert!(db.relocate_docs("hot", "nope", &["h1".to_string()]).is_err());
        assert!(db.relocate_docs("__groups", "hot", &["g".to_string()]).is_err());
        assert!(db.relocate_docs("loc", "hot", &["l".to_string()]).is_err());
        assert!(db.relocate_docs("cold", "hot", &["c1".to_string()]).is_err());
        // Sync withholds exactly the same set (loc is enumerator-listed
        // but per-op filters drop it on every transport; relocate refuses).
        let sync = db.sync_collections().unwrap();
        for c in ["nope", "__groups", "cold"] {
            assert!(!sync.contains(&c.to_string()));
            assert!(db.is_sync_withheld(c));
        }
        assert!(sync.contains(&"hot".to_string()));
        // And the normal path still moves.
        let rep = db.relocate_docs("hot", "hot2", &["h1".to_string()]).expect("move");
        assert_eq!(rep.moved, vec!["h1".to_string()]);
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// Lazy archive group: skipped at recovery, withheld from sync until
    /// touched, fully indexed on touch (no FullCollection degradation),
    /// evictable, and covered by FS-aware backup while unopened.
    #[test]
    fn lazy_collections_skip_touch_sync_backup() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-lazy-{nanos}"));
        // Phase 1: seed eager (so recovery has something to skip later).
        {
            let mut cfg = HakoConfig::default();
            cfg.durability_mode = DurabilityMode::Manual;
            let db = Hako::open(&dir, cfg).expect("open");
            let mut a1 = HakoDoc::default();
            a1.insert("kind", Value::String("report".into()));
            a1.insert("big", Value::String("B".repeat(20_000)));
            db.put_owned("archive", "a1", a1).expect("seed");
            let mut a2 = HakoDoc::default();
            a2.insert("kind", Value::String("memo".into()));
            db.put_owned("archive", "a2", a2).expect("seed");
            db.put_owned("hot", "h1", test_doc(1)).expect("seed");
            db.create_index("archive", "kind").expect("secondary");
            assert!(db.await_quiescent(std::time::Duration::from_secs(10)));
        }
        // Phase 2: reopen with the archive group lazy.
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.lazy_collections = vec!["archive*".to_string()];
        let db = Hako::open(&dir, cfg).expect("open");
        assert!(db.is_lazy_unloaded("archive"));
        assert!(!db.is_lazy_unloaded("hot"));
        // Listing is normal (hidden rule only); sync withholds unloaded.
        assert!(db.list_collections().unwrap().contains(&"archive".to_string()));
        let sync = db.sync_collections().unwrap();
        assert!(!sync.contains(&"archive".to_string()));
        assert!(sync.contains(&"hot".to_string()));
        assert_eq!(db.unloaded_lazy_collections(), vec!["archive".to_string()]);
        // Phase 3: touch loads fully (data + blob + RAM indexes).
        let got = db.get("archive", "a1").expect("get").expect("touched");
        assert_eq!(got.get("kind"), Some(&Value::String("report".into())));
        assert_eq!(got.get("big").map(|v| match v {
            Value::String(s) => s.len(),
            _ => 0,
        }), Some(20_000));
        assert!(!db.is_lazy_unloaded("archive"));
        assert!(db.await_quiescent(std::time::Duration::from_secs(10)));
        let enc = crate::index::index_key::encode_scalar(&Value::String("report".into()));
        let hits = db.indexes.read().unwrap()
            .lookup_secondary("archive", "kind", &enc)
            .unwrap_or_default();
        assert!(hits.iter().any(|id| id.as_ref() == "a1"));
        assert!(db.sync_collections().unwrap().contains(&"archive".to_string()));
        // Phase 4: evict â†’ withheld again, data intact, hot protected.
        db.unload_collection("archive").expect("unload");
        db.unload_collection("archive").expect("idempotent");
        assert!(db.is_lazy_unloaded("archive"));
        assert!(!db.sync_collections().unwrap().contains(&"archive".to_string()));
        assert!(db.unload_collection("hot").is_err());
        // Phase 5: backup covers the unopened archive bytes too.
        let bdir = std::env::temp_dir().join(format!("fl-lazybak-{nanos}"));
        db.backup(&bdir).expect("backup");
        assert!(bdir.join("archive").join("wal.log").exists());
        assert!(bdir.join("hot").join("wal.log").exists());
        // Phase 6: re-touch after evict â†’ identical data.
        let back = db.get("archive", "a2").expect("get").expect("reloaded");
        assert_eq!(back.get("kind"), Some(&Value::String("memo".into())));
        std::fs::remove_dir_all(&bdir).ok();
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }

    /// A pending index op is scoped to its collection: `cold` must stay on
    /// its index while `hot` waits for the worker. The write lock blocks the
    /// worker before it applies, making the window deterministic.
    #[test]
    fn index_readiness_is_collection_scoped() {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("fl-idxscope-{nanos}"));
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Hako::open(&dir, cfg).expect("open");
        assert!(
            db.await_quiescent(std::time::Duration::from_secs(30)),
            "initial quiescence"
        );

        let mut doc = HakoDoc::default();
        doc.insert("v", Value::Int(1));
        let indexes_guard = db.indexes.write().unwrap();
        assert!(db.enqueue_index_update(
            "hot".into(),
            Arc::new(vec![("d1".into(), Arc::new(doc))]),
            vec![],
        ));
        assert!(
            db.index_query_ready("cold"),
            "cold inherited hot's pending index op"
        );
        assert!(
            !db.index_query_ready("hot"),
            "hot planned before its index op applied"
        );
        drop(indexes_guard);

        assert!(db.await_quiescent(std::time::Duration::from_secs(30)));
        assert!(db.index_query_ready("hot"));
        drop(db);
        std::fs::remove_dir_all(&dir).ok();
    }
}