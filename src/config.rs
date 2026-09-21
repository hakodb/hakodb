use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    Always,
    Interval,
    Manual,
    OnCommit,
}

#[derive(Debug, Clone)]
pub struct HakoConfig {
    pub mmap_size: usize,
    pub page_size: usize,
    pub page_cache_capacity: usize,
    pub query_workers: usize,
    pub auto_compaction_threshold_bytes: usize,
    pub durability_mode: DurabilityMode,
    pub group_commit_max_ops: usize,
    pub encryption_key: Option<String>,
    pub encrypted_cols: Option<HashSet<String>>,
    pub enable_audit_log: bool,
    pub audit_log_path: Option<String>,
    pub max_inlined_memory_bytes: usize,
    pub use_compression: bool,
    pub compression_level: i32,
pub value_blob_threshold_bytes: usize,
pub replication_collections: Option<Vec<String>>,
/// WAL headroom reservation in bytes (default 4MB since v0.8.18).
/// When set, preallocated ahead of the write position so steady-state
/// appends never extend the file (fewer tiny extensions => less
/// fragmentation => cheaper per-commit fsync on durable modes). Sparse:
/// consumes no disk until written. Measured: no delta on fast local
/// disks (v0.7.12 A/B), but decisive on cloud disks with slow
/// file-growth metadata — Codespace Always singles went 1417us to
/// 855us wal phase with 16MB reserved, and 2MB performs identically
/// (782us) while no-reserve never broke 800 across 5-6 runs. The effect
/// is presence-not-size, so 4MB covers typical runs for ~4MB logical
/// size (internal collections still skip it). Override per workload
/// with `--wal-reserve-mb` (benchmark) / `hk_config_set_wal_reserve_bytes`.
/// Re-measured after the single-write flush fix (local Windows, fast disk,
/// --no-maintenance, 3 runs/arm): no reserve delta locally, consistent
/// with v0.7.12 — the Codespace figures above are cloud-disk-specific
/// (and predate the flush fix, so were measured at 2x WAL bytes).
/// Ignored for Manual.
pub wal_reserve_bytes: u64,
/// Background maintenance (5s system tick: checkpoint, compaction,
/// tombstone purge, index snapshots). Disable for deterministic
/// benchmarking or hard latency bounds — the engine stays correct
/// (writes/reads never depend on it), but the WAL/blob files grow until
/// re-enabled and maintenance runs. Default true.
pub background_maintenance: bool,
/// Extra sync-excluded collection names for this deployment, merged over
/// the builtin `SYNC_EXCLUDED_COLLECTIONS`. Sync is opt-OUT, not opt-in:
/// every collection on disk (including `_`-hidden ones) replicates unless
/// it is excluded here or builtin-excluded. Use this to pin down server
/// planes (e.g. cloudserver declares its room/user/group stores here so
/// the guarantee never depends on naming conventions). Default empty.
pub sync_excluded: Vec<String>,
}

impl Default for HakoConfig {
    fn default() -> Self {
        Self {
            mmap_size: 256 * 1024 * 1024,
            page_size: 4096,
            page_cache_capacity: 8192,
            query_workers: 4,
            auto_compaction_threshold_bytes: 8 * 1024 * 1024,
            durability_mode: DurabilityMode::Interval,
            group_commit_max_ops: 128,
            encryption_key: None,
            encrypted_cols: None,
            enable_audit_log: false,
            audit_log_path: None,
            max_inlined_memory_bytes: 64 * 1024 * 1024, // 64MB Default
            use_compression: false, // Disabled by default
            compression_level: 3,
value_blob_threshold_bytes: 16 * 1024,
replication_collections: None,
wal_reserve_bytes: 4 * 1024 * 1024,
background_maintenance: true,
sync_excluded: Vec::new(),
        }
    }
}
