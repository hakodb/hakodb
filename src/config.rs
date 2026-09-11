use std::collections::HashSet;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DurabilityMode {
    Always,
    Interval,
    Manual,
    OnCommit,
}

#[derive(Debug, Clone)]
pub struct FireLiteConfig {
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
/// WAL headroom reservation in bytes (0 = off, the default). When set,
/// preallocated ahead of the write position so steady-state appends never
/// extend the file (fewer tiny extensions => less fragmentation => cheaper
/// per-commit fsync on durable modes). Sparse: consumes no disk until
/// written. Measured: no delta on fast local disks (v0.7.12 A/B), but
/// decisive on cloud disks with slow metadata — Codespace Always singles
/// went 1417us to 855us wal phase (620 to 1111 WPS) with 16MB reserved.
/// Default stays 0 (phantom logical size + mobile storage); reach for
/// `--wal-reserve-mb` (benchmark) / `fl_config_set_wal_reserve_bytes`
/// when fdatasync dominates on network-attached storage. Ignored for Manual.
pub wal_reserve_bytes: u64,
}

impl Default for FireLiteConfig {
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
wal_reserve_bytes: 0,
        }
    }
}
