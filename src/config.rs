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
    pub enable_audit_log: bool,
    pub audit_log_path: Option<String>,
    pub max_inlined_memory_bytes: usize,
    pub use_compression: bool,
    pub compression_level: i32,
    pub value_blob_threshold_bytes: usize,
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
            enable_audit_log: true,
            audit_log_path: None,
            max_inlined_memory_bytes: 64 * 1024 * 1024, // 64MB Default
            use_compression: false, // Disabled by default
            compression_level: 3,
            value_blob_threshold_bytes: 16 * 1024,
        }
    }
}
