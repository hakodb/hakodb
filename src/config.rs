#[derive(Debug, Clone)]
pub struct FireLiteConfig {
    pub mmap_size: usize,
    pub page_size: usize,
    pub page_cache_capacity: usize,
    pub query_workers: usize,
    pub auto_compaction_threshold_bytes: usize,
}

impl Default for FireLiteConfig {
    fn default() -> Self {
        Self {
            mmap_size: 64 * 1024 * 1024,
            page_size: 4096,
            page_cache_capacity: 512,
            query_workers: 4,
            auto_compaction_threshold_bytes: 256 * 1024 * 1024,
        }
    }
}
