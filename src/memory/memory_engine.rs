use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::{Result, FireLiteError};
use crate::memory::page_cache::{BlockKey, PageCache};
use super::allocator::Allocator;
use super::mmap_store::MmapStore;
use super::page::DEFAULT_PAGE_SIZE;
// use super::page_cache::PageCache;

pub struct MemoryEngine {
    page_size: usize,
    store: Arc<super::mmap_store::MmapStore>,
    allocator: super::allocator::Allocator,
    cache: Mutex<PageCache>,
}

impl MemoryEngine {
    pub fn open(path: impl AsRef<Path>, size: usize, cache_capacity: usize) -> Result<Self> {
        Ok(Self {
            page_size: DEFAULT_PAGE_SIZE,
            store: Arc::new(MmapStore::open(path, size)?),
            allocator: Allocator::new(),
            cache: Mutex::new(PageCache::new(cache_capacity)),
        })
    }

    pub fn write_doc(&self, data: &[u8]) -> Result<usize> {
        let offset = self.allocator.allocate(data.len());
        if offset + data.len() > self.store.size {
            return Err(FireLiteError::InvalidInput("mmap exhausted".into()));
        }
        self.store.write_slice(offset, data);
        Ok(offset)
    }

    pub fn read_doc(&self, offset: usize, len: usize) -> Vec<u8> {
        self.store.read_slice(offset, len)
    }

    pub fn load_page(&self, page_id: u64) -> Vec<u8> {
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        // Using segment_id 0 to represent the MemoryEngine's linear address space
        let key = BlockKey { 
            segment_id: 0, 
            offset: page_id * self.page_size as u64 
        };

        if let Some(data) = cache.get(&key) {
            return (*data).clone();
        }

        let offset = page_id as usize * self.page_size;
        let data = self.store.read_slice(offset, self.page_size);
        cache.put(key, data.clone());
        data
    }
}
