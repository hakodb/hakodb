use std::collections::{HashMap, VecDeque};
use std::path::Path;
use std::sync::{Arc, Mutex};

use crate::error::{FireLiteError, Result};

use super::allocator::Allocator;
use super::mmap_store::MmapStore;
use super::page::{Page, DEFAULT_PAGE_SIZE};

#[derive(Debug)]
struct LruPageCache {
    capacity: usize,
    order: VecDeque<u64>,
    pages: HashMap<u64, Page>,
}

impl LruPageCache {
    fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            pages: HashMap::new(),
        }
    }

    fn get(&mut self, id: u64) -> Option<Page> {
        if let Some(pos) = self.order.iter().position(|v| *v == id) {
            self.order.remove(pos);
            self.order.push_back(id);
        }
        self.pages.get(&id).cloned()
    }

    fn put(&mut self, page: Page) {
        if self.pages.contains_key(&page.id) {
            self.order.retain(|id| *id != page.id);
        }
        self.order.push_back(page.id);
        self.pages.insert(page.id, page);
        if self.pages.len() > self.capacity {
            if let Some(old) = self.order.pop_front() {
                self.pages.remove(&old);
            }
        }
    }
}

pub struct MemoryEngine {
    page_size: usize,
    store: Arc<MmapStore>,
    allocator: Allocator,
    cache: Mutex<LruPageCache>,
}

impl MemoryEngine {
    pub fn open(path: impl AsRef<Path>, size: usize, cache_capacity: usize) -> Result<Self> {
        Ok(Self {
            page_size: DEFAULT_PAGE_SIZE,
            store: Arc::new(MmapStore::open(path, size)?),
            allocator: Allocator::new(),
            cache: Mutex::new(LruPageCache::new(cache_capacity)),
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

    pub fn load_page(&self, page_id: u64) -> Page {
        let mut cache = self.cache.lock().expect("cache lock poisoned");
        if let Some(page) = cache.get(page_id) {
            return page;
        }
        let offset = page_id as usize * self.page_size;
        let data = self.store.read_slice(offset, self.page_size);
        let page = Page { id: page_id, data };
        cache.put(page.clone());
        page
    }
}
