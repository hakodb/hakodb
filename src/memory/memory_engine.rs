use std::sync::{Arc,RwLock};

use super::mmap_store::MmapStore;
use super::allocator::Allocator;
use super::page_cache::PageCache;

pub struct MemoryEngine {

    store:Arc<MmapStore>,

    allocator:Allocator,

    cache:RwLock<PageCache>,

}

impl MemoryEngine {

    pub fn open(
        path:&str,
        size:usize
    )->Self{

        let store = MmapStore::open(path,size).unwrap();

        Self{

            store:Arc::new(store),

            allocator:Allocator::new(),

            cache:RwLock::new(
                PageCache::new(1024)
            )

        }

    }

    pub fn write_doc(
        &self,
        data:&[u8]
    )->usize{

        let offset =
            self.allocator.allocate(data.len());

        self.store.write_slice(offset,data);

        offset

    }

    pub fn read_doc(
        &self,
        offset:usize,
        len:usize
    )->Vec<u8>{

        self.store.read_slice(offset,len)

    }

}
