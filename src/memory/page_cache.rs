use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BlockKey {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug)]
pub struct ProcessedBlock {
    pub data: Arc<Vec<u8>>,
}

#[derive(Debug)]
pub struct PageCache {
    max_bytes: usize,
    current_bytes: usize,
    order: VecDeque<BlockKey>,
    blocks: HashMap<BlockKey, ProcessedBlock>,
}

impl PageCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: 0,
            order: VecDeque::new(),
            blocks: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        if self.blocks.contains_key(key) {
            // Update MRU order
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                let k = self.order.remove(pos).unwrap();
                self.order.push_back(k);
            }
            return Some(Arc::clone(&self.blocks.get(key).unwrap().data));
        }
        None
    }

    pub fn put(&mut self, key: BlockKey, data: Vec<u8>) {
        let size = data.len();

        // If the single block is larger than our whole cache, don't cache it
        if size > self.max_bytes { return; }

        // 1. Evict until we have space
        while self.current_bytes + size > self.max_bytes && !self.order.is_empty() {
            if let Some(victim_key) = self.order.pop_front() {
                if let Some(block) = self.blocks.remove(&victim_key) {
                    self.current_bytes -= block.data.len();
                }
            }
        }

        // 2. Insert new block
        self.current_bytes += size;
        self.order.push_back(key.clone());
        self.blocks.insert(key, ProcessedBlock { data: Arc::new(data) });
    }
}