use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct BlockKey {
    pub segment_id: u64,
    pub offset: u64,
}

#[derive(Debug)] // <--- ADD THIS
pub struct ProcessedBlock {
    pub data: Arc<Vec<u8>>,
}

#[derive(Debug)]
pub struct PageCache {
    capacity: usize,
    order: VecDeque<BlockKey>,
    blocks: HashMap<BlockKey, ProcessedBlock>,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            blocks: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        if self.blocks.contains_key(key) {
            if let Some(pos) = self.order.iter().position(|k| k == key) {
                let k = self.order.remove(pos).unwrap();
                self.order.push_back(k);
            }
            return Some(Arc::clone(&self.blocks.get(key).unwrap().data));
        }
        None
    }

    pub fn put(&mut self, key: BlockKey, data: Vec<u8>) {
        if self.blocks.len() >= self.capacity {
            if let Some(victim) = self.order.pop_front() {
                self.blocks.remove(&victim);
            }
        }
        self.order.push_back(key.clone());
        self.blocks.insert(key, ProcessedBlock { data: Arc::new(data) });
    }
}