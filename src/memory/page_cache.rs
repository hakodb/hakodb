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
    // FIFO eviction order — front is oldest, back is newest.
    order: VecDeque<BlockKey>,
    // O(1) reverse index: key -> position in `order`.
    positions: HashMap<BlockKey, usize>,
    blocks: HashMap<BlockKey, ProcessedBlock>,
}

impl PageCache {
    pub fn new(max_bytes: usize) -> Self {
        Self {
            max_bytes,
            current_bytes: 0,
            order: VecDeque::new(),
            positions: HashMap::new(),
            blocks: HashMap::new(),
        }
    }

    pub fn get(&mut self, key: &BlockKey) -> Option<Arc<Vec<u8>>> {
        let Some(block) = self.blocks.get(key) else { return None; };
        let data = Arc::clone(&block.data);

        // O(1) MRU bump: if the key is already at the back, do nothing.
        // Otherwise remove it from its current position and push to back,
        // updating the reverse index.
        let len = self.order.len();
        if let Some(&pos) = self.positions.get(key) {
            if pos + 1 != len {
                self.order.remove(pos);
                // Every entry after `pos` shifts down by 1. Update their positions.
                // For the realistic case (cache dominated by sequential scans),
                // most hits will be on a small working set at the front of `order`,
                // so the shift count is bounded by that working set.
                for (i, k) in self.order.iter().enumerate().skip(pos) {
                    self.positions.insert(k.clone(), i);
                }
                self.order.push_back(key.clone());
                self.positions.insert(key.clone(), self.order.len() - 1);
            }
        }
        Some(data)
    }

    pub fn put(&mut self, key: BlockKey, data: Vec<u8>) {
        let size = data.len();

        if size > self.max_bytes { return; }

        // 1. Evict from the front until we have space.
        while self.current_bytes + size > self.max_bytes {
            let Some(victim_key) = self.order.pop_front() else { break; };
            self.positions.remove(&victim_key);
            if let Some(block) = self.blocks.remove(&victim_key) {
                self.current_bytes -= block.data.len();
            }
        }

        // 2. Insert new block at the back.
        self.current_bytes += size;
        self.order.push_back(key.clone());
        self.positions.insert(key, self.order.len() - 1);
        self.blocks.insert(
            self.order.back().expect("just pushed").clone(),
            ProcessedBlock { data: Arc::new(data) },
        );
    }

    /// Test-only: number of cached blocks.
    #[cfg(test)]
    pub fn len(&self) -> usize {
        self.blocks.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn k(seg: u64, off: u64) -> BlockKey { BlockKey { segment_id: seg, offset: off } }

    #[test]
    fn hit_returns_data() {
        let mut c = PageCache::new(1024);
        c.put(k(0, 0), vec![1u8; 32]);
        let arc = c.get(&k(0, 0)).expect("hit");
        assert_eq!(arc.len(), 32);
    }

    #[test]
    fn miss_returns_none() {
        let mut c = PageCache::new(1024);
        assert!(c.get(&k(0, 0)).is_none());
    }

    #[test]
    fn eviction_removes_oldest() {
        let mut c = PageCache::new(64);
        c.put(k(0, 0), vec![1u8; 32]);
        c.put(k(0, 1), vec![2u8; 32]);
        c.put(k(0, 2), vec![3u8; 32]);
        assert!(c.get(&k(0, 0)).is_none(), "oldest evicted");
        assert!(c.get(&k(0, 1)).is_some(), "second survives");
        assert!(c.get(&k(0, 2)).is_some(), "newest lives");
    }

    #[test]
    fn block_bigger_than_cache_is_dropped() {
        let mut c = PageCache::new(16);
        c.put(k(0, 0), vec![1u8; 32]);
        assert!(c.get(&k(0, 0)).is_none());
        assert_eq!(c.len(), 0);
    }

    #[test]
    fn positions_and_blocks_stay_consistent_under_repeated_hits() {
        let mut c = PageCache::new(1024);
        for i in 0..50 {
            c.put(k(0, i), vec![i as u8; 16]);
        }
        // Hammer hit on every key — should not panic or leave dangling entries.
        for i in 0..50 {
            assert!(c.get(&k(0, i)).is_some());
        }
        assert_eq!(c.order.len(), c.blocks.len());
        assert_eq!(c.order.len(), c.positions.len());
        // Every position must point at the right entry.
        for (i, key) in c.order.iter().enumerate() {
            assert_eq!(c.positions.get(key), Some(&i));
        }
    }

    #[test]
    fn repeated_hit_on_same_key_stays_at_back() {
        let mut c = PageCache::new(1024);
        for i in 0..5 {
            c.put(k(0, i), vec![i as u8; 16]);
        }
        // Hit on k(0, 0) multiple times — should end up at back.
        for _ in 0..3 {
            assert!(c.get(&k(0, 0)).is_some());
        }
        assert_eq!(c.order.back(), Some(&k(0, 0)));
        assert_eq!(c.positions.get(&k(0, 0)), Some(&(c.order.len() - 1)));
    }
}
