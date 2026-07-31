use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};
use std::collections::hash_map::DefaultHasher;

pub struct ConsistentHashRing {
    vnodes_per_node: usize,
    ring: BTreeMap<u64, usize>,
}

impl ConsistentHashRing {
    pub fn new() -> Self {
        Self {
            vnodes_per_node: 100,
            ring: BTreeMap::new(),
        }
    }

    pub fn add_node(&mut self, node_id: usize) {
        for vnode in 0..self.vnodes_per_node {
            let key = format!("NODE_{}_VNODE_{}", node_id, vnode);
            let hash = Self::hash_key(&key);
            self.ring.insert(hash, node_id);
        }
    }

    pub fn get_node(&self, key: &str) -> usize {
        if self.ring.is_empty() {
            return 0;
        }
        let hash = Self::hash_key(key);
        if let Some((_, &node_id)) = self.ring.range(hash..).next() {
            node_id
        } else {
            *self.ring.values().next().unwrap()
        }
    }

    fn hash_key(key: &str) -> u64 {
        let mut hasher = DefaultHasher::new();
        key.hash(&mut hasher);
        hasher.finish()
    }
}