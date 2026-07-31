use std::collections::BTreeMap;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

#[derive(Clone)]
pub struct ConsistentHashRing {
    nodes: BTreeMap<u64, String>,
}

impl ConsistentHashRing {
    pub fn new(nodes: &[String], vnodes: usize) -> Self {
        let mut ring = BTreeMap::new();
        for node in nodes {
            for vnode_id in 0..vnodes {
                let mut hasher = DefaultHasher::new();
                format!("{}-vnode-{}", node, vnode_id).hash(&mut hasher);
                ring.insert(hasher.finish(), node.clone());
            }
        }
        Self { nodes: ring }
    }

    pub fn get_owner_node(&self, doc_id: &str) -> Option<String> {
        if self.nodes.is_empty() {
            return None;
        }

        let mut hasher = DefaultHasher::new();
        doc_id.hash(&mut hasher);
        let hash = hasher.finish();

        match self.nodes.range(hash..).next() {
            Some((_, node_uri)) => Some(node_uri.clone()),
            None => self.nodes.values().next().cloned(),
        }
    }
}