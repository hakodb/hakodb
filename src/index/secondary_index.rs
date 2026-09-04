use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use serde::{Serialize, Deserialize};

#[derive(Default, Debug, Serialize, Deserialize)]
pub struct SecondaryIndex {
    map: BTreeMap<Vec<u8>, BTreeSet<Arc<str>>>,
}

impl SecondaryIndex {

    pub fn get_map(&self) -> &BTreeMap<Vec<u8>, BTreeSet<Arc<str>>> {
        &self.map
    }

    pub fn insert(&mut self, key: Vec<u8>, doc_id: String) {
        self.map.entry(key).or_default().insert(Arc::from(doc_id));
    }

    /// ponytail: borrow the key — hot values (tenant-2, active=true) hit the
    /// existing entry and never allocate a key Vec. The doc id rides an Arc
    /// shared across every secondary field of the same doc.
    pub fn insert_borrowed(&mut self, key: &[u8], doc_id: Arc<str>) {
        if let Some(ids) = self.map.get_mut(key) {
            ids.insert(doc_id);
        } else {
            self.map.entry(key.to_vec()).or_default().insert(doc_id);
        }
    }

    pub fn remove(&mut self, key: &[u8], doc_id: &str) {
        if let Some(ids) = self.map.get_mut(key) {
            ids.remove(doc_id);
            if ids.is_empty() {
                self.map.remove(key);
            }
        }
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Vec<Arc<str>> {
        self.map
            .range(start.to_vec()..=end.to_vec())
            .flat_map(|(_, ids)| ids.iter().cloned())
            .collect()
    }
}
