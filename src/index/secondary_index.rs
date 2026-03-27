use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Debug)]
pub struct SecondaryIndex {
    map: BTreeMap<Vec<u8>, BTreeSet<String>>,
}

impl SecondaryIndex {

    pub fn get_map(&self) -> &BTreeMap<Vec<u8>, BTreeSet<String>> {
        &self.map
    }

    pub fn insert(&mut self, key: Vec<u8>, doc_id: String) {
        self.map.entry(key).or_default().insert(doc_id);
    }

    pub fn remove(&mut self, key: &[u8], doc_id: &str) {
        if let Some(ids) = self.map.get_mut(key) {
            ids.remove(doc_id);
            if ids.is_empty() {
                self.map.remove(key);
            }
        }
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Vec<String> {
        self.map
            .range(start.to_vec()..=end.to_vec())
            .flat_map(|(_, ids)| ids.iter().cloned())
            .collect()
    }
}
