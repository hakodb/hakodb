use std::collections::{BTreeMap, BTreeSet};

#[derive(Default, Debug)]
pub struct InvertedIndex {
    // Word -> Set of Document IDs
    pub map: BTreeMap<String, BTreeSet<String>>,
}

impl InvertedIndex {
    pub fn insert(&mut self, text: &str, doc_id: String) {
        for word in self.tokenize(text) {
            self.map.entry(word).or_default().insert(doc_id.clone());
        }
    }

    pub fn remove(&mut self, text: &str, doc_id: &str) {
        for word in self.tokenize(text) {
            if let Some(ids) = self.map.get_mut(&word) {
                ids.remove(doc_id);
                if ids.is_empty() {
                    self.map.remove(&word);
                }
            }
        }
    }

    pub fn search(&self, query_text: &str) -> Option<BTreeSet<String>> {
        let words = self.tokenize(query_text);
        if words.is_empty() { return None; }

        let mut results: Option<BTreeSet<String>> = None;

        for word in words {
            if let Some(ids) = self.map.get(&word) {
                if let Some(ref mut current_set) = results {
                    // Intersection: Must contain ALL words in the query
                    *current_set = current_set.intersection(ids).cloned().collect();
                } else {
                    results = Some(ids.clone());
                }
            } else {
                // One word not found = no matches for the whole phrase
                return None;
            }
        }
        results
    }

    fn tokenize(&self, text: &str) -> Vec<String> {
        text.to_lowercase()
            .split(|c: char| !c.is_alphanumeric())
            .filter(|s| s.len() > 1) // Ignore single letters/stop words
            .map(|s| s.to_string())
            .collect()
    }
}