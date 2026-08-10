use std::collections::{BTreeMap, BTreeSet};
use serde::{Serialize, Deserialize}; 

#[derive(Default, Debug, Serialize, Deserialize)]
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

    /// Word-prefix search (autocomplete) over the term map.
    ///
    /// Because the term map is a `BTreeMap`, each query word maps to one
    /// lexicographic range scan (`prefix` .. successor); the doc-id sets of all
    /// matching terms are unioned, and the per-word results are intersected so
    /// multi-word queries keep AND semantics, exactly like [`InvertedIndex::search`].
    /// `"indom"` therefore matches the term `"indomie"`. The caller is expected to
    /// apply its own result cap; the executor honours `limit`.
    pub fn search_prefix(&self, query_text: &str) -> Option<BTreeSet<String>> {
        let words = self.tokenize(query_text);
        if words.is_empty() {
            return None;
        }

        let mut results: Option<BTreeSet<String>> = None;

        for word in words {
            // Exclusive upper bound: `prefix + '\u{10FFFF}'` is strictly greater
            // than every term starting with `prefix`, because the maximum Unicode
            // scalar can never occur inside a token (tokenization splits on
            // non-alphanumerics). Using owned String bounds keeps the range type
            // unambiguous.
            let mut end_bound = word.clone();
            end_bound.push('\u{10FFFF}');

            let mut word_ids: BTreeSet<String> = BTreeSet::new();
            for (_term, ids) in self.map.range(word.clone()..end_bound) {
                word_ids.extend(ids.iter().cloned());
            }
            if word_ids.is_empty() {
                // One word has no term with this prefix => no match at all.
                return None;
            }
            if let Some(ref mut current_set) = results {
                *current_set = current_set.intersection(&word_ids).cloned().collect();
                if current_set.is_empty() {
                    return Some(current_set.clone());
                }
            } else {
                results = Some(word_ids);
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn prefix_matches_whole_words() {
        let mut idx = InvertedIndex::default();
        idx.insert("Indomie Goreng", "d1".to_string());
        idx.insert("Indomie Rendang", "d2".to_string());
        idx.insert("Mie Sedap Ayam", "d3".to_string());

        assert_eq!(
            idx.search_prefix("indom"),
            Some(BTreeSet::from(["d1".into(), "d2".into()]))
        );
        assert_eq!(idx.search_prefix("mie"), Some(BTreeSet::from(["d3".into()])));
        assert_eq!(
            idx.search_prefix("indomie goreng"),
            Some(BTreeSet::from(["d1".into()]))
        );
        // AND semantics: an unmatched prefix yields no results.
        assert!(idx.search_prefix("nope").is_none());
        assert!(idx.search_prefix("goreng nope").is_none());
    }

    #[test]
    fn prefix_bound_is_exclusive_and_strict() {
        let mut idx = InvertedIndex::default();
        idx.insert("abc def", "d1".to_string());
        idx.insert("abdg hmm", "d2".to_string());
        // "ab" must match both "abc" and "abdg"; "abd" only "abdg".
        assert_eq!(
            idx.search_prefix("ab"),
            Some(BTreeSet::from(["d1".into(), "d2".into()]))
        );
        assert_eq!(idx.search_prefix("abd"), Some(BTreeSet::from(["d2".into()])));
    }
}