use std::collections::HashMap;

use crate::document::firelite_doc::Value;

use super::index_key::encode_composite_key;
use super::secondary_index::SecondaryIndex;

#[derive(Default)]
pub struct IndexManager {
    // (collection, fields joined by \x1f) -> index
    indexes: HashMap<(String, String), SecondaryIndex>,
}

impl IndexManager {
    pub fn create_composite_index(&mut self, collection: &str, fields: &[String]) {
        self.indexes
            .entry((collection.to_string(), fields.join("\x1f")))
            .or_default();
    }

    pub fn has_index(&self, collection: &str, fields: &[String]) -> bool {
        self.indexes
            .contains_key(&(collection.to_string(), fields.join("\x1f")))
    }

    pub fn index_doc(
        &mut self,
        collection: &str,
        fields: &[String],
        values: &[Value],
        doc_id: &str,
    ) {
        if let Some(index) = self
            .indexes
            .get_mut(&(collection.to_string(), fields.join("\x1f")))
        {
            index.insert(encode_composite_key(values, doc_id), doc_id.to_string());
        }
    }

    pub fn range_scan(
        &self,
        collection: &str,
        fields: &[String],
        start: &[u8],
        end: &[u8],
    ) -> Vec<String> {
        self.indexes
            .get(&(collection.to_string(), fields.join("\x1f")))
            .map(|idx| idx.range_scan(start, end))
            .unwrap_or_default()
    }
}
