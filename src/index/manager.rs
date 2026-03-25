use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use std::sync::Arc;
use std::collections::HashMap;

use super::composite::definition::CompositeIndexDefinition;
use super::composite::manager::CompositeIndexManager;
use super::inverted_index::InvertedIndex;
use crate::index::composite::composite_index::CompositeIndex;
use crate::index::secondary_index::SecondaryIndex;

#[derive(Default)]
pub struct IndexManager {
    pub composite: CompositeIndexManager,
    pub secondary: HashMap<String, HashMap<String, SecondaryIndex>>,
    pub fts: HashMap<String, HashMap<String, InvertedIndex>>, 
}

impl IndexManager {
    pub fn create_fts_index(&mut self, collection: &str, field: &str) {
        self.fts.entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), InvertedIndex::default());
    }

    pub fn indexes_for_collection(&self, collection: &str) -> impl Iterator<Item = &CompositeIndex> {
        self.composite.indexes_for_collection(collection)
    }

    pub fn create_index(&mut self, definition: CompositeIndexDefinition) -> u32 {
        self.composite.create_index(definition)
    }

    pub fn has_index(&self, collection: &str, fields: &[String]) -> bool {
        if fields.is_empty() { return false; }
        
        self.composite
            .indexes_for_collection(collection)
            .any(|idx| {
                let idx_fields: Vec<&str> = idx.definition.fields.iter()
                    .map(|f| f.field.as_str()).collect();

                if idx_fields.len() < fields.len() {
                    return false;
                }

                // FIX: Check if every field required by the query exists in the index's prefix.
                let prefix = &idx_fields[0..fields.len()];
                fields.iter().all(|f| prefix.contains(&f.as_str()))
            })
    }

    pub fn exact_match_doc_ids(
        &self,
        collection: &str,
        fields: &[String],
        values: &[Value],
    // ) -> Option<Vec<String>> {
    ) -> Option<Vec<Arc<str>>> {
        self.composite
            .exact_match_doc_ids(collection, fields, values)
    }

    pub fn index_document(&mut self, collection: &str, doc_id: &str, doc: &FireLiteDoc) {
        // 1. Update Composite
        self.composite.index_document(collection, doc_id, doc);
        
        // 2. Update FTS
        if let Some(fields) = self.fts.get_mut(collection) {
            for (field_name, index) in fields.iter_mut() {
                if let Some(Value::String(text)) = doc.get(field_name) {
                    index.insert(text, doc_id.to_string());
                }
            }
        }

        // 3. ADD THIS: Update Secondary Indexes automatically
        if let Some(sec_map) = self.secondary.get_mut(collection) {
            for (field_name, index) in sec_map.iter_mut() {
                if let Some(val) = doc.get(field_name) {
                    index.insert(crate::index::index_key::encode_scalar(val), doc_id.to_string());
                }
            }
        }
    }

    pub fn index_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)> + Clone,
    {
        self.composite.index_batch(collection, docs)
    }

    pub fn remove_document(&mut self, collection: &str, doc_id: &str, doc: &FireLiteDoc) {
        self.composite.remove_document(collection, doc_id, doc);
    }

    pub fn remove_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)> + Clone,
    {
        self.composite.remove_batch(collection, docs)
    }

        /// Registers a new single-field index
    pub fn create_secondary_index(&mut self, collection: &str, field: &str) {
        self.secondary
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), SecondaryIndex::default());
    }

    /// Optimized lookup: Check secondary indexes if no composite exists
    pub fn lookup_secondary(&self, collection: &str, field: &str, value: &[u8]) -> Option<Vec<String>> {
        self.secondary.get(collection)?
            .get(field)?
            .range_scan(value, value) // Exact match scan
            .into()
    }
}
