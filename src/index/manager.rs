use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
// use std::collections::HashMap;
use hashbrown::HashMap;
use std::sync::Arc;

use super::composite::definition::CompositeIndexDefinition;
use super::composite::manager::CompositeIndexManager;
use super::inverted_index::InvertedIndex;
use crate::index::composite::composite_index::CompositeIndex;
use crate::index::secondary_index::SecondaryIndex;

use crate::error::HakoError;

#[derive(Default)]
pub struct IndexManager {
    pub composite: CompositeIndexManager,
    pub secondary: HashMap<String, HashMap<String, SecondaryIndex>>,
    pub fts: HashMap<String, HashMap<String, InvertedIndex>>,
}

impl IndexManager {
    pub fn create_fts_index(&mut self, collection: &str, field: &str) {
        self.fts
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), InvertedIndex::default());
    }

    pub fn indexes_for_collection(
        &self,
        collection: &str,
    ) -> impl Iterator<Item = &CompositeIndex> {
        self.composite.indexes_for_collection(collection)
    }

    pub fn create_index(&mut self, definition: CompositeIndexDefinition) -> u32 {
        self.composite.create_index(definition)
    }

    pub fn has_index(&self, collection: &str, fields: &[String]) -> bool {
        if fields.is_empty() {
            return false;
        }

        self.composite
            .indexes_for_collection(collection)
            .any(|idx| {
                // An index can satisfy a query if the query fields 
                // are the leading prefix of the index fields.
                if idx.definition.fields.len() < fields.len() {
                    return false;
                }

                for i in 0..fields.len() {
                    if idx.definition.fields[i].field != fields[i] {
                        return false;
                    }
                }
                true
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

    pub fn index_document(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
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
        // ponytail: one shared id Arc across every field of this doc (was a
        // String clone per field), keys encoded into a reused scratch buffer
        // with zero-alloc hits for hot values via `insert_borrowed`, and no
        // deep Value clone — `encode_scalar_into` borrows straight from the
        // doc (the old code cloned every indexed String value per write).
        if let Some(sec_map) = self.secondary.get_mut(collection) {
            let id_shared: Arc<str> = Arc::from(doc_id);
            crate::index::index_key::ENC_SCRATCH.with(|scratch| {
                let mut enc = scratch.borrow_mut();
                for (field_name, index) in sec_map.iter_mut() {
                    enc.clear();
                    if field_name.as_str() == "id" {
                        crate::index::index_key::encode_str_scalar_into(doc_id, &mut enc);
                    } else if field_name.as_str() == "_time" {
                        crate::index::index_key::encode_scalar_into(&Value::Int(doc._time), &mut enc);
                    } else if let Some(val) = doc.get(field_name) {
                        crate::index::index_key::encode_scalar_into(val, &mut enc);
                    } else {
                        continue;
                    }
                    index.insert_borrowed(&enc, id_shared.clone());
                }
            });
        }
    }

    pub fn index_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        for (doc_id, doc) in docs {
            self.index_document(collection, doc_id, doc);
        }
    }

    pub fn remove_document(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
        self.composite.remove_document(collection, doc_id, doc);

        if let Some(fields) = self.fts.get_mut(collection) {
            for (field_name, index) in fields.iter_mut() {
                if let Some(Value::String(text)) = doc.get(field_name) {
                    index.remove(text, doc_id);
                }
            }
        }

        if let Some(sec_map) = self.secondary.get_mut(collection) {
            for (field_name, index) in sec_map.iter_mut() {
                if let Some(val) = doc.get(field_name) {
                    index.remove(&crate::index::index_key::encode_scalar(val), doc_id);
                }
            }
        }
    }

    pub fn remove_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        for (doc_id, doc) in docs {
            self.remove_document(collection, doc_id, doc);
        }
    }

    /// Registers a new single-field index
    pub fn create_secondary_index(&mut self, collection: &str, field: &str) {
        self.secondary
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), SecondaryIndex::default());
    }

    /// Optimized lookup: Check secondary indexes if no composite exists
    pub fn lookup_secondary(
        &self,
        collection: &str,
        field: &str,
        value: &[u8],
    ) -> Option<Vec<Arc<str>>> {
        self.secondary
            .get(collection)?
            .get(field)?
            .range_scan(value, value) // Exact match scan
            .into()
    }

    pub fn export_state(&self) -> Result<Vec<u8>, HakoError> {
        bincode::serialize(&(&self.secondary, &self.fts))
            .map_err(|e| HakoError::Corrupt(format!("Index export failed: {}", e)))
    }

    // pub fn import_state(&mut self, bytes: &[u8]) -> Result<(), HakoError> {
    //     // By importing hashbrown::HashMap at the top, 'HashMap' here 
    //     // now correctly refers to the hashbrown version.
    //     let (sec, fts): (
    //         HashMap<String, HashMap<String, crate::index::secondary_index::SecondaryIndex>>,
    //         HashMap<String, HashMap<String, crate::index::inverted_index::InvertedIndex>>
    //     ) = bincode::deserialize(bytes)
    //         .map_err(|e| HakoError::Corrupt(format!("Index import failed: {}", e)))?;
        
    //     self.secondary = sec;
    //     self.fts = fts;
    //     Ok(())
    // }
     pub fn import_state(&mut self, bytes: &[u8]) -> Result<(), HakoError> {
        let (sec, fts): (
            HashMap<String, HashMap<String, crate::index::secondary_index::SecondaryIndex>>,
            HashMap<String, HashMap<String, crate::index::inverted_index::InvertedIndex>>
        ) = bincode::deserialize(bytes)
            .map_err(|e| HakoError::Corrupt(format!("Index import failed: {}", e)))?;
        
        // Merge the RAM data into existing definitions instead of blindly overwriting
        for (col, fields) in sec {
            let col_map = self.secondary.entry(col).or_default();
            for (field, index) in fields {
                if let Some(existing_index) = col_map.get_mut(&field) {
                    *existing_index = index;
                }
            }
        }

        for (col, fields) in fts {
            let col_map = self.fts.entry(col).or_default();
            for (field, index) in fields {
                if let Some(existing_index) = col_map.get_mut(&field) {
                    *existing_index = index;
                }
            }
        }

        Ok(())
    }
}
