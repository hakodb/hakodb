use std::collections::BTreeMap;

use crate::document::firelite_doc::{FireLiteDoc, Value};
use crate::index::index_key::encode_index_key;

pub struct SecondaryIndex {

    pub collection_id: u32,
    pub field: String,

    tree: BTreeMap<Vec<u8>, String>,
}

impl SecondaryIndex {

    pub fn new(collection_id: u32, field: &str) -> Self {

        Self {
            collection_id,
            field: field.to_string(),
            tree: BTreeMap::new(),
        }
    }

    pub fn index_document(
        &mut self,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        if let Some(value) = doc.fields.get(&self.field) {

            let key =
                encode_index_key(
                    self.collection_id,
                    &self.field,
                    value,
                    doc_id
                );

            self.tree.insert(key, doc_id.to_string());
        }
    }

    pub fn remove_document(
        &mut self,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        if let Some(value) = doc.fields.get(&self.field) {

            let key =
                encode_index_key(
                    self.collection_id,
                    &self.field,
                    value,
                    doc_id
                );

            self.tree.remove(&key);
        }
    }

    pub fn find_equal(
        &self,
        value: &Value
    ) -> Vec<String> {

        let mut results = Vec::new();

        let start =
            encode_index_key(
                self.collection_id,
                &self.field,
                value,
                ""
            );

        for (k, v) in self.tree.range(start..) {

            if !k.starts_with(&start[..start.len()-1]) {
                break;
            }

            results.push(v.clone());
        }

        results
    }

    pub fn find_range(
        &self,
        min: &Value,
        max: &Value
    ) -> Vec<String> {

        let start =
            encode_index_key(
                self.collection_id,
                &self.field,
                min,
                ""
            );

        let end =
            encode_index_key(
                self.collection_id,
                &self.field,
                max,
                "\xff"
            );

        self.tree
            .range(start..=end)
            .map(|(_,v)| v.clone())
            .collect()
    }
}
