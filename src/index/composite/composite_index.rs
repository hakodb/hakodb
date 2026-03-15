use std::collections::BTreeMap;

use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;

use super::definition::CompositeIndexDefinition;
use super::key_encoder::encode_composite_key;

#[derive(Debug, Clone)]
pub struct CompositeIndex {
    pub definition: CompositeIndexDefinition,
    pub tree: BTreeMap<Vec<u8>, String>,
}

impl CompositeIndex {
    pub fn new(definition: CompositeIndexDefinition) -> Self {
        Self {
            definition,
            tree: BTreeMap::new(),
        }
    }

    pub fn document_values(&self, doc: &FireLiteDoc) -> Option<Vec<Value>> {
        self.definition
            .fields
            .iter()
            .map(|f| doc.fields.get(&f.field).cloned())
            .collect()
    }

    pub fn index_document(&mut self, doc_id: &str, doc: &FireLiteDoc) {
        if let Some(values) = self.document_values(doc) {
            self.tree.insert(
                encode_composite_key(&self.definition, &values, doc_id),
                doc_id.to_string(),
            );
        }
    }

    pub fn remove_document(&mut self, doc_id: &str, doc: &FireLiteDoc) {
        if let Some(values) = self.document_values(doc) {
            self.tree
                .remove(&encode_composite_key(&self.definition, &values, doc_id));
        }
    }

    pub fn range_scan(&self, start: &[u8], end: &[u8]) -> Vec<String> {
        self.tree
            .range(start.to_vec()..=end.to_vec())
            .map(|(_, doc_id)| doc_id.clone())
            .collect()
    }
}
