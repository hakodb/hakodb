use std::collections::BTreeMap;
use smallvec::SmallVec;
use std::sync::Arc;

use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;

use super::definition::CompositeIndexDefinition;
use super::key_encoder::encode_composite_key;

#[derive(Debug, Clone)]
pub struct CompositeIndex {
    pub definition: CompositeIndexDefinition,
    // pub tree: BTreeMap<Vec<u8>, String>,
    pub tree: BTreeMap<SmallVec<[u8; 32]>, Arc<str>>,
}

impl CompositeIndex {
    pub fn new(definition: CompositeIndexDefinition) -> Self {
        Self {
            definition,
            tree: BTreeMap::new(),
        }
    }

    pub fn document_values(&self, doc: &FireLiteDoc) -> Option<Vec<Value>> {
        let mut values = Vec::with_capacity(self.definition.fields.len());

        for f in &self.definition.fields {
            let v = doc.get(&f.field)?.clone();
            values.push(v);
        }

        Some(values)
    }

    pub fn index_document(&mut self, doc_id: &str, doc: &FireLiteDoc) {
        if let Some(values) = self.document_values(doc) {
            self.tree.insert(
                encode_composite_key(&self.definition, &values, doc_id),
                // doc_id.to_string(),
                Arc::from(doc_id),
            );
        }
    }

    pub fn index_batch<'a, I>(&mut self, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)>,
    {
        let new_entries = docs
            .into_iter()
            .filter_map(|(doc_id, doc)| {
                self.document_values(doc).map(|values| {
                    let key = encode_composite_key(&self.definition, &values, doc_id);
                    (key, Arc::from(doc_id))
                })
            })
            .collect::<Vec<_>>();

        self.tree.extend(new_entries);
    }

    pub fn remove_document(&mut self, doc_id: &str, doc: &FireLiteDoc) {
        if let Some(values) = self.document_values(doc) {
            self.tree
                .remove(&encode_composite_key(&self.definition, &values, doc_id));
        }
    }

    pub fn remove_batch<'a, I>(&mut self, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)>,
    {
        for (doc_id, doc) in docs {
            if let Some(values) = self.document_values(doc) {
                let key = encode_composite_key(&self.definition, &values, doc_id);
                self.tree.remove(&key);
            }
        }
    }

    pub fn range_scan(
        &self,
        start: &SmallVec<[u8; 32]>,
        end: &SmallVec<[u8; 32]>,
    ) -> Vec<Arc<str>> {

        self.tree
            .range::<SmallVec<[u8; 32]>, _>(start..=end)
            .map(|(_, doc_id)| doc_id.clone())
            .collect()
    }
}