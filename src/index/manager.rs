use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use std::sync::Arc;

use super::composite::definition::CompositeIndexDefinition;
use super::composite::manager::CompositeIndexManager;

#[derive(Default)]
pub struct IndexManager {
    composite: CompositeIndexManager,
}

impl IndexManager {
    pub fn create_index(&mut self, definition: CompositeIndexDefinition) -> u32 {
        self.composite.create_index(definition)
    }

    pub fn has_index(&self, collection: &str, fields: &[String]) -> bool {
        self.composite
            .indexes_for_collection(collection)
            .any(|idx| {
                idx.definition
                    .fields
                    .iter()
                    .map(|f| f.field.as_str())
                    .eq(fields.iter().map(String::as_str))
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
        self.composite.index_document(collection, doc_id, doc);
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
}
