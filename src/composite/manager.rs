use std::collections::HashMap;

use crate::document::firelite_doc::FireLiteDoc;

use super::definition::CompositeIndexDefinition;
use super::composite_index::CompositeIndex;

pub struct CompositeIndexManager {

    indexes: HashMap<u32, Vec<CompositeIndex>>,
}

impl CompositeIndexManager {

    pub fn new() -> Self {

        Self {
            indexes: HashMap::new(),
        }
    }

    pub fn create_index(
        &mut self,
        definition: CompositeIndexDefinition,
    ) {

        let cid = definition.collection_id;

        let index = CompositeIndex::new(definition);

        self.indexes
            .entry(cid)
            .or_insert(Vec::new())
            .push(index);
    }

    pub fn index_document(
        &mut self,
        collection_id: u32,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        if let Some(indexes) = self.indexes.get_mut(&collection_id) {

            for index in indexes {

                index.index_document(doc_id, doc);
            }
        }
    }

    pub fn remove_document(
        &mut self,
        collection_id: u32,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        if let Some(indexes) = self.indexes.get_mut(&collection_id) {

            for index in indexes {

                index.remove_document(doc_id, doc);
            }
        }
    }

    pub fn get_indexes(
        &self,
        collection_id: u32,
    ) -> Option<&Vec<CompositeIndex>> {

        self.indexes.get(&collection_id)
    }
}
