use std::collections::HashMap;

use crate::document::firelite_doc::FireLiteDoc;

use super::composite_index::CompositeIndex;
use super::definition::CompositeIndexDefinition;

#[derive(Default)]
pub struct CompositeIndexManager {
    next_id: u32,
    by_id: HashMap<u32, CompositeIndex>,
    by_collection: HashMap<String, Vec<u32>>,
}

impl CompositeIndexManager {
    pub fn create_index(&mut self, mut definition: CompositeIndexDefinition) -> u32 {
        self.next_id += 1;
        definition.id = self.next_id;
        let id = definition.id;
        self.by_collection
            .entry(definition.collection.clone())
            .or_default()
            .push(id);
        self.by_id.insert(id, CompositeIndex::new(definition));
        id
    }

    pub fn get(&self, id: u32) -> Option<&CompositeIndex> {
        self.by_id.get(&id)
    }

    pub fn get_mut(&mut self, id: u32) -> Option<&mut CompositeIndex> {
        self.by_id.get_mut(&id)
    }

    pub fn indexes_for_collection(
        &self,
        collection: &str,
    ) -> impl Iterator<Item = &CompositeIndex> {
        self.by_collection
            .get(collection)
            .into_iter()
            .flat_map(|ids| ids.iter())
            .filter_map(|id| self.by_id.get(id))
    }

    pub fn index_document(&mut self, collection: &str, doc_id: &str, doc: &FireLiteDoc) {
        let ids = self
            .by_collection
            .get(collection)
            .cloned()
            .unwrap_or_default();
        for id in ids {
            if let Some(index) = self.by_id.get_mut(&id) {
                index.index_document(doc_id, doc);
            }
        }
    }

    pub fn remove_document(&mut self, collection: &str, doc_id: &str, doc: &FireLiteDoc) {
        let ids = self
            .by_collection
            .get(collection)
            .cloned()
            .unwrap_or_default();
        for id in ids {
            if let Some(index) = self.by_id.get_mut(&id) {
                index.remove_document(doc_id, doc);
            }
        }
    }
}
