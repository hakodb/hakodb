use hashbrown::HashMap;
use std::sync::Arc;

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;

use super::composite_index::CompositeIndex;
use super::definition::CompositeIndexDefinition;
use super::range_builder::build_prefix_range;

#[derive(Default, serde::Serialize, serde::Deserialize)]
pub struct CompositeIndexManager {
    next_id: u32,
    by_id: HashMap<u32, CompositeIndex>,
    by_collection: HashMap<String, Vec<u32>>,
}

impl CompositeIndexManager {
    /// Clear the ENTRIES of every tree registered for `collection`,
    /// keeping definitions and registrations (rescan refills them).
    pub fn clear_collection(&mut self, collection: &str) {
        if let Some(ids) = self.by_collection.get(collection) {
            for id in ids.clone() {
                if let Some(idx) = self.by_id.get_mut(&id) {
                    idx.clear();
                }
            }
        }
    }

    /// True when any tree is registered for `collection`.
    pub fn has_collection(&self, collection: &str) -> bool {
        self.by_collection.contains_key(collection)
    }

    /// Snapshot for persistence: full trees keyed by index id, plus the
    /// id allocator (definitions themselves live in definitions.json).
    pub(crate) fn export_snapshot(&self) -> (HashMap<u32, CompositeIndex>, u32) {
        (self.by_id.clone(), self.next_id)
    }

    /// Merge persisted trees back in. Only ids that exist in the current
    /// definitions (restored from definitions.json first) and whose
    /// definition still matches are overwritten — anything else is a
    /// stale/foreign entry and stays out. Allocator takes the max so a
    /// future create never reuses a live id.
    pub(crate) fn import_snapshot(&mut self, trees: HashMap<u32, CompositeIndex>, next_id: u32) {
        for (id, index) in trees {
            if let Some(existing) = self.by_id.get(&id) {
                if existing.definition == index.definition {
                    self.by_id.insert(id, index);
                }
            }
        }
        self.next_id = self.next_id.max(next_id);
    }

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

    pub fn index_document(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
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

    pub fn restore_index(&mut self, definition: CompositeIndexDefinition) {
        if definition.id > self.next_id {
            self.next_id = definition.id;
        }
        let id = definition.id;
        self.by_collection
            .entry(definition.collection.clone())
            .or_default()
            .push(id);
        self.by_id.insert(id, CompositeIndex::new(definition));
    }

    pub fn index_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        let ids = self.by_collection.get(collection).cloned().unwrap_or_default();
        for id in ids {
            if let Some(index) = self.by_id.get_mut(&id) {
                index.index_batch(docs.clone());
            }
        }
    }

    pub fn remove_document(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
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

    pub fn remove_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        let ids = self.by_collection.get(collection).cloned().unwrap_or_default();
        for id in ids {
            if let Some(index) = self.by_id.get_mut(&id) {
                index.remove_batch(docs.clone());
            }
        }
    }

    pub fn exact_match_doc_ids(
        &self,
        collection: &str,
        fields: &[String],
        values: &[Value],
    ) -> Option<Vec<Arc<str>>> {
        for idx in self.indexes_for_collection(collection) {
            let idx_fields: Vec<&str> = idx.definition.fields.iter()
                .map(|f| f.field.as_str()).collect();

            // FIX: Ensure this uses >= and uses .take()
            if idx_fields.len() >= fields.len()
                && idx_fields
                    .iter()
                    .take(fields.len())
                    .copied()
                    .eq(fields.iter().map(String::as_str))
            {
                let range = build_prefix_range(&idx.definition, values);
                return Some(idx.range_scan(&range.start, &range.end));
            }
        }
        None
    }

    pub fn all_indexes(&self) -> impl Iterator<Item = &CompositeIndex> {
        self.by_id.values()
    }
}
