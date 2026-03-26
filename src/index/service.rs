use crate::document::firelite_doc::FireLiteDoc;
use crate::index::manager::IndexManager;

/// Unified indexing service for all index families.
/// Keeps update/backfill logic in one place for easier maintenance.
pub struct IndexingService;

impl IndexingService {
    pub fn apply_put(
        manager: &mut IndexManager,
        collection: &str,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {
        manager.index_document(collection, doc_id, doc);
    }

    pub fn apply_delete(
        manager: &mut IndexManager,
        collection: &str,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {
        manager.remove_document(collection, doc_id, doc);
    }

    pub fn backfill_secondary<'a, I>(manager: &mut IndexManager, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)>,
    {
        for (doc_id, doc) in docs {
            if let Some(sec_map) = manager.secondary.get_mut(collection) {
                for (field_name, index) in sec_map.iter_mut() {
                    if let Some(val) = doc.get(field_name) {
                        index.insert(
                            crate::index::index_key::encode_scalar(val),
                            doc_id.to_string(),
                        );
                    }
                }
            }
        }
    }

    pub fn backfill_fts<'a, I>(manager: &mut IndexManager, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)>,
    {
        for (doc_id, doc) in docs {
            if let Some(fts_map) = manager.fts.get_mut(collection) {
                for (field_name, index) in fts_map.iter_mut() {
                    if let Some(crate::document::value::Value::String(text)) = doc.get(field_name) {
                        index.insert(text, doc_id.to_string());
                    }
                }
            }
        }
    }

    pub fn backfill_composite<'a, I>(manager: &mut IndexManager, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a FireLiteDoc)> + Clone,
    {
        manager.composite.index_batch(collection, docs);
    }
}
