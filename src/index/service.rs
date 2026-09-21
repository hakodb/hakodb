use crate::document::hako_doc::HakoDoc;
use crate::index::manager::IndexManager;
use crate::document::value::Value;

/// Unified indexing service for all index families.
/// Keeps update/backfill logic in one place for easier maintenance.
pub struct IndexingService;

impl IndexingService {
    pub fn apply_put(
        manager: &mut IndexManager,
        collection: &str,
        doc_id: &str,
        doc: &HakoDoc,
    ) {
        manager.index_document(collection, doc_id, doc);
    }

    pub fn apply_delete(
        manager: &mut IndexManager,
        collection: &str,
        doc_id: &str,
        doc: &HakoDoc,
    ) {
        manager.remove_document(collection, doc_id, doc);
    }

    pub fn backfill_secondary<'a, I>(manager: &mut IndexManager, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)>,
    {
        for (doc_id, doc) in docs {
            if let Some(sec_map) = manager.secondary.get_mut(collection) {
                for (field_name, index) in sec_map.iter_mut() {
                    // VIRTUAL FIELD MAPPING
                    let val_opt = match field_name.as_str() {
                        "id" => Some(Value::String(doc_id.to_string())),
                        "_time" => Some(Value::Int(doc._time)),
                        _ => doc.get(field_name).cloned(),
                    };

                    if let Some(val) = val_opt {
                        index.insert(
                            crate::index::index_key::encode_scalar(&val),
                            doc_id.to_string(),
                        );
                    }
                }
            }
        }
    }

    pub fn backfill_fts<'a, I>(manager: &mut IndexManager, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)>,
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
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        manager.composite.index_batch(collection, docs);
    }
}
