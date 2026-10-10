use crate::document::hako_doc::HakoDoc;
use crate::index::manager::IndexManager;
use crate::document::value::Value;
use std::sync::Arc;

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

    /// Borrowed secondary backfill over stored bytes: framing walk plus
    /// direct key encoding, no HakoDoc materialization. Returns false
    /// unless the collection holds ONLY secondary indexes — fts and
    /// composite entries need the owned path (caller falls back, same
    /// outcome). All-or-nothing per doc: a doc any of whose matched
    /// fields fail is skipped whole, exactly like owned decode failing.
    pub fn backfill_secondary_borrowed(
        manager: &mut IndexManager,
        collection: &str,
        docs: &[(String, Vec<u8>)],
    ) -> bool {
        if manager.fts.contains_key(collection) {
            return false;
        }
        // Vector defs need the owned path too: embeddings decode through
        // index_document's HNSW hook, which the byte-level path below
        // never touches.
        if manager.vector.contains_key(collection) {
            return false;
        }
        if manager
            .composite
            .all_indexes()
            .any(|idx| idx.definition.collection == collection)
        {
            return false;
        }
        let Some(sec_map) = manager.secondary.get_mut(collection) else {
            // Nothing registered: trivially complete.
            return true;
        };
        let fields: Vec<String> = sec_map.keys().cloned().collect();
        // ponytail: virtual fields resolve without stored bytes (same as
        // the owned path, which maps them from doc_id/_time, never from a
        // literal same-named field).
        let want_id = fields.iter().any(|f| f == "id");
        let want_time = fields.iter().any(|f| f == "_time");
        crate::index::index_key::ENC_SCRATCH.with(|scratch| {
            let mut enc = scratch.borrow_mut();
            for (doc_id, bytes) in docs {
                let Some(view) = crate::document::hako_doc::HakoDocView::new(bytes)
                else {
                    continue;
                };
                // Pass 1: match + exact framing walk (owned-decode
                // strictness: short/truncated docs index nothing).
                let mut it = view.iter();
                let mut matched: Vec<(&str, u8, &[u8])> = Vec::new();
                for (key, tag, vdata) in it.by_ref() {
                    // Virtuals handled below from doc_id/_time.
                    if key == "id" || key == "_time" {
                        continue;
                    }
                    if fields.iter().any(|f| f == key) {
                        matched.push((key, tag, vdata));
                    }
                }
                if it.remaining() != 0 || it.position() != bytes.len() {
                    continue;
                }
                // Pass 2: encode all keys first; a single bad field
                // discards the doc (owned parity), then insert.
                let id_shared: Arc<str> = Arc::from(doc_id.as_str());
                let mut staged: Vec<(usize, Vec<u8>)> = Vec::with_capacity(matched.len());
                let mut ok = true;
                for (key, tag, vdata) in matched {
                    let Some(pos) = fields.iter().position(|f| f == key) else {
                        ok = false;
                        break;
                    };
                    enc.clear();
                    if !crate::index::index_key::encode_raw_scalar(tag, vdata, &mut enc) {
                        enc.clear();
                        match crate::document::hako_doc::decode_value(tag, vdata) {
                            Some(v) => crate::index::index_key::encode_scalar_into(&v, &mut enc),
                            None => {
                                ok = false;
                                break;
                            }
                        }
                    }
                    staged.push((pos, enc.clone()));
                }
                if !ok {
                    continue;
                }
                // Virtual entries (owned path maps these identically).
                if want_id {
                    if let Some(pos) = fields.iter().position(|f| f == "id") {
                        enc.clear();
                        crate::index::index_key::encode_str_scalar_into(doc_id, &mut enc);
                        staged.push((pos, enc.clone()));
                    }
                }
                if want_time {
                    if let Some(pos) = fields.iter().position(|f| f == "_time") {
                        enc.clear();
                        crate::index::index_key::encode_scalar_into(
                            &crate::document::value::Value::Int(view._time),
                            &mut enc,
                        );
                        staged.push((pos, enc.clone()));
                    }
                }
                for (pos, key) in staged {
                    if let Some(sec) = sec_map.get_mut(fields[pos].as_str()) {
                        sec.insert_borrowed(&key, id_shared.clone());
                    }
                }
            }
        });
        true
    }

    pub fn backfill_composite<'a, I>(manager: &mut IndexManager, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        manager.composite.index_batch(collection, docs);
    }
}
