use std::collections::HashMap;

use crate::index::secondary_index::SecondaryIndex;
use crate::document::firelite_doc::FireLiteDoc;

pub struct IndexManager {

    indexes: HashMap<(u32,String), SecondaryIndex>,
}

impl IndexManager {

    pub fn new() -> Self {

        Self {
            indexes: HashMap::new()
        }
    }

    pub fn create_index(
        &mut self,
        collection_id: u32,
        field: &str
    ) {

        let index =
            SecondaryIndex::new(collection_id, field);

        self.indexes.insert(
            (collection_id, field.to_string()),
            index
        );
    }

    pub fn index_document(
        &mut self,
        collection_id: u32,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        for ((cid, field), index) in &mut self.indexes {

            if *cid == collection_id {

                if doc.fields.contains_key(field) {

                    index.index_document(doc_id, doc);
                }
            }
        }
    }

    pub fn remove_document(
        &mut self,
        collection_id: u32,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        for ((cid, field), index) in &mut self.indexes {

            if *cid == collection_id {

                if doc.fields.contains_key(field) {

                    index.remove_document(doc_id, doc);
                }
            }
        }
    }
}
