use std::collections::BTreeMap;

use crate::document::firelite_doc::{FireLiteDoc, Value};
use super::definition::CompositeIndexDefinition;
use super::key_encoder::encode_composite_key;

pub struct CompositeIndex {

    pub definition: CompositeIndexDefinition,

    tree: BTreeMap<Vec<u8>, String>,
}

impl CompositeIndex {

    pub fn new(definition: CompositeIndexDefinition) -> Self {

        Self {
            definition,
            tree: BTreeMap::new(),
        }
    }

    pub fn index_document(
        &mut self,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        let mut values = Vec::new();

        for field in &self.definition.fields {

            if let Some(v) = doc.fields.get(&field.field) {
                values.push(v.clone());
            } else {
                return;
            }
        }

        let key =
            encode_composite_key(
                &self.definition,
                &values,
                doc_id
            );

        self.tree.insert(key, doc_id.to_string());
    }

    pub fn remove_document(
        &mut self,
        doc_id: &str,
        doc: &FireLiteDoc,
    ) {

        let mut values = Vec::new();

        for field in &self.definition.fields {

            if let Some(v) = doc.fields.get(&field.field) {
                values.push(v.clone());
            } else {
                return;
            }
        }

        let key =
            encode_composite_key(
                &self.definition,
                &values,
                doc_id
            );

        self.tree.remove(&key);
    }

    pub fn range_scan(
        &self,
        start: Vec<u8>,
        end: Vec<u8>,
    ) -> Vec<String> {

        self.tree
            .range(start..=end)
            .map(|(_,doc)| doc.clone())
            .collect()
    }

        fn build_key(
        &self,
        doc_id:&str,
        doc:&FireLiteDoc
    )->Option<Vec<u8>>{

        let mut buf = Vec::new();

        buf.extend(&self.definition.collection_id.to_be_bytes());

        for field in &self.definition.fields {

            let v = doc.fields.get(&field.field)?;

            encode_value(v,&mut buf,&field.direction);

        }

        encode_doc_id(doc_id,&mut buf);

        Some(buf)

    }

    pub fn insert(
        &mut self,
        doc_id:&str,
        doc:&FireLiteDoc
    ){

        if let Some(key) = self.build_key(doc_id,doc) {

            self.tree.insert(key,doc_id.to_string());

        }

    }

    pub fn remove(
        &mut self,
        doc_id:&str,
        doc:&FireLiteDoc
    ){

        if let Some(key) = self.build_key(doc_id,doc) {

            self.tree.remove(&key);

        }

    }

    pub fn range_query(
        &self,
        start:Vec<u8>,
        end:Vec<u8>
    )->Vec<String>{

        self.tree
            .range(start..=end)
            .map(|(_,v)|v.clone())
            .collect()

    }
}
