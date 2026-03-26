// use std::collections::BTreeMap;

use crate::document::value::Value;
use std::sync::Arc;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 1;

const TAG_POOLED_KEY: u8 = 128;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FireLiteDoc {
    // pub fields: Vec<(String, Value)>,
    pub fields: Vec<(Arc<str>, Value)>,
}

impl FireLiteDoc {
    // lookup helper
    pub fn get(&self, key: &str) -> Option<&Value> {
        // self.fields.iter().find(|(k, _)| k == key).map(|(_, v)| v)
        self.fields
            .iter()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.fields
            .iter_mut()
            .find(|(k, _)| &**k == key)
            .map(|(_, v)| v)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        let key_str = key.into();
        for (k, v) in &mut self.fields {
            if &**k == key_str {
                *v = value;
                return;
            }
        }
        self.fields.push((Arc::from(key_str), value));
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![MAGIC, VERSION];
        out.extend((self.fields.len() as u16).to_le_bytes());
        for (k, v) in &self.fields {
            out.push(k.len() as u8);
            out.extend(k.as_bytes());
            let (tag, bytes) = encode_value(v, None);
            out.push(tag);
            out.extend((bytes.len() as u32).to_le_bytes());
            out.extend(bytes);
        }
        out
    }

    pub fn decode(bytes: &[u8], catalog: Option<&crate::util::catalog::Catalog>) -> Option<Self> {
        if bytes.len() < 4 || bytes[0] != MAGIC || bytes[1] != VERSION {
            return None;
        }

        let mut doc = FireLiteDoc::default();
        let fields_count = u16::from_le_bytes(bytes[2..4].try_into().ok()?);
        let mut pos = 4;

        for _ in 0..fields_count {
            let tag_or_len = *bytes.get(pos)?;
            pos += 1;

            let key: Arc<str> = if tag_or_len == TAG_POOLED_KEY {
                let id = u16::from_le_bytes(bytes.get(pos..pos + 2)?.try_into().ok()?);
                pos += 2;
                if let Some(cat) = catalog {
                    cat.resolve_key_shared(id) // ZERO ALLOCATION HERE
                } else {
                    Arc::from(format!("$id:{}", id))
                }
            } else {
                let len = tag_or_len as usize;
                let s = std::str::from_utf8(bytes.get(pos..pos + len)?).ok()?;
                pos += len;
                Arc::from(s) // One-time allocation for non-pooled keys
            };

            let tag = *bytes.get(pos)?;
            pos += 1;
            let v_len = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?) as usize;
            pos += 4;
            let val_data = bytes.get(pos..pos + v_len)?;
            pos += v_len;

            // decode_value still allocates for Value::String, but keys are now optimized
            doc.fields
                .push((key, decode_value_with_catalog(tag, val_data, catalog)?));
        }
        Some(doc)
    }

    pub fn decode_projected(
        bytes: &[u8],
        projection: &[String],
        catalog: Option<&crate::util::catalog::Catalog>,
    ) -> Option<Self> {
        let view = FireLiteDocView::new(bytes)?;
        let mut doc = FireLiteDoc::default();

        // If no projection is specified, perform a standard full decode
        // if projection.is_empty() {
        //     for (k, v) in view.iter() {
        //         doc.insert(k.to_string(), v.to_owned_value()?);
        //     }
        // } else {
        //     // Cherry-pick only the fields requested in the projection
        //     for (k, v) in view.iter() {
        //         if projection.iter().any(|p| p == k) {
        //             doc.insert(k.to_string(), v.to_owned_value()?);
        //         }
        //     }
        // }
        for (k, v) in view.iter(catalog) {
            if projection.is_empty() || projection.iter().any(|p| p == &*k) {
                doc.fields
                    .push((Arc::from(&*k), v.to_owned_value_with_catalog(catalog)?));
            }
        }
        Some(doc)
    }

    pub fn encode_compact(&self, catalog: &crate::util::catalog::Catalog) -> Vec<u8> {
        let mut out = vec![MAGIC, VERSION];
        out.extend((self.fields.len() as u16).to_le_bytes());

        for (k, v) in &self.fields {
            // Write the key ID instead of the string
            out.push(TAG_POOLED_KEY);
            out.extend(catalog.get_key_id(k).to_le_bytes());

            let (tag, bytes) = encode_value(v, Some(catalog));
            out.push(tag);
            out.extend((bytes.len() as u32).to_le_bytes());
            out.extend(bytes);
        }
        out
    }

    pub fn apply_patch_binary(
        old_bytes: &[u8], 
        updates: &[(String, Value)],
        catalog: &crate::util::catalog::Catalog
    ) -> Option<Vec<u8>> {
        let view = FireLiteDocView::new(old_bytes)?;
        let mut final_fields: Vec<(Arc<str>, Value)> = Vec::new();

        // Track which updates we have already applied
        let mut applied_updates = vec![false; updates.len()];

        // 1. Iterate through existing fields
        for (key, borrowed_val) in view.iter(Some(catalog)) {
            let update_idx = updates.iter().position(|(uk, _)| uk == &*key);
            if let Some(idx) = update_idx {
                final_fields.push((Arc::from(&*key), updates[idx].1.clone()));
                applied_updates[idx] = true;
            } else {
                final_fields.push((Arc::from(&*key), borrowed_val.to_owned_value()?));
            }
        }

        // 2. Add any completely new fields that didn't exist before
        for (i, is_applied) in applied_updates.iter().enumerate() {
            if !is_applied {
                final_fields.push((Arc::from(updates[i].0.clone()), updates[i].1.clone()));
            }
        }

        // 3. Create a temporary doc and encode
        let new_doc = FireLiteDoc {
            fields: final_fields,
        };
        Some(new_doc.encode_compact(catalog))
    }
}

pub struct FireLiteDocView<'a> {
    bytes: &'a [u8],
    pos: usize,
    fields: u16,
}

impl<'a> FireLiteDocView<'a> {
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 4 || bytes[0] != MAGIC || bytes[1] != VERSION {
            return None;
        }
        let fields = u16::from_le_bytes(bytes[2..4].try_into().ok()?);
        Some(Self {
            bytes,
            pos: 4,
            fields,
        })
    }

    pub fn iter(&self, catalog: Option<&'a crate::util::catalog::Catalog>) -> FireLiteDocIter<'a> {
        FireLiteDocIter {
            bytes: self.bytes,
            pos: self.pos,
            remaining: self.fields,
            catalog,
        }
    }

    pub fn get_field_value(
        &self,
        target_key: &str,
        catalog: Option<&'a crate::util::catalog::Catalog>,
    ) -> Option<BorrowedValue<'a>> {
        for (key, val) in self.iter(catalog) {
            if &*key == target_key {
                return Some(val);
            }
        }
        None
    }
}

pub struct BorrowedValue<'a> {
    tag: u8,
    data: &'a [u8],
}

impl<'a> BorrowedValue<'a> {
    pub fn to_owned_value(&self) -> Option<Value> {
        self.to_owned_value_with_catalog(None)
    }

    pub fn to_owned_value_with_catalog(
        &self,
        catalog: Option<&crate::util::catalog::Catalog>,
    ) -> Option<Value> {
        decode_value_with_catalog(self.tag, self.data, catalog)
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self.tag {
            3 => {
                // Int
                let b = self.data.get(..8)?;
                Some(i64::from_le_bytes(b.try_into().ok()?) as f64)
            }
            4 => {
                // Float
                let b = self.data.get(..8)?;
                Some(f64::from_le_bytes(b.try_into().ok()?))
            }
            _ => None,
        }
    }
}

pub struct FireLiteDocIter<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: u16,
    catalog: Option<&'a crate::util::catalog::Catalog>,
}

impl<'a> Iterator for FireLiteDocIter<'a> {
    // type Item = (&'a str, BorrowedValue<'a>);
    type Item = (std::borrow::Cow<'a, str>, BorrowedValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }

        let tag_or_len = *self.bytes.get(self.pos)?;
        self.pos += 1;

        let key = if tag_or_len == TAG_POOLED_KEY {
            let id = u16::from_le_bytes(self.bytes.get(self.pos..self.pos + 2)?.try_into().ok()?);
            self.pos += 2;
            if let Some(cat) = self.catalog {
                std::borrow::Cow::Owned(cat.resolve_key_shared(id).to_string())
            } else {
                std::borrow::Cow::Owned(format!("$id:{}", id))
            }
        } else {
            let len = tag_or_len as usize;
            let s = std::str::from_utf8(self.bytes.get(self.pos..self.pos + len)?).ok()?;
            self.pos += len;
            std::borrow::Cow::Borrowed(s)
        };

        // Parse the value Tag
        let tag = *self.bytes.get(self.pos)?;
        self.pos += 1;

        // Parse the value Length (u32)
        let len =
            u32::from_le_bytes(self.bytes.get(self.pos..self.pos + 4)?.try_into().ok()?) as usize;
        self.pos += 4;

        // Slice the value Data
        let data = self.bytes.get(self.pos..self.pos + len)?;
        self.pos += len;

        self.remaining -= 1;
        Some((key, BorrowedValue { tag, data }))
    }
}

fn encode_value(v: &Value, catalog: Option<&crate::util::catalog::Catalog>) -> (u8, Vec<u8>) {
    match v {
        Value::Null => (1, vec![]),
        Value::Bool(v) => (2, vec![*v as u8]),
        Value::Int(v) => (3, v.to_le_bytes().to_vec()),
        Value::Float(v) => (4, v.to_le_bytes().to_vec()),
        Value::String(v) => (5, v.as_bytes().to_vec()),
        Value::Binary(v) => (6, v.clone()),
        Value::Timestamp(v) => (7, v.to_le_bytes().to_vec()),
        Value::ServerTimestamp => (1, vec![]),
        Value::Map(fields) => {
            let mut out = vec![];
            out.extend((fields.len() as u16).to_le_bytes()); // Number of sub-fields
            for (k, v) in fields {
                // let k_str: &str = &*k;
                // out.push(k_str.len() as u8);
                // out.extend(k_str.as_bytes());
                if let Some(cat) = catalog {
                    // Use Key Pooling for nested maps too!
                    out.push(TAG_POOLED_KEY);
                    out.extend(cat.get_key_id(k).to_le_bytes());
                } else {
                    let k_str: &str = &*k;
                    out.push(k_str.len() as u8);
                    out.extend(k_str.as_bytes());
                }
                let (tag, bytes) = encode_value(v, catalog);
                // let (tag, bytes) = encode_value(v); // RECURSION
                out.push(tag);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
            (8, out) // Tag 8 for Map
        }
        Value::Array(items) => {
            let mut out = vec![];
            out.extend((items.len() as u32).to_le_bytes()); // Element count
            for item in items {
                let (tag, bytes) = encode_value(item, catalog); // RECURSION
                out.push(tag);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
            (9, out) // Tag 9 for Array
        }
        Value::Reference { collection, doc_id } => {
            let mut out = vec![];
            out.push(collection.len() as u8);
            out.extend(collection.as_bytes());
            out.push(doc_id.len() as u8);
            out.extend(doc_id.as_bytes());
            (10, out) // Tag 10
        }
    }
}

fn decode_value_with_catalog(
    tag: u8,
    bytes: &[u8],
    catalog: Option<&crate::util::catalog::Catalog>,
) -> Option<Value> {
    match tag {
        1 => Some(Value::Null),
        2 => Some(Value::Bool(*bytes.first()? == 1)),
        3 => Some(Value::Int(i64::from_le_bytes(
            bytes.get(..8)?.try_into().ok()?,
        ))),
        4 => Some(Value::Float(f64::from_le_bytes(
            bytes.get(..8)?.try_into().ok()?,
        ))),
        5 => Some(Value::String(String::from_utf8(bytes.to_vec()).ok()?)),
        6 => Some(Value::Binary(bytes.to_vec())),
        7 => Some(Value::Timestamp(i64::from_le_bytes(
            bytes.get(..8)?.try_into().ok()?,
        ))),
        8 => {
            let mut pos = 0;
            let field_count = u16::from_le_bytes(bytes.get(pos..pos + 2)?.try_into().ok()?);
            pos += 2;
            let mut fields = Vec::with_capacity(field_count as usize);
            for _ in 0..field_count {
                let key_tag_or_len = *bytes.get(pos)?;
                pos += 1;
                let key: Arc<str> = if key_tag_or_len == TAG_POOLED_KEY {
                    let id = u16::from_le_bytes(bytes.get(pos..pos + 2)?.try_into().ok()?);
                    pos += 2;
                    if let Some(cat) = catalog {
                        cat.resolve_key_shared(id)
                    } else {
                        Arc::from(format!("$id:{}", id))
                    }
                } else {
                    let k_len = key_tag_or_len as usize;
                    let key = std::str::from_utf8(bytes.get(pos..pos + k_len)?).ok()?;
                    pos += k_len;
                    Arc::from(key)
                };
                let tag = *bytes.get(pos)?;
                pos += 1;
                let v_len = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let val = decode_value_with_catalog(tag, bytes.get(pos..pos + v_len)?, catalog)?;
                pos += v_len;
                fields.push((key, val));
            }
            Some(Value::Map(fields))
        }
        9 => {
            let mut pos = 0;
            let count = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?) as usize;
            pos += 4;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let tag = *bytes.get(pos)?;
                pos += 1;
                let len = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                let val = decode_value_with_catalog(tag, bytes.get(pos..pos + len)?, catalog)?;
                pos += len;
                items.push(val);
            }
            Some(Value::Array(items))
        }
        10 => {
            let mut pos = 0;
            let col_len = *bytes.get(pos)? as usize;
            pos += 1;
            let collection = std::str::from_utf8(bytes.get(pos..pos + col_len)?)
                .ok()?
                .to_string();
            pos += col_len;
            let id_len = *bytes.get(pos)? as usize;
            pos += 1;
            let doc_id = std::str::from_utf8(bytes.get(pos..pos + id_len)?)
                .ok()?
                .to_string();
            Some(Value::Reference { collection, doc_id })
        }
        _ => Some(Value::Null),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trip() {
        let mut doc = FireLiteDoc::default();
        doc.insert("name", Value::String("alice".into()));
        doc.insert("age", Value::Int(42));
        let encoded = doc.encode();
        let decoded = FireLiteDoc::decode(&encoded, None).unwrap();
        assert_eq!(decoded, doc);
    }
}
