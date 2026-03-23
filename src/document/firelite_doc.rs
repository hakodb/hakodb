// use std::collections::BTreeMap;

use crate::document::value::Value;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FireLiteDoc {
    pub fields: Vec<(String, Value)>,
}

impl FireLiteDoc {
    // lookup helper
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.iter().find(|(k, _)| k == key).map(|(_, v)| v)
    }

    pub fn get_mut(&mut self, key: &str) -> Option<&mut Value> {
        self.fields
            .iter_mut()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        // self.fields.insert(key.into(), value);
        let key = key.into();

        for (k, v) in &mut self.fields {
            if k == &key {
                *v = value;
                return;
            }
        }

        self.fields.push((key, value));
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![MAGIC, VERSION];
        out.extend((self.fields.len() as u16).to_le_bytes());
        for (k, v) in &self.fields {
            out.push(k.len() as u8);
            out.extend(k.as_bytes());
            let (tag, bytes) = encode_value(v);
            out.push(tag);
            out.extend((bytes.len() as u32).to_le_bytes());
            out.extend(bytes);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let view = FireLiteDocView::new(bytes)?;
        let mut doc = FireLiteDoc::default();
        for (k, v) in view.iter() {
            doc.insert(k.to_string(), v.to_owned_value()?);
        }
        Some(doc)
    }

    pub fn decode_projected(bytes: &[u8], projection: &[String]) -> Option<Self> {
        let view = FireLiteDocView::new(bytes)?;
        let mut doc = FireLiteDoc::default();
        
        // If no projection is specified, perform a standard full decode
        if projection.is_empty() {
            for (k, v) in view.iter() {
                doc.insert(k.to_string(), v.to_owned_value()?);
            }
        } else {
            // Cherry-pick only the fields requested in the projection
            for (k, v) in view.iter() {
                if projection.iter().any(|p| p == k) {
                    doc.insert(k.to_string(), v.to_owned_value()?);
                }
            }
        }
        Some(doc)
    }

    pub fn apply_patch_binary(old_bytes: &[u8], updates: &[(String, Value)]) -> Option<Vec<u8>> {
        let view = FireLiteDocView::new(old_bytes)?;
        let mut final_fields: Vec<(String, Value)> = Vec::new();
        
        // Track which updates we have already applied
        let mut applied_updates = vec![false; updates.len()];

        // 1. Iterate through existing fields
        for (key, borrowed_val) in view.iter() {
            // Check if this field is in our update list
            let update_idx = updates.iter().position(|(uk, _)| uk == key);

            if let Some(idx) = update_idx {
                // Use the NEW value
                final_fields.push(updates[idx].clone());
                applied_updates[idx] = true;
            } else {
                // CRITICAL OPTIMIZATION:
                // Instead of decoding, we convert the borrowed_val back to an owned Value.
                // In a future "Extreme" version, we would copy the [u8] slice directly.
                // For now, to keep the TLV logic safe, we decode just this one value.
                final_fields.push((key.to_string(), borrowed_val.to_owned_value()?));
            }
        }

        // 2. Add any completely new fields that didn't exist before
        for (i, is_applied) in applied_updates.iter().enumerate() {
            if !is_applied {
                final_fields.push(updates[i].clone());
            }
        }

        // 3. Create a temporary doc and encode
        let new_doc = FireLiteDoc { fields: final_fields };
        Some(new_doc.encode())
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

    pub fn iter(&self) -> FireLiteDocIter<'a> {
        FireLiteDocIter {
            bytes: self.bytes,
            pos: self.pos,
            remaining: self.fields,
        }
    }

    pub fn get_field_value(&self, target_key: &str) -> Option<BorrowedValue<'a>> {
        for (key, val) in self.iter() {
            if key == target_key {
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
        decode_value(self.tag, self.data)
    }
    pub fn as_f64(&self) -> Option<f64> {
        match self.tag {
            3 => { // Int
                let b = self.data.get(..8)?;
                Some(i64::from_le_bytes(b.try_into().ok()?) as f64)
            }
            4 => { // Float
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
}

impl<'a> Iterator for FireLiteDocIter<'a> {
    type Item = (&'a str, BorrowedValue<'a>);

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 {
            return None;
        }
        let key_len = *self.bytes.get(self.pos)? as usize;
        self.pos += 1;
        let key = std::str::from_utf8(self.bytes.get(self.pos..self.pos + key_len)?).ok()?;
        self.pos += key_len;

        let tag = *self.bytes.get(self.pos)?;
        self.pos += 1;
        let len =
            u32::from_le_bytes(self.bytes.get(self.pos..self.pos + 4)?.try_into().ok()?) as usize;
        self.pos += 4;
        let data = self.bytes.get(self.pos..self.pos + len)?;
        self.pos += len;

        self.remaining -= 1;
        Some((key, BorrowedValue { tag, data }))
    }
}

fn encode_value(v: &Value) -> (u8, Vec<u8>) {
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
                out.push(k.len() as u8);
                out.extend(k.as_bytes());
                let (tag, bytes) = encode_value(v); // RECURSION
                out.push(tag);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
            (8, out) // Tag 8 for Map
        },
        Value::Array(items) => {
            let mut out = vec![];
            out.extend((items.len() as u32).to_le_bytes()); // Element count
            for item in items {
                let (tag, bytes) = encode_value(item); // RECURSION
                out.push(tag);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend(bytes);
            }
            (9, out) // Tag 9 for Array
        },
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

fn decode_value(tag: u8, bytes: &[u8]) -> Option<Value> {
    match tag {
        1 => Some(Value::Null),
        2 => Some(Value::Bool(*bytes.first()? == 1)),
        3 => Some(Value::Int(i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?))),
        4 => Some(Value::Float(f64::from_le_bytes(bytes.get(..8)?.try_into().ok()?))),
        5 => Some(Value::String(String::from_utf8(bytes.to_vec()).ok()?)),
        6 => Some(Value::Binary(bytes.to_vec())),
        7 => Some(Value::Timestamp(i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?))),
        8 => {
            let mut pos = 0;
            let field_count = u16::from_le_bytes(bytes.get(pos..pos+2)?.try_into().ok()?);
            pos += 2;
            let mut fields = Vec::with_capacity(field_count as usize);
            for _ in 0..field_count {
                let k_len = *bytes.get(pos)? as usize;
                pos += 1;
                let key = std::str::from_utf8(bytes.get(pos..pos+k_len)?).ok()?.to_string();
                pos += k_len;
                let tag = *bytes.get(pos)?;
                pos += 1;
                let v_len = u32::from_le_bytes(bytes.get(pos..pos+4)?.try_into().ok()?) as usize;
                pos += 4;
                // Recursive call: the outer '?' will return None if parsing fails
                let val = decode_value(tag, bytes.get(pos..pos+v_len)?)?; 
                pos += v_len;
                fields.push((key, val));
            }
            Some(Value::Map(fields))
        },
        9 => {
            let mut pos = 0;
            let count = u32::from_le_bytes(bytes.get(pos..pos+4)?.try_into().ok()?) as usize;
            pos += 4;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let tag = *bytes.get(pos)?;
                pos += 1;
                let len = u32::from_le_bytes(bytes.get(pos..pos+4)?.try_into().ok()?) as usize;
                pos += 4;
                let val = decode_value(tag, bytes.get(pos..pos+len)?)?;
                pos += len;
                items.push(val);
            }
            Some(Value::Array(items))
        },
        10 => {
            let mut pos = 0;
            let col_len = *bytes.get(pos)? as usize;
            pos += 1;
            let collection = std::str::from_utf8(bytes.get(pos..pos+col_len)?).ok()?.to_string();
            pos += col_len;
            let id_len = *bytes.get(pos)? as usize;
            pos += 1;
            let doc_id = std::str::from_utf8(bytes.get(pos..pos+id_len)?).ok()?.to_string();
            Some(Value::Reference { collection, doc_id })
        },
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
        let decoded = FireLiteDoc::decode(&encoded).unwrap();
        assert_eq!(decoded, doc);
    }
}
