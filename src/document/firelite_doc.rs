use crate::document::value::Value;
use std::sync::Arc;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 2; // Format version 2: No Catalog / Raw Strings

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FireLiteDoc {
    pub fields: Vec<(Arc<str>, Value)>,
}

impl FireLiteDoc {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.iter().find(|(k, _)| &**k == key).map(|(_, v)| v)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        let key_str = key.into();
        if let Some(pos) = self.fields.iter().position(|(k, _)| &**k == key_str) {
            self.fields[pos].1 = value;
        } else {
            self.fields.push((Arc::from(key_str), value));
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = vec![MAGIC, VERSION];
        out.extend((self.fields.len() as u16).to_le_bytes());
        for (k, v) in &self.fields {
            out.push(k.len() as u8);
            out.extend_from_slice(k.as_bytes());
            let (tag, bytes) = encode_value(v);
            out.push(tag);
            out.extend((bytes.len() as u32).to_le_bytes());
            out.extend_from_slice(&bytes);
        }
        out
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        if bytes.len() < 4 || bytes[0] != MAGIC || bytes[1] != VERSION {
            return None;
        }

        let mut doc = FireLiteDoc::default();
        let fields_count = u16::from_le_bytes(bytes[2..4].try_into().ok()?);
        let mut pos = 4;

        for _ in 0..fields_count {
            let len = *bytes.get(pos)? as usize;
            pos += 1;
            let key = std::str::from_utf8(bytes.get(pos..pos + len)?).ok()?;
            pos += len;

            let tag = *bytes.get(pos)?;
            pos += 1;
            let v_len = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?) as usize;
            pos += 4;
            let val_data = bytes.get(pos..pos + v_len)?;
            pos += v_len;

            doc.fields.push((Arc::from(key), decode_value(tag, val_data)?));
        }
        Some(doc)
    }

    /// Decodes only specific fields from the binary data without fully parsing the document.
    pub fn decode_projected(bytes: &[u8], projection: &[String]) -> Option<Self> {
        let view = FireLiteDocView::new(bytes)?;
        let mut doc = FireLiteDoc::default();

        for (key, tag, data) in view.iter() {
            if projection.is_empty() || projection.iter().any(|p| p == key) {
                doc.fields.push((Arc::from(key), decode_value(tag, data)?));
            }
        }
        Some(doc)
    }

    pub fn apply_patch_binary(old_bytes: &[u8], updates: &[(String, Value)]) -> Option<Vec<u8>> {
        let mut doc = Self::decode(old_bytes)?;
        for (k, v) in updates {
            doc.insert(k.clone(), v.clone());
        }
        Some(doc.encode())
    }
}

pub struct FireLiteDocView<'a> {
    bytes: &'a [u8],
    fields_count: u16,
}

impl<'a> FireLiteDocView<'a> {
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 4 || bytes[0] != MAGIC || bytes[1] != VERSION { return None; }
        let fields_count = u16::from_le_bytes(bytes[2..4].try_into().ok()?);
        Some(Self { bytes, fields_count })
    }

    pub fn iter(&self) -> FireLiteDocIter<'a> {
        FireLiteDocIter { bytes: self.bytes, pos: 4, remaining: self.fields_count }
    }
}

/// Convenience wrapper for document fields during iteration
pub struct BorrowedValue<'a> {
    pub tag: u8,
    pub data: &'a [u8],
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
    type Item = (&'a str, u8, &'a [u8]); // (Key, Tag, ValueData)

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 { return None; }
        let k_len = *self.bytes.get(self.pos)? as usize;
        self.pos += 1;
        let key = std::str::from_utf8(self.bytes.get(self.pos..self.pos + k_len)?).ok()?;
        self.pos += k_len;
        
        let tag = *self.bytes.get(self.pos)?;
        self.pos += 1;
        
        let v_len = u32::from_le_bytes(self.bytes.get(self.pos..self.pos + 4)?.try_into().ok()?) as usize;
        self.pos += 4;
        
        let data = self.bytes.get(self.pos..self.pos + v_len)?;
        self.pos += v_len;
        
        self.remaining -= 1;
        Some((key, tag, data))
    }
}

fn encode_value(v: &Value) -> (u8, Vec<u8>) {
    match v {
        Value::Null | Value::ServerTimestamp => (1, vec![]),
        Value::Bool(v) => (2, vec![*v as u8]),
        Value::Int(v) => (3, v.to_le_bytes().to_vec()),
        Value::Float(v) => (4, v.to_le_bytes().to_vec()),
        Value::String(v) => (5, v.as_bytes().to_vec()),
        Value::Binary(v) => (6, v.clone()),
        Value::Timestamp(v) => (7, v.to_le_bytes().to_vec()),
        Value::Map(fields) => {
            let mut out = vec![];
            out.extend((fields.len() as u16).to_le_bytes());
            for (k, v) in fields {
                out.push(k.len() as u8);
                out.extend_from_slice(k.as_bytes());
                let (tag, bytes) = encode_value(v);
                out.push(tag);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
            (8, out)
        }
        Value::Array(items) => {
            let mut out = vec![];
            out.extend((items.len() as u32).to_le_bytes());
            for item in items {
                let (tag, bytes) = encode_value(item);
                out.push(tag);
                out.extend((bytes.len() as u32).to_le_bytes());
                out.extend_from_slice(&bytes);
            }
            (9, out)
        }
        Value::Reference { collection, doc_id } => {
            let mut out = vec![];
            out.push(collection.len() as u8);
            out.extend_from_slice(collection.as_bytes());
            out.push(doc_id.len() as u8);
            out.extend_from_slice(doc_id.as_bytes());
            (10, out)
        }
    }
}

pub(crate) fn decode_value(tag: u8, bytes: &[u8]) -> Option<Value> {
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
            let count = u16::from_le_bytes(bytes.get(pos..pos + 2)?.try_into().ok()?);
            pos += 2;
            let mut fields = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let k_len = *bytes.get(pos)? as usize;
                pos += 1;
                let key = std::str::from_utf8(bytes.get(pos..pos + k_len)?).ok()?;
                pos += k_len;
                let tag = *bytes.get(pos)?;
                pos += 1;
                let v_len = u32::from_le_bytes(bytes.get(pos..pos + 4)?.try_into().ok()?) as usize;
                pos += 4;
                fields.push((Arc::from(key), decode_value(tag, bytes.get(pos..pos + v_len)?)?));
                pos += v_len;
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
                items.push(decode_value(tag, bytes.get(pos..pos + len)?)?);
                pos += len;
            }
            Some(Value::Array(items))
        }
        10 => {
             let mut pos = 0;
             let c_len = *bytes.get(pos)? as usize;
             pos += 1;
             let collection = std::str::from_utf8(bytes.get(pos..pos+c_len)?).ok()?.to_string();
             pos += c_len;
             let d_len = *bytes.get(pos)? as usize;
             pos += 1;
             let doc_id = std::str::from_utf8(bytes.get(pos..pos+d_len)?).ok()?.to_string();
             Some(Value::Reference { collection, doc_id })
        }
        _ => Some(Value::Null),
    }
}