use crate::document::value::Value;
use crate::util::varint::*;
use std::sync::Arc;
use std::cell::RefCell;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 4; 

thread_local! {
    static ENCODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
}

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct FireLiteDoc {
    pub fields: Vec<(Arc<str>, Value)>,
    pub _time: i64,
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
        let mut out = Vec::with_capacity(512);
        self.encode_into(&mut out);
        out
    }

    pub fn encode_buffered(&self) -> Vec<u8> {
        ENCODE_BUF.with(|buf| {
            let mut b = buf.borrow_mut();
            b.clear();
            self.encode_into(&mut b);
            b.to_vec() 
        })
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&self._time.to_le_bytes()); 
        out.extend_from_slice(&(self.fields.len() as u16).to_le_bytes()); 
        
        for (key, value) in &self.fields {
            out.push(key.len() as u8);
            out.extend_from_slice(key.as_bytes());
            Self::encode_value_to(value, out);
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let view = FireLiteDocView::new(bytes)?;
        let mut doc = FireLiteDoc::default();
        doc._time = view._time;
        for (key, tag, data) in view.iter() {
            doc.fields.push((Arc::from(key), decode_value(tag, data)?));
        }
        Some(doc)
    }

    pub fn decode_projected(bytes: &[u8], projection: &[String]) -> Option<Self> {
        let view = FireLiteDocView::new(bytes)?;
        let mut doc = FireLiteDoc::default();
        doc._time = view._time;
        for (key, tag, data) in view.iter() {
            if projection.is_empty() || projection.iter().any(|p| p == key) {
                doc.fields.push((Arc::from(key), decode_value(tag, data)?));
            }
        }
        Some(doc)
    }

    pub fn get_logical_time(&self) -> i64 { self._time }

    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("_time".to_string(), serde_json::json!(self._time));
        for (k, v) in &self.fields {
            map.insert(k.to_string(), v.to_json());
        }
        serde_json::Value::Object(map)
    }

    pub fn to_json_with_id(&self, id: &str) -> serde_json::Value {
        let mut json = self.to_json();
        if let Some(obj) = json.as_object_mut() {
            obj.insert("id".to_string(), serde_json::Value::String(id.to_string()));
        }
        json
    }

    fn encode_value_to(v: &Value, out: &mut Vec<u8>) {
        match v {
            Value::Null | Value::ServerTimestamp => {
                out.push(1);
                out.extend_from_slice(&0u32.to_le_bytes());
            }
            Value::Bool(b) => {
                out.push(2);
                out.extend_from_slice(&1u32.to_le_bytes());
                out.push(*b as u8);
            }
            Value::Int(v) => {
                out.push(3);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]); // Reserve length
                encode_varint(zigzag_encode(*v), out);
                let len = (out.len() - start - 4) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Float(v) => {
                out.push(4);
                out.extend_from_slice(&8u32.to_le_bytes());
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::String(v) => {
                out.push(5);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v.as_bytes());
            }
            Value::Binary(v) => {
                out.push(6);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v);
            }
            Value::Timestamp(v) => {
                out.push(7);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]); 
                encode_varint(zigzag_encode(*v), out);
                let len = (out.len() - start - 4) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Map(fields) => {
                out.push(8);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]); 
                let body_start = out.len();
                out.extend_from_slice(&(fields.len() as u16).to_le_bytes());
                for (k, v) in fields {
                    out.push(k.len() as u8);
                    out.extend_from_slice(k.as_bytes());
                    Self::encode_value_to(v, out);
                }
                let len = (out.len() - body_start) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Array(items) => {
                out.push(9);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]);
                let body_start = out.len();
                out.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for item in items { Self::encode_value_to(item, out); }
                let len = (out.len() - body_start) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Reference { collection, doc_id } => {
                out.push(10);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]);
                let body_start = out.len();
                out.push(collection.len() as u8);
                out.extend_from_slice(collection.as_bytes());
                out.push(doc_id.len() as u8);
                out.extend_from_slice(doc_id.as_bytes());
                let len = (out.len() - body_start) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::BlobLink { offset, len } => {
                out.push(11);
                out.extend_from_slice(&12u32.to_le_bytes());
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&len.to_le_bytes());
            }
        }
    }
}

pub struct FireLiteDocView<'a> {
    bytes: &'a [u8],
    fields_count: u16,
    pub _time: i64,
}

impl<'a> FireLiteDocView<'a> {
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 12 || bytes[0] != MAGIC || bytes[1] != VERSION { return None; }
        let _time = i64::from_le_bytes(bytes[2..10].try_into().ok()?);
        let fields_count = u16::from_le_bytes(bytes[10..12].try_into().ok()?);
        Some(Self { bytes, fields_count, _time })
    }
    pub fn iter(&self) -> FireLiteDocIter<'a> {
        FireLiteDocIter { bytes: self.bytes, pos: 12, remaining: self.fields_count }
    }
}

// RESTORED: BorrowedValue for executor.rs
pub struct BorrowedValue<'a> {
    pub tag: u8,
    pub data: &'a [u8],
}

impl<'a> BorrowedValue<'a> {
    pub fn to_owned_value(&self) -> Option<Value> { decode_value(self.tag, self.data) }
    pub fn as_f64(&self) -> Option<f64> {
        match self.tag {
            3 => { 
                let mut p = 0;
                let v = decode_varint(self.data, &mut p)?;
                Some(zigzag_decode(v) as f64)
            }
            4 => Some(f64::from_le_bytes(self.data.get(..8)?.try_into().ok()?)),
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
    type Item = (&'a str, u8, &'a [u8]);
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

pub(crate) fn decode_value(tag: u8, bytes: &[u8]) -> Option<Value> {
    let mut p = 0;
    match tag {
        1 => Some(Value::Null),
        2 => Some(Value::Bool(*bytes.first()? == 1)),
        3 => Some(Value::Int(zigzag_decode(decode_varint(bytes, &mut p)?))),
        4 => Some(Value::Float(f64::from_le_bytes(bytes.get(..8)?.try_into().ok()?))),
        5 => Some(Value::String(String::from_utf8(bytes.to_vec()).ok()?)),
        6 => Some(Value::Binary(bytes.to_vec())),
        7 => Some(Value::Timestamp(zigzag_decode(decode_varint(bytes, &mut p)?))),
        8 => {
            let count = u16::from_le_bytes(bytes.get(..2)?.try_into().ok()?);
            p = 2;
            let mut fields = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let k_len = *bytes.get(p)? as usize;
                p += 1;
                let key = std::str::from_utf8(bytes.get(p..p + k_len)?).ok()?;
                p += k_len;
                let inner_tag = *bytes.get(p)?;
                p += 1;
                let v_len = u32::from_le_bytes(bytes.get(p..p+4)?.try_into().ok()?) as usize;
                p += 4;
                fields.push((Arc::from(key), decode_value(inner_tag, bytes.get(p..p + v_len)?)?));
                p += v_len;
            }
            Some(Value::Map(fields))
        }
        9 => {
            let count = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
            p = 4;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let inner_tag = *bytes.get(p)?;
                p += 1;
                let v_len = u32::from_le_bytes(bytes.get(p..p+4)?.try_into().ok()?) as usize;
                p += 4;
                items.push(decode_value(inner_tag, bytes.get(p..p + v_len)?)?);
                p += v_len;
            }
            Some(Value::Array(items))
        }
        10 => {
            let c_len = *bytes.get(0)? as usize;
            let collection = std::str::from_utf8(bytes.get(1..1+c_len)?).ok()?.to_string();
            let d_len = *bytes.get(1+c_len)? as usize;
            let doc_id = std::str::from_utf8(bytes.get(2+c_len..2+c_len+d_len)?).ok()?.to_string();
            Some(Value::Reference { collection, doc_id })
        }
        11 => {
            let offset = u64::from_le_bytes(bytes.get(..8)?.try_into().ok()?);
            let len = u32::from_le_bytes(bytes.get(8..12)?.try_into().ok()?);
            Some(Value::BlobLink { offset, len })
        }
        _ => Some(Value::Null),
    }
}