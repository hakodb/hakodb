use std::collections::BTreeMap;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
}

#[derive(Debug, Clone, PartialEq, Default)]
pub struct FireLiteDoc {
    pub fields: BTreeMap<String, Value>,
}

impl FireLiteDoc {
    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.fields.insert(key.into(), value);
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
}

pub struct FireLiteDocView<'a> {
    bytes: &'a [u8],
    data_offset: usize,
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
            data_offset: 4,
            fields,
        })
    }

    pub fn iter(&self) -> FireLiteDocIter<'a> {
        FireLiteDocIter {
            bytes: self.bytes,
            pos: self.data_offset,
            remaining: self.fields,
        }
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
    }
}

fn decode_value(tag: u8, bytes: &[u8]) -> Option<Value> {
    Some(match tag {
        1 => Value::Null,
        2 => Value::Bool(*bytes.first()? == 1),
        3 => Value::Int(i64::from_le_bytes(bytes.get(..8)?.try_into().ok()?)),
        4 => Value::Float(f64::from_le_bytes(bytes.get(..8)?.try_into().ok()?)),
        5 => Value::String(String::from_utf8(bytes.to_vec()).ok()?),
        6 => Value::Binary(bytes.to_vec()),
        _ => return None,
    })
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
