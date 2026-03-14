use std::collections::HashMap;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 2;

const TYPE_NULL: u8 = 1;
const TYPE_BOOL: u8 = 2;
const TYPE_INT: u8 = 3;
const TYPE_FLOAT: u8 = 4;
const TYPE_STRING: u8 = 5;
const TYPE_BINARY: u8 = 6;
const TYPE_ARRAY: u8 = 7;
const TYPE_OBJECT: u8 = 8;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Array(Vec<Value>),
    Object(FireLiteDoc),
}

#[derive(Debug, Clone)]
pub struct FireLiteDoc {
    pub fields: HashMap<String, Value>,
}

impl FireLiteDoc {

    pub fn new() -> Self {
        Self {
            fields: HashMap::new()
        }
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        self.fields.insert(key.into(), value);
    }

    pub fn encode(&self) -> Vec<u8> {

        let mut header = Vec::new();
        let mut data = Vec::new();

        header.push(MAGIC);
        header.push(VERSION);

        header.extend(&(self.fields.len() as u16).to_le_bytes());

        for (key, value) in &self.fields {

            let key_bytes = key.as_bytes();

            header.push(key_bytes.len() as u8);
            header.extend(key_bytes);

            let (value_type, encoded) = encode_value(value);

            header.push(value_type);

            let offset = data.len() as u32;
            header.extend(&offset.to_le_bytes());

            data.extend(encoded);
        }

        header.extend(data);

        header
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {

        let mut pos = 0;

        if bytes[pos] != MAGIC {
            return None;
        }

        pos += 1;

        let version = bytes[pos];
        pos += 1;

        if version != VERSION {
            return None;
        }

        let field_count =
            u16::from_le_bytes(bytes[pos..pos+2].try_into().ok()?) as usize;

        pos += 2;

        let mut fields = HashMap::new();

        let mut entries = Vec::new();

        for _ in 0..field_count {

            let key_len = bytes[pos] as usize;
            pos += 1;

            let key =
                String::from_utf8(bytes[pos..pos+key_len].to_vec()).ok()?;

            pos += key_len;

            let value_type = bytes[pos];
            pos += 1;

            let offset =
                u32::from_le_bytes(bytes[pos..pos+4].try_into().ok()?) as usize;

            pos += 4;

            entries.push((key, value_type, offset));
        }

        let data_start = pos;

        for (key, t, offset) in entries {

            let value = decode_value(
                t,
                &bytes[data_start + offset..]
            )?;

            fields.insert(key, value);
        }

        Some(Self { fields })
    }
}

fn encode_value(v: &Value) -> (u8, Vec<u8>) {

    match v {

        Value::Null => (TYPE_NULL, vec![]),

        Value::Bool(b) => (TYPE_BOOL, vec![*b as u8]),

        Value::Int(i) => (TYPE_INT, i.to_le_bytes().to_vec()),

        Value::Float(f) => (TYPE_FLOAT, f.to_le_bytes().to_vec()),

        Value::String(s) => {

            let mut buf = Vec::new();

            buf.extend(&(s.len() as u32).to_le_bytes());
            buf.extend(s.as_bytes());

            (TYPE_STRING, buf)
        }

        Value::Binary(b) => {

            let mut buf = Vec::new();

            buf.extend(&(b.len() as u32).to_le_bytes());
            buf.extend(b);

            (TYPE_BINARY, buf)
        }

        Value::Array(arr) => {

            let mut buf = Vec::new();

            buf.extend(&(arr.len() as u32).to_le_bytes());

            for v in arr {

                let (t, data) = encode_value(v);

                buf.push(t);
                buf.extend(data);
            }

            (TYPE_ARRAY, buf)
        }

        Value::Object(doc) => {

            let encoded = doc.encode();

            let mut buf = Vec::new();

            buf.extend(&(encoded.len() as u32).to_le_bytes());
            buf.extend(encoded);

            (TYPE_OBJECT, buf)
        }
    }
}

fn decode_value(t: u8, bytes: &[u8]) -> Option<Value> {

    let mut pos = 0;

    match t {

        TYPE_NULL => Some(Value::Null),

        TYPE_BOOL => Some(Value::Bool(bytes[pos] == 1)),

        TYPE_INT => {

            let v =
                i64::from_le_bytes(bytes[pos..pos+8].try_into().ok()?);

            Some(Value::Int(v))
        }

        TYPE_FLOAT => {

            let v =
                f64::from_le_bytes(bytes[pos..pos+8].try_into().ok()?);

            Some(Value::Float(v))
        }

        TYPE_STRING => {

            let len =
                u32::from_le_bytes(bytes[pos..pos+4].try_into().ok()?) as usize;

            pos += 4;

            let s =
                String::from_utf8(bytes[pos..pos+len].to_vec()).ok()?;

            Some(Value::String(s))
        }

        TYPE_BINARY => {

            let len =
                u32::from_le_bytes(bytes[pos..pos+4].try_into().ok()?) as usize;

            pos += 4;

            Some(Value::Binary(bytes[pos..pos+len].to_vec()))
        }

        TYPE_ARRAY => {

            let count =
                u32::from_le_bytes(bytes[pos..pos+4].try_into().ok()?) as usize;

            pos += 4;

            let mut arr = Vec::new();

            for _ in 0..count {

                let t = bytes[pos];
                pos += 1;

                let val = decode_value(t, &bytes[pos..])?;

                arr.push(val);
            }

            Some(Value::Array(arr))
        }

        TYPE_OBJECT => {

            let len =
                u32::from_le_bytes(bytes[pos..pos+4].try_into().ok()?) as usize;

            pos += 4;

            let doc =
                FireLiteDoc::decode(&bytes[pos..pos+len])?;

            Some(Value::Object(doc))
        }

        _ => None
    }
}
