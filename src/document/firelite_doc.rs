use std::collections::HashMap;
use crate::error::FireLiteError;

pub type Result<T> = std::result::Result<T, FireLiteError>;

const MAGIC: u8 = 0xF1;

const TYPE_NULL: u8 = 1;
const TYPE_BOOL: u8 = 2;
const TYPE_INT: u8 = 3;
const TYPE_FLOAT: u8 = 4;
const TYPE_STRING: u8 = 5;
const TYPE_BINARY: u8 = 6;

#[derive(Debug, Clone)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
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

    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.get(key)
    }

    pub fn encode(&self) -> Vec<u8> {

        let mut buf = Vec::new();

        buf.push(MAGIC);

        buf.extend(&(self.fields.len() as u16).to_le_bytes());

        for (key, value) in &self.fields {

            let key_bytes = key.as_bytes();

            buf.push(key_bytes.len() as u8);
            buf.extend(key_bytes);

            match value {

                Value::Null => {
                    buf.push(TYPE_NULL);
                }

                Value::Bool(v) => {
                    buf.push(TYPE_BOOL);
                    buf.push(if *v {1} else {0});
                }

                Value::Int(v) => {
                    buf.push(TYPE_INT);
                    buf.extend(&v.to_le_bytes());
                }

                Value::Float(v) => {
                    buf.push(TYPE_FLOAT);
                    buf.extend(&v.to_le_bytes());
                }

                Value::String(s) => {

                    let b = s.as_bytes();

                    buf.push(TYPE_STRING);
                    buf.extend(&(b.len() as u32).to_le_bytes());
                    buf.extend(b);
                }

                Value::Binary(b) => {

                    buf.push(TYPE_BINARY);
                    buf.extend(&(b.len() as u32).to_le_bytes());
                    buf.extend(b);
                }
            }
        }

        buf
    }

    pub fn decode(bytes: &[u8]) -> Result<Self> {

        let mut pos = 0;

        if bytes[pos] != MAGIC {
            return Err(FireLiteError::InvalidRecord);
        }

        pos += 1;

        let field_count =
            u16::from_le_bytes(bytes[pos..pos+2].try_into().unwrap());
        pos += 2;

        let mut fields = HashMap::new();

        for _ in 0..field_count {

            let key_len = bytes[pos] as usize;
            pos += 1;

            let key = String::from_utf8(
                bytes[pos..pos+key_len].to_vec()
            ).map_err(|_| FireLiteError::InvalidRecord)?;

            pos += key_len;

            let value_type = bytes[pos];
            pos += 1;

            let value = match value_type {

                TYPE_NULL => Value::Null,

                TYPE_BOOL => {

                    let v = bytes[pos] == 1;
                    pos += 1;

                    Value::Bool(v)
                }

                TYPE_INT => {

                    let v = i64::from_le_bytes(
                        bytes[pos..pos+8].try_into().unwrap()
                    );

                    pos += 8;

                    Value::Int(v)
                }

                TYPE_FLOAT => {

                    let v = f64::from_le_bytes(
                        bytes[pos..pos+8].try_into().unwrap()
                    );

                    pos += 8;

                    Value::Float(v)
                }

                TYPE_STRING => {

                    let len =
                        u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap())
                        as usize;

                    pos += 4;

                    let s = String::from_utf8(
                        bytes[pos..pos+len].to_vec()
                    ).map_err(|_| FireLiteError::InvalidRecord)?;

                    pos += len;

                    Value::String(s)
                }

                TYPE_BINARY => {

                    let len =
                        u32::from_le_bytes(bytes[pos..pos+4].try_into().unwrap())
                        as usize;

                    pos += 4;

                    let b = bytes[pos..pos+len].to_vec();
                    pos += len;

                    Value::Binary(b)
                }

                _ => return Err(FireLiteError::InvalidRecord)
            };

            fields.insert(key, value);
        }

        Ok(Self { fields })
    }
}
