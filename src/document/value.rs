use std::cmp::Ordering;
use std::sync::Arc;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    BlobLink { offset: u64, len: u32 }, 
    Reference { collection: String, doc_id: String },
    Timestamp(i64), 
    ServerTimestamp,
    // Map(Vec<(String, Value)>),
    Map(#[serde(with = "serde_arc_str_map")] Vec<(Arc<str>, Value)>), 
    Array(Vec<Value>),
}

impl Value {
    /// Internal weight to allow sorting different types against each other.
    fn type_weight(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 2, // Numbers sort together
            Value::Timestamp(_) => 3,
            Value::String(_) => 4,
            Value::Binary(_) | Value::BlobLink { .. } => 5,
            Value::Reference { .. } => 6,
            Value::Array(_) => 7,
            Value::Map(_) => 8,
            Value::ServerTimestamp => 9,
        }
    }

    /// Converts a FireLite Value into a serde_json::Value.
    /// This is used by the FFI, Tauri Gateway, and CLI.
    pub fn to_json(&self) -> serde_json::Value {
        match self {
            Value::Null | Value::ServerTimestamp => serde_json::Value::Null,
            Value::Bool(b) => serde_json::Value::Bool(*b),
            Value::Int(i) => serde_json::Value::Number((*i).into()),
            Value::Float(f) => serde_json::Number::from_f64(*f)
                .map(serde_json::Value::Number)
                .unwrap_or(serde_json::Value::Null),
            Value::String(s) => serde_json::Value::String(s.clone()),
            Value::Binary(bytes) => serde_json::Value::Array(
                bytes.iter().map(|b| serde_json::Value::Number((*b as u64).into())).collect()
            ),
            Value::Timestamp(micros) => serde_json::Value::Number((*micros).into()),
            Value::Reference { collection, doc_id } => {
                let mut map = serde_json::Map::new();
                map.insert("__ref__".to_string(), serde_json::Value::String(format!("{collection}/{doc_id}")));
                serde_json::Value::Object(map)
            }
            Value::BlobLink { offset, len } => {
                let mut map = serde_json::Map::new();
                let mut meta = serde_json::Map::new();
                meta.insert("offset".to_string(), (*offset).into());
                meta.insert("len".to_string(), (*len).into());
                map.insert("__blob__".to_string(), serde_json::Value::Object(meta));
                serde_json::Value::Object(map)
            }
            Value::Map(fields) => {
                let mut map = serde_json::Map::new();
                for (k, v) in fields {
                    map.insert(k.to_string(), v.to_json());
                }
                serde_json::Value::Object(map)
            }
            Value::Array(values) => {
                serde_json::Value::Array(values.iter().map(|v| v.to_json()).collect())
            }
        }
    }

    /// Converts a serde_json::Value into a FireLite Value.
    /// Automatically detects special keys like __ref__ and __blob__.
    pub fn from_json(json: serde_json::Value) -> std::result::Result<Self, String> {
        match json {
            serde_json::Value::Null => Ok(Value::Null),
            serde_json::Value::Bool(b) => Ok(Value::Bool(b)),
            serde_json::Value::Number(n) => {
                if let Some(i) = n.as_i64() { Ok(Value::Int(i)) }
                else { Ok(Value::Float(n.as_f64().unwrap_or(0.0))) }
            }
            serde_json::Value::String(s) => Ok(Value::String(s)),
            serde_json::Value::Array(arr) => {
                let mut values = Vec::with_capacity(arr.len());
                for val in arr { values.push(Value::from_json(val)?); }
                Ok(Value::Array(values))
            }
            serde_json::Value::Object(obj) => {
                // Handle References
                if let Some(serde_json::Value::String(path)) = obj.get("__ref__") {
                    if let Some((col, id)) = path.split_once('/') {
                        return Ok(Value::Reference {
                            collection: col.to_string(),
                            doc_id: id.to_string(),
                        });
                    }
                }
                // Handle Blobs
                if let Some(serde_json::Value::Object(meta)) = obj.get("__blob__") {
                    let offset = meta.get("offset").and_then(|o| o.as_u64()).unwrap_or(0);
                    let len = meta.get("len").and_then(|l| l.as_u64()).unwrap_or(0) as u32;
                    return Ok(Value::BlobLink { offset, len });
                }
                // Handle Maps
                let mut map = Vec::with_capacity(obj.len());
                for (k, v) in obj {
                    map.push((Arc::from(k.as_str()), Value::from_json(v)?));
                }
                Ok(Value::Map(map))
            }
        }
    }

    pub fn len_bytes(&self) -> usize {
        match self {
            Value::String(s) => s.len(),
            Value::Binary(b) => b.len(),
            _ => 0,
        }
    }
}

// Ensure PartialEq matches the logic in Ord
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Int(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Int(b)) => *a == (*b as f64),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::BlobLink { offset: a, len: al }, Value::BlobLink { offset: b, len: bl }) => a == b && al == bl,
            (Value::Binary(_a), Value::BlobLink { .. }) => false,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            (Value::ServerTimestamp, Value::ServerTimestamp) => true,
            _ => false,
        }
    }
}

impl Eq for Value {}

// Add this module at the bottom of src/document/value.rs
mod serde_arc_str_map {
    use super::*;
    use serde::{Serializer, Deserializer, Deserialize}; // <--- ADD THIS
    // use serde::ser::SerializeSeq;

    pub fn serialize<S>(vec: &[(Arc<str>, Value)], s: S) -> Result<S::Ok, S::Error> where S: Serializer {
        use serde::ser::SerializeSeq;
        let mut seq = s.serialize_seq(Some(vec.len()))?;
        for (k, v) in vec { seq.serialize_element(&(k.to_string(), v))? }
        seq.end()
    }
    pub fn deserialize<'de, D>(d: D) -> Result<Vec<(Arc<str>, Value)>, D::Error> where D: Deserializer<'de> {
        let raw: Vec<(String, Value)> = Deserialize::deserialize(d)?;
        Ok(raw.into_iter().map(|(k, v)| (Arc::from(k), v)).collect())
    }
}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let w1 = self.type_weight();
        let w2 = other.type_weight();

        if w1 != w2 {
            return w1.cmp(&w2);
        }

        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            
            // Numeric comparison (cross-type support)
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.total_cmp(&(*b as f64)),

            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            
            (Value::Reference { collection: c1, doc_id: i1 }, Value::Reference { collection: c2, doc_id: i2 }) => {
                c1.cmp(c2).then(i1.cmp(i2))
            }
            
            (Value::Map(a), Value::Map(b)) => {
                // Compare Map lengths, then lexicographically by fields
                let res = a.len().cmp(&b.len());
                if res != Ordering::Equal { return res; }
                for ((k1, v1), (k2, v2)) in a.iter().zip(b.iter()) {
                    // HELP THE COMPILER WITH TYPES
                    let k_cmp = k1.as_ref().cmp(k2.as_ref());
                    if k_cmp != Ordering::Equal { return k_cmp; }
                    let v_cmp = v1.cmp(v2);
                    if v_cmp != Ordering::Equal { return v_cmp; }
                }
                Ordering::Equal
            }

            (Value::Array(a), Value::Array(b)) => {
                let res = a.len().cmp(&b.len());
                if res != Ordering::Equal { return res; }
                a.cmp(b) // Lexicographical comparison of elements
            }
            
            (Value::ServerTimestamp, Value::ServerTimestamp) => Ordering::Equal,
            _ => Ordering::Equal,
        }
    }
}