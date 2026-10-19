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
    pub(crate) fn type_weight(&self) -> u8 {
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

    /// Converts a HakoDB Value into a serde_json::Value.
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

    /// Converts a serde_json::Value into a HakoDB Value.
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
            (Value::Binary(a), Value::Binary(b)) => a == b,
            (Value::Array(a), Value::Array(b)) => a == b,
            (Value::Reference { collection: a, doc_id: ad }, Value::Reference { collection: b, doc_id: bd }) => {
                a == b && ad == bd
            }
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
// --- hakodb#71 F6: prefix-path projection -------------------------------
// A projection path may descend with dots (profile.city). Pruning keeps
// only the selected subtrees and is leaf-preserving: when a path cannot
// descend (the value is not a Map), the value is kept as-is rather than
// dropped — returning nothing would hide data. Fields absent under a
// selected prefix are simply omitted (an empty Map is left behind), and
// arrays prune every element with the same relative path.

/// Prune alue down to the subtrees named by paths, where every path is
/// a *relative* non-empty segment list.
pub fn prune_value(value: &Value, paths: &[Vec<&str>]) -> Value {
    if paths.is_empty() {
        return value.clone();
    }
    match value {
        Value::Map(entries) => {
            let mut out: Vec<(std::sync::Arc<str>, Value)> = Vec::new();
            for (k, v) in entries {
                let mut subs: Vec<Vec<&str>> = Vec::new();
                let mut whole = false;
                for p in paths {
                    match p.first() {
                        Some(seg) if *seg == k.as_ref() => {
                            if p.len() == 1 {
                                whole = true;
                                break;
                            }
                            subs.push(p[1..].to_vec());
                        }
                        _ => {}
                    }
                }
                if whole {
                    out.push((k.clone(), v.clone()));
                } else if !subs.is_empty() {
                    out.push((k.clone(), prune_value(v, &subs)));
                }
            }
            Value::Map(out)
        }
        Value::Array(items) => Value::Array(items.iter().map(|i| prune_value(i, paths)).collect()),
        other => other.clone(),
    }
}

/// Prune the value of top-level key for a full dotted projection.
/// None means the key is not selected at all.
pub fn prune_field_value(value: &Value, projection: &[String], key: &str) -> Option<Value> {
    let mut whole = false;
    let mut subs: Vec<Vec<&str>> = Vec::new();
    for p in projection {
        let mut segs = p.split('.');
        if segs.next() != Some(key) {
            continue;
        }
        let rest: Vec<&str> = segs.collect();
        if rest.is_empty() {
            whole = true;
            break;
        }
        if rest.iter().any(|s| s.is_empty()) {
            continue; // malformed path selects nothing
        }
        subs.push(rest);
    }
    if whole {
        Some(value.clone())
    } else if subs.is_empty() {
        None
    } else {
        Some(prune_value(value, &subs))
    }
}

/// Top-level field names a projection needs decoded (prefix segments).
pub fn projection_prefixes(projection: &[String]) -> Vec<&str> {
    let mut out: Vec<&str> = Vec::with_capacity(projection.len());
    for p in projection {
        if let Some(first) = p.split('.').next() {
            if !first.is_empty() && !out.contains(&first) {
                out.push(first);
            }
        }
    }
    out
}
