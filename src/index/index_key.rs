use crate::document::value::Value;
use std::cell::RefCell;

thread_local! {
    /// Reused encode buffer for index maintenance (`index_document` encodes
    /// every indexed field per write). Borrowed per field, never escapes.
    pub static ENC_SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128));
}

pub fn encode_scalar(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_scalar_into(value, &mut out);
    out
}

/// ponytail: borrow-friendly form — encode into a caller-provided buffer
/// (e.g. a reused thread-local scratch) instead of allocating per call.
/// Byte-identical output to `encode_scalar`.
pub fn encode_scalar_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(0),
        Value::Bool(v) => { out.push(1); out.push(*v as u8); }
        Value::Int(v) => {
            out.push(2);
            out.extend(v.to_be_bytes());
        }
        Value::Float(v) => {
            out.push(3);
            out.extend(v.to_bits().to_be_bytes());
        }
        Value::String(v) => encode_str_scalar_into(v, out),
        Value::Binary(v) => {
            out.push(5);
            out.extend((v.len() as u32).to_be_bytes());
            out.extend(v);
        }
        Value::Timestamp(v) => {
            out.push(6);
            out.extend(v.to_be_bytes());
        }
        Value::ServerTimestamp => out.push(0), // Fallback to Null
        Value::Map(_) => out.push(8),
        Value::Array(_) => out.push(9),
        Value::Reference { collection, doc_id } => {
            out.reserve(1 + 1 + collection.len() + 1 + doc_id.len());
            out.push(10); // Tag 10
            out.push(collection.len() as u8);
            out.extend_from_slice(collection.as_bytes());
            out.push(doc_id.len() as u8);
            out.extend_from_slice(doc_id.as_bytes());
        }
        Value::BlobLink { offset, len } => {
            out.push(11); // Tag 11
            out.extend_from_slice(&offset.to_be_bytes());
            out.extend_from_slice(&len.to_be_bytes());
        }
    }
}

/// String scalar encoding shared by `encode_scalar_into` and the id fast
/// path in index maintenance (avoids building a temp `Value::String`).
pub fn encode_str_scalar_into(s: &str, out: &mut Vec<u8>) {
    out.push(4);
    out.extend((s.len() as u32).to_be_bytes());
    out.extend(s.as_bytes());
}

pub fn decode_scalar_as_f64(bytes: &[u8]) -> Option<f64> {
    let tag = *bytes.first()?;
    match tag {
        2 => { // Int
            let b: [u8; 8] = bytes.get(1..9)?.try_into().ok()?;
            Some(i64::from_be_bytes(b) as f64)
        }
        3 => { // Float
            let b: [u8; 8] = bytes.get(1..9)?.try_into().ok()?;
            Some(f64::from_bits(u64::from_be_bytes(b)))
        }
        _ => None
    }
}
