use crate::document::value::Value;

pub fn encode_scalar(value: &Value) -> Vec<u8> {
    match value {
        Value::Null => vec![0],
        Value::Bool(v) => vec![1, *v as u8],
        Value::Int(v) => {
            let mut out = vec![2];
            out.extend(v.to_be_bytes());
            out
        }
        Value::Float(v) => {
            let mut out = vec![3];
            out.extend(v.to_bits().to_be_bytes());
            out
        }
        Value::String(v) => {
            let mut out = vec![4];
            out.extend((v.len() as u32).to_be_bytes());
            out.extend(v.as_bytes());
            out
        }
        Value::Binary(v) => {
            let mut out = vec![5];
            out.extend((v.len() as u32).to_be_bytes());
            out.extend(v);
            out
        }
        Value::Timestamp(v) => {
            let mut out = vec![6];
            out.extend(v.to_be_bytes());
            out
        }
        Value::ServerTimestamp => vec![0], // Fallback to Null
        Value::Map(_) => vec![8],
        Value::Array(_) => vec![9],
        Value::Reference { collection, doc_id } => {
            let mut b = vec![10]; // Tag 10
            b.extend_from_slice(collection.as_bytes());
            b.push(b':');
            b.extend_from_slice(doc_id.as_bytes());
            b
        },
        // ADD THIS ARM:
        Value::BlobLink { offset, len } => {
            let mut b = vec![11]; // Tag 11
            b.extend_from_slice(&offset.to_be_bytes());
            b.extend_from_slice(&len.to_be_bytes());
            b
        }
    }
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
