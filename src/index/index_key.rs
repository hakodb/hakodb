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
    }
}
