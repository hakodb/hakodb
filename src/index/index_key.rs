use crate::document::firelite_doc::Value;

pub fn encode_value(v: &Value) -> Vec<u8> {
    match v {
        Value::Null => vec![0],
        Value::Bool(b) => vec![1, *b as u8],
        Value::Int(i) => {
            let mut out = vec![2];
            out.extend(i.to_be_bytes());
            out
        }
        Value::Float(f) => {
            let mut out = vec![3];
            out.extend(f.to_bits().to_be_bytes());
            out
        }
        Value::String(s) => {
            let mut out = vec![4];
            out.extend((s.len() as u32).to_be_bytes());
            out.extend(s.as_bytes());
            out
        }
        Value::Binary(b) => {
            let mut out = vec![5];
            out.extend((b.len() as u32).to_be_bytes());
            out.extend(b);
            out
        }
    }
}

pub fn encode_composite_key(values: &[Value], doc_id: &str) -> Vec<u8> {
    let mut out = Vec::new();
    for value in values {
        let enc = encode_value(value);
        out.extend((enc.len() as u32).to_be_bytes());
        out.extend(enc);
    }
    out.extend((doc_id.len() as u16).to_be_bytes());
    out.extend(doc_id.as_bytes());
    out
}
