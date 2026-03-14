use crate::document::firelite_doc::Value;

pub fn encode_index_key(
    collection_id: u32,
    field: &str,
    value: &Value,
    doc_id: &str,
) -> Vec<u8> {

    let mut key = Vec::new();

    key.extend(&collection_id.to_be_bytes());

    key.push(field.len() as u8);
    key.extend(field.as_bytes());

    encode_value(value, &mut key);

    key.push(doc_id.len() as u8);
    key.extend(doc_id.as_bytes());

    key
}

fn encode_value(value: &Value, buf: &mut Vec<u8>) {

    match value {

        Value::Null => buf.push(1),

        Value::Bool(v) => {
            buf.push(2);
            buf.push(*v as u8);
        }

        Value::Int(v) => {
            buf.push(3);
            buf.extend(&v.to_be_bytes());
        }

        Value::Float(v) => {
            buf.push(4);
            buf.extend(&v.to_be_bytes());
        }

        Value::String(s) => {
            buf.push(5);
            buf.extend(&(s.len() as u16).to_be_bytes());
            buf.extend(s.as_bytes());
        }

        _ => {}
    }
}
