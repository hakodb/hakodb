use crate::document::firelite_doc::Value;
use super::definition::{CompositeIndexDefinition, SortDirection};

pub fn encode_composite_key(
    def: &CompositeIndexDefinition,
    values: &[Value],
    doc_id: &str,
) -> Vec<u8> {

    let mut key = Vec::new();

    key.extend(&def.collection_id.to_be_bytes());

    for (value, field_def) in values.iter().zip(def.fields.iter()) {

        encode_value(value, &mut key, &field_def.direction);
    }

    key.push(doc_id.len() as u8);
    key.extend(doc_id.as_bytes());

    key
}

fn encode_value(
    value: &Value,
    buf: &mut Vec<u8>,
    direction: &SortDirection,
) {

    match value {

        Value::Int(v) => {

            buf.push(1);

            let mut bytes = v.to_be_bytes();

            if matches!(direction, SortDirection::Desc) {
                for b in &mut bytes { *b = !*b; }
            }

            buf.extend(bytes);
        }

        Value::Float(v) => {

            buf.push(2);

            let mut bytes = v.to_be_bytes();

            if matches!(direction, SortDirection::Desc) {
                for b in &mut bytes { *b = !*b; }
            }

            buf.extend(bytes);
        }

        Value::String(s) => {

            buf.push(3);

            let mut bytes = s.as_bytes().to_vec();

            if matches!(direction, SortDirection::Desc) {
                for b in &mut bytes { *b = !*b; }
            }

            buf.extend(&(bytes.len() as u16).to_be_bytes());
            buf.extend(bytes);
        }

        _ => {}
    }
}
