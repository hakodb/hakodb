use crate::document::value::Value;

use super::definition::{CompositeIndexDefinition, SortDirection};

pub fn encode_composite_key(
    def: &CompositeIndexDefinition,
    values: &[Value],
    doc_id: &str,
) -> Vec<u8> {
    let mut out = Vec::new();
    out.extend(def.id.to_be_bytes());
    for (value, field) in values.iter().zip(def.fields.iter()) {
        encode_value(value, &field.direction, &mut out);
    }
    out.extend((doc_id.len() as u16).to_be_bytes());
    out.extend(doc_id.as_bytes());
    out
}

fn encode_value(value: &Value, direction: &SortDirection, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(0),
        Value::Bool(v) => {
            out.push(1);
            out.push(*v as u8);
        }
        Value::Int(v) => {
            out.push(2);
            let mut bytes = v.to_be_bytes();
            maybe_flip(direction, &mut bytes);
            out.extend(bytes);
        }
        Value::Float(v) => {
            out.push(3);
            let mut bytes = v.to_bits().to_be_bytes();
            maybe_flip(direction, &mut bytes);
            out.extend(bytes);
        }
        Value::String(v) => {
            out.push(4);
            let mut bytes = v.as_bytes().to_vec();
            maybe_flip(direction, &mut bytes);
            out.extend((bytes.len() as u32).to_be_bytes());
            out.extend(bytes);
        }
        Value::Binary(v) => {
            out.push(5);
            let mut bytes = v.clone();
            maybe_flip(direction, &mut bytes);
            out.extend((bytes.len() as u32).to_be_bytes());
            out.extend(bytes);
        }
    }
}

fn maybe_flip(direction: &SortDirection, bytes: &mut [u8]) {
    if matches!(direction, SortDirection::Desc) {
        for b in bytes {
            *b = !*b;
        }
    }
}
