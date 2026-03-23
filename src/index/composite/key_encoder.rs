use crate::document::value::Value;
use smallvec::SmallVec;

use super::definition::{CompositeIndexDefinition, SortDirection};

// pub fn encode_composite_key(
//     def: &CompositeIndexDefinition,
//     values: &[Value],
//     doc_id: &str,
// ) -> Vec<u8> {
//     let mut out = Vec::new();
//     out.extend(def.id.to_be_bytes());
//     for (value, field) in values.iter().zip(def.fields.iter()) {
//         encode_value(value, &field.direction, &mut out);
//     }
//     out.extend((doc_id.len() as u16).to_be_bytes());
//     out.extend(doc_id.as_bytes());
//     out
// }

// pub fn encode_composite_key(
//     def: &CompositeIndexDefinition,
//     values: &[Value],
//     doc_id: &str,
// ) -> SmallVec<[u8; 32]> {

//     let mut key: SmallVec<[u8; 32]> = SmallVec::new();

//     for v in values {
//         match v {
//             Value::Int(i) => key.extend_from_slice(&i.to_le_bytes()),
//             Value::Float(f) => key.extend_from_slice(&f.to_le_bytes()),
//             Value::Bool(b) => key.push(*b as u8),
//             Value::String(s) => {
//                 key.extend_from_slice(&(s.len() as u16).to_le_bytes());
//                 key.extend_from_slice(s.as_bytes());
//             }
//             Value::Binary(b) => {
//                 key.extend_from_slice(&(b.len() as u16).to_le_bytes());
//                 key.extend_from_slice(b);
//             }
//             Value::Null => key.push(0),
//         }
//     }

//     key.extend_from_slice(doc_id.as_bytes());

//     key
// }

// fn encode_value(value: &Value, direction: &SortDirection, out: &mut Vec<u8>) {
//     match value {
//         Value::Null => out.push(0),
//         Value::Bool(v) => {
//             out.push(1);
//             out.push(*v as u8);
//         }
//         Value::Int(v) => {
//             out.push(2);
//             let mut bytes = v.to_be_bytes();
//             maybe_flip(direction, &mut bytes);
//             out.extend(bytes);
//         }
//         Value::Float(v) => {
//             out.push(3);
//             let mut bytes = v.to_bits().to_be_bytes();
//             maybe_flip(direction, &mut bytes);
//             out.extend(bytes);
//         }
//         Value::String(v) => {
//             out.push(4);
//             let mut bytes = v.as_bytes().to_vec();
//             maybe_flip(direction, &mut bytes);
//             out.extend((bytes.len() as u32).to_be_bytes());
//             out.extend(bytes);
//         }
//         Value::Binary(v) => {
//             out.push(5);
//             let mut bytes = v.clone();
//             maybe_flip(direction, &mut bytes);
//             out.extend((bytes.len() as u32).to_be_bytes());
//             out.extend(bytes);
//         }
//     }
// }

// fn maybe_flip(direction: &SortDirection, bytes: &mut [u8]) {
//     if matches!(direction, SortDirection::Desc) {
//         for b in bytes {
//             *b = !*b;
//         }
//     }
// }


pub fn encode_composite_key(
    def: &CompositeIndexDefinition,
    values: &[Value],
    doc_id: &str,
) -> SmallVec<[u8; 32]> {

    let mut out: SmallVec<[u8; 32]> = SmallVec::new();

    // index id (helps when merging indexes)
    out.extend_from_slice(&def.id.to_be_bytes());

    for (value, field) in values.iter().zip(def.fields.iter()) {
        encode_value(value, &field.direction, &mut out);
    }

    // doc_id length + doc_id
    out.extend_from_slice(&(doc_id.len() as u16).to_be_bytes());
    out.extend_from_slice(doc_id.as_bytes());

    out
}

fn encode_value(
    value: &Value,
    direction: &SortDirection,
    out: &mut SmallVec<[u8; 32]>,
) {
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
            out.extend_from_slice(&bytes);
        }

        Value::Float(v) => {
            out.push(3);
            let mut bytes = v.to_bits().to_be_bytes();
            maybe_flip(direction, &mut bytes);
            out.extend_from_slice(&bytes);
        }

        Value::String(v) => {
            out.push(4);
            let mut bytes = v.as_bytes().to_vec();
            maybe_flip(direction, &mut bytes);

            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }

        Value::Binary(v) => {
            out.push(5);

            let mut bytes = v.clone();
            maybe_flip(direction, &mut bytes);

            out.extend_from_slice(&(bytes.len() as u32).to_be_bytes());
            out.extend_from_slice(&bytes);
        }

        Value::Timestamp(v) => {
            out.push(6); // Tag for Timestamp
            let mut bytes = v.to_be_bytes(); // Use big-endian for correct sorting
            maybe_flip(direction, &mut bytes);
            out.extend_from_slice(&bytes);
        }

        Value::Map(_) => {
            out.push(8); // Tag for Map
            // For now, we don't support sorting by the entire Map structure
        }

        Value::Array(_) => { // <--- ADD THIS
            out.push(9); // Tag for Array
        }

        Value::Reference { .. } => {
            out.push(10); // Tag 10
            // We don't support range sorting by the reference contents yet
        }

        Value::ServerTimestamp => {
            out.push(0); // Treat as Null if it somehow hits the index
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
