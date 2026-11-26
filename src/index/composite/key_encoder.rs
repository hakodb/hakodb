use crate::document::value::Value;
use smallvec::SmallVec;

use super::definition::{CompositeIndexDefinition, SortDirection};

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

/// Unique-index claim prefix (hakodb#71): the index-id + tuple head of a
/// composite key WITHOUT the doc-id tail. Two documents share a claim iff
/// their full keys differ only in the tail — the uniqueness condition.
pub fn encode_composite_prefix(
    def: &CompositeIndexDefinition,
    values: &[Value],
) -> SmallVec<[u8; 32]> {
    let mut out: SmallVec<[u8; 32]> = SmallVec::new();
    out.extend_from_slice(&def.id.to_be_bytes());
    for (value, field) in values.iter().zip(def.fields.iter()) {
        encode_value(value, &field.direction, &mut out);
    }
    out
}

pub(crate) fn encode_value(
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
            // ponytail: encode in place and flip the tail directly instead
            // of cloning the bytes to a temp Vec first.
            out.push(4);
            out.extend_from_slice(&(v.len() as u32).to_be_bytes());
            let body_pos = out.len();
            out.extend_from_slice(v.as_bytes());
            if matches!(direction, SortDirection::Desc) {
                maybe_flip(direction, &mut out[body_pos..]);
            }
        }

        Value::Binary(v) => {
            // ponytail: same — no temp clone.
            out.push(5);
            out.extend_from_slice(&(v.len() as u32).to_be_bytes());
            let body_pos = out.len();
            out.extend_from_slice(v);
            if matches!(direction, SortDirection::Desc) {
                maybe_flip(direction, &mut out[body_pos..]);
            }
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

        // ADD THIS ARM:
        Value::BlobLink { .. } => {
            out.push(11); // Tag for Link
            // We don't encode the offset into the index key because 
            // the index should ideally be based on content, not file address.
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

fn value_span(key: &[u8], pos: usize, tag: u8) -> Option<usize> {
    let len = match tag {
        0 => 1,
        1 => 2,
        2 | 3 | 6 => 9,
        4 | 5 => {
            let mut bytes = [0u8; 4];
            bytes.copy_from_slice(key.get(pos + 1..pos + 5)?);
            5 + u32::from_be_bytes(bytes) as usize
        }
        _ => return None,
    };
    pos.checked_add(len).filter(|end| *end <= key.len())
}

/// Decode only the numeric target field from a composite tree key. Prefix
/// values are skipped by width, so no `Value` is allocated per row. The
/// doc-id tail is never read.
pub(crate) fn decode_composite_numeric(
    def: &CompositeIndexDefinition,
    key: &[u8],
    skip: usize,
    target: &str,
) -> Option<f64> {
    if key.len() < 4 {
        return None;
    }
    let mut pos = 4; // index id
    for _ in 0..skip {
        let tag = *key.get(pos)?;
        pos = value_span(key, pos, tag)?;
    }
    for field in def.fields.iter().skip(skip) {
        let tag = *key.get(pos)?;
        if field.field == target {
            let bytes = key.get(pos + 1..pos + 9)?;
            let mut raw = [0u8; 8];
            raw.copy_from_slice(bytes);
            maybe_flip(&field.direction, &mut raw);
            return match tag {
                2 => Some(i64::from_be_bytes(raw) as f64),
                3 => Some(f64::from_bits(u64::from_be_bytes(raw))),
                _ => None,
            };
        }
        pos = value_span(key, pos, tag)?;
    }
    None
}
