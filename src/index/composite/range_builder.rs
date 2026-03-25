use crate::document::value::Value;
use smallvec::SmallVec;

use super::definition::CompositeIndexDefinition;
use super::key_encoder::encode_composite_key;

#[derive(Debug, Clone)]
pub struct ScanRange {
    // pub start: Vec<u8>,
    // pub end: Vec<u8>,
    pub start: SmallVec<[u8; 32]>,
    pub end: SmallVec<[u8; 32]>,
}

pub fn build_prefix_range(def: &CompositeIndexDefinition, values: &[Value]) -> ScanRange {
    let start = build_prefix_key(def, values);
    let end = build_prefix_open_end(&start);

    ScanRange { start, end }
}

pub fn build_prefix_key(def: &CompositeIndexDefinition, values: &[Value]) -> SmallVec<[u8; 32]> {
    let mut key: SmallVec<[u8; 32]> = SmallVec::new();
    key.extend_from_slice(&def.id.to_be_bytes());
    for (value, field) in values.iter().zip(def.fields.iter()) {
        crate::index::composite::key_encoder::encode_value(value, &field.direction, &mut key);
    }
    key
}

pub fn build_prefix_open_end(prefix: &SmallVec<[u8; 32]>) -> SmallVec<[u8; 32]> {
    let mut end = prefix.clone();
    end.push(0xFF);
    end
}

pub fn build_value_key(
    def: &CompositeIndexDefinition,
    eq_prefix_values: &[Value],
    range_value: &Value,
) -> Option<SmallVec<[u8; 32]>> {
    let range_pos = eq_prefix_values.len();
    let field = def.fields.get(range_pos)?;
    let mut key = build_prefix_key(def, eq_prefix_values);
    crate::index::composite::key_encoder::encode_value(range_value, &field.direction, &mut key);
    Some(key)
}

pub fn key_with_upper_sentinel(key: &SmallVec<[u8; 32]>) -> SmallVec<[u8; 32]> {
    let mut out = key.clone();
    out.push(0xFF);
    out
}

pub fn build_cursor_range(
    def: &CompositeIndexDefinition,
    cursor_values: &[Value],
    is_after: bool,
) -> SmallVec<[u8; 32]> {
    let mut key = encode_composite_key(def, cursor_values, "");
    if is_after {
        key.push(0xFF);
    }
    key
}
