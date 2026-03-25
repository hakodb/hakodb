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
    let mut start: SmallVec<[u8; 32]> = SmallVec::new();
    
    // 1. Write Index ID
    start.extend_from_slice(&def.id.to_be_bytes());
    
    // 2. Write exactly the prefix values (NO doc_id or trailing lengths)
    for (value, field) in values.iter().zip(def.fields.iter()) {
        crate::index::composite::key_encoder::encode_value(value, &field.direction, &mut start);
    }
    
    let mut end = start.clone();
    end.push(0xFF); // Upper bound for the prefix search
    
    ScanRange { start, end }
}

pub fn build_cursor_range(
    def: &CompositeIndexDefinition, 
    cursor_values: &[Value],
    is_after: bool
) -> SmallVec<[u8; 32]> {
    let mut key = encode_composite_key(def, cursor_values, "");
    if is_after {
        key.push(0xFF);
    }
    key
}
