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
    let mut start = encode_composite_key(def, values, "");
    let mut end = start.clone();
    end.push(0xFF);
    start.truncate(start.len().saturating_sub(2));
    ScanRange { start, end }
}

pub fn build_cursor_range(
    def: &CompositeIndexDefinition, 
    cursor_values: &[Value],
    is_after: bool
) -> SmallVec<[u8; 32]> {
    // We encode the cursor values just like a standard index key
    // We leave the doc_id empty for the start bound
    let mut key = encode_composite_key(def, cursor_values, "");
    
    if is_after {
        // To start "after", we append a high-byte to ensure the 
        // B-Tree search lands strictly past the exact match
        key.push(0xFF);
    }
    key
}
