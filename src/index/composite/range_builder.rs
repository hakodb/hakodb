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
