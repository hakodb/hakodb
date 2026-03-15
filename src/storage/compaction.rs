use std::collections::HashMap;

use crate::error::Result;

use super::engine::Pointer;
use super::segment::Segment;

pub fn compact_segment(
    segment: &mut Segment,
    entries: &[(String, Vec<u8>)],
    index: &mut HashMap<String, Pointer>,
) -> Result<()> {
    segment.truncate()?;
    index.clear();
    for (key, value) in entries {
        let offset = segment.append(value)?;
        index.insert(
            key.clone(),
            Pointer {
                offset,
                len: value.len() as u32,
            },
        );
    }
    Ok(())
}
