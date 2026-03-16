// use std::collections::HashMap;
use hashbrown::HashMap;

use crate::error::Result;

use super::engine::Pointer;
use super::segment::Segment;

pub fn compact_segment(
    segment: &mut Segment,
    entries: &[(String, Vec<u8>)],
    index: &mut HashMap<String, Pointer>,
    segment_id: u64,
) -> Result<()> {
    segment.truncate()?;
    index.clear();
    for (key, value) in entries {
        let (offset, stored_len) = segment.append(value)?;
        index.insert(
            key.clone(),
            Pointer {
                segment_id,
                offset,
                len: stored_len,
            },
        );
    }
    Ok(())
}
