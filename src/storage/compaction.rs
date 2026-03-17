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
    let values: Vec<&[u8]> = entries.iter().map(|(_, v)| v.as_slice()).collect();
    let offsets = segment.append_batch(&values)?;

    for ((key, _), (offset, stored_len)) in entries.iter().zip(offsets) {
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
