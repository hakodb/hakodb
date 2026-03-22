use hashbrown::HashMap;
use crate::error::Result;
use super::engine::Pointer;
use super::segment::Segment;

pub fn compact_segment(
    segment: &mut Segment,
    entries: &[(String, Vec<u8>)],
    index: &mut HashMap<String, Pointer>,
    segment_id: u64,
    use_compression: bool, // NEW: Controlled by config
) -> Result<()> {
    segment.truncate()?;
    index.clear();

    for (key, raw_value) in entries {
        let mut final_payload = raw_value.clone();
        let mut compression_bit = 0u32;

        // --- PHASE 1: COMPRESSION ---
        // Only compress if enabled and document is large enough (> 2KB)
        // Compression on tiny documents is a waste of CPU.
        if use_compression && raw_value.len() > 2048 {
            // Level 3 is a good balance between speed and ratio
            if let Ok(compressed) = zstd::encode_all(&raw_value[..], 3) {
                // Only use compressed data if it's actually smaller
                if compressed.len() < raw_value.len() {
                    final_payload = compressed;
                    compression_bit = 1 << 31; // Set the 31st bit to 1
                }
            }
        }

        // --- PHASE 2: ENCRYPTION ---
        // We encrypt the payload (whether it is compressed or raw)
        if let Some(enc) = &segment.encryption {
            final_payload = enc.encrypt(&final_payload)?;
        }

        // --- PHASE 3: PERSISTENCE ---
        // Combine the payload length with the compression flag
        let stored_len = (final_payload.len() as u32) | compression_bit;
        
        let offset = segment.append_raw_with_len(&final_payload, stored_len)?;

        index.insert(
            key.clone(),
            Pointer::Segment {
                segment_id,
                offset,
                len: stored_len,
            },
        );
    }
    
    // Final sync for the new segment
    segment.flush()?;
    Ok(())
}