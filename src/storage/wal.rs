use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

// use rayon::string;

use crate::config::DurabilityMode;
use crate::error::{HakoError, Result};

use super::crypto::EncryptionContext;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum WalOp {
    BeginTx {
        tx_id: u64,
    },
    Put {
        key: String,
        segment_id: u64,
        segment_offset: u64,
        len: u32,
    },
    Delete {
        key: String,
        timestamp: i64,
    },
    CommitTx {
        tx_id: u64,
    },
    PutInlined {
        key: String,
        value: Vec<u8>,
    },
    PutBlob {
        key: String,
        offset: u64,
        len: u32,
    },
}

pub struct Wal {
    pub(crate) file: File,
    /// Path, kept so tail() can open an independent read handle (see tail()).
    path: std::path::PathBuf,
    mode: DurabilityMode,
    group_commit_max_ops: usize,
    pending_ops_since_sync: usize,
    encryption: Option<EncryptionContext>,
    write_buffer: Vec<u8>,
    last_sync: Instant,
    group_commit_interval: Duration,
    reserve_bytes: u64,
}

/// ponytail: every encoded op is >= 1 byte, so an all-zero 8-byte header can
/// only be unwritten preallocation padding (see open()). Readers treat it as
/// clean end-of-records: stop WITHOUT truncating (the reservation must
/// survive recovery) and WITHOUT erroring (tail() callers progress normally).
#[inline]
fn is_zero_header(header: &[u8; 8]) -> bool {
    header.iter().all(|&b| b == 0)
}

impl Wal {
    pub fn open(
        path: impl AsRef<Path>,
        mode: DurabilityMode,
        group_commit_max_ops: usize,
        encryption: Option<EncryptionContext>,
        reserve_bytes: u64,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;

        // ponytail: keep `reserve_bytes` of headroom ahead of the write
        // position so steady-state appends never extend the file. Thousands
        // of 1KB extensions fragment the file and inflate every fsync (which
        // must flush file metadata too). One reservation per open amortizes
        // it. 0 disables. The config default also skips Manual (never
        // syncs mid-session — reserving there only inflates apparent size).
        // The zero padding is replay-safe: record headers are never
        // all-zero (every op encodes to >= 1 byte), so readers treat an
        // all-zero header as clean end-of-records and never truncate the
        // reservation (see replay()/tail()).
        // Writer position stays at the end of REAL data, never inside the
        // padding — otherwise replay would stop early and lose records.
        let end = file.seek(SeekFrom::End(0))?;
        // Skipped for Manual regardless of the knob (never fsyncs).
        if mode != DurabilityMode::Manual && reserve_bytes > 0 && file.metadata()?.len() < end.saturating_add(reserve_bytes) {
            let _ = file.set_len(end.saturating_add(reserve_bytes));
        }
        file.seek(SeekFrom::Start(end))?;

        Ok(Self {
            file,
            path: path_buf,
            mode,
            group_commit_max_ops: group_commit_max_ops.max(1),
            pending_ops_since_sync: 0,
            encryption,
            write_buffer: Vec::with_capacity(1024 * 1024), // 1MB WAL buffer
            last_sync: Instant::now(),
            group_commit_interval: Duration::from_millis(5),
            reserve_bytes,
        })
    }

    pub fn append(&mut self, op: &WalOp, is_remote: bool) -> Result<()> {
        // 1. Remember where this specific record starts in the buffer
        let start_pos = self.write_buffer.len();

        // 2. Reserve 8 bytes for the Header (Length + CRC)
        self.write_buffer.extend_from_slice(&[0u8; 8]);

        // 3. ZERO-ALLOCATION ENCODING: Encode directly into the tail of the buffer
        // Note: We need a temporary buffer ONLY if encryption is enabled for the whole record
        if let Some(enc) = &self.encryption {
            let mut temp = Vec::with_capacity(128); // Small scratchpad
            encode_into(&mut temp, op);
            let ciphertext = enc.encrypt(&temp)?;
            self.write_buffer.extend_from_slice(&ciphertext);
        } else {
            encode_into(&mut self.write_buffer, op);
        }

        // 4. Calculate stats for THIS record
        let record_payload_end = self.write_buffer.len();
        let payload_len = (record_payload_end - (start_pos + 8)) as u32;
        let crc = crc32fast::hash(&self.write_buffer[start_pos + 8..record_payload_end]);

        // 5. Patch the header for this specific record in the buffer
        self.write_buffer[start_pos..start_pos + 4].copy_from_slice(&payload_len.to_le_bytes());
        self.write_buffer[start_pos + 4..start_pos + 8].copy_from_slice(&crc.to_le_bytes());

        self.pending_ops_since_sync += 1;

        // 6. DURABILITY CHECK: Decide if we should flush the buffer to disk
        let is_commit = matches!(op, WalOp::CommitTx { .. });
        self.maybe_sync(is_commit, is_remote)
    }

    pub fn append_raw(&mut self, op: &WalOp) -> Result<()> {
        // 1. Clear internal WAL buffer
        self.write_buffer.clear();
        
        // 2. Reserve header
        self.write_buffer.extend_from_slice(&[0u8; 8]);
        
        // 3. Encode Op metadata into buffer
        encode_into(&mut self.write_buffer, op);

        // 4. Calculate payload stats
        let payload_len = (self.write_buffer.len() - 8) as u32;
        let crc = crc32fast::hash(&self.write_buffer[8..]);

        // 5. Fill header (Write directly into the buffer)
        self.write_buffer[0..4].copy_from_slice(&payload_len.to_le_bytes());
        self.write_buffer[4..8].copy_from_slice(&crc.to_le_bytes());

        // 6. IO operation
        self.file.write_all(&self.write_buffer)?;
        
        if self.mode == DurabilityMode::Always {
            // ponytail: fdatasync, not fsync (restores pre-0.7 behavior).
            // POSIX requires fdatasync to persist whatever metadata is
            // needed to access the data (i.e. file size on growth), which is
            // all a WAL reader needs — mtime/atime lag is irrelevant, and
            // our preallocated size barely changes anyway. Same guarantee
            // SQLite/LMDB rely on; ~2-3x cheaper per commit on Linux.
            // (On Windows both map to FlushFileBuffers: no-op difference.)
            self.file.sync_data()?;
        }
        
        Ok(())
    }

    pub fn append_batch(&mut self, ops: &[WalOp], is_remote: bool) -> Result<()> {
        for op in ops {
            let start = self.write_buffer.len();
            self.write_buffer.extend_from_slice(&[0u8; 8]); // Header Space

            let mut temp_payload = Vec::new();
            encode_into(&mut temp_payload, op);

            let final_payload = if let Some(enc) = &self.encryption {
                enc.encrypt(&temp_payload)?
            } else {
                temp_payload
            };

            let crc = crc32fast::hash(&final_payload);
            let len = final_payload.len() as u32;

            self.write_buffer[start..start + 4].copy_from_slice(&len.to_le_bytes());
            self.write_buffer[start + 4..start + 8].copy_from_slice(&crc.to_le_bytes());
            self.write_buffer.extend_from_slice(&final_payload);
            
            self.pending_ops_since_sync += 1;
        }

        // --- NEW PERFORMANCE LOGIC ---
        let has_commit = ops.iter().any(|op| matches!(op, WalOp::CommitTx { .. }));
        
        // If this is remote sync data, we write to OS Page Cache but SKIP fsync.
        // This prevents the Manager's sync from slowing down the Cashier's disk.
        self.maybe_sync(has_commit, is_remote)
    }

    pub fn append_batch_fast(&mut self, tx_id: u64, ops: &[WalOp], is_remote: bool) -> Result<()> {
        // 1. Encode BeginTx
        self.append_to_buffer(&WalOp::BeginTx { tx_id });

        // 2. Encode all ops
        for op in ops {
            self.append_to_buffer(op);
        }

        // 3. Encode CommitTx
        self.append_to_buffer(&WalOp::CommitTx { tx_id });

        self.maybe_sync(true, is_remote)
    }

    fn append_to_buffer(&mut self, op: &WalOp) {
        let start = self.write_buffer.len();
        self.write_buffer.extend_from_slice(&[0u8; 8]); // Header

        // Encode directly into the buffer if not encrypted
        if let Some(enc) = &self.encryption {
            let mut temp = Vec::new(); // Fallback for encryption
            encode_into(&mut temp, op);
            let ciphertext = enc.encrypt(&temp).unwrap();
            self.write_buffer.extend_from_slice(&ciphertext);
        } else {
            encode_into(&mut self.write_buffer, op);
        }

        let payload_len = (self.write_buffer.len() - start - 8) as u32;
        let crc = crc32fast::hash(&self.write_buffer[start + 8..]);
        
        self.write_buffer[start..start+4].copy_from_slice(&payload_len.to_le_bytes());
        self.write_buffer[start+4..start+8].copy_from_slice(&crc.to_le_bytes());
        self.pending_ops_since_sync += 1;
    }

    pub fn flush(&mut self) -> Result<()> {
        if self.write_buffer.is_empty() {
            return Ok(());
        }

        // ONE Syscall to write multiple operations
        self.file.write_all(&self.write_buffer)?;

        // ponytail: fdatasync (see append() above for why). The torn-tail
        // crash window this theoretically widens is exactly what replay's
        // partial-record repair already handles.
        self.file.sync_data()?;

        // NOW we clear, after the data is safe on the platter
        self.write_buffer.clear();
        self.pending_ops_since_sync = 0;
        self.last_sync = Instant::now();
        Ok(())
    }

    fn maybe_sync(&mut self, is_commit: bool, is_remote: bool) -> Result<()> {
        if is_remote && self.mode != DurabilityMode::Always {
            // Write to OS buffer but skip the expensive physical disk flip (fsync)
            self.file.write_all(&self.write_buffer)?;
            self.write_buffer.clear();
            return Ok(());
        }

        let now = Instant::now();

        let should_flush = match self.mode {
            // Write and Sync every single time
            DurabilityMode::Always => true,

            // Pool operations, only write to disk when a transaction finishes
            DurabilityMode::OnCommit => is_commit,

            // The most performant mode: pool until time or volume threshold is hit
            DurabilityMode::Interval => {
                is_commit && (
                    self.pending_ops_since_sync >= self.group_commit_max_ops
                    || now.duration_since(self.last_sync) >= self.group_commit_interval
                    || self.write_buffer.len() > 124 * 1024 // Flush if buffer > 64KB
                )
            }

            DurabilityMode::Manual => false,
        };

        if should_flush {
            self.flush()?; // This writes the WHOLE buffer in one syscall and calls sync_all
        }
        Ok(())
    }

    pub fn replay_from_offset(&mut self, offset: u64) -> Result<Vec<WalOp>> {
        self.file.seek(SeekFrom::Start(offset))?;
        // ... (reuse existing replay logic)

        // 1. Move to start of file
        self.file.seek(SeekFrom::Start(0))?;
        
        // 2. Use BufReader to reduce syscalls during replay (Huge win for small records)
        let mut reader = BufReader::with_capacity(64 * 1024, &self.file);
        let mut raw_ops = Vec::new();
        let mut last_valid_pos = 0;
        
        // Scratchpad to avoid re-allocating memory for every record
        let mut payload_scratch = Vec::with_capacity(8192);
        let mut stopped_on_padding = false;

        loop {
            // A. Read Header (8 bytes: 4 for Len, 4 for CRC)
            let mut header = [0u8; 8];
            match reader.read_exact(&mut header) {
                Ok(_) => {}, // Successfully read exactly 8 bytes
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

            // Preallocation padding (see open()): clean stop, keep reservation.
            if is_zero_header(&header) {
                stopped_on_padding = true;
                break;
            }

            // B. Read Payload into scratchpad
            payload_scratch.resize(len, 0);
            if let Err(e) = reader.read_exact(&mut payload_scratch) {
                // If we reach EOF here, it means the record was partially written during a crash
                if e.kind() == std::io::ErrorKind::UnexpectedEof { break; }
                return Err(e.into());
            }

            // C. Validate Integrity
            if crc32fast::hash(&payload_scratch) != expected_crc {
                // CRC Mismatch: Stop here. Data following this point is likely corrupt.
                // We don't return Err because we want to recover as much as possible.
                break;
            }

            // D. Handle Decryption
            let decoded_payload = if let Some(enc) = &self.encryption {
                enc.decrypt(&payload_scratch)?
            } else {
                // If no encryption, we borrow the scratchpad data
                payload_scratch.clone()
            };

            // E. Deserialize
            raw_ops.push(decode(&decoded_payload)?);
            
            // Increment the "Safe" position in the file
            last_valid_pos += (8 + len) as u64;
        }

        // 3. AUTO-REPAIR: If we stopped early due to corruption or partial write, 
        // truncate the file so future runs don't get stuck on the same bad data.
        // ponytail: never truncate preallocation padding — file length beyond
        // the last record is reservation, not junk (replay() below shares it).
        if last_valid_pos < self.file.metadata()?.len() && !stopped_on_padding {
            crate::util::log::info(&format!("WAL repair: truncating at {} bytes", last_valid_pos));
            self.file.set_len(last_valid_pos)?;
        }

        // 4. Seek so future appends continue exactly at end-of-records.
        // (NOT End(0): with preallocation those differ — see replay().)
        self.file.seek(SeekFrom::Start(last_valid_pos))?;

        // 5. Apply Transaction Logic (Only return ops from committed TXs)
        Ok(filter_committed_ops(raw_ops))
    }

    pub fn replay(&mut self) -> Result<Vec<WalOp>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut reader = BufReader::with_capacity(64 * 1024, &self.file);
        let mut raw_ops = Vec::new();
        let mut last_valid_pos = 0;
        let mut payload_scratch = Vec::with_capacity(8192);
        let mut stopped_on_padding = false;

        loop {
            let mut header = [0u8; 8];
            if reader.read_exact(&mut header).is_err() { break; }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

            // Preallocation padding (see open()): clean stop, keep reservation.
            if is_zero_header(&header) {
                stopped_on_padding = true;
                break;
            }

            if len == 0 { break; } 

            payload_scratch.resize(len, 0);
            if reader.read_exact(&mut payload_scratch).is_err() { 
                // Partial record at end of file - this is a candidate for truncation
                break; 
            }

            // PHYSICAL INTEGRITY CHECK
            if crc32fast::hash(&payload_scratch) != expected_crc {
                // Physical corruption detected. Stop and allow truncation.
                break; 
            }

            // DECRYPTION
            let decoded_payload = if let Some(enc) = &self.encryption {
                match enc.decrypt(&payload_scratch) {
                    Ok(p) => p,
                    Err(_) => {
                        // Decryption failed but CRC was correct! 
                        // This means the KEY is wrong. Do NOT truncate.
                        return Err(HakoError::Corrupt("Decryption failed. Wrong encryption key?".into()));
                    }
                }
            } else {
                payload_scratch.clone()
            };

            // LOGICAL DECODE
            match decode(&decoded_payload) {
                Ok(op) => {
                    raw_ops.push(op);
                    last_valid_pos += (8 + len) as u64;
                }
                Err(_) => {
                    // CRC was valid, but we can't read the data.
                    // If we are NOT in encryption mode, this might be encrypted data we're trying to read as plain.
                    if self.encryption.is_none() {
                        return Err(HakoError::Corrupt("Recognized valid data but failed to decode. Is this collection encrypted?".into()));
                    }
                    // Otherwise, this is a logical corruption, stop but don't truncate.
                    break;
                }
            }
        }

        // ONLY TRUNCATE if we actually hit a CRC failure or partial write.
        // If we stopped because of a logical Err above, we return before this line.
        // ponytail: never truncate preallocation padding (see open()).
        let current_file_len = self.file.metadata()?.len();
        if last_valid_pos < current_file_len && !stopped_on_padding {
            // Double check: if the next byte is valid, don't truncate, just error.
            // For now, let's just log it.
            crate::util::log::info(&format!("WAL repair: truncating {} bytes of tail junk", current_file_len - last_valid_pos));
            let _ = self.file.set_len(last_valid_pos);
        }

        // ponytail: resume exactly at end-of-records, NOT End(0) — with a
        // preallocated file those differ, and appending past the padding
        // would strand records replay can no longer reach.
        self.file.seek(SeekFrom::Start(last_valid_pos))?;
        Ok(filter_committed_ops(raw_ops))
    }

    pub fn reset(&mut self) -> Result<()> {
        // ponytail: clear the in-memory buffer too — otherwise the next
        // append_batch keeps the old WAL data in self.write_buffer and
        // flush() writes BOTH the historical ops and whatever was appended
        // after reset. Manual mode never flushes during a session, so the
        // buffer grows unbounded; on shutdown the rewrite_wal_snapshot
        // snapshot ends up double-written to disk.
        self.write_buffer.clear();
        self.file.set_len(0)?;
        // ponytail: re-reserve headroom after the wipe (see open()) —
        // metadata-only, so post-checkpoint appends don't regrow 1KB at a
        // time. Skipped for Manual (never fsyncs; reservation would only
        // inflate apparent DB size). Position stays at 0 where records begin.
        if self.mode != DurabilityMode::Manual && self.reserve_bytes > 0 {
            let _ = self.file.set_len(self.reserve_bytes);
        }
        self.file.seek(SeekFrom::Start(0))?;
        self.pending_ops_since_sync = 0;
        Ok(())
    }

    pub fn durability_mode(&self) -> DurabilityMode {
        self.mode
    }

    pub fn set_durability_mode(&mut self, mode: DurabilityMode) {
        self.mode = mode;
    }

    pub fn tail(&self, start_offset: u64) -> Result<(Vec<WalOp>, u64)> {
        // ponytail: fresh read handle per tail, never touch self.file's
        // cursor. try_clone shares the file position (DuplicateHandle/dup),
        // and even positional reads (pread/seek_read) advance the cursor on
        // some platforms — either way the next append would strand inside
        // preallocation padding where readers stop at the first zero header.
        // A separate open() is an independent description with its own
        // cursor, so tailing can never disturb concurrent appends.
        let file = File::open(&self.path)?;
        let file_len = file.metadata()?.len();

        if start_offset >= file_len {
            return Ok((vec![], file_len));
        }

        let mut reader = BufReader::new(file);
        let mut ops = Vec::new();
        let mut current_pos = start_offset;
        // Skip to start_offset on the fresh handle (its cursor is ours alone).
        reader.seek(SeekFrom::Start(start_offset))?;

        loop {
            let mut header = [0u8; 8];
            if reader.read_exact(&mut header).is_err() {
                break;
            }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

            // Preallocation padding (see open()): clean end, not an error —
            // callers use the returned position to keep tailing.
            if is_zero_header(&header) {
                break;
            }

            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() {
                break;
            }

            if crc32fast::hash(&payload) == expected_crc {
                let decoded_payload = if let Some(enc) = &self.encryption {
                    enc.decrypt(&payload)?
                } else {
                    payload
                };
                ops.push(decode(&decoded_payload)?);
                current_pos += 8 + len as u64;
            } else {
                break; // Stop at corruption
            }
        }

        Ok((ops, current_pos))
    }
    
}

impl WalOp {
    /// Returns the document key associated with this operation.
    /// Returns an empty string for transaction markers (Begin/Commit).
    pub fn get_key(&self) -> &str {        match self {
            WalOp::Put { key, .. } => key,
            WalOp::Delete { key, .. } => key,
            WalOp::PutInlined { key, .. } => key,
            WalOp::PutBlob { key, .. } => key,
            WalOp::BeginTx { .. } | WalOp::CommitTx { .. } => "",
        }
    }
}

impl Drop for Wal {
    fn drop(&mut self) {
        // Final attempt to save data when the database handle is closed
        let _ = self.flush();
    }
}

fn filter_committed_ops(raw_ops: Vec<WalOp>) -> Vec<WalOp> {
    let mut output = Vec::new();
    let mut current_tx: Option<u64> = None;
    let mut tx_ops = Vec::new();

    for op in raw_ops {
        match op {
            WalOp::BeginTx { tx_id } => {
                current_tx = Some(tx_id);
                tx_ops.clear();
            }
            WalOp::CommitTx { tx_id } => {
                if current_tx == Some(tx_id) {
                    output.append(&mut tx_ops);
                }
                current_tx = None;
                tx_ops.clear();
            }
            WalOp::Put { .. } | WalOp::Delete { .. } | WalOp::PutInlined { .. } | WalOp::PutBlob { .. }  => {
                if current_tx.is_some() {
                    tx_ops.push(op);
                } else {
                    output.push(op);
                }
            }
        }
    }

    output
}

fn encode_into(buf: &mut Vec<u8>, op: &WalOp) {
    match op {
        WalOp::BeginTx { tx_id } => {
            buf.push(0);
            buf.extend_from_slice(&tx_id.to_le_bytes());
        }

        WalOp::Put {
            key,
            segment_id,
            segment_offset,
            len,
        } => {
            buf.push(1);

            buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());

            buf.extend_from_slice(&segment_id.to_le_bytes());
            buf.extend_from_slice(&segment_offset.to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
        }

        WalOp::Delete { key, timestamp } => {
            buf.push(2);
            buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&timestamp.to_le_bytes());
        }

        WalOp::CommitTx { tx_id } => {
            buf.push(3);
            buf.extend_from_slice(&tx_id.to_le_bytes());
        }

        WalOp::PutInlined { key, value } => {
            buf.push(4); // New Tag
            buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
            buf.extend_from_slice(value);
        }

        WalOp::PutBlob { key, offset, len } => {
            buf.push(5);
            buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
            buf.extend_from_slice(&offset.to_le_bytes());
            buf.extend_from_slice(&len.to_le_bytes());
        }
    }
}

fn decode(payload: &[u8]) -> Result<WalOp> {
    let tag = *payload
        .first()
        .ok_or_else(|| HakoError::Corrupt("empty wal record".into()))?;
    let mut pos = 1;
    match tag {
        0 => {
            let tx_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad begin tx id".into()))?,
            );
            Ok(WalOp::BeginTx { tx_id })
        }
        1 => {
            let key_len = u16::from_le_bytes(
                payload[pos..pos + 2]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal key len".into()))?,
            ) as usize;
            pos += 2;
            if pos + key_len > payload.len() {
                return Err(HakoError::Corrupt("wal put key overruns record".into()));
            }
            let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
                .map_err(|_| HakoError::Corrupt("bad wal key".into()))?;
            pos += key_len;
            let segment_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal segment id".into()))?,
            );
            pos += 8;
            let offset = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal offset".into()))?,
            );
            pos += 8;
            let len = u32::from_le_bytes(
                payload[pos..pos + 4]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal len".into()))?,
            );
            Ok(WalOp::Put {
                key,
                segment_id,
                segment_offset: offset,
                len,
            })
        }
        2 => {
            let key_len = u16::from_le_bytes(
                payload[pos..pos + 2]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal key len".into()))?,
            ) as usize;
            pos += 2;
            if pos + key_len > payload.len() {
                return Err(HakoError::Corrupt("wal delete key overruns record".into()));
            }
            let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
                .map_err(|_| HakoError::Corrupt("bad wal key".into()))?;
            pos += key_len;
            // FIX: was reading timestamp from key bytes (missing pos += key_len);
            // would silently misread timestamps for any key >= 8 bytes.
            let timestamp = i64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal delete timestamp".into()))?,
            );
            Ok(WalOp::Delete { key, timestamp })
        }
        3 => {
            let tx_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad commit tx id".into()))?,
            );
            Ok(WalOp::CommitTx { tx_id })
        }
        4 => {
            let key_len = u16::from_le_bytes(
                payload[pos..pos + 2]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal key len".into()))?,
            ) as usize;
            pos += 2;
            if pos + key_len > payload.len() {
                return Err(HakoError::Corrupt("wal putinlined key overruns record".into()));
            }
            let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
                .map_err(|_| HakoError::Corrupt("bad key".into()))?;
            pos += key_len;
            let val_len = u32::from_le_bytes(
                payload[pos..pos + 4]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal putinlined val len".into()))?,
            ) as usize;
            pos += 4;
            if pos + val_len > payload.len() {
                return Err(HakoError::Corrupt("wal putinlined val overruns record".into()));
            }
            let value = payload[pos..pos + val_len].to_vec();
            Ok(WalOp::PutInlined { key, value })
        }
        5 => {
            let key_len = u16::from_le_bytes(
                payload[pos..pos + 2]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal key len".into()))?,
            ) as usize;
            pos += 2;
            if pos + key_len > payload.len() {
                return Err(HakoError::Corrupt("wal putblob key overruns record".into()));
            }
            let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
                .map_err(|_| HakoError::Corrupt("bad key".into()))?;
            pos += key_len;
            let offset = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal putblob offset".into()))?,
            );
            pos += 8;
            let len = u32::from_le_bytes(
                payload[pos..pos + 4]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("bad wal putblob len".into()))?,
            );
            Ok(WalOp::PutBlob { key, offset, len })
        }
        _ => Err(HakoError::Corrupt("unknown wal op".into())),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::config::DurabilityMode;

    use super::{Wal, WalOp};

    #[test]
    fn replay_ignores_uncommitted_transaction() {
        let path = std::env::temp_dir().join(format!(
            "hako-wal-{}.log",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));

        let mut wal = Wal::open(&path, DurabilityMode::Always, 2, None, 0).expect("open");
        wal.append(&WalOp::BeginTx { tx_id: 1 }, false).expect("begin");
        wal.append(&WalOp::Put {
            key: "users:1".into(),
            segment_id: 0,
            segment_offset: 10,
            len: 3,
        }, false)
        .expect("put");

        let replayed = wal.replay().expect("replay");
        assert!(replayed.is_empty());

        wal.append(&WalOp::CommitTx { tx_id: 1 }, false).expect("commit");
        let replayed = wal.replay().expect("replay2");
        assert_eq!(replayed.len(), 1);

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn roundtrip_delete_via_replay() {
        // Regression test for the wal decode() bug: tag 2 (Delete) was missing
        // `pos += key_len` and silently misread the timestamp. We can't poke
        // private decode() directly, but we can replay a Delete through the
        // public Wal API and check the recovered timestamp matches what we
        // wrote.
        let path = std::env::temp_dir().join(format!(
            "hako-wal-rt-{}.log",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));

        let ts: i64 = 1_700_000_123;

        let mut wal = Wal::open(&path, DurabilityMode::Always, 2, None, 0).expect("open");
        wal.append(&WalOp::BeginTx { tx_id: 9 }, false)
            .expect("begin");
        wal.append(
            &WalOp::Delete {
                key: "doc-with-long-key".into(),
                timestamp: ts,
            },
            false,
        )
        .expect("delete");
        wal.append(&WalOp::CommitTx { tx_id: 9 }, false)
            .expect("commit");

        let replayed = wal.replay().expect("replay");
        assert_eq!(replayed.len(), 1, "expected the Delete op back");
        match &replayed[0] {
            WalOp::Delete { key, timestamp } => {
                assert_eq!(key, "doc-with-long-key");
                assert_eq!(
                    *timestamp, ts,
                    "timestamp round-trip (regression test for pos += key_len bug)"
                );
            }
            other => panic!("expected Delete, got {:?}", other),
        }

        fs::remove_file(path).expect("cleanup");
    }

    #[test]
    fn preallocation_survives_replay_and_appends() {
        // The 4MB reservation must be invisible to recovery: records written
        // into it replay fully, the padding is never truncated, and appends
        // after a reopen continue exactly at end-of-records (not EOF).
        let path = std::env::temp_dir().join(format!(
            "hako-wal-prealloc-{}.log",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));

        let mut wal = Wal::open(&path, DurabilityMode::OnCommit, 100, None, 4 * 1024 * 1024).expect("open");
        assert!(fs::metadata(&path).expect("meta").len() >= 4 * 1024 * 1024 - 1);

        for tx in 1..=3u64 {
            wal.append(&WalOp::BeginTx { tx_id: tx }, false).expect("begin");
            wal.append(&WalOp::PutInlined {
                key: format!("k{tx}"),
                value: vec![tx as u8; 64],
            }, false).expect("put");
            wal.append(&WalOp::CommitTx { tx_id: tx }, false).expect("commit");
        }
        drop(wal);

        // Reopen: reservation intact, all 3 puts recover, no truncation.
        let mut wal2 = Wal::open(&path, DurabilityMode::OnCommit, 100, None, 4 * 1024 * 1024).expect("reopen");
        let replayed = wal2.replay().expect("replay");
        assert_eq!(replayed.len(), 3, "all puts must survive, got {replayed:?}");
        assert!(fs::metadata(&path).expect("meta2").len() >= 4 * 1024 * 1024 - 1);

        // Append after reopen lands right after the records (position check:
        // file length must NOT jump — the reservation absorbs it).
        let len_before = fs::metadata(&path).expect("meta3").len();
        wal2.append(&WalOp::BeginTx { tx_id: 4 }, false).expect("begin2");
        wal2.append(&WalOp::PutInlined { key: "k4".into(), value: vec![4u8; 64] }, false).expect("put2");
        wal2.append(&WalOp::CommitTx { tx_id: 4 }, false).expect("commit2");
        assert_eq!(fs::metadata(&path).expect("meta4").len(), len_before);
        drop(wal2);

        let mut wal3 = Wal::open(&path, DurabilityMode::OnCommit, 100, None, 4 * 1024 * 1024).expect("reopen2");
        let replayed = wal3.replay().expect("replay2");
        assert_eq!(replayed.len(), 4, "post-reopen append must be reachable, got {replayed:?}");

        fs::remove_file(path).expect("cleanup");
    }
}
