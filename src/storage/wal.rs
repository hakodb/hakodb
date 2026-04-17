use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write, BufReader};
use std::path::Path;
use std::time::{Duration, Instant};

use crate::config::DurabilityMode;
use crate::error::{FireLiteError, Result};

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
    mode: DurabilityMode,
    group_commit_max_ops: usize,
    pending_ops_since_sync: usize,
    encryption: Option<EncryptionContext>,
    write_buffer: Vec<u8>,
    last_sync: Instant,
    group_commit_interval: Duration,
}

impl Wal {
    pub fn open(
        path: impl AsRef<Path>,
        mode: DurabilityMode,
        group_commit_max_ops: usize,
        encryption: Option<EncryptionContext>,
    ) -> Result<Self> {
        let mut file = OpenOptions::new()
        .create(true)
        .read(true)
        .write(true)
        .open(path)?;
   
        file.seek(SeekFrom::End(0))?;

        Ok(Self {
            file,
            mode,
            group_commit_max_ops: group_commit_max_ops.max(1),
            pending_ops_since_sync: 0,
            encryption,
            write_buffer: Vec::with_capacity(1024 * 1024), // 1MB WAL buffer
            last_sync: Instant::now(),
            group_commit_interval: Duration::from_millis(5),
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
            self.file.sync_all()?;
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
        
        // Physically flip the bits on the disk
        self.file.sync_all()?;
        // self.file.sync_data()?;

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
        if last_valid_pos < self.file.metadata()?.len() {
            crate::util::log::info(&format!("WAL repair: truncating at {} bytes", last_valid_pos));
            self.file.set_len(last_valid_pos)?;
        }

        // 4. Seek to end so future appends happen correctly
        self.file.seek(SeekFrom::End(0))?;

        // 5. Apply Transaction Logic (Only return ops from committed TXs)
        Ok(filter_committed_ops(raw_ops))
    }

    pub fn replay(&mut self) -> Result<Vec<WalOp>> {
        // 1. Move to start of file
        self.file.seek(SeekFrom::Start(0))?;
        
        // 2. Use BufReader to reduce syscalls during replay (Huge win for small records)
        let mut reader = BufReader::with_capacity(64 * 1024, &self.file);
        let mut raw_ops = Vec::new();
        let mut last_valid_pos = 0;
        
        // Scratchpad to avoid re-allocating memory for every record
        let mut payload_scratch = Vec::with_capacity(8192);

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
        if last_valid_pos < self.file.metadata()?.len() {
            crate::util::log::info(&format!("WAL repair: truncating at {} bytes", last_valid_pos));
            self.file.set_len(last_valid_pos)?;
        }

        // 4. Seek to end so future appends happen correctly
        self.file.seek(SeekFrom::End(0))?;

        // 5. Apply Transaction Logic (Only return ops from committed TXs)
        Ok(filter_committed_ops(raw_ops))
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
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
        let mut file = self.file.try_clone()?; // Clone handle for independent seeking
        let file_len = file.metadata()?.len();
        
        if start_offset >= file_len {
            return Ok((vec![], file_len));
        }

        file.seek(SeekFrom::Start(start_offset))?;
        let mut reader = BufReader::new(file);
        let mut ops = Vec::new();
        let mut current_pos = start_offset;

        loop {
            let mut header = [0u8; 8];
            if reader.read_exact(&mut header).is_err() { break; }

            let len = u32::from_le_bytes(header[0..4].try_into().unwrap()) as usize;
            let expected_crc = u32::from_le_bytes(header[4..8].try_into().unwrap());

            let mut payload = vec![0u8; len];
            if reader.read_exact(&mut payload).is_err() { break; }

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
    pub fn get_key(&self) -> &str {
        match self {
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
        .ok_or_else(|| FireLiteError::Corrupt("empty wal record".into()))?;
    let mut pos = 1;
    match tag {
        0 => {
            let tx_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad begin tx id".into()))?,
            );
            Ok(WalOp::BeginTx { tx_id })
        }
        1 => {
            let key_len = u16::from_le_bytes(
                payload[pos..pos + 2]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad wal key len".into()))?,
            ) as usize;
            pos += 2;
            let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
                .map_err(|_| FireLiteError::Corrupt("bad wal key".into()))?;
            pos += key_len;
            let segment_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad wal segment id".into()))?,
            );
            pos += 8;
            let offset = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad wal offset".into()))?,
            );
            pos += 8;
            let len = u32::from_le_bytes(
                payload[pos..pos + 4]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad wal len".into()))?,
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
                    .map_err(|_| FireLiteError::Corrupt("bad wal key len".into()))?,
            ) as usize;
            pos += 2;
            let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
                .map_err(|_| FireLiteError::Corrupt("bad wal key".into()))?;
            let timestamp = i64::from_le_bytes(payload[pos..pos + 8].try_into().unwrap());
            Ok(WalOp::Delete { key, timestamp })
        }
        3 => {
            let tx_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad commit tx id".into()))?,
            );
            Ok(WalOp::CommitTx { tx_id })
        }
        4 => { // NEW
            let key_len = u16::from_le_bytes(payload[pos..pos+2].try_into().unwrap()) as usize;
            pos += 2;
            let key = String::from_utf8(payload[pos..pos+key_len].to_vec()).map_err(|_| FireLiteError::Corrupt("bad key".into()))?;
            pos += key_len;
            let val_len = u32::from_le_bytes(payload[pos..pos+4].try_into().unwrap()) as usize;
            pos += 4;
            let value = payload[pos..pos+val_len].to_vec();
            Ok(WalOp::PutInlined { key, value })
        }
        5 => { // FIX: Error E0004
            let key_len = u16::from_le_bytes(payload[pos..pos+2].try_into().unwrap()) as usize;
            pos += 2;
            let key = String::from_utf8(payload[pos..pos+key_len].to_vec()).map_err(|_| FireLiteError::Corrupt("bad key".into()))?;
            pos += key_len;
            let offset = u64::from_le_bytes(payload[pos..pos+8].try_into().unwrap());
            pos += 8;
            let len = u32::from_le_bytes(payload[pos..pos+4].try_into().unwrap());
            Ok(WalOp::PutBlob { key, offset, len })
        }
        _ => Err(FireLiteError::Corrupt("unknown wal op".into())),
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
            "firelite-wal-{}.log",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock")
                .as_nanos()
        ));

        let mut wal = Wal::open(&path, DurabilityMode::Always, 2, None).expect("open");
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
}
