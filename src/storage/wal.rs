use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;
use std::time::{Duration, Instant};

// use crc32fast::Hasher;

use crate::config::DurabilityMode;
use crate::error::{FireLiteError, Result};

use super::crypto::EncryptionContext;

#[derive(Debug, Clone)]
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
    },
    CommitTx {
        tx_id: u64,
    },
}

pub struct Wal {
    file: File,
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
        let file = OpenOptions::new()
        .create(true)
        .read(true)
        .append(true)
        .open(path)?;
        Ok(Self {
            file,
            mode,
            group_commit_max_ops: group_commit_max_ops.max(1),
            pending_ops_since_sync: 0,
            encryption,
            write_buffer: Vec::with_capacity(512 * 1024), // 512KB WAL buffer
            last_sync: Instant::now(),
            group_commit_interval: Duration::from_millis(2),
        })
    }

    // pub fn append(&mut self, op: &WalOp) -> Result<()> {
    //     // self.append_batch(std::slice::from_ref(op))
    //     let payload = encode(op);

    //     let crc = crc32fast::hash(&payload);

    //     self.write_buffer
    //         .extend_from_slice(&(payload.len() as u32).to_le_bytes());

    //     self.write_buffer
    //         .extend_from_slice(&crc.to_le_bytes());

    //     self.write_buffer.extend_from_slice(&payload);

    //     self.pending_ops_since_sync += 1;

    //     let is_commit = matches!(op, WalOp::CommitTx { .. });
    //     self.maybe_sync(is_commit)
    // }

    pub fn append(&mut self, op: &WalOp) -> Result<()> {

        let start = self.write_buffer.len();

        // reserve header space (len + crc)
        self.write_buffer.extend_from_slice(&[0u8; 8]);

        // encode payload directly
        encode_into(&mut self.write_buffer, op);

        let payload = &self.write_buffer[start + 8..];

        let crc = crc32fast::hash(payload);
        let len = payload.len() as u32;

        // fill header
        self.write_buffer[start..start + 4]
            .copy_from_slice(&len.to_le_bytes());

        self.write_buffer[start + 4..start + 8]
            .copy_from_slice(&crc.to_le_bytes());

        self.pending_ops_since_sync += 1;

        let is_commit = matches!(op, WalOp::CommitTx { .. });

        self.maybe_sync(is_commit)
    }

    // pub fn append_batch(&mut self, ops: &[WalOp]) -> Result<()> {
    //     let mut has_commit = false;

    //     for op in ops {
    //         let payload = encode(op);
    //         let crc = crc32fast::hash(&payload);

    //         self.write_buffer
    //             .extend_from_slice(&(payload.len() as u32).to_le_bytes());

    //         self.write_buffer
    //             .extend_from_slice(&crc.to_le_bytes());

    //         self.write_buffer.extend_from_slice(&payload);

    //         self.pending_ops_since_sync += 1;

    //         if matches!(op, WalOp::CommitTx { .. }) {
    //             has_commit = true;
    //         }
    //     }

    //     self.maybe_sync(has_commit)
    // }

    pub fn append_batch(&mut self, ops: &[WalOp]) -> Result<()> {

        let mut has_commit = false;

        for op in ops {

            let start = self.write_buffer.len();

            // reserve header
            self.write_buffer.extend_from_slice(&[0u8; 8]);

            encode_into(&mut self.write_buffer, op);

            let payload = &self.write_buffer[start + 8..];

            let crc = crc32fast::hash(payload);
            let len = payload.len() as u32;

            self.write_buffer[start..start + 4]
                .copy_from_slice(&len.to_le_bytes());

            self.write_buffer[start + 4..start + 8]
                .copy_from_slice(&crc.to_le_bytes());

            self.pending_ops_since_sync += 1;

            if matches!(op, WalOp::CommitTx { .. }) {
                has_commit = true;
            }
        }

        self.maybe_sync(has_commit)
    }

    pub fn flush(&mut self) -> Result<()> {
        // Write buffered WAL records first
        if !self.write_buffer.is_empty() {
            self.file.write_all(&self.write_buffer)?;
            self.write_buffer.clear();
        }
        
        // 1. Push data from BufWriter to the Operating System
        self.file.flush()?; 

        // 2. Push data from Operating System to physical Disk
        self.file.sync_data()?;
        self.pending_ops_since_sync = 0;
        Ok(())
    }

    fn maybe_sync(&mut self, is_commit: bool) -> Result<()> {
        let now = Instant::now();

        let should_flush = match self.mode {

            DurabilityMode::Always => true,

            DurabilityMode::Interval => {

                is_commit &&
                (
                    self.pending_ops_since_sync >= self.group_commit_max_ops
                    || now.duration_since(self.last_sync) >= self.group_commit_interval
                )

            }

            DurabilityMode::Manual => false,
        };

        if !should_flush {
            return Ok(());
        }

        if !self.write_buffer.is_empty() {
            self.file.write_all(&self.write_buffer)?;
            self.write_buffer.clear();
        }

        self.file.sync_data()?;

        self.pending_ops_since_sync = 0;
        self.last_sync = now;

        Ok(())
    }

    pub fn replay(&mut self) -> Result<Vec<WalOp>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut raw_ops = Vec::new();
        loop {

            let mut len_buf = [0u8; 4];
            match self.file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }

            let len = u32::from_le_bytes(len_buf) as usize;

            let mut crc_buf = [0u8; 4];
            self.file.read_exact(&mut crc_buf)?;
            let expected = u32::from_le_bytes(crc_buf);

            let mut payload = vec![0; len];
            self.file.read_exact(&mut payload)?;

            if crc32fast::hash(&payload) != expected {
                return Err(FireLiteError::Corrupt("wal checksum mismatch".into()));
            }

            if let Some(enc) = &self.encryption {
                payload = enc.decrypt(&payload)?;
            }

            raw_ops.push(decode(&payload)?);
        }
        self.file.seek(SeekFrom::End(0))?;

        Ok(filter_committed_ops(raw_ops))
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        self.pending_ops_since_sync = 0;
        Ok(())
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
            WalOp::Put { .. } | WalOp::Delete { .. } => {
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

// fn encode(op: &WalOp) -> Vec<u8> {
//     // let mut out = Vec::new();
//     let mut out = Vec::with_capacity(64);
//     match op {
//         WalOp::BeginTx { tx_id } => {
//             out.push(0);
//             out.extend(tx_id.to_le_bytes());
//         }
//         WalOp::Put {
//             key,
//             segment_id,
//             segment_offset,
//             len,
//         } => {
//             out.push(1);
//             out.extend((key.len() as u16).to_le_bytes());
//             out.extend(key.as_bytes());
//             out.extend(segment_id.to_le_bytes());
//             out.extend(segment_offset.to_le_bytes());
//             out.extend(len.to_le_bytes());
//         }
//         WalOp::Delete { key } => {
//             out.push(2);
//             out.extend((key.len() as u16).to_le_bytes());
//             out.extend(key.as_bytes());
//         }
//         WalOp::CommitTx { tx_id } => {
//             out.push(3);
//             out.extend(tx_id.to_le_bytes());
//         }
//     }
//     out
// }

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

        WalOp::Delete { key } => {
            buf.push(2);

            buf.extend_from_slice(&(key.len() as u16).to_le_bytes());
            buf.extend_from_slice(key.as_bytes());
        }

        WalOp::CommitTx { tx_id } => {
            buf.push(3);
            buf.extend_from_slice(&tx_id.to_le_bytes());
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
            Ok(WalOp::Delete { key })
        }
        3 => {
            let tx_id = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| FireLiteError::Corrupt("bad commit tx id".into()))?,
            );
            Ok(WalOp::CommitTx { tx_id })
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
        wal.append(&WalOp::BeginTx { tx_id: 1 }).expect("begin");
        wal.append(&WalOp::Put {
            key: "users:1".into(),
            segment_id: 0,
            segment_offset: 10,
            len: 3,
        })
        .expect("put");

        let replayed = wal.replay().expect("replay");
        assert!(replayed.is_empty());

        wal.append(&WalOp::CommitTx { tx_id: 1 }).expect("commit");
        let replayed = wal.replay().expect("replay2");
        assert_eq!(replayed.len(), 1);

        fs::remove_file(path).expect("cleanup");
    }
}
