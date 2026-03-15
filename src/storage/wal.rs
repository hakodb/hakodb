use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crc32fast::Hasher;

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
        })
    }

    pub fn append(&mut self, op: &WalOp) -> Result<()> {
        self.append_batch(std::slice::from_ref(op))
    }

    pub fn append_batch(&mut self, ops: &[WalOp]) -> Result<()> {
        for op in ops {
            let mut payload = encode(op);
            if let Some(enc) = &self.encryption {
                payload = enc.encrypt(&payload)?;
            }
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            let crc = hasher.finalize();
            self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
            self.file.write_all(&crc.to_le_bytes())?;
            self.file.write_all(&payload)?;
            self.pending_ops_since_sync += 1;
        }
        self.maybe_sync(true)
    }

    pub fn flush(&mut self) -> Result<()> {
        self.file.sync_data()?;
        self.pending_ops_since_sync = 0;
        Ok(())
    }

    fn maybe_sync(&mut self, is_batch_boundary: bool) -> Result<()> {
        match self.mode {
            DurabilityMode::Always => self.flush(),
            DurabilityMode::Interval => {
                if self.pending_ops_since_sync >= self.group_commit_max_ops || is_batch_boundary {
                    self.flush()
                } else {
                    Ok(())
                }
            }
            DurabilityMode::Manual => Ok(()),
        }
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

            let mut hasher = Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != expected {
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

fn encode(op: &WalOp) -> Vec<u8> {
    let mut out = Vec::new();
    match op {
        WalOp::BeginTx { tx_id } => {
            out.push(0);
            out.extend(tx_id.to_le_bytes());
        }
        WalOp::Put {
            key,
            segment_id,
            segment_offset,
            len,
        } => {
            out.push(1);
            out.extend((key.len() as u16).to_le_bytes());
            out.extend(key.as_bytes());
            out.extend(segment_id.to_le_bytes());
            out.extend(segment_offset.to_le_bytes());
            out.extend(len.to_le_bytes());
        }
        WalOp::Delete { key } => {
            out.push(2);
            out.extend((key.len() as u16).to_le_bytes());
            out.extend(key.as_bytes());
        }
        WalOp::CommitTx { tx_id } => {
            out.push(3);
            out.extend(tx_id.to_le_bytes());
        }
    }
    out
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
