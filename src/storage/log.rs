use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crc32fast::Hasher;

use crate::error::{HakoError, Result};

#[derive(Debug, Clone)]
pub enum WalOp {
    Put {
        key: String,
        segment_offset: u64,
        len: u32,
    },
    Delete {
        key: String,
    },
}

pub struct Wal {
    file: File,
}

impl Wal {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(path)?;
        Ok(Self { file })
    }

    pub fn append(&mut self, op: &WalOp) -> Result<()> {
        let payload = encode_op(op);
        let mut hasher = Hasher::new();
        hasher.update(&payload);
        let crc = hasher.finalize();
        self.file.write_all(&(payload.len() as u32).to_le_bytes())?;
        self.file.write_all(&crc.to_le_bytes())?;
        self.file.write_all(&payload)?;
        self.file.sync_data()?;
        Ok(())
    }

    pub fn replay(&mut self) -> Result<Vec<WalOp>> {
        self.file.seek(SeekFrom::Start(0))?;
        let mut out = Vec::new();
        loop {
            let mut len_buf = [0; 4];
            match self.file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e.into()),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut crc_buf = [0; 4];
            self.file.read_exact(&mut crc_buf)?;
            let expected = u32::from_le_bytes(crc_buf);
            let mut payload = vec![0; len];
            self.file.read_exact(&mut payload)?;
            let mut hasher = Hasher::new();
            hasher.update(&payload);
            if hasher.finalize() != expected {
                return Err(HakoError::Corrupt("wal crc mismatch".into()));
            }
            out.push(decode_op(&payload)?);
        }
        self.file.seek(SeekFrom::End(0))?;
        Ok(out)
    }

    pub fn reset(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}

fn encode_op(op: &WalOp) -> Vec<u8> {
    let mut out = Vec::new();
    match op {
        WalOp::Put {
            key,
            segment_offset,
            len,
        } => {
            out.push(1);
            out.extend((key.len() as u16).to_le_bytes());
            out.extend(key.as_bytes());
            out.extend(segment_offset.to_le_bytes());
            out.extend(len.to_le_bytes());
        }
        WalOp::Delete { key } => {
            out.push(2);
            out.extend((key.len() as u16).to_le_bytes());
            out.extend(key.as_bytes());
        }
    }
    out
}

fn decode_op(payload: &[u8]) -> Result<WalOp> {
    let tag = *payload
        .first()
        .ok_or_else(|| HakoError::Corrupt("empty wal payload".into()))?;
    let mut pos = 1;
    let key_len = u16::from_le_bytes(
        payload[pos..pos + 2]
            .try_into()
            .map_err(|_| HakoError::Corrupt("wal key len".into()))?,
    ) as usize;
    pos += 2;
    let key = String::from_utf8(payload[pos..pos + key_len].to_vec())
        .map_err(|_| HakoError::Corrupt("wal utf8".into()))?;
    pos += key_len;
    Ok(match tag {
        1 => {
            let offset = u64::from_le_bytes(
                payload[pos..pos + 8]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("wal put offset".into()))?,
            );
            pos += 8;
            let len = u32::from_le_bytes(
                payload[pos..pos + 4]
                    .try_into()
                    .map_err(|_| HakoError::Corrupt("wal put len".into()))?,
            );
            WalOp::Put {
                key,
                segment_offset: offset,
                len,
            }
        }
        2 => WalOp::Delete { key },
        _ => return Err(HakoError::Corrupt("wal unknown op".into())),
    })
}
