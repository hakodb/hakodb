use std::collections::HashMap;
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{FireLiteError, Result};

use super::log::{Wal, WalOp};

#[derive(Debug, Clone)]
struct Pointer {
    offset: u64,
    len: u32,
}

pub struct StorageEngine {
    segment: File,
    wal: Wal,
    index: HashMap<String, Pointer>,
}

impl StorageEngine {
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(base_dir.as_ref())?;
        let segment_path = base_dir.as_ref().join("segment-0.dat");
        let wal_path = base_dir.as_ref().join("wal.log");
        let segment = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(segment_path)?;
        let wal = Wal::open(wal_path)?;
        let mut engine = Self {
            segment,
            wal,
            index: HashMap::new(),
        };
        engine.recover()?;
        Ok(engine)
    }

    fn recover(&mut self) -> Result<()> {
        for op in self.wal.replay()? {
            match op {
                WalOp::Put {
                    key,
                    segment_offset,
                    len,
                } => {
                    self.index.insert(
                        key,
                        Pointer {
                            offset: segment_offset,
                            len,
                        },
                    );
                }
                WalOp::Delete { key } => {
                    self.index.remove(&key);
                }
            }
        }
        Ok(())
    }

    pub fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        let offset = self.segment.seek(SeekFrom::End(0))?;
        self.segment
            .write_all(&(value.len() as u32).to_le_bytes())?;
        self.segment.write_all(value)?;
        self.segment.sync_data()?;
        self.wal.append(&WalOp::Put {
            key: key.clone(),
            segment_offset: offset,
            len: value.len() as u32,
        })?;
        self.index.insert(
            key,
            Pointer {
                offset,
                len: value.len() as u32,
            },
        );
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(ptr) = self.index.get(key).cloned() else {
            return Ok(None);
        };
        self.segment.seek(SeekFrom::Start(ptr.offset))?;
        let mut len_buf = [0; 4];
        self.segment.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf);
        if len != ptr.len {
            return Err(FireLiteError::Corrupt("segment length mismatch".into()));
        }
        let mut value = vec![0; len as usize];
        self.segment.read_exact(&mut value)?;
        Ok(Some(value))
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        self.wal.append(&WalOp::Delete {
            key: key.to_string(),
        })?;
        self.index.remove(key);
        Ok(())
    }

    pub fn scan_prefix(&mut self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let keys: Vec<String> = self
            .index
            .keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect();
        let mut out = Vec::new();
        for key in keys {
            if let Some(value) = self.get(&key)? {
                out.push((key, value));
            }
        }
        Ok(out)
    }

    pub fn compact(&mut self) -> Result<()> {
        let mut entries = Vec::new();
        for key in self.index.keys().cloned().collect::<Vec<_>>() {
            if let Some(v) = self.get(&key)? {
                entries.push((key, v));
            }
        }
        self.segment.set_len(0)?;
        self.segment.seek(SeekFrom::Start(0))?;
        self.index.clear();
        self.wal.reset()?;
        for (key, value) in entries {
            self.put(key, &value)?;
        }
        Ok(())
    }
}
