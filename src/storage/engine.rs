use std::collections::HashMap;
use std::path::Path;

use crate::error::Result;

use super::compaction::compact_segment;
use super::segment::Segment;
use super::wal::{Wal, WalOp};

#[derive(Debug, Clone)]
pub struct Pointer {
    pub offset: u64,
    pub len: u32,
}

pub struct StorageEngine {
    segment: Segment,
    wal: Wal,
    index: HashMap<String, Pointer>,
}

impl StorageEngine {
    pub fn open(base_dir: impl AsRef<Path>) -> Result<Self> {
        std::fs::create_dir_all(base_dir.as_ref())?;
        let segment = Segment::open(base_dir.as_ref().join("segment-0.dat"))?;
        let wal = Wal::open(base_dir.as_ref().join("wal.log"))?;

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
        let offset = self.segment.append(value)?;
        let pointer = Pointer {
            offset,
            len: value.len() as u32,
        };
        self.wal.append(&WalOp::Put {
            key: key.clone(),
            segment_offset: pointer.offset,
            len: pointer.len,
        })?;
        self.index.insert(key, pointer);
        Ok(())
    }

    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(pointer) = self.index.get(key).cloned() else {
            return Ok(None);
        };
        Ok(Some(self.segment.read_at(pointer.offset, pointer.len)?))
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

        let mut out = Vec::with_capacity(keys.len());
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
            if let Some(value) = self.get(&key)? {
                entries.push((key, value));
            }
        }

        compact_segment(&mut self.segment, &entries, &mut self.index)?;
        self.wal.reset()?;
        for (key, pointer) in self.index.clone() {
            self.wal.append(&WalOp::Put {
                key,
                segment_offset: pointer.offset,
                len: pointer.len,
            })?;
        }
        Ok(())
    }
}
