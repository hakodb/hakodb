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

#[derive(Debug, Clone)]
pub enum StorageMutation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

pub struct StorageEngine {
    segment: Segment,
    wal: Wal,
    index: HashMap<String, Pointer>,
    next_tx_id: u64,
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
            next_tx_id: 1,
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
                WalOp::BeginTx { .. } | WalOp::CommitTx { .. } => {}
            }
        }
        Ok(())
    }

    pub fn apply_batch(&mut self, mutations: &[StorageMutation]) -> Result<()> {
        if mutations.is_empty() {
            return Ok(());
        }

        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;

        let mut wal_ops = Vec::with_capacity(mutations.len() + 2);
        wal_ops.push(WalOp::BeginTx { tx_id });

        let mut index_updates = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            match mutation {
                StorageMutation::Put { key, value } => {
                    let offset = self.segment.append(value)?;
                    let pointer = Pointer {
                        offset,
                        len: value.len() as u32,
                    };
                    wal_ops.push(WalOp::Put {
                        key: key.clone(),
                        segment_offset: pointer.offset,
                        len: pointer.len,
                    });
                    index_updates.push((key.clone(), Some(pointer)));
                }
                StorageMutation::Delete { key } => {
                    wal_ops.push(WalOp::Delete { key: key.clone() });
                    index_updates.push((key.clone(), None));
                }
            }
        }

        wal_ops.push(WalOp::CommitTx { tx_id });
        self.wal.append_batch(&wal_ops)?;

        for (key, pointer) in index_updates {
            match pointer {
                Some(pointer) => {
                    self.index.insert(key, pointer);
                }
                None => {
                    self.index.remove(&key);
                }
            }
        }

        Ok(())
    }

    pub fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        self.apply_batch(&[StorageMutation::Put {
            key,
            value: value.to_vec(),
        }])
    }

    pub fn get(&mut self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(pointer) = self.index.get(key).cloned() else {
            return Ok(None);
        };
        Ok(Some(self.segment.read_at(pointer.offset, pointer.len)?))
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        self.apply_batch(&[StorageMutation::Delete {
            key: key.to_string(),
        }])
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
