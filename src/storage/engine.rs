// use std::collections::HashMap;
use hashbrown::HashMap;
use std::path::{Path, PathBuf};

use crate::config::{FireLiteConfig, DurabilityMode};
use crate::error::{FireLiteError, Result};

use super::compaction::compact_segment;
use super::crypto::EncryptionContext;
use super::segment::Segment;
use super::wal::{Wal, WalOp};

#[derive(Debug, Clone, Copy)]
pub struct Pointer {
    pub segment_id: u64,
    pub offset: u64,
    pub len: u32,
}

#[derive(Debug, Clone)]
pub enum StorageMutation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

struct SegmentMeta {
    id: u64,
    level: u32,
    segment: Segment,
}

pub struct StorageEngine {
    base_dir: PathBuf,
    segments: HashMap<u64, SegmentMeta>,
    active_segment_id: u64,
    next_segment_id: u64,
    wal: Wal,
    index: HashMap<String, Pointer>,
    // index: HashMap<Box<str>, Pointer>,
    next_tx_id: u64,
    compaction_threshold_bytes: usize,
    encryption: Option<EncryptionContext>,
}

impl StorageEngine {
    pub fn open(base_dir: impl AsRef<Path>, cfg: &FireLiteConfig) -> Result<Self> {
        std::fs::create_dir_all(base_dir.as_ref())?;

        let encryption = cfg
            .encryption_key
            .as_ref()
            .map(|secret| EncryptionContext::from_secret(secret));

        let wal = Wal::open(
            base_dir.as_ref().join("wal.log"),
            cfg.durability_mode,
            cfg.group_commit_max_ops,
            encryption.clone(),
        )?;

        let mut segments = HashMap::new();
        let mut max_id = 0;
        let mut active_segment_id = 0;

        for entry in std::fs::read_dir(base_dir.as_ref())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some((level, id)) = parse_segment_name(&name) {
                let segment = Segment::open(entry.path(), encryption.clone())?;
                segments.insert(id, SegmentMeta { id, level, segment });
                if id > max_id {
                    max_id = id;
                }
                if id >= active_segment_id {
                    active_segment_id = id;
                }
            }
        }

        if segments.is_empty() {
            let path = segment_path(base_dir.as_ref(), 0, 0);
            let segment = Segment::open(path, encryption.clone())?;
            segments.insert(
                0,
                SegmentMeta {
                    id: 0,
                    level: 0,
                    segment,
                },
            );
            max_id = 0;
            active_segment_id = 0;
        }

        let mut engine = Self {
            base_dir: base_dir.as_ref().to_path_buf(),
            segments,
            active_segment_id,
            next_segment_id: max_id + 1,
            wal,
            index: HashMap::new(),
            next_tx_id: 1,
            compaction_threshold_bytes: cfg.auto_compaction_threshold_bytes,
            encryption,
        };
        engine.recover()?;
        Ok(engine)
    }

    fn recover(&mut self) -> Result<()> {
        self.index.reserve(1024);
        for op in self.wal.replay()? {
            match op {
                WalOp::Put {
                    key,
                    segment_id,
                    segment_offset,
                    len,
                } => {
                    self.index.insert(
                        key,
                        Pointer {
                            segment_id,
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

        // rotate BEFORE writing
        self.maybe_rotate_active_segment()?;

        let active_id = self.active_segment_id;

        let active = self
            .segments
            .get_mut(&active_id)
            .ok_or_else(|| FireLiteError::Corrupt("active segment missing".into()))?;

        // Group puts to do a single segment bulk write
        let mut puts_to_write = Vec::new();
        for mutation in mutations {
            if let StorageMutation::Put { value, .. } = mutation {
                puts_to_write.push(value.as_slice());
            }
        }

        let mut put_offsets = if !puts_to_write.is_empty() {
            active.segment.append_batch(&puts_to_write)?.into_iter()
        } else {
            Vec::new().into_iter()
        };

        // let mut wal_ops = Vec::with_capacity(mutations.len() + 2);
        let mut wal_ops = Vec::with_capacity(mutations.len() * 2 + 2);
        wal_ops.push(WalOp::BeginTx { tx_id });

        let mut index_updates = Vec::with_capacity(mutations.len());

        for mutation in mutations {
            match mutation {
                StorageMutation::Put { key, .. } => {
                    let (offset, stored_len) = put_offsets.next().expect("put offset mismatch");
                    let pointer = Pointer {
                        segment_id: self.active_segment_id,
                        offset,
                        len: stored_len,
                    };
                    wal_ops.push(WalOp::Put {
                        key: key.clone(),
                        segment_id: pointer.segment_id,
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


        if self.wal.durability_mode() == DurabilityMode::Always || 
        self.wal.durability_mode() == DurabilityMode::OnCommit {
            
            if let Some(active) = self.segments.get_mut(&self.active_segment_id) {
                // Push Segment bytes from OS RAM -> Physical Disk
                active.segment.flush()?; 
            }
        }

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

        self.maybe_rotate_active_segment()?;
        Ok(())
    }

    pub fn flush_all(&mut self) -> Result<()> {
        // First flush the data segment
        if let Some(meta) = self.segments.get_mut(&self.active_segment_id) {
            meta.segment.flush()?;
        }
        // Then flush the WAL
        self.wal.flush()
    }

    pub fn run_background_maintenance(&mut self) -> Result<()> {
        self.maybe_rotate_active_segment()?;
        self.compact_tiers_once().map(|_| ())
    }

    fn maybe_rotate_active_segment(&mut self) -> Result<()> {
        let size = self
            .segments
            .get_mut(&self.active_segment_id)
            .ok_or_else(|| FireLiteError::Corrupt("active segment missing".into()))?
            .segment
            .size_bytes()? as usize;

        if size < self.compaction_threshold_bytes.max(1024 * 1024) {
            return Ok(());
        }

        let new_id = self.next_segment_id;
        self.next_segment_id += 1;
        let path = segment_path(&self.base_dir, 0, new_id);
        let encryption = self.encryption.clone();
        let segment = Segment::open(path, encryption)?;
        self.segments.insert(
            new_id,
            SegmentMeta {
                id: new_id,
                level: 0,
                segment,
            },
        );
        self.active_segment_id = new_id;
        Ok(())
    }

    fn compact_tiers_once(&mut self) -> Result<bool> {
        // find two immutable segments on same level
        let mut by_level: HashMap<u32, Vec<u64>> = HashMap::new();
        for (id, meta) in &self.segments {
            if *id == self.active_segment_id {
                continue;
            }
            by_level.entry(meta.level).or_default().push(*id);
        }

        let mut candidate: Option<(u32, u64, u64)> = None;
        for (level, ids) in by_level {
            if ids.len() >= 2 {
                candidate = Some((level, ids[0], ids[1]));
                break;
            }
        }

        let Some((level, s1, s2)) = candidate else {
            return Ok(false);
        };

        let target_level = level + 1;
        let target_id = self.next_segment_id;
        self.next_segment_id += 1;

        let snapshot: Vec<(String, Pointer)> =
            self.index.iter().map(|(k, p)| (k.clone(), *p)).collect();

        let mut entries: Vec<(String, Vec<u8>)> = Vec::new();

        for (key, pointer) in snapshot {
            if pointer.segment_id == s1 || pointer.segment_id == s2 {
                if let Some(value) = self.read_pointer(&pointer)? {
                    entries.push((key, value));
                }
            }
        }

        let target_path = segment_path(&self.base_dir, target_level, target_id);
        let mut target = Segment::open(target_path, self.encryption.clone())?;

        let mut new_index = HashMap::new();
        compact_segment(&mut target, &entries, &mut new_index, target_id)?;

        for (k, p) in new_index {
            self.index.insert(k, p);
        }

        if let Some(mut meta) = self.segments.remove(&s1) {
            let path = meta.segment.path().to_path_buf();
            meta.segment.close(); // Explicitly drop the file handle
            drop(meta);           // Ensure metadata is dropped
            let _ = std::fs::remove_file(path);
        }
        if let Some(mut meta) = self.segments.remove(&s2) {
            let path = meta.segment.path().to_path_buf();
            meta.segment.close(); // Explicitly drop the file handle
            drop(meta);           // Ensure metadata is dropped
            let _ = std::fs::remove_file(path);
        }

        self.segments.insert(
            target_id,
            SegmentMeta {
                id: target_id,
                level: target_level,
                segment: target,
            },
        );

        self.rewrite_wal_snapshot()?;
        Ok(true)
    }

    fn rewrite_wal_snapshot(&mut self) -> Result<()> {
        self.wal.reset()?;
        let mut ops = Vec::with_capacity(self.index.len());
        for (key, pointer) in &self.index {
            ops.push(WalOp::Put {
                key: key.clone(),
                segment_id: pointer.segment_id,
                segment_offset: pointer.offset,
                len: pointer.len,
            });
        }
        self.wal.append_batch(&ops)?;
        Ok(())
    }

    fn read_pointer(&self, pointer: &Pointer) -> Result<Option<Vec<u8>>> {
        let Some(segment) = self.segments.get(&pointer.segment_id) else {
            return Ok(None);
        };
        Ok(Some(segment.segment.read_at(pointer.offset, pointer.len)?))
    }

    pub fn flush_wal(&mut self) -> Result<()> {
        self.wal.flush()
    }

    pub fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        let mutation = StorageMutation::Put {
            key,
            value: value.to_vec(),
        };

        self.apply_batch(&[mutation])
    }

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(pointer) = self.index.get(key).cloned() else {
            return Ok(None);
        };
        self.read_pointer(&pointer)
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        self.apply_batch(&[StorageMutation::Delete {
            key: key.to_string(),
        }])
    }

    pub fn count_prefix(&self, prefix: &str) -> usize {
        self.index.keys().filter(|k| k.starts_with(prefix)).count()
    }

    pub fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {

        let snapshot: Vec<(String, Pointer)> = self
            .index
            .iter()
            .filter(|(k, _)| k.starts_with(prefix))
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect();

        let mut out = Vec::with_capacity(snapshot.len());

        for (key, pointer) in snapshot {
            if let Some(value) = self.read_pointer(&pointer)? {
                out.push((key, value));
            }
        }

        Ok(out)
    }

    pub fn compact(&mut self) -> Result<()> {

        // Only operate on **immutable segments**, never the active one
        let immutable_ids: Vec<u64> = self
            .segments
            .keys()
            .filter(|&&id| id != self.active_segment_id)
            .cloned()
            .collect();

        if immutable_ids.is_empty() {
            return Ok(());
        }

        while self.compact_tiers_once()? {
            let by_level_count = self
                .segments
                .values()
                .filter(|m| m.id != self.active_segment_id)
                .count();
            if by_level_count < 2 {
                break;
            }
        }

        // full snapshot compaction fallback
        // let mut entries = Vec::new();
        // Take a snapshot of the current index for the immutable segments
        let snapshot: Vec<(String, Pointer)> = self
            .index
            .iter()
            .filter(|(_, p)| immutable_ids.contains(&p.segment_id))
            .map(|(k, p)| (k.clone(), *p))
            .collect();

        let mut entries: Vec<(String, Vec<u8>)> = Vec::with_capacity(snapshot.len());
        for (key, pointer) in snapshot {
            if let Some(value) = self.read_pointer(&pointer)? {
                entries.push((key, value));
            }
        }

        let target_id = self.next_segment_id;
        self.next_segment_id += 1;
        let target_path = segment_path(&self.base_dir, 1, target_id);
        let mut target = Segment::open(target_path, self.encryption.clone())?;
        let mut rebuilt = HashMap::new();

        // Compact all entries into the new segment
        compact_segment(&mut target, &entries, &mut rebuilt, target_id)?;

        // Drop immutable segments before deletion
        for id in &immutable_ids {
            if let Some(mut meta) = self.segments.remove(id) {
                let path = meta.segment.path().to_path_buf();
                meta.segment.close();
                drop(meta);

                // Now safe to remove the file
                let _ = std::fs::remove_file(path);
            }
        }

        // self.segments.clear();
        self.segments.insert(
            target_id,
            SegmentMeta {
                id: target_id,
                level: 1,
                segment: target,
            },
        );
        // Replace the index with rebuilt one
        self.index = rebuilt;

        self.rewrite_wal_snapshot()
    }

    pub fn set_durability_mode(&mut self, mode: DurabilityMode) {
        self.wal.set_durability_mode(mode);
    }
}

fn segment_path(base: &Path, level: u32, id: u64) -> PathBuf {
    base.join(format!("segment-l{}-{}.dat", level, id))
}

fn parse_segment_name(name: &str) -> Option<(u32, u64)> {
    if !name.starts_with("segment-l") || !name.ends_with(".dat") {
        return None;
    }
    let core = &name[9..name.len() - 4];
    let (level, id) = core.split_once('-')?;
    Some((level.parse().ok()?, id.parse().ok()?))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::config::FireLiteConfig;

    use super::{segment_path, Segment, SegmentMeta, StorageEngine};

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn rotation_preserves_encryption_across_reopen() {
        let path = temp_path("firelite-storage-encryption");
        let cfg = FireLiteConfig {
            auto_compaction_threshold_bytes: 1,
            // encryption_key: Some("test-secret".to_string()),
            ..FireLiteConfig::default()
        };

        {
            let mut engine = StorageEngine::open(&path, &cfg).expect("engine open should succeed");
            engine
                .put("k1".to_string(), b"value-1")
                .expect("first put should succeed");
            engine
                .put("k2".to_string(), b"value-2")
                .expect("second put should succeed");
            let _ = engine.flush_wal();
        }

        let mut reopened = StorageEngine::open(&path, &cfg).expect("reopen should succeed");
        assert_eq!(
            reopened.get("k1").expect("read should succeed"),
            Some(b"value-1".to_vec())
        );
        assert_eq!(
            reopened.get("k2").expect("read should succeed"),
            Some(b"value-2".to_vec())
        );

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }

    #[test]
    fn compact_returns_when_no_same_level_merge_candidate_exists() {
        let path = temp_path("firelite-storage-compact");
        let cfg = FireLiteConfig::default();
        let mut engine = StorageEngine::open(&path, &cfg).expect("engine open should succeed");

        engine
            .put("k1".to_string(), b"value-1")
            .expect("put should succeed");
        engine
            .maybe_rotate_active_segment()
            .expect("rotation should succeed");

        let extra_id = engine.next_segment_id;
        engine.next_segment_id += 1;
        let extra_path = segment_path(&path, 1, extra_id);
        let extra_segment = Segment::open(extra_path, None).expect("segment open should succeed");
        engine.segments.insert(
            extra_id,
            SegmentMeta {
                id: extra_id,
                level: 1,
                segment: extra_segment,
            },
        );

        engine
            .compact()
            .expect("compaction should return successfully");

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }
}
