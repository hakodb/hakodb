use hashbrown::HashMap;
use std::path::{Path, PathBuf};
// use std::fs::File;

use crate::config::{FireLiteConfig, DurabilityMode};
use crate::error::{FireLiteError, Result};
use std::sync::{Arc, Mutex}; 
use std::sync::atomic::AtomicU64;
use std::sync::mpsc::SyncSender;
use std::time::{SystemTime, UNIX_EPOCH};
use crate::memory::page_cache::PageCache; 

use super::compaction::compact_segment;
use super::crypto::EncryptionContext;
use super::segment::Segment;
use super::wal::{Wal, WalOp};


#[derive(Debug, Clone)] 
pub enum Pointer {
    Segment {
        segment_id: u64,
        offset: u64,
        len: u32,
    },
    Inlined(Vec<u8>),
    Blob {
        offset: u64,
        len: u32,
    },
    BlobPending(Arc<Vec<u8>>),
    Deleted { timestamp: i64 },
}

#[derive(Debug, Clone)]
pub enum StorageMutation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
}

pub enum BlobWork {
    PutRaw {
        collection: String,
        offset: u64,
        data: Arc<Vec<u8>>,
    },
    Put {
        collection: String,
        key: String,
        data: Arc<Vec<u8>>,
    },
}

struct SegmentMeta {
    // id: u64,
    level: u32,
    segment: Segment,
}

pub struct StorageEngine {
    base_dir: PathBuf,
    segments: HashMap<u64, SegmentMeta>,
    active_segment_id: u64,
    next_segment_id: u64,
    pub(crate) wal: Wal,
    next_tx_id: u64,
    compaction_threshold_bytes: usize,
    pub(crate) encryption: Option<EncryptionContext>,
    inlined_bytes: usize,
    max_inlined_bytes: usize,
    use_compression: bool, 
    pub(crate) collection_counts: HashMap<String, usize>,
    pub cache: Arc<Mutex<PageCache>>,
    pub mmap_size: usize, 
    pub index: HashMap<String, Pointer>,
    // pub(crate) blob_file: Option<Arc<std::sync::Mutex<std::fs::File>>>,
    pub(crate) blob_file: Option<Arc<std::fs::File>>,
    pub(crate) blob_tx: Option<SyncSender<BlobWork>>,
    pub(crate) blob_size: AtomicU64,
    pub logical_name: String,
}

impl StorageEngine {
    pub fn open(
        base_dir: impl AsRef<Path>, 
        cfg: &FireLiteConfig,
        logical_name: String
    ) -> Result<Self> {
        let base_path = base_dir.as_ref().to_path_buf(); 
        std::fs::create_dir_all(&base_path)?;
        // std::fs::create_dir_all(base_dir.as_ref())?;

        let encryption = cfg
            .encryption_key
            .as_ref()
            .map(|secret| EncryptionContext::from_secret(secret));

        let cache_limit_bytes = cfg.page_cache_capacity * cfg.page_size;
        let cache = Arc::new(Mutex::new(PageCache::new(cache_limit_bytes)));

        let wal = Wal::open(
            base_dir.as_ref().join("wal.log"),
            cfg.durability_mode,
            cfg.group_commit_max_ops,
            encryption.clone(),
        )?;

        let mut segments = HashMap::new();
        let mut max_id = 0;
        let mut active_segment_id = 0;

        // let blob_file = std::fs::OpenOptions::new()
        //     .create(true).read(true).append(true)
        //     .open(base_path.join("blobs.dat"))?;
        let blob_file_raw = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open(base_path.join("blobs.dat"))?;

        // let blob_file = Arc::new(Mutex::new(blob_file_raw));
        // 1. Get metadata while we still have ownership of blob_file_raw
        let initial_size = blob_file_raw.metadata()?.len();

        // 2. Now move it into the Arc
        let blob_file = Arc::new(blob_file_raw);
            

        for entry in std::fs::read_dir(base_dir.as_ref())? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some((level, id)) = parse_segment_name(&name) {
                let segment = Segment::open(
                    entry.path(), 
                    id, 
                    encryption.clone(), 
                    Arc::clone(&cache), 
                    cfg.mmap_size
                )?;
                
                segments.insert(id, SegmentMeta { level, segment });
                if id > max_id { max_id = id; }
                if id >= active_segment_id { active_segment_id = id; }
            }
        }

        if segments.is_empty() {
            let path = segment_path(base_dir.as_ref(), 0, 0);
            let segment = Segment::open(
                path, 
                0, 
                encryption.clone(), 
                Arc::clone(&cache), 
                cfg.mmap_size
            )?;
            
            segments.insert(0, SegmentMeta { level: 0, segment });
            max_id = 0;
            active_segment_id = 0;
        }

        let mut engine = Self {
            base_dir: base_path,
            segments,
            active_segment_id,
            next_segment_id: max_id + 1,
            wal,
            index: HashMap::new(),
            next_tx_id: 1,
            compaction_threshold_bytes: cfg.auto_compaction_threshold_bytes,
            encryption,
            use_compression: cfg.use_compression,
            inlined_bytes: 0, 
            max_inlined_bytes: cfg.max_inlined_memory_bytes,
            collection_counts: HashMap::new(),
            cache, 
            mmap_size: cfg.mmap_size,
            blob_file: Some(blob_file),
            blob_tx: None,
            blob_size: AtomicU64::new(initial_size),
            logical_name,
        };

        engine.recover()?;
        Ok(engine)
    }

    fn recover(&mut self) -> Result<()> {
        self.index.reserve(1024);
        for op in self.wal.replay()? {
            match op {
                WalOp::Put { key, segment_id, segment_offset, len } => {
                    let pointer = Pointer::Segment { 
                        segment_id, 
                        offset: segment_offset, 
                        len 
                    };
                    self.update_index_entry(key, Some(pointer));
                }
                WalOp::Delete { key, timestamp  } => {
                    self.update_index_entry(key, Some(Pointer::Deleted { timestamp }));
                }
                WalOp::BeginTx { .. } | WalOp::CommitTx { .. } => {}
                WalOp::PutInlined { key, value } => {
                    let pointer = Pointer::Inlined(value);
                    self.update_index_entry(key, Some(pointer));
                }
                WalOp::PutBlob { key, offset, len } => {
                    self.update_index_entry(key, Some(Pointer::Blob { offset, len }));
                }
            }
        }
        Ok(())
    }

    fn update_index_entry(&mut self, key: String, new_pointer: Option<Pointer>) {
        let collection = &self.logical_name;
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;

        // --- STEP 1: CLEANUP OLD ENTRY ---
        if let Some(old_p) = self.index.remove(&key) {
            match &old_p {
                Pointer::Inlined(data) => { self.inlined_bytes = self.inlined_bytes.saturating_sub(data.len()); }
                _ => {}
            }

            let is_tomb = matches!(old_p, Pointer::Deleted { .. });
            
            // If it was a real document, we MUST decrement now, 
            // because it's either being replaced or deleted.
            if !is_tomb {
                if let Some(count) = self.collection_counts.get_mut(collection) {
                    *count = count.saturating_sub(1);
                }
            }
            is_tomb
        } else {
            false // Brand new key
        };

        // --- STEP 2: APPLY NEW ENTRY ---
        match new_pointer {
            Some(p) => {
                if let Pointer::Inlined(ref data) = p { self.inlined_bytes += data.len(); }

                // Only increment if we are putting a REAL document 
                // AND we aren't just replacing one tombstone with another.
                if !matches!(p, Pointer::Deleted { .. }) {
                    *self.collection_counts.entry(collection.to_string()).or_insert(0) += 1;
                }
                self.index.insert(key, p);
            }
            None => {
                // LOCAL DELETE PATH
                // We already decremented the count in Step 1 if it was a real doc.
                self.index.insert(key, Pointer::Deleted { timestamp: now });
            }
        }
    }

    pub fn checkpoint_inlined_data(&mut self) -> Result<bool> {
        // Only trigger if we are over the limit
        if self.inlined_bytes < self.max_inlined_bytes {
            return Ok(false);
        }

        // 1. Gather all inlined documents
        let mut to_flush = Vec::new();
        for (key, pointer) in &self.index {
            if let Pointer::Inlined(data) = pointer {
                to_flush.push((key.clone(), data.clone()));
            }
        }

        if to_flush.is_empty() { return Ok(false); }

        // 2. Write to a new Segment (Level 0)
        let target_id = self.next_segment_id;
        self.next_segment_id += 1;
        let target_path = segment_path(&self.base_dir, 0, target_id);

        let mut target_segment = Segment::open(
            target_path, 
            target_id, 
            self.encryption.clone(), 
            Arc::clone(&self.cache), 
            self.mmap_size
        )?;

        // RENAME TO MATCH THE LOOP BELOW
        let mut new_pointers = HashMap::new(); 
        
        crate::storage::compaction::compact_segment(
            &mut target_segment, 
            &to_flush, 
            &mut new_pointers, 
            target_id,
            self.use_compression
        )?;
        
        target_segment.flush()?;

        // 3. Update the Index (Moves Pointer::Inlined -> Pointer::Segment)
        for (key, pointer) in new_pointers {
            self.update_index_entry(key, Some(pointer));
        }

        // 4. Register the new segment
        self.segments.insert(target_id, SegmentMeta {
            // id: target_id,
            level: 0,
            segment: target_segment,
        });

        // 5. Cleanup WAL (Remove the raw 'PutInlined' data from the log)
        self.rewrite_wal_snapshot()?;

        Ok(true)
    }

    /// Now this becomes Instant (O(1)) and accurate!
    pub fn list_collections(&self) -> Result<Vec<String>> {
        let mut cols: Vec<String> = self.collection_counts.keys().cloned().collect();
        cols.sort();
        Ok(cols)
    }

    pub fn apply_batch(
        &mut self, 
        mutations: &[StorageMutation], 
        is_remote: bool // Added to distinguish Local vs Sync
    ) -> Result<(Vec<BlobWork>, Vec<crate::storage::wal::WalOp>)> {
        let now = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64;
        if mutations.is_empty() { return Ok((Vec::new(), Vec::new())); }

        let tx_id = self.next_tx_id;
        self.next_tx_id += 1;

        // Ensure we have space in the active segment
        self.maybe_rotate_active_segment()?;

        let mut wal_ops = Vec::with_capacity(mutations.len() + 2);
        wal_ops.push(WalOp::BeginTx { tx_id });

        let mut index_updates = Vec::new();
        let mut puts_to_segment = Vec::new();
        let mut segment_mutation_indices = Vec::new();
        let mut blob_work_todo = Vec::new();

        // --- THE HOT LOOP: No Networking, No Cloning ---
        for (i, mutation) in mutations.iter().enumerate() {
            match mutation {
                StorageMutation::Put { key, value } => {
                    let len = value.len();
                    
                    if len < 4096 { // Path 1: Tiny (Inline)
                        wal_ops.push(WalOp::PutInlined { key: key.clone(), value: value.clone() });
                        index_updates.push((key.clone(), Some(Pointer::Inlined(value.clone()))));
                    } else if len > 32768 { // Path 2: Large (Side-load to Blob File)
                        let arc_data = Arc::new(value.clone());
                        index_updates.push((key.clone(), Some(Pointer::BlobPending(arc_data.clone()))));
                        
                        // Keep the local Blob Worker active!
                        blob_work_todo.push(BlobWork::Put {
                            collection: self.logical_name.clone(),
                            key: key.clone(),
                            data: arc_data,
                        });
                    } else { // Path 3: Medium (Standard Segment)
                        puts_to_segment.push(value.as_slice());
                        segment_mutation_indices.push(i);
                    }
                }
                StorageMutation::Delete { key } => {
                    wal_ops.push(WalOp::Delete { key: key.clone(), timestamp: now });
                    index_updates.push((key.clone(), Some(Pointer::Deleted { timestamp: now })));
                }
            }
        }

        // Write medium values to the active segment
        if !puts_to_segment.is_empty() {
            let active_id = self.active_segment_id;
            let active = self.segments.get_mut(&active_id).unwrap();
            let offsets = active.segment.append_batch(&puts_to_segment)?;

            for ((offset, len), mut_idx) in offsets.into_iter().zip(segment_mutation_indices) {
                if let StorageMutation::Put { key, .. } = &mutations[mut_idx] {
                    wal_ops.push(WalOp::Put { key: key.clone(), segment_id: active_id, segment_offset: offset, len });
                    index_updates.push((key.clone(), Some(Pointer::Segment { segment_id: active_id, offset, len })));
                }
            }
        }

        wal_ops.push(WalOp::CommitTx { tx_id });

        // --- THE DURABILITY STEP ---
        // Write to WAL with the 'is_remote' priority flag.
        self.wal.append_batch(&wal_ops, is_remote)?;

        // Update RAM Index
        for (key, pointer) in index_updates {
            self.update_index_entry(key, pointer);
        }

        // RETURN: The blob work goes to the background thread. 
        // The replication work is handled by the Standalone Agent tailing the WAL.
        Ok((blob_work_todo, wal_ops))
    }

    pub fn flush_all(&mut self) -> Result<()> {
        // First flush the data segment
        if let Some(meta) = self.segments.get_mut(&self.active_segment_id) {
            meta.segment.flush()?;
        }
        // Then flush the WAL
        self.wal.flush()?;
        Ok(())
    }

    pub fn run_background_maintenance(&mut self) -> Result<()> {
        self.maybe_rotate_active_segment()?;
        // Check if RAM is full and spill to disk if needed
        self.checkpoint_inlined_data()?;
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
        // let encryption = self.encryption.clone();
        // FIX: Add missing 3 arguments
        let segment = Segment::open(
            path, 
            new_id, 
            self.encryption.clone(), 
            Arc::clone(&self.cache), 
            self.mmap_size
        )?;
        self.segments.insert(
            new_id,
            SegmentMeta {
                // id: new_id,
                level: 0,
                segment,
            },
        );
        self.active_segment_id = new_id;
        Ok(())
    }

    fn compact_tiers_once(&mut self) -> Result<bool> {
        // 1. Find two immutable segments on the same level
        let mut by_level: HashMap<u32, Vec<u64>> = HashMap::new();
        for (id, meta) in &self.segments {
            if *id == self.active_segment_id { continue; }
            by_level.entry(meta.level).or_default().push(*id);
        }

        let mut candidate: Option<(u32, u64, u64)> = None;
        for (level, ids) in by_level {
            if ids.len() >= 2 {
                candidate = Some((level, ids[0], ids[1]));
                break;
            }
        }

        let Some((level, s1, s2)) = candidate else { return Ok(false); };

        // 2. Prepare merge
        let target_level = level + 1;
        let target_id = self.next_segment_id;
        self.next_segment_id += 1;

        let mut entries: Vec<(String, Vec<u8>)> = Vec::new();
        for (key, pointer) in &self.index {
            if let Pointer::Segment { segment_id, .. } = pointer {
                if *segment_id == s1 || *segment_id == s2 {
                    if let Some(value) = self.read_pointer(pointer)? {
                        entries.push((key.clone(), value));
                    }
                }
            }
        }

        // 3. Perform merge with Compression support
        let target_path = segment_path(&self.base_dir, target_level, target_id);
        // FIX: Add missing 3 arguments
        let mut target = Segment::open(
            target_path, 
            target_id, 
            self.encryption.clone(), 
            Arc::clone(&self.cache), 
            self.mmap_size
        )?;
        let mut new_index_subset = HashMap::new();

        // FIX: Pass self.use_compression here!
        compact_segment(&mut target, &entries, &mut new_index_subset, target_id, self.use_compression)?;

        // 4. Cleanup old files
        for id in &[s1, s2] {
            if let Some(mut meta) = self.segments.remove(id) {
                let path = meta.segment.path().to_path_buf();
                meta.segment.close();
                let _ = std::fs::remove_file(path);
            }
        }

        // 5. Update master index
        for (k, p) in new_index_subset { self.index.insert(k, p); }
        self.segments.insert(
            target_id, 
            SegmentMeta { 
                // id: target_id, 
                level: target_level, 
                segment: target 
            });
        self.rewrite_wal_snapshot()?;

        Ok(true)
    }

    fn rewrite_wal_snapshot(&mut self) -> Result<()> {
        self.wal.reset()?;
        let mut ops = Vec::with_capacity(self.index.len());
        for (key, pointer) in &self.index {
            match pointer {
                Pointer::Segment { segment_id, offset, len } => {
                    ops.push(WalOp::Put {
                        key: key.clone(),
                        segment_id: *segment_id,
                        segment_offset: *offset,
                        len: *len,
                    });
                }
                Pointer::Inlined(value) => {
                    ops.push(WalOp::PutInlined {
                        key: key.clone(),
                        value: value.clone(),
                    });
                }
                // v0.8.0 Recovery variants
                Pointer::Blob { offset, len } => {
                    ops.push(WalOp::PutBlob { key: key.clone(), offset: *offset, len: *len });
                }
                Pointer::BlobPending(data) => {
                    // If we crash while pending, treat it as inlined in WAL for safety
                    ops.push(WalOp::PutInlined { key: key.clone(), value: (**data).clone() });
                }
                Pointer::Deleted { timestamp } => {
                    ops.push(WalOp::Delete { key: key.clone(), timestamp: *timestamp });
                }
            }
        }
        self.wal.append_batch(&ops, false)?;
        Ok(())
    }


    pub(crate) fn read_pointer_internal(&self, pointer: &Pointer, use_cache: bool) -> Result<Option<Vec<u8>>> {
        match pointer {
            Pointer::BlobPending(data) => Ok(Some((**data).clone())),
            Pointer::Inlined(data) => Ok(Some(data.clone())),
            Pointer::Blob { offset, len } => {
                let mut buf = vec![0u8; *len as usize];
                
                let file = self.blob_file.as_ref()
                    .ok_or_else(|| FireLiteError::StorageError("Blob file missing".into()))?;

                #[cfg(unix)] {
                    use std::os::unix::fs::FileExt;
                    file.read_exact_at(&mut buf, *offset)?;
                }
                #[cfg(windows)] {
                    use std::os::windows::fs::FileExt;
                    file.seek_read(&mut buf, *offset)?;
                }

                if let Some(enc) = &self.encryption {
                    Ok(Some(enc.decrypt(&buf)?))
                } else {
                    Ok(Some(buf))
                }
            },
            Pointer::Segment { segment_id, offset, len } => {
                let Some(meta) = self.segments.get(segment_id) else { return Ok(None); };
                Ok(Some(meta.segment.read_at(*offset, *len, use_cache)?))
            },
            Pointer::Deleted { .. } => Ok(None),
        }
    }


    // UPDATED: Use the internal helper to avoid E0004
    pub fn read_pointer(&self, pointer: &Pointer) -> Result<Option<Vec<u8>>> {
        self.read_pointer_internal(pointer, true)
    }

    // UPDATED: Use the internal helper to avoid E0004
    pub fn read_pointer_uncached(&self, pointer: &Pointer) -> Result<Option<Vec<u8>>> {
        self.read_pointer_internal(pointer, false)
    }

    // UPDATED: Use the internal helper to avoid E0004
    pub fn read_pointer_uncached_by_key(&self, key: &str) -> Result<Option<Vec<u8>>> {
        let Some(pointer) = self.index.get(key) else { return Ok(None); }; 
        self.read_pointer_internal(pointer, false)
    }

    pub fn flush_wal(&mut self) -> Result<()> {
        self.wal.flush()
    }

    pub fn put(&mut self, key: String, value: &[u8]) -> Result<()> {
        let mutation = StorageMutation::Put {
            key,
            value: value.to_vec(),
        };

        // self.apply_batch(&[mutation])
        // let work = self.apply_batch(&[mutation])?;
        let (work, _committed_ops) = self.apply_batch(&[mutation], false)?;

        // 2. Since this is a synchronous put, we send the work here
        for w in work {
            if let Some(tx) = &self.blob_tx {
                let _ = tx.send(w);
            }
        }

        Ok(())
    }

    // pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
    //     let Some(pointer) = self.index.get(key).cloned() else { return Ok(None); };
    //     match pointer {
    //         Pointer::Inlined(data) => Ok(Some(data.clone())),
    //         Pointer::Segment { segment_id, offset, len } => {
    //             let Some(meta) = self.segments.get(&segment_id) else { return Ok(None); };
    //             Ok(Some(meta.segment.read_at(offset, len, true)?))
    //         }
    //     }
    // }
    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        // // 1. Look up the pointer in the index
        // let Some(pointer) = self.index.get(key) else { 
        //     return Ok(None); 
        // };
        
        // // 2. Use the centralized internal reader which handles 
        // // Inlined, BlobPending, Blob, and Segment exhaustive matching.
        // self.read_pointer_internal(pointer, true)
        match self.index.get(key) {
            Some(Pointer::Deleted { .. }) => Ok(None), // Treat as non-existent
            Some(pointer) => self.read_pointer_internal(pointer, true),
            None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        // self.apply_batch(&[StorageMutation::Delete {
        //     key: key.to_string(),
        // }])
        let mutation = StorageMutation::Delete {
            key: key.to_string(),
        };

        // 1. Capture the work
        // let work = self.apply_batch(&[mutation])?;
        let (work, _committed_ops) = self.apply_batch(&[mutation], false)?;

        // 2. Send to background worker
        for w in work {
            if let Some(tx) = &self.blob_tx {
                let _ = tx.send(w);
            }
        }

        Ok(())
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
        // 1. FORCED ROTATION: Ensure current data is eligible for compaction
        if self.segments.get(&self.active_segment_id)
            .map_or(false, |m| m.segment.size_bytes().unwrap_or(0) > 0) 
        {
            let new_id = self.next_segment_id;
            self.next_segment_id += 1;
            let path = segment_path(&self.base_dir, 0, new_id);
            // let segment = Segment::open(path, self.encryption.clone())?;
            // FIX: Add missing 3 arguments
            let segment = Segment::open(
                path, 
                new_id, 
                self.encryption.clone(), 
                Arc::clone(&self.cache), 
                self.mmap_size
            )?;
            self.segments.insert(
                new_id, 
                SegmentMeta { 
                    // id: new_id, 
                    level: 0, 
                    segment 
                });
            self.active_segment_id = new_id;
        }

        let immutable_ids: Vec<u64> = self.segments.keys()
            .filter(|&&id| id != self.active_segment_id)
            .cloned().collect();

        if immutable_ids.is_empty() { return Ok(()); }

        // 2. GLOBAL MERGE: Collect ALL data from ALL immutable segments
        let mut entries = Vec::new();
        for (key, pointer) in &self.index {
            if let Pointer::Segment { segment_id, .. } = pointer {
                if immutable_ids.contains(segment_id) {
                    if let Some(value) = self.read_pointer(pointer)? {
                        entries.push((key.clone(), value));
                    }
                }
            }
        }

        // 3. Write to ONE highly-optimized, compressed segment
        let target_id = self.next_segment_id;
        self.next_segment_id += 1;
        let target_path = segment_path(&self.base_dir, 1, target_id);
        // let mut target = Segment::open(target_path, self.encryption.clone())?;
        // FIX: Add missing 3 arguments
        let mut target = Segment::open(
            target_path, 
            target_id, 
            self.encryption.clone(), 
            Arc::clone(&self.cache), 
            self.mmap_size
        )?;
        let mut rebuilt_index_subset = HashMap::new();

        compact_segment(&mut target, &entries, &mut rebuilt_index_subset, target_id, self.use_compression)?;

        // 4. Atomic Swap
        for id in &immutable_ids {
            if let Some(mut meta) = self.segments.remove(id) {
                let path = meta.segment.path().to_path_buf();
                meta.segment.close();
                let _ = std::fs::remove_file(path);
            }
        }

        self.segments.insert(
            target_id, 
            SegmentMeta { 
                // id: target_id, 
                level: 1, 
                segment: target 
            });
        for (k, p) in rebuilt_index_subset { self.index.insert(k, p); }
        
        self.rewrite_wal_snapshot()?;
        Ok(())
    }

    pub fn set_durability_mode(&mut self, mode: DurabilityMode) { self.wal.set_durability_mode(mode); }

    pub fn base_dir(&self) -> &Path { &self.base_dir }

    pub fn backup(&mut self, destination_path: impl AsRef<Path>) -> Result<()> {
        // 1. Move all inlined data from RAM into segment files on Disk.
        // This ensures the backup is complete.
        self.checkpoint_inlined_data()?;
        
        // 2. Perform a physical sync of all files.
        self.flush_all()?;

        // 3. Create destination directory.
        std::fs::create_dir_all(destination_path.as_ref())?;

        // 4. Copy only relevant data and log files.
        for entry in std::fs::read_dir(&self.base_dir)? {
            let entry = entry?;
            let file_name = entry.file_name();
            let name_str = file_name.to_string_lossy();
            
            // We only back up data segments and the WAL.
            if name_str.ends_with(".dat") || name_str == "wal.log" {
                let dest = destination_path.as_ref().join(file_name);
                std::fs::copy(entry.path(), dest)?;
            }
        }
        Ok(())
    }

    /// Returns only the keys matching a prefix. 
    /// Extremely memory efficient because it doesn't touch the disk/mmap bodies.
    pub fn scan_prefix_keys(&self, prefix: &str) -> Vec<String> {
        self.index.keys()
            .filter(|k| k.starts_with(prefix))
            .cloned()
            .collect()
    }

    pub fn scan_chunks<F>(&self, prefix: &str, chunk_size: usize, mut f: F) -> Result<()> 
    where F: FnMut(Vec<(String, Vec<u8>)>) -> Result<()> 
    {
        let keys = self.scan_prefix_keys(prefix);

        for chunk_keys in keys.chunks(chunk_size) {
            let mut chunk_data = Vec::with_capacity(chunk_keys.len());
            for key in chunk_keys {
                if let Some(ptr) = self.index.get(key) {
                    if let Some(val) = self.read_pointer(ptr)? {
                        chunk_data.push((key.clone(), val));
                    }
                }
            }
            f(chunk_data)?;
        }
        Ok(())
    }

    pub fn get_physical_index_snapshot(&self) -> Vec<(String, Pointer)> {
        self.index.iter()
            .map(|(k, p)| (k.clone(), p.clone()))
            .collect()
    }

    pub fn apply_replicated_ops(&mut self, ops: &[crate::storage::wal::WalOp]) -> Result<()> {
        self.wal.append_batch(ops, true)?;
        for op in ops {
            match op {
                crate::storage::wal::WalOp::Put { key, segment_id, segment_offset, len } => {
                    self.index.insert(key.clone(), Pointer::Segment { 
                        segment_id: *segment_id, offset: *segment_offset, len: *len 
                    });
                }
                crate::storage::wal::WalOp::PutInlined { key, value } => {
                    self.update_index_entry(key.clone(), Some(Pointer::Inlined(value.clone())));
                    // self.index.insert(key.clone(), Pointer::Inlined(value.clone()));
                }
                crate::storage::wal::WalOp::Delete { key, timestamp } => {
                    // self.index.remove(key);
                    // self.update_index_entry(key.clone(), None);
                    self.update_index_entry(key.clone(), Some(Pointer::Deleted { timestamp: *timestamp }));
                }
                crate::storage::wal::WalOp::PutBlob { key, offset, len } => {
                    self.index.insert(key.clone(), Pointer::Blob { offset: *offset, len: *len });
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub fn get_wal_checkpoint(&self) -> u64 {
        self.wal.file.metadata()
            .map(|m: std::fs::Metadata| m.len())
            .unwrap_or(0)
    }

    pub fn purge_old_tombstones(&mut self, max_age: std::time::Duration) {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap().as_micros() as i64;
        let max_age_micros = max_age.as_micros() as i64;

        self.index.retain(|_, pointer| {
            if let Pointer::Deleted { timestamp } = pointer {
                // Keep if it's younger than the max age
                (now - *timestamp) < max_age_micros
            } else {
                true // Keep all real documents
            }
        });
    }

}

impl Drop for StorageEngine { fn drop(&mut self) { let _ = self.flush_all(); } }

fn segment_path(base: &Path, level: u32, id: u64) -> PathBuf { base.join(format!("segment-l{}-{}.dat", level, id)) }

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
            ..FireLiteConfig::default()
        };

        {
            let mut engine = StorageEngine::open(&path, &cfg, "test_collection".to_string()).expect("engine open should succeed");
            engine
                .put("k1".to_string(), b"value-1")
                .expect("first put should succeed");
            engine
                .put("k2".to_string(), b"value-2")
                .expect("second put should succeed");
            let _ = engine.flush_wal();
        }

        let mut reopened = StorageEngine::open(&path, &cfg, "test_collection".to_string()).expect("reopen should succeed");
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
        let mut engine = StorageEngine::open(&path, &cfg, "test_collection".to_string()).expect("engine open should succeed");

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
                // id: extra_id,
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
