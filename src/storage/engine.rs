use hashbrown::HashMap;
use std::path::{Path, PathBuf};
// use std::fs::File;

use crate::config::{HakoConfig, DurabilityMode};
use crate::error::{HakoError, Result};
use std::sync::{Arc, Mutex, RwLock};
// use std::sync::atomic::Ordering;
use std::time::UNIX_EPOCH;
use crate::memory::page_cache::PageCache;

use super::blob::{BlobManager, BlobWork};
use super::compaction::compact_segment;
use super::crypto::EncryptionContext;
use super::segment::Segment;
use super::wal::{Wal, WalOp};
use crossbeam_channel::Sender as CrossbeamSender;
use crate::document::hako_doc::HakoDoc;
// use crate::document::value::Value;

/// Successful narrow-lock tier merges since process start: test
/// tripwire (a compaction race test that never merges is vacuous) and
/// operational visibility. One Relaxed increment per merge.
static TIER_COMPACTIONS: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// How many tier compactions completed (see above).
pub fn tier_compactions() -> usize {
    TIER_COMPACTIONS.load(std::sync::atomic::Ordering::Relaxed)
}


#[derive(Debug, Clone)] 
pub enum Pointer {
    Segment {
        segment_id: u64,
        offset: u64,
        len: u32,
    },
    Inlined(Arc<Vec<u8>>),
    Blob {
        offset: u64,
        len: u32,
    },
    BlobPending(Arc<HakoDoc>),
    BlobPendingData { 
        data: Arc<Vec<u8>>, 
        skeleton: Vec<u8> 
    },
    Deleted { timestamp: i64 },
}

#[derive(Debug, Clone)]
pub enum StorageMutation {
    Put { key: String, value: Vec<u8> },
    Delete { key: String },
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
    pub(crate) next_tx_id: u64,
    compaction_threshold_bytes: usize,
    pub encryption: Option<EncryptionContext>,
    pub(crate) inlined_bytes: usize,
    pub(crate) max_inlined_bytes: usize,
    pub(crate) blob_threshold: usize,
    pub(crate) use_compression: bool, 
    pub(crate) collection_counts: HashMap<String, usize>,
    pub cache: Arc<Mutex<PageCache>>,
    pub mmap_size: usize,
    // Primary index: O(1) hash lookup. Ordered by insertion only when iterated
    // explicitly via `index.iter()`; reads use `get(key)` which is hash-based.
    pub index: HashMap<String, Pointer>,
    // Sorted key view for offset/cursor. Maintained on every index write under
    // the same exclusive lock that owns `index` (caller holds &mut StorageEngine
    // when calling update_index_entry), so no extra synchronisation needed.
    // Read by the executor via `sorted_key_range` and direct slice.
    pub(crate) sorted_keys: Vec<String>,
    pub blob_manager: Option<Arc<BlobManager>>,
    pub(crate) blob_tx: Option<CrossbeamSender<BlobWork>>,
    pub logical_name: String,
    // pub(crate) in_flight_blob_bytes: Arc<std::sync::atomic::AtomicUsize>,
    pub(crate) blob_flush_queue: std::collections::VecDeque<BlobWork>, 
    pub(crate) total_pending_blob_bytes: std::sync::atomic::AtomicUsize,
}

impl StorageEngine {
    pub fn open(
        base_dir: impl AsRef<Path>, 
        cfg: &HakoConfig,
        logical_name: String,
        encryption: Option<EncryptionContext>,
    ) -> Result<Self> {
        let base_path = base_dir.as_ref().to_path_buf(); 
        std::fs::create_dir_all(&base_path)?;
        // std::fs::create_dir_all(base_dir.as_ref())?;

        // let encryption = cfg
        //     .encryption_key
        //     .as_ref()
        //     .map(|secret| EncryptionContext::from_secret(secret));

        let cache_limit_bytes = cfg.page_cache_capacity * cfg.page_size;
        let cache = Arc::new(Mutex::new(PageCache::new(cache_limit_bytes)));

        // ponytail: internal collections (checkpoints, room registry, scope
        // markers) hold bytes of data — a multi-MB WAL headroom per system
        // shard is phantom size (sparse zeros still count in logical length).
        let wal_reserve = if logical_name.starts_with("__") { 0 } else { cfg.wal_reserve_bytes };
        let mut wal = Wal::open(
            base_dir.as_ref().join("wal.log"),
            cfg.durability_mode,
            cfg.group_commit_max_ops,
            encryption.clone(),
            wal_reserve,
        )?;
        // Per-instance group-commit window (cluster fsync staggering).
        // Clamped + strict-armed inside the setter; see HakoConfig docs.
        wal.set_group_commit_interval(cfg.group_commit_interval_ms);

        let mut segments = HashMap::new();
        let mut max_id = 0;
        let mut active_segment_id = 0;

        let blob_file_raw = std::fs::OpenOptions::new()
            .create(true)
            .read(true)
            // .append(true)
            .write(true)
            .open(base_path.join("blobs.dat"))?;

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
            encryption: encryption.clone(),
            use_compression: cfg.use_compression,
            inlined_bytes: 0,
            blob_threshold: cfg.value_blob_threshold_bytes,
            max_inlined_bytes: cfg.max_inlined_memory_bytes,
            collection_counts: HashMap::new(),
            cache,
            mmap_size: cfg.mmap_size,
            sorted_keys: Vec::new(),
            blob_manager: Some(Arc::new(BlobManager::new(
                blob_file,
                initial_size,
            ))),
            blob_tx: None,
            logical_name,
            blob_flush_queue: std::collections::VecDeque::with_capacity(1024),
            total_pending_blob_bytes: std::sync::atomic::AtomicUsize::new(0),
        };

        engine.recover()?;
        // WAL hygiene: hot-small collections pile history that nothing ever
        // compacts (segments never spill at this volume). One bounded rewrite
        // on open when stale history dominates; best-effort, never fails open.
        // 2-a (insiden-hako-wal-20260928): the rewrite resets the file and
        // stages the snapshot in the RAM buffer — WITHOUT this flush a
        // SIGTERM before the next threshold-triggered append wipes the
        // collection (file zeros + data only in RAM, indefinitely on a
        // quiet server). One write_all+fdatasync per bloated open: off the
        // write hot path by construction.
        if engine.wal_snapshot_worthwhile() {
            if engine.rewrite_wal_snapshot().is_ok() {
                let _ = engine.wal.flush();
            }
        }
        Ok(engine)
    }

    fn recover(&mut self) -> Result<()> {
        // ponytail: size the map from the WAL footprint (avoids rehash
        // churn on big replays; ops average well above 64B, so this
        // under-reserves rather than over-).
        let est = (self.wal.data_len() / 64).clamp(1024, 1_000_000) as usize;
        self.index.reserve(est);
        for op in self.wal.replay()? {
            match op {
                WalOp::Put { key, segment_id, segment_offset, len } => {
                    let pointer = Pointer::Segment {
                        segment_id,
                        offset: segment_offset,
                        len
                    };
                    self.update_index_entry_nosort(key, pointer);
                }
                WalOp::Delete { key, timestamp  } => {
                    self.update_index_entry_nosort(key, Pointer::Deleted { timestamp });
                }
                WalOp::BeginTx { .. } | WalOp::CommitTx { .. } => {}
                WalOp::PutInlined { key, value } => {
                    let pointer = Pointer::Inlined(Arc::new(value));
                    self.update_index_entry_nosort(key, pointer);
                }
                WalOp::PutBlob { key, offset, len } => {
                    self.update_index_entry_nosort(key, Pointer::Blob { offset, len });
                }
            }
        }
        // Rebuild sorted_keys from the recovered index. We pay the O(N log N)
        // sort once here instead of N x O(N) Vec::insert shifts during replay.
        let mut keys: Vec<String> = self
            .index
            .iter()
            .filter(|(_, p)| !matches!(p, Pointer::Deleted { .. }))
            .map(|(k, _)| k.clone())
            .collect();
        keys.sort();
        self.sorted_keys = keys;
        // ponytail: the live-count cache is exactly this set — seed it here
        // so count_prefix is O(1) from the first query (update_index_entry
        // maintains it afterwards; absent entries fall back to a scan).
        let live = self.sorted_keys.len();
        self.collection_counts.insert(self.logical_name.clone(), live);
        Ok(())
    }

    /// Vacuum: physically drop tombstones from the RAM index. The docs are
    /// already gone (a tombstone holds only a timestamp); this removes the
    /// markers themselves. Emits no WAL op, so it never replicates.
    /// Effect on sync: the collection version drops to the newest LIVE doc,
    /// so the next handshake pulls peers' state (restore-on-rejoin), and
    /// nothing is pushed outward. Orphaned blob bytes (if any) are left for
    /// blob-compaction, which owns that space.
    pub(crate) fn purge_tombstones(&mut self) -> usize {
        let dead: Vec<String> = self.index.iter()
            .filter(|(_, p)| matches!(p, Pointer::Deleted { .. }))
            .map(|(k, _)| k.clone())
            .collect();
        let n = dead.len();
        // ponytail: route through update_index_entry (None = remove) so the
        // sorted_keys view and byte accounting stay in lockstep.
        for k in dead {
            self.update_index_entry(k, None);
        }
        n
    }

    /// Bulk replay path: map insert + inlined-bytes accounting only.
    /// Skips `sorted_keys` maintenance AND live-count churn — `recover()`
    /// rebuilds both once afterwards. Routing replay through the regular
    /// path is O(N²) Vec memmoves on unsorted WAL order plus a key clone
    /// per op. Old-pointer bytes ARE unaccounted (overwrites during
    /// replay must net out, or the snapshot heuristic misfires).
    fn update_index_entry_nosort(&mut self, key: String, new_pointer: Pointer) {
        match &new_pointer {
            Pointer::Inlined(d) => { self.inlined_bytes += d.len(); }
            Pointer::BlobPendingData { skeleton, .. } => { self.inlined_bytes += skeleton.len(); }
            _ => {}
        }
        if let Some(old) = self.index.insert(key, new_pointer) {
            match old {
                Pointer::Inlined(d) => { self.inlined_bytes = self.inlined_bytes.saturating_sub(d.len()); }
                Pointer::BlobPendingData { skeleton, .. } => { self.inlined_bytes = self.inlined_bytes.saturating_sub(skeleton.len()); }
                _ => {}
            }
        }
    }

    pub(crate) fn update_index_entry(&mut self, key: String, new_pointer: Option<Pointer>) {
        // 1. Perform the map operation once.
        // .insert() returns the previous value if it existed.
        let old_p = if let Some(p) = &new_pointer {
            // Track memory stats for the NEW pointer
            match p {
                Pointer::Inlined(d) => { self.inlined_bytes += d.len(); }
                Pointer::BlobPendingData { skeleton, .. } => { self.inlined_bytes += skeleton.len(); }
                _ => {}
            }
            self.index.insert(key.clone(), p.clone())
        } else {
            self.index.remove(&key)
        };

        // 1b. Maintain the sorted_keys view in lockstep with `index`.
        // Only LIVE pointers go in sorted_keys — tombstones are excluded so the
        // sorted view is a clean offset/cursor index.
        // Insert: binary_search + insert (O(n) worst case but keys arrive in WAL
        //   replay order which is mostly sorted, so amortised O(log n)).
        // Remove: binary_search + remove (O(n) worst case but deletes are rare
        //   relative to inserts in steady state).
        // The caller holds &mut StorageEngine so no extra synchronisation needed.
        let new_is_live = matches!(new_pointer, Some(ref p) if !matches!(p, Pointer::Deleted { .. }));
        if new_is_live {
            match self.sorted_keys.binary_search(&key) {
                Ok(_) => {} // already present — overwrite kept key
                Err(pos) => self.sorted_keys.insert(pos, key.clone()),
            }
        } else if let Ok(pos) = self.sorted_keys.binary_search(&key) {
            self.sorted_keys.remove(pos);
        }

        // 2. Adjust stats based on the OLD pointer
        // ponytail: liveness computed before the move below.
        let old_was_live = old_p.as_ref().map(|v| !matches!(v, Pointer::Deleted { .. })).unwrap_or(false);
        if let Some(old_val) = old_p {
            // Subtract memory stats for the OLD pointer
            match old_val {
                Pointer::Inlined(ref d) => { self.inlined_bytes = self.inlined_bytes.saturating_sub(d.len()); }
                Pointer::BlobPendingData { ref skeleton, .. } => { self.inlined_bytes = self.inlined_bytes.saturating_sub(skeleton.len()); }
                _ => {}
            }
        }

        // --- COUNT LOGIC (live = non-Deleted) ---
        // ponytail: single entry()-based accounting for all transitions.
        // The old three-branch get_mut silently skipped absent keys, so
        // brand-new collections never got an entry and the map stayed
        // dead. Live<->tombstone flips adjust; live<->live and dead
        // removals (purge) don't.
        if old_was_live && !new_is_live {
            if let Some(count) = self.collection_counts.get_mut(&self.logical_name) {
                *count = count.saturating_sub(1);
            }
        } else if !old_was_live && new_is_live {
            *self.collection_counts.entry(self.logical_name.clone()).or_insert(0) += 1;
        }
    }

    /// Returns a half-half-open range `[start_pos, end_pos)` over the sorted key list,
    /// or `None` if the start key isn't found. The caller can then slice
    /// `self.sorted_keys[start_pos..end_pos]` and look up pointers via `self.index`.
    pub(crate) fn sorted_key_range(
        &self,
        start: Option<&str>,
        start_exclusive: bool,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Option<(usize, usize)> {
        let total = self.sorted_keys.len();
        if total == 0 { return Some((0, 0)); }

        let start_pos = match start {
            // ponytail: mirror BTree range semantics — a missing anchor
            // starts at the next-greater key (insertion point), not empty.
            Some(s) => match self.sorted_keys.binary_search_by(|k| k.as_str().cmp(s)) {
                Ok(p) => p + (start_exclusive as usize) + offset.unwrap_or(0),
                Err(pos) => pos + offset.unwrap_or(0),
            },
            None => offset.unwrap_or(0),
        };
        let end_pos = match limit {
            Some(l) => (start_pos + l).min(total),
            None => total,
        };
        if start_pos >= total { return Some((total, total)); }
        Some((start_pos, end_pos))
    }

    /// Reverse mirror of [`Self::sorted_key_range`]: bounds for a descending
    /// walk over `sorted_keys` with an optional upper anchor. Returns the
    /// ascending slice `[s, e)` the executor walks with `.rev()`.
    /// `start_after(anchor)` = keys strictly below anchor;
    /// `start_at(anchor)` = keys at-or-below anchor; `None` = from the top.
    /// `offset` skips from the top, `limit` takes the next rows downward.
    pub(crate) fn sorted_key_range_reverse(
        &self,
        start: Option<&str>,
        start_exclusive: bool,
        offset: Option<usize>,
        limit: Option<usize>,
    ) -> Option<(usize, usize)> {
        let total = self.sorted_keys.len();
        if total == 0 { return Some((0, 0)); }

        let raw_end = match start {
            Some(s) => match self.sorted_keys.binary_search_by(|k| k.as_str().cmp(s)) {
                Ok(p) => p + (!start_exclusive as usize),
                Err(pos) => pos,
            },
            None => total,
        };
        let end = raw_end.saturating_sub(offset.unwrap_or(0));
        let s = match limit {
            Some(l) => end.saturating_sub(l),
            None => 0,
        };
        Some((s.min(end), end))
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
                to_flush.push((key.clone(), (**data).clone()));
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
        // let threshold = 

        // --- THE HOT LOOP: No Networking, No Cloning ---
        for (i, mutation) in mutations.iter().enumerate() {
            match mutation {
                StorageMutation::Put { key, value } => {
                    let len = value.len();
                    
                    
                    if len < 4096 { // Path 1: Tiny (Inline)
                        wal_ops.push(WalOp::PutInlined { key: key.clone(), value: value.clone() });
                        index_updates.push((key.clone(), Some(Pointer::Inlined(Arc::new(value.clone())))));
                    } else if len > self.blob_threshold { // Path 2: Large (Side-load to Blob File)
                        let arc_data = Arc::new(value.clone());
                        // FIX: Use Pointer::Inlined for raw byte batches
                        index_updates.push((key.clone(), Some(Pointer::Inlined(Arc::new(value.clone())))));
                        
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
        self.drain_blob_queue()?;
        if let Some(meta) = self.segments.get_mut(&self.active_segment_id) {
            meta.segment.flush()?;
        }
        self.wal.flush()?;
        Ok(())
    }

    pub fn drain_blob_queue(&mut self) -> Result<()> {
        let bm_handle = self.blob_manager.clone();
        let Some(bm) = bm_handle else { return Ok(()); };
        
        while let Some(work) = self.blob_flush_queue.pop_front() {
            // Note the addition of timestamp
            if let BlobWork::PutRaw { key, offset, data, skeleton, timestamp, .. } = work {
                // 1. Physical Write
                bm.write_at(data.as_slice(), offset)?;
                
                // 2. Atomic Swap with Timestamp Verification
                if !skeleton.is_empty() {
                    if let Some(Pointer::BlobPending(current_doc)) = self.index.get(&key) {
                        if current_doc.get_logical_time() == timestamp {
                            self.update_index_entry(key, Some(Pointer::Inlined(Arc::new(skeleton))));
                        }
                    }
                }
            }
        }
        bm.file().sync_all()?;
        Ok(())
    }

    /// `defer_heavy`: skip the segment spill while queries are in flight
    /// — the maintenance tick sheds it. Tier compaction left the &mut
    /// world entirely (see try_compact_tiers: merge I/O never holds a
    /// guard). WAL flush + segment rotation stay (cheap, and durability
    /// is never deferred). Bounded by the caller (max consecutive skips)
    /// so backlog drains under sustained load.
    pub fn run_background_maintenance(&mut self, defer_heavy: bool) -> Result<()> {
        // 2-c: Interval shards flush dirty buffers on the existing 500ms
        // system tick (off the request path) — closes the quiet-window where
        // a SIGKILL/power loss would take unflushed appends. Mode contracts
        // hold: Manual never auto-flushes, OnCommit flushes only at commit,
        // Always is already per-op. Idle shards = one is_empty check.
        // The predicate is interval-aware (not blind): a custom long window
        // must not degrade to tick cadence.
        if self.wal.should_tick_flush() {
            let _ = self.wal.flush();
        }
        self.maybe_rotate_active_segment()?;
        if defer_heavy {
            return Ok(());
        }
        // Check if RAM is full and spill to disk if needed.
        self.checkpoint_inlined_data()?;
        Ok(())
    }

    fn maybe_rotate_active_segment(&mut self) -> Result<()> {
        let size = self
            .segments
            .get_mut(&self.active_segment_id)
            .ok_or_else(|| HakoError::Corrupt("active segment missing".into()))?
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

    /// Narrow-lock tier compaction: reserve+collect (brief write) →
    /// merge (NO lock: segment reads, new-file writes) → commit (brief
    /// write: verify-then-swap + WAL rewrite/flush; old files closed and
    /// deleted only AFTER the guard drops). The tick calls this without
    /// holding any guard, so merge I/O never queues queries.
    ///
    /// Commit race rule: only entries whose live pointer still addresses
    /// a merged segment are swapped. Puts/deletes that landed mid-merge
    /// moved the pointer (or tombstoned it) — those keys are skipped:
    /// their bytes live elsewhere now, and the merged copy is dead space
    /// until this segment itself compacts (standard LSM behavior,
    /// bounded). Sources that vanished (a concurrent full `compact` ate
    /// them) abort the merge and delete the orphan target file.
    pub fn try_compact_tiers(shard: &Arc<RwLock<StorageEngine>>) -> Result<bool> {
        struct Prep {
            level: u32,
            s1: u64,
            s2: u64,
            target_level: u32,
            target_id: u64,
            base_dir: PathBuf,
            encryption: Option<EncryptionContext>,
            cache: Arc<Mutex<PageCache>>,
            mmap_size: usize,
            use_compression: bool,
        }
        // -- Phase 1: candidate + id reservation (brief write, metadata).
        let prep = {
            let mut storage = shard
                .write()
                .map_err(|_| HakoError::LockPoisoned("compact-prep".into()))?;
            let mut by_level: HashMap<u32, Vec<u64>> = HashMap::new();
            for (id, meta) in &storage.segments {
                if *id == storage.active_segment_id {
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
            let target_id = storage.next_segment_id;
            storage.next_segment_id += 1;
            Prep {
                level,
                s1,
                s2,
                target_level: level + 1,
                target_id,
                base_dir: storage.base_dir.clone(),
                encryption: storage.encryption.clone(),
                cache: Arc::clone(&storage.cache),
                mmap_size: storage.mmap_size,
                use_compression: storage.use_compression,
            }
        };
        // -- Phase 2: collect + merge with NO lock held.
        let entries: Vec<(String, Vec<u8>)> = {
            let storage = shard
                .read()
                .map_err(|_| HakoError::LockPoisoned("compact-collect".into()))?;
            let mut out = Vec::new();
            for (key, pointer) in &storage.index {
                if let Pointer::Segment { segment_id, .. } = pointer {
                    if *segment_id == prep.s1 || *segment_id == prep.s2 {
                        if let Some(value) = storage.read_pointer(pointer)? {
                            out.push((key.clone(), value));
                        }
                    }
                }
            }
            out
        };
        let target_path = segment_path(&prep.base_dir, prep.target_level, prep.target_id);
        let mut target = Segment::open(
            target_path.clone(),
            prep.target_id,
            prep.encryption,
            prep.cache,
            prep.mmap_size,
        )?;
        let mut new_index_subset = HashMap::new();
        if let Err(e) = compact_segment(
            &mut target,
            &entries,
            &mut new_index_subset,
            prep.target_id,
            prep.use_compression,
        ) {
            drop(target);
            let _ = std::fs::remove_file(&target_path);
            return Err(e);
        }
        // -- Phase 3: commit (brief write). Verify-then-swap per key.
        let mut target_opt = Some(target);
        let olds: Option<Vec<SegmentMeta>> = {
            let mut storage = shard
                .write()
                .map_err(|_| HakoError::LockPoisoned("compact-commit".into()))?;
            let sources_ok = [prep.s1, prep.s2].iter().all(|id| {
                matches!(storage.segments.get(id), Some(m) if m.level == prep.level)
            });
            if !sources_ok {
                None
            } else {
                let mut olds = Vec::with_capacity(2);
                for id in [prep.s1, prep.s2] {
                    if let Some(meta) = storage.segments.remove(&id) {
                        olds.push(meta);
                    }
                }
                for (k, p) in new_index_subset {
                    let still_merged = matches!(
                        storage.index.get(&k),
                        Some(Pointer::Segment { segment_id, .. })
                            if *segment_id == prep.s1 || *segment_id == prep.s2
                    );
                    if still_merged {
                        storage.index.insert(k, p);
                    }
                }
                storage.segments.insert(
                    prep.target_id,
                    SegmentMeta {
                        level: prep.target_level,
                        segment: target_opt.take().expect("merge target"),
                    },
                );
                storage.rewrite_wal_snapshot()?;
                // Same staged-without-flush hazard as before: sync now
                // (one fdatasync per compaction, not per write).
                storage.wal.flush()?;
                Some(olds)
            }
        };
        match olds {
            Some(olds) => {
                // File teardown outside any guard (close/remove can block
                // on some platforms — never hold a lock across it).
                for mut meta in olds {
                    let path = meta.segment.path().to_path_buf();
                    meta.segment.close();
                    let _ = std::fs::remove_file(path);
                }
                TIER_COMPACTIONS.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                Ok(true)
            }
            None => {
                drop(target_opt);
                let _ = std::fs::remove_file(&target_path);
                Ok(false)
            }
        }
    }

    pub(crate) fn rewrite_wal_snapshot(&mut self) -> Result<()> {
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
                        value: (**value).clone(),
                    });
                }
                // v0.8.0 Recovery variants
                Pointer::Blob { offset, len } => {
                    ops.push(WalOp::PutBlob { key: key.clone(), offset: *offset, len: *len });
                }
                Pointer::BlobPending(pending_doc) => {
                    // pending_doc is Arc<HakoDoc>
                    let mut skeleton = (**pending_doc).clone();
                    if let Some(bm) = &self.blob_manager {
                        bm.extract_blobs_placeholder(&self.logical_name, &mut skeleton, self.blob_threshold);
                    }
                    ops.push(WalOp::PutInlined { key: key.clone(), value: skeleton.encode() });
                }
                Pointer::BlobPendingData { skeleton, .. } => {
                    // Transform worker finished. We already have the skeleton bytes.
                    ops.push(WalOp::PutInlined { key: key.clone(), value: skeleton.clone() });
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
            // PENDING: Serve directly from the Arc in memory (Fastest)
            Pointer::BlobPending(doc) => Ok(Some(doc.encode())),
            
            // Standard small documents stored directly in RAM.
            // ponytail: shared buffer — the query fast paths decode straight
            // from the Arc (no clone); this owned copy remains for callers
            // that must own their bytes (disk-spill paths, FFI snapshots).
            Pointer::Inlined(data) => Ok(Some((**data).clone())),
            
            // FINAL DISK STATE: Standard File/Mmap Read
            Pointer::Blob { offset, len } => {
                let bm = self.blob_manager.as_ref().unwrap();
                Ok(Some(bm.read_at(*offset, *len)?))
            },
            
            // PENDING REFINERY: Handle the intermediate state if used
            Pointer::BlobPendingData { data, .. } => Ok(Some((**data).clone())),

            // STANDARD SEGMENT DATA: (Handles its own compression/encryption)
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

    /// Shared-bytes read: `Inlined` / `BlobPendingData` hand back the live
    /// Arc (zero copies); every other variant does one owned read wrapped
    /// in an Arc. Backs raw scans — bytes are opaque storage encoding.
    pub(crate) fn read_pointer_shared(&self, pointer: &Pointer) -> Result<Option<Arc<Vec<u8>>>> {
        match pointer {
            Pointer::Inlined(shared) => Ok(Some(Arc::clone(shared))),
            Pointer::BlobPendingData { data, .. } => Ok(Some(Arc::clone(data))),
            Pointer::BlobPending(doc) => Ok(Some(Arc::new(doc.encode()))),
            Pointer::Deleted { .. } => Ok(None),
            other => Ok(self.read_pointer_internal(other, true)?.map(Arc::new)),
        }
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

    pub fn get(&self, key: &str) -> Result<Option<Vec<u8>>> {
        match self.index.get(key) {
            Some(Pointer::Deleted { .. }) => Ok(None), // Treat as non-existent
            Some(pointer) => self.read_pointer_internal(pointer, true),
            None => Ok(None),
        }
    }

    /// Shared-bytes point read: same lookup as [`Self::get`] but hands back
    /// the resident Arc (zero copies for inlined) instead of an owned Vec.
    /// Backs `DocView` point reads.
    pub(crate) fn get_shared(&self, key: &str) -> Result<Option<Arc<Vec<u8>>>> {
        match self.index.get(key) {
            Some(Pointer::Deleted { .. }) => Ok(None),
            Some(pointer) => self.read_pointer_shared(pointer),
            None => Ok(None),
        }
    }

    pub fn delete(&mut self, key: &str) -> Result<()> {
        let mutation = StorageMutation::Delete {
            key: key.to_string(),
        };

        // 1. Capture the work
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
        if prefix.is_empty() || prefix == self.logical_name {
            // ponytail: O(1) live count (maintained by update_index_entry,
            // seeded at open). Absent entry falls back to the scan rather
            // than lying — correctness over speed, always.
            if let Some(n) = self.collection_counts.get(&self.logical_name) {
                return *n;
            }
            return self.index.values()
                .filter(|p| !matches!(p, Pointer::Deleted { .. }))
                .count();
        }
        // Fallback for sub-collection support if needed
        self.index.iter()
            .filter(|(k, p)| k.starts_with(prefix) && !matches!(p, Pointer::Deleted { .. }))
            .count()
    }

    pub fn scan_prefix(&self, prefix: &str) -> Result<Vec<(String, Vec<u8>)>> {
        let mut out = Vec::new();

        for (key, pointer) in &self.index {
            // Skip deleted and filter by prefix if one is provided
            if matches!(pointer, Pointer::Deleted { .. }) { continue; }
            if !prefix.is_empty() && !key.starts_with(prefix) { continue; }

            if let Some(value) = self.read_pointer(pointer)? {
                out.push((key.clone(), value));
            }
        }

        Ok(out)
    }

    /// WAL-bloat check: true when the log file holds mostly stale history
    /// and a snapshot rewrite would actually reclaim. Heuristic, not exact:
    /// past the compaction threshold AND over ~3x live inlined bytes.
    /// Tombstones count as zero live, so a dead-only WAL always qualifies
    /// once past the threshold. `inlined_bytes` is maintained on every index
    /// write, so this is O(1) — safe on open and on every manual compact.
    /// 2-b: measures REAL record bytes (Wal::data_len), never the sparse
    /// file length — the 4-8MB zero reservation used to qualify tiny
    /// collections and fire rewrites that (pre-2-a) wiped them on SIGTERM.
    pub(crate) fn wal_snapshot_worthwhile(&self) -> bool {
        let data_len = self.wal.data_len();
        data_len > self.compaction_threshold_bytes as u64
            && data_len > (self.inlined_bytes as u64).saturating_mul(3)
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

        // ponytail: no segments to merge, but a hot-small collection can
        // still hold megabytes of stale WAL history (nothing ever spills).
        // Rewrite the snapshot when bloat dominates; otherwise a no-op.
        if immutable_ids.is_empty() {
            if self.wal_snapshot_worthwhile() {
                self.rewrite_wal_snapshot()?;
            }
            return Ok(());
        }

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
                level: 1, 
                segment: target 
            });
        for (k, p) in rebuilt_index_subset { self.index.insert(k, p); }
        
        self.rewrite_wal_snapshot()?;
        Ok(())
    }

    pub fn set_durability_mode(&mut self, mode: DurabilityMode) { self.wal.set_durability_mode(mode); }

    /// Live group-commit window (clamped inside the WAL call, same as open).
    pub fn set_group_commit_interval_ms(&mut self, ms: u64) { self.wal.set_group_commit_interval(ms); }

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
        self.index.iter()
            .filter(|(k, p)| {
                !matches!(p, Pointer::Deleted { .. }) && (prefix.is_empty() || k.starts_with(prefix))
            })
            .map(|(k, _)| k.clone())
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
                // ponytail: route through update_index_entry (not direct
                // index.insert) so sorted_keys and the live-count cache stay
                // in lockstep — the old direct inserts skipped both.
                WalOp::Put { key, segment_id, segment_offset, len } => {
                    self.update_index_entry(key.clone(), Some(Pointer::Segment {
                        segment_id: *segment_id, offset: *segment_offset, len: *len
                    }));
                }
                WalOp::PutInlined { key, value } => {
                    self.update_index_entry(key.clone(), Some(Pointer::Inlined(Arc::new(value.clone()))));
                }
                WalOp::Delete { key, timestamp } => {
                    self.update_index_entry(key.clone(), Some(Pointer::Deleted { timestamp: *timestamp }));
                }
                WalOp::PutBlob { key, offset, len } => {
                    self.update_index_entry(key.clone(), Some(Pointer::Blob { offset: *offset, len: *len }));
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
    use super::*;
    use crate::storage::wal::WalOp;

    fn tmp(label: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!("hako-eng-{label}-{nanos}-{}", std::process::id()))
    }

    fn cfg_for(mode: DurabilityMode) -> HakoConfig {
        HakoConfig {
            durability_mode: mode,
            wal_reserve_bytes: 0,
            auto_compaction_threshold_bytes: 1024,
            ..Default::default()
        }
    }

    /// 2-a: the open-time snapshot rewrite must hit disk, not just RAM.
    /// Builds stale history (10x overwrite, ~10KB WAL, 1KB live) so the
    /// rewrite fires on reopen, then asserts the file holds records
    /// immediately — pre-fix it was zeros until the next lucky append.
    #[test]
    fn open_rewrite_is_flushed() {
        let dir = tmp("rewrite");
        let big = vec![7u8; 1024];
        {
            let mut eng = StorageEngine::open(&dir, &cfg_for(DurabilityMode::Interval), "c".into(), None).unwrap();
            for _ in 0..10 {
                eng.put("k".into(), &big).unwrap();
            }
            // Drop flushes (Drop impl) — history lands, then reopen fires it.
        }
        let wal_path = dir.join("wal.log");
        assert!(std::fs::metadata(&wal_path).unwrap().len() > 4096, "history setup");
        {
            let eng = StorageEngine::open(&dir, &cfg_for(DurabilityMode::Interval), "c".into(), None).unwrap();
            // The rewrite fired on open (10KB history vs 1KB live) and
            // collapsed it: data_len is snapshot-sized, not history-sized.
            // Pre-fix the file was zeros here with data_len == 0 (staged).
            assert!(eng.wal.data_len() > 8, "snapshot flushed, data_len {}", eng.wal.data_len());
            assert!(eng.wal.data_len() < 4096, "history collapsed, data_len {}", eng.wal.data_len());
            assert_eq!(eng.get("k").unwrap().unwrap().len(), 1024, "live doc readable");
        }
        // No write happened after reopen: the file itself must hold records.
        let raw = std::fs::read(&wal_path).unwrap();
        assert!(raw.len() > 8, "snapshot flushed, not just staged");
        assert!(raw[0..8].iter().any(|&b| b != 0), "first header is a record, not zeros");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// 2-c: the maintenance tick flushes Interval buffers only.
    /// Manual never auto-flushes; OnCommit only at commit — both files
    /// must stay pristine while Interval lands.
    #[test]
    fn maintenance_flush_respects_mode() {
        for (mode, expect_bytes) in [
            (DurabilityMode::Interval, true),
            (DurabilityMode::Manual, false),
            (DurabilityMode::OnCommit, false),
        ] {
            let dir = tmp(&format!("maint-{mode:?}"));
            let mut eng = StorageEngine::open(&dir, &cfg_for(mode), "c".into(), None).unwrap();
            // One op, no commit marker, no thresholds: sits in the buffer.
            eng.wal
                .append(&WalOp::PutInlined { key: "k".into(), value: vec![1, 2, 3] }, false)
                .unwrap();
            // The tick predicate is time-aware (elapsed >= interval): sleep
            // past the 5ms test window so Interval is due. Monotonic clock
            // makes this one-directional (slowness only helps).
            std::thread::sleep(std::time::Duration::from_millis(10));
            eng.run_background_maintenance(false).unwrap();
            let len = std::fs::metadata(dir.join("wal.log")).unwrap().len();
            assert_eq!(len > 0, expect_bytes, "mode {mode:?}: file len {len}");
            let _ = std::fs::remove_dir_all(&dir);
        }
    }

    /// Configured interval gates flushing: a huge window holds appends in
    /// RAM (precondition for cluster fsync staggering — instances flush on
    /// their own cadence, not on every write).
    #[test]
    fn configured_interval_holds_and_releases() {
        let dir = tmp("interval");
        let mut cfg = HakoConfig {
            durability_mode: DurabilityMode::Interval,
            wal_reserve_bytes: 0,
            auto_compaction_threshold_bytes: 1024,
            group_commit_interval_ms: 30_000,
            ..Default::default()
        };
        // Clamp is honored at open (not just documented).
        cfg.group_commit_interval_ms = crate::config::clamp_group_commit_interval_ms(cfg.group_commit_interval_ms);
        let mut eng = StorageEngine::open(&dir, &cfg, "c".into(), None).unwrap();
        eng.put("k".into(), b"value-bytes").unwrap();
        assert!(eng.wal.has_pending(), "30s window must buffer, not flush");
        assert_eq!(eng.wal.data_len(), 0);
        eng.flush_all().unwrap();
        assert!(!eng.wal.has_pending());
        assert!(eng.wal.data_len() > 0, "explicit flush lands");
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Strict-time ignores the count trigger: 200 ops (> 128 legacy max)
    /// stay buffered inside a long window. This is the compromise that
    /// makes staggering work under burst (count-driven flushes would
    /// re-synchronize a balanced fleet).
    #[test]
    fn strict_time_ignores_count_trigger() {
        use crate::storage::wal::WalOp;

        let dir = tmp("strict-count");
        let cfg = HakoConfig {
            durability_mode: DurabilityMode::Interval,
            wal_reserve_bytes: 0,
            auto_compaction_threshold_bytes: 1024,
            group_commit_interval_ms: 30_000,
            ..Default::default()
        };
        let mut eng = StorageEngine::open(&dir, &cfg, "c".into(), None).unwrap();
        let ops: Vec<WalOp> = (0..200)
            .map(|i| WalOp::PutInlined { key: format!("k{i}"), value: vec![i as u8; 16] })
            .collect();
        eng.wal.append_batch_fast(1, &ops, false).unwrap();
        assert!(eng.wal.has_pending(), "strict: 200 ops must NOT trip count flush");
        assert_eq!(eng.wal.data_len(), 0);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
