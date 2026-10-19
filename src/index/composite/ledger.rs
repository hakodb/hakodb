//! Unique-index claims ledger (hakodb#71).
//!
//! The synchronous admission source of truth for unique composite
//! indexes. The BTreeMap tree is maintained by the async indexer worker,
//! which leaves a duplicate window between batches â€” so the write path
//! consults THIS dict instead. One Mutex serializes claim/release; the
//! hot path (no unique index on the collection) never touches it.
//!
//! Durability: the ledger is REBUILDABLE from storage â€” trees restore
//! from the RAM snapshot and the recovery fold refills claims (plus the
//! historical-duplicate check). WAL remains the sole durability source.
//!
//! ponytail: single global Mutex, one lock session per operation. A
//! contention hot spot would justify sharded ledgers (by index id) â€”
//! add when the profile says priority, 10k writes/s on one collection
//! needs ~Âµs-scale ops so this is fine for now.

use hashbrown::{HashMap, HashSet};
use smallvec::SmallVec;
use std::sync::{Arc, Mutex};

use crate::error::HakoError;

/// Encoded tuple prefix (index id + field encodings, no doc-id tail).
pub type ClaimPrefix = SmallVec<[u8; 32]>;

struct ClaimMap {
    /// prefix -> owning doc id
    fwd: HashMap<ClaimPrefix, Arc<str>>,
    /// doc id -> live prefix (O(1) release on delete / tuple switch)
    by_doc: HashMap<Arc<str>, ClaimPrefix>,
    /// Docs whose claim this process has RETRACTED since load (delete
    /// or tuple switch). A late recovery fold must not re-introduce
    /// claims for them (hakodb#71 BUG-1): the fold's tree can lag the
    /// live ledger because tree rows are removed asynchronously, and
    /// merge semantics cannot retract — only tombstones prevent a
    /// fold from reviving a released claim as a ghost.
    ///
    /// ponytail: bounded by the number of DISTINCT deleted doc ids per
    /// unique index; entries free on re-claim. Shrink Sawtooth-style if
    /// churn profiles show unbounded growth (add a low-water sweep).
    freed: HashSet<Arc<str>>,
}

impl Default for ClaimMap {
    fn default() -> Self {
        Self {
            fwd: HashMap::default(),
            by_doc: HashMap::default(),
            freed: HashSet::default(),
        }
    }
}

/// One mutation the write path made to the ledger during batch
/// preparation. Rollback replays in reverse order, which restores the
/// exact pre-batch state even across release/claim pairs of the same
/// tuple (release+claim interleavings invert correctly).
#[derive(Debug, Clone)]
pub(crate) enum LedgerUndo {
    /// A claim was inserted â€” undo removes it.
    Remove(u32, ClaimPrefix),
    /// A release happened â€” undo re-inserts the claim for `owner`.
    Restore(u32, ClaimPrefix, Arc<str>),
}

#[derive(Default)]
pub struct UniqueLedger {
    inner: Mutex<HashMap<u32, ClaimMap>>,
    /// Tombstones for releases that happened while the index had no
    /// claim set yet (delete racing the very first fold). Drained into
    /// the live map at fold/prepare time; never flips is_ready.
    pending: Mutex<HashMap<u32, HashSet<Arc<str>>>>,
}

impl UniqueLedger {
    /// True when `index_id` holds a ready (fully backfilled) claim set.
    pub fn is_ready(&self, index_id: u32) -> bool {
        self.inner.lock().unwrap().contains_key(&index_id)
    }

    /// Drop a claim set (index removal / failed creation rollback).
    /// Pending tombstones belong to the same lifecycle — drop them too
    /// (index id can be recycled by a later create).
    pub fn reset(&self, index_id: u32) {
        self.pending.lock().unwrap().remove(&index_id);
        self.inner.lock().unwrap().remove(&index_id);
    }

    /// Mark ready with no initial claims (fresh collection: nothing to
    /// enforce against yet).
    pub fn prepare_empty(&self, index_id: u32) {
        let mut pendings = self.pending.lock().unwrap();
        let shipped = pendings.remove(&index_id);
        let mut map = self.inner.lock().unwrap();
        let claims = map.entry(index_id).or_default();
        if let Some(set) = shipped {
            claims.freed.extend(set);
        }
    }

    /// Bulk-load claims from a backfilled tree (hakodb#71). MERGE
    /// semantics: existing claims survive (a live write's gate claim
    /// must not be wiped by a late backfill fold), absent ones get
    /// claimed if nobody holds them. Duplicate detection: the tree
    /// sorts by (prefix, doc-id), so two adjacent entries sharing a
    /// prefix collide — historical duplicates surface here. On violation
    /// nothing is merged; the caller refuses the index.
    pub fn load_from_tree(
        &self,
        index_id: u32,
        tree: &std::collections::BTreeMap<ClaimPrefix, Arc<str>>,
    ) -> Result<usize, HakoError> {
        // Tree keys = prefix + doc-id-tail per the key-encoder layout;
        // the tail rides the last (2 + len) bytes.
        let mut fresh: Vec<(ClaimPrefix, Arc<str>)> = Vec::new();
        // Pure scan first — nothing merges when a duplicate exists.
        let mut last: Option<(ClaimPrefix, Arc<str>)> = None;
        for (key, doc_id) in tree {
            // Tree key layout (key_encoder): [index_id 4B][tuple bytes]
            // [doc-id-len u16][doc-id bytes]. The value carries the
            // doc id, so the tail is exactly (2 + id.len()) bytes —
            // verify the self-check bytes before trusting the slice.
            let n = key.len();
            let idl = doc_id.len();
            if n < idl + 4
                || &key[n - idl..] != doc_id.as_bytes()
                || &key[(n - idl - 2)..(n - idl)]
                    != (idl as u16).to_be_bytes()
            {
                continue;
            }
            let prefix = ClaimPrefix::from_slice(&key[..n - idl - 2]);
            if let Some((last_prefix, last_doc)) = &last {
                if *last_prefix == prefix {
                    crate::engine::engine::record_unique_violation();
                    return Err(HakoError::UniqueViolation {
                        collection: String::new(),
                        index: index_id.to_string(),
                        key: last_doc.to_string(),
                    });
                }
            }
            fresh.push((prefix.clone(), doc_id.clone()));
            last = Some((prefix, doc_id.clone()));
        }
        let n = fresh.len();
        let mut pendings = self.pending.lock().unwrap();
        let shipped = pendings.remove(&index_id);
        let mut map = self.inner.lock().unwrap();
        let claims = map.entry(index_id).or_default();
        if let Some(set) = shipped {
            claims.freed.extend(set);
        }
        for (prefix, doc_id) in fresh {
            // Ghost-claim guards (BUG-1): a fold's tree can lag live
            // state (tree rows clear asynchronously), so the fold may
            // not overrule a release or a newer live claim.
            if claims.freed.contains(&doc_id) || claims.by_doc.contains_key(&doc_id) {
                continue;
            }
            match claims.fwd.entry(prefix.clone()) {
                hashbrown::hash_map::Entry::Vacant(v) => {
                    v.insert(doc_id.clone());
                    claims.by_doc.insert(doc_id, prefix);
                }
                hashbrown::hash_map::Entry::Occupied(o) => {
                    // Live write beat the fold to this tuple (or the
                    // fold ran twice). Same owner: nothing to do.
                    if o.get() != &doc_id {
                        crate::engine::engine::record_unique_violation();
                        return Err(HakoError::UniqueViolation {
                            collection: String::new(),
                            index: index_id.to_string(),
                            key: o.get().to_string(),
                        });
                    }
                }
            }
        }
        Ok(n)
    }

    /// Claim `prefix` for `doc_id`. Violation when another doc owns the
    /// prefix. Same-doc re-claim (update with unchanged tuple, the
    /// common put case) is a no-op. `undo` records the change for
    /// batch rollback.
    pub(crate) fn claim(
        &self,
        index_id: u32,
        prefix: ClaimPrefix,
        doc_id: &Arc<str>,
        undo: &mut Vec<LedgerUndo>,
    ) -> Result<(), HakoError> {
        let mut map = self.inner.lock().unwrap();
        let Some(claims) = map.get_mut(&index_id) else {
            // Not ready yet â€” caller checks is_ready first; this is the
            // fail-closed backstop.
            return Err(HakoError::IndexBackfilling {
                collection: String::new(),
                index: index_id.to_string(),
            });
        };
        if let Some(owner) = claims.fwd.get(&prefix) {
            if owner != doc_id {
                crate::engine::engine::record_unique_violation();
                return Err(HakoError::UniqueViolation {
                    collection: String::new(),
                    index: index_id.to_string(),
                    key: owner.to_string(),
                });
            }
            // Same owner, same tuple â€” nothing to record.
            return Ok(());
        }
        if let Some(old) = claims.by_doc.remove(doc_id) {
            claims.fwd.remove(&old);
            undo.push(LedgerUndo::Restore(index_id, old, doc_id.clone()));
        }
        claims.fwd.insert(prefix.clone(), doc_id.clone());
        claims.by_doc.insert(doc_id.clone(), prefix.clone());
        // A live claim retracts any tombstone: a later fold may safely
        // see this doc's rows again.
        claims.freed.remove(doc_id);
        undo.push(LedgerUndo::Remove(index_id, prefix));
        Ok(())
    }

    /// Release all claims owned by `doc_id` (delete path / tuple
    /// switch). Tombstones the doc either way — even when it held no
    /// claim or the ledger is not ready yet (pending shelf, drained at
    /// the next fold) — so no late recovery fold can revive its claim
    /// (BUG-1: folds' trees can lag the live ledger until the async
    /// worker clears the rows).
    pub(crate) fn release_doc(&self, index_id: u32, doc_id: &str, undo: &mut Vec<LedgerUndo>) {
        let id_arc: Arc<str> = Arc::from(doc_id);
        // Lock order: pending BEFORE inner (matches fold/prepare paths).
        let mut pendings = self.pending.lock().unwrap();
        let slot = pendings.entry(index_id).or_default();
        let mut map = self.inner.lock().unwrap();
        let Some(claims) = map.get_mut(&index_id) else {
            // Index not ready yet: shelf the tombstone for the fold.
            slot.insert(id_arc);
            return;
        };
        let prefix = claims.by_doc.remove(doc_id);
        if let Some(p) = &prefix {
            claims.fwd.remove(p);
            undo.push(LedgerUndo::Restore(index_id, p.clone(), id_arc.clone()));
        }
        claims.freed.insert(id_arc);
    }

    /// Replay a batch's ledger mutations in reverse, restoring the
    /// exact pre-batch state on any abort (violation, WAL failure).
    pub(crate) fn rollback(&self, undo: &[LedgerUndo]) {
        if undo.is_empty() {
            return;
        }
        let mut map = self.inner.lock().unwrap();
        for u in undo.iter().rev() {
            match u {
                LedgerUndo::Remove(index_id, prefix) => {
                    if let Some(claims) = map.get_mut(index_id) {
                        if let Some(owner) = claims.fwd.remove(prefix) {
                            claims.by_doc.remove(&owner);
                        }
                    }
                }
                LedgerUndo::Restore(index_id, prefix, owner) => {
                    let Some(claims) = map.get_mut(index_id) else { continue };
                    // The owner may meanwhile hold a different claim only
                    // when a mutation after this one rewrote it; reverse
                    // order means the later mutation was already undone.
                    claims.by_doc.remove(owner.as_ref());
                    claims.fwd.insert(prefix.clone(), owner.clone());
                    claims.by_doc.insert(owner.clone(), prefix.clone());
                    // The release this restore undoes had tombstoned the
                    // doc; resurrecting the claim un-tombstones it.
                    claims.freed.remove(owner);
                }
            }
        }
    }
}
