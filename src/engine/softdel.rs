//! Soft-delete retention (#177): restoreable deletes.
//!
//! A soft delete writes the tombstone exactly like the legacy path and,
//! in the same batch, one small put into the `__hako_softdel` system
//! collection: key = `<col>/<id>`, value = a record doc carrying the
//! original encoded bytes, the delete stamp, and the scheduled sweep
//! time. Records are ordinary persisted docs, so restore survives
//! restart (no RAM-only map, no WAL-history dependency). The record put
//! travels as a normal put over sync, so replicas hold the retention
//! window too and sweep on their own clock.
//!
//! Sweeping is a storage-level pass on the existing maintenance tick:
//! due records are purged (retention ended) or forgotten (the live
//! tombstone slid away via a re-put/restore, so the record is inert).
//! Sweeps never enter the user write path.

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::engine::engine::{BatchMutation, Hako};
use crate::error::{HakoError, Result};
use crate::storage::engine::{Pointer, StorageEngine};
use hashbrown::{HashMap, HashSet};
use serde::Serialize;
use std::sync::{Arc, RwLock};

pub(crate) const SOFTDEL_COLLECTION: &str = "__hako_softdel";

/// Sweep batch size (~1000 hard deletes/txn, the Bulk Upd/Del lane).
pub(crate) const SWEEP_CHUNK: usize = 1000;

/// Record id inside `__hako_softdel`: `<col>/<id>`. Collection names are
/// gated slash-free, so the first `/` always splits the pair; ids may
/// contain anything (including further slashes).
pub(crate) fn record_id(col: &str, id: &str) -> String {
    format!("{col}/{id}")
}

/// One pending-deletion record (listing view: no payload).
#[derive(Debug, Clone, Serialize)]
pub struct SoftDelEntry {
    pub col: String,
    pub id: String,
    pub deleted_at: i64,
    pub scheduled_at: i64,
    pub bytes: usize,
}

/// Build the retention record for a soft-deleted doc. `payload` = the
/// doc's encoded bytes as read at delete time.
pub(crate) fn build_record(
    col: &str,
    id: &str,
    deleted_at: i64,
    scheduled_at: i64,
    payload: Vec<u8>,
) -> HakoDoc {
    let mut rec = HakoDoc::default();
    rec.insert("col", Value::String(col.to_string()));
    rec.insert("id", Value::String(id.to_string()));
    rec.insert("deleted_at", Value::Int(deleted_at));
    rec.insert("scheduled_at", Value::Int(scheduled_at));
    rec.insert("payload", Value::Binary(payload));
    rec._time = deleted_at;
    rec
}

impl Hako {
    /// Read the to-be-retained payload for a soft delete. Never creates
    /// a shard (a delete against an unknown collection has nothing to
    /// retain). Returns `(record_id, record_doc)`.
    pub(crate) fn softdel_record(
        &self,
        col: &str,
        doc_id: &str,
        now_micros: i64,
    ) -> Option<(String, HakoDoc)> {
        if self.config.soft_delete_retention_secs == 0 {
            return None;
        }
        let shard_arc = {
            let map = self.shards.read().unwrap();
            map.get(col).cloned()?
        };
        let bytes = {
            let guard = shard_arc.read().unwrap();
            guard.get(doc_id).ok().flatten()?
        };
        let scheduled_at =
            now_micros + (self.config.soft_delete_retention_secs as i64) * 1_000_000;
        Some((
            record_id(col, doc_id),
            build_record(col, doc_id, now_micros, scheduled_at, bytes),
        ))
    }

    /// Restoreable records (all collections, or one). Payload bytes are
    /// counted, not returned - read them via [`Hako::restore`].
    pub fn restore_available(&self, col: Option<&str>) -> Result<Vec<SoftDelEntry>> {
        Ok(self
            .softdel_entries(col)?
            .into_iter()
            .map(|(_, e)| e)
            .collect())
    }

    /// Due-for-sweep subset of the queue (what the next sweep acts on).
    pub fn delete_queue(&self, col: Option<&str>) -> Result<Vec<SoftDelEntry>> {
        let now = crate::util::clock::now_micros();
        Ok(self
            .softdel_entries(col)?
            .into_iter()
            .filter(|(_, e)| e.scheduled_at <= now)
            .map(|(_, e)| e)
            .collect())
    }

    /// Restore a soft-deleted doc from its retention record: the payload
    /// is put back as a fresh version, the record is then hard-removed.
    /// Refuses when the live tombstone no longer matches the record's
    /// stamp (the cancel rule: a re-put owns the id).
    pub fn restore(&self, col: &str, id: &str) -> Result<()> {
        let key = record_id(col, id);
        let rec = self.get(SOFTDEL_COLLECTION, &key)?.ok_or_else(|| {
            HakoError::InvalidInput(format!("restore: no retention record for {col}/{id}"))
        })?;
        let payload = match rec.get("payload") {
            Some(Value::Binary(b)) => b.clone(),
            _ => {
                return Err(HakoError::Corrupt(format!(
                    "restore: record {key} has no payload"
                )))
            }
        };
        let deleted_at = match rec.get("deleted_at") {
            Some(Value::Int(ts)) => *ts,
            _ => {
                return Err(HakoError::Corrupt(format!(
                    "restore: record {key} has no stamp"
                )))
            }
        };
        let doc = HakoDoc::decode(&payload).ok_or_else(|| {
            HakoError::Corrupt(format!("restore: record {key} payload undecodable"))
        })?;

        let shard = self.get_shard(col)?;
        let restorable = {
            let guard = shard.read().unwrap();
            matches!(
                guard.index.get(id),
                Some(Pointer::Deleted { timestamp }) if *timestamp == deleted_at
            )
        };
        if !restorable {
            return Err(HakoError::InvalidInput(
                "restore: doc is live or the tombstone moved (cancel rule)".into(),
            ));
        }
        self.put_owned(col, id, doc)?;
        self.write_batch(vec![BatchMutation::Delete {
            collection: SOFTDEL_COLLECTION.to_string(),
            doc_id: key,
        }])?;
        Ok(())
    }

    /// Run one sweep pass now (tests + operators). Returns the number of
    /// records purged. Never touches user-visible state: the tombstone
    /// clock (24h purge) is untouched.
    pub fn sweep_now(&self) -> Result<u64> {
        // Touch-load: recovery discovery skips `_`-prefixed dirs, so the
        // retention shard may not be in the map yet.
        let _ = self.get_shard(SOFTDEL_COLLECTION)?;
        let map = self.shards.read().unwrap();
        Ok(sweep_pass(
            &map,
            crate::util::clock::now_micros(),
            self.config.soft_delete_max_bytes,
            self.config.soft_delete_max_count,
            SWEEP_CHUNK,
        ))
    }

    /// Decode every record in `__hako_softdel` (listing helper).
    fn softdel_entries(&self, col: Option<&str>) -> Result<Vec<(String, SoftDelEntry)>> {
        // Touch-load (see sweep_now): recovery never pre-loads this shard.
        let soft = self.get_shard(SOFTDEL_COLLECTION)?;
        let guard = soft.read().unwrap();
        let mut out = Vec::new();
        for (key, ptr) in guard.index.iter() {
            if let Some(entry) = decode_record(&guard, key, ptr) {
                if col.map(|c| c == entry.col).unwrap_or(true) {
                    out.push((key.clone(), entry));
                }
            }
        }
        Ok(out)
    }
}

/// Decode one record doc from a shard pointer (non-deleted only).
fn decode_record(storage: &StorageEngine, _key: &str, ptr: &Pointer) -> Option<SoftDelEntry> {
    if matches!(ptr, Pointer::Deleted { .. }) {
        return None;
    }
    let bytes = storage.read_pointer_internal(ptr, false).ok()??;
    let doc = HakoDoc::decode(&bytes)?;
    let (Value::String(col), Value::String(id)) = (doc.get("col")?, doc.get("id")?) else {
        return None;
    };
    let (Value::Int(deleted_at), Value::Int(scheduled_at)) =
        (doc.get("deleted_at")?, doc.get("scheduled_at")?)
    else {
        return None;
    };
    let Value::Binary(payload) = doc.get("payload")? else {
        return None;
    };
    Some(SoftDelEntry {
        col: col.clone(),
        id: id.clone(),
        deleted_at: *deleted_at,
        scheduled_at: *scheduled_at,
        bytes: payload.len(),
    })
}

/// One sweep pass over `__hako_softdel` (storage-level; no user-visible
/// state changes). Order: evict over-cap oldest-first (logged), then
/// forget everything due - a due record whose tombstone still matches
/// ends its retention (content is already just a tombstone), and a due
/// record whose tombstone slid (re-put/restore owns the id) is inert.
/// Returns the number of records removed.
pub(crate) fn sweep_pass(
    shards: &HashMap<String, Arc<RwLock<StorageEngine>>>,
    now_micros: i64,
    max_bytes: u64,
    max_count: u64,
    chunk: usize,
) -> u64 {
    // Key + stamps + record-doc framing, excluding the payload itself.
    const REC_OVERHEAD: u64 = 96;
    let Some(soft) = shards.get(SOFTDEL_COLLECTION) else {
        return 0;
    };

    let mut records: Vec<(String, SoftDelEntry)> = Vec::new();
    {
        let guard = soft.read().unwrap();
        for (key, ptr) in guard.index.iter() {
            if let Some(entry) = decode_record(&guard, key, ptr) {
                records.push((key.clone(), entry));
            }
        }
    }
    if records.is_empty() {
        return 0;
    }

    // Retention budget: evict oldest-first over the caps (documented,
    // not silent - one line per evicting pass).
    records.sort_by_key(|(_, e)| e.deleted_at);
    let mut evict: HashSet<String> = HashSet::new();
    let mut total: u64 = 0;
    let mut count: u64 = 0;
    for (key, e) in &records {
        total += e.bytes as u64 + REC_OVERHEAD;
        count += 1;
        if total > max_bytes || count > max_count {
            evict.insert(key.clone());
        }
    }
    if !evict.is_empty() {
        eprintln!(
            "hako softdel: retention cap hit - evicting {} oldest record(s)",
            evict.len()
        );
    }

    let mut forget: Vec<String> = evict.iter().cloned().collect();
    for (key, e) in &records {
        if e.scheduled_at > now_micros || evict.contains(key) {
            continue;
        }
        forget.push(key.clone());
    }

    let mut purged = 0u64;
    for chunk_keys in forget.chunks(chunk.max(1)) {
        let mut storage = soft.write().unwrap();
        match storage.hard_delete_keys(chunk_keys) {
            Ok(n) => purged += n,
            Err(_) => break, // next pass retries; never wedge the tick
        }
    }
    purged
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::HakoConfig;

    fn tmp(label: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "hako-softdel-{label}-{}",
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir
    }

    fn doc(a: i64) -> HakoDoc {
        let mut d = HakoDoc::default();
        d.insert("a", Value::Int(a));
        d
    }

    fn day_ahead() -> i64 {
        crate::util::clock::now_micros() + 25 * 3600 * 1_000_000
    }

    #[test]
    fn soft_delete_visibility_and_record() {
        let dir = tmp("vis");
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        db.put("t", "1", &doc(42)).unwrap();
        db.delete("t", "1").unwrap();
        assert!(db.get("t", "1").unwrap().is_none());
        let avail = db.restore_available(None).unwrap();
        assert_eq!(avail.len(), 1);
        assert_eq!(avail[0].col, "t");
        assert_eq!(avail[0].id, "1");
        // Not due yet: the queue stays empty until the window ends, and
        // an early sweep changes nothing.
        assert!(db.delete_queue(None).unwrap().is_empty());
        {
            let map = db.shards.read().unwrap();
            assert_eq!(
                sweep_pass(&map, crate::util::clock::now_micros(), u64::MAX, u64::MAX, SWEEP_CHUNK),
                0
            );
        }
        assert_eq!(db.restore_available(None).unwrap().len(), 1);
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restore_roundtrip_and_cancel_rule() {
        let dir = tmp("round");
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        db.put("t", "1", &doc(42)).unwrap();
        db.delete("t", "1").unwrap();

        // Cancel rule: a re-put owns the id; restore refuses.
        db.put("t", "1", &doc(7)).unwrap();
        assert!(db.restore("t", "1").is_err());
        match db.get("t", "1").unwrap().unwrap().get("a") {
            Some(Value::Int(7)) => {}
            other => panic!("unexpected value {other:?}"),
        }
        // Swept-forget: once due, the stale record leaves; live doc untouched.
        {
            let map = db.shards.read().unwrap();
            assert_eq!(sweep_pass(&map, day_ahead(), u64::MAX, u64::MAX, SWEEP_CHUNK), 1);
        }
        assert!(db.get("t", "1").unwrap().is_some());
        assert!(db.restore_available(None).unwrap().is_empty());

        // Restore roundtrip on a fresh soft delete.
        db.delete("t", "1").unwrap();
        db.restore("t", "1").unwrap();
        match db.get("t", "1").unwrap().unwrap().get("a") {
            Some(Value::Int(7)) => {}
            other => panic!("unexpected value {other:?}"),
        }
        assert!(db.restore_available(None).unwrap().is_empty());
        assert!(db.restore("t", "1").is_err());
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn retention_zero_is_force() {
        let dir = tmp("zero");
        let cfg = HakoConfig { soft_delete_retention_secs: 0, ..Default::default() };
        let db = Hako::open(&dir, cfg).unwrap();
        db.put("t", "1", &doc(1)).unwrap();
        db.delete("t", "1").unwrap();
        assert!(db.get("t", "1").unwrap().is_none());
        assert!(db.restore_available(None).unwrap().is_empty());
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn cap_evicts_oldest_first() {
        let dir = tmp("cap");
        let cfg = HakoConfig { soft_delete_max_bytes: 1, ..Default::default() };
        let db = Hako::open(&dir, cfg).unwrap();
        for i in 0..4 {
            db.put("t", &i.to_string(), &doc(i)).unwrap();
            db.delete("t", &i.to_string()).unwrap();
        }
        assert_eq!(db.restore_available(None).unwrap().len(), 4);
        {
            let map = db.shards.read().unwrap();
            assert_eq!(sweep_pass(&map, day_ahead(), 1, u64::MAX, SWEEP_CHUNK), 4);
        }
        assert!(db.restore_available(None).unwrap().is_empty());
        // Eviction forgets retention, never resurrects.
        for i in 0..4 {
            assert!(db.get("t", &i.to_string()).unwrap().is_none());
        }
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn restart_preserves_records() {
        let dir = tmp("restart");
        {
            let db = Hako::open(&dir, HakoConfig::default()).unwrap();
            db.put("t", "1", &doc(42)).unwrap();
            db.delete("t", "1").unwrap();
        }
        let db = Hako::open(&dir, HakoConfig::default()).unwrap();
        assert_eq!(db.restore_available(None).unwrap().len(), 1);
        db.restore("t", "1").unwrap();
        assert!(db.get("t", "1").unwrap().is_some());
        drop(db);
        let _ = std::fs::remove_dir_all(&dir);
    }
}
