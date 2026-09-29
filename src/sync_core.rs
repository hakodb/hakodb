//! sync_core: transport-independent sync primitives (0.9.0).
//!
//! `net_sync` (TCP mesh) and `cloud_sync` (WS star) duplicated this logic
//! op-by-op; `socket_sync` is the third consumer. Anything here must stay
//! free of transport, timers, and I/O — pure functions over WAL ops plus
//! the shared echo-cache discipline. Behavioral changes here ripple to
//! every topology, so keep it boring and tested.
//!
//! Deliberately NOT extracted (yet): the tail loops themselves (offset
//! bookkeeping differs: DB-persisted vs in-memory), blob inflation
//! (resolve paths differ), framing (bincode vs msgpack vs length-prefix).

use std::collections::HashMap;
use std::sync::{Arc, Mutex, RwLock};
use std::sync::atomic::Ordering;

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::engine::engine::IndexOp;
use crate::engine::{ChangeEvent, ChangeKind, Hako};
use crate::storage::blob::BlobWork;
use crate::storage::engine::{Pointer, StorageEngine};
use crate::storage::wal::WalOp;

/// Logical timestamp carried by a WAL op: the `_time` prefix of inlined
/// values (Version 3 layout: magic(1) ver(1) time(8)) or the delete marker.
/// Anything else (tx markers, segment/blob pointers, short/corrupt values)
/// carries none → 0 and is never echo-matched. One deliberate deviation
/// from the three inlined copies it replaces: they sliced `value[2..10]`
/// directly (panic on a short value, killing the tailer thread); this
/// falls through to 0 instead — the op still replicates, only the echo
/// match is skipped. Fail-open on corrupt tails, never thread death.
pub fn op_logical_time(op: &WalOp) -> i64 {
    match op {
        WalOp::PutInlined { value, .. } => value
            .get(2..10)
            .and_then(|s| s.try_into().ok())
            .map(i64::from_le_bytes)
            .unwrap_or(0),
        WalOp::Delete { timestamp, .. } => *timestamp,
        _ => 0,
    }
}

/// Echo discipline, shared: an op we just ingested from a peer must not
/// bounce straight back out. Check-and-REMOVE on hit (a lingering entry
/// would swallow a later legitimate same-timestamp write). The caller
/// computes the cache key (raw key on mesh, room-prefixed on cloud) —
/// the take itself is identical everywhere.
pub fn echo_take(cache: &Mutex<HashMap<String, i64>>, key: &str, ts: i64) -> bool {
    let mut cache = cache.lock().unwrap();
    if let Some(&cached_ts) = cache.get(key) {
        if cached_ts == ts {
            cache.remove(key);
            return true;
        }
    }
    false
}

/// Shared ingest: apply one batch of replicated ops (any transport) with
/// LWW conflict resolution, echo-cache insert, WAL commit (remote: skip
/// fsync), index + watcher notification. Ported verbatim from the mesh
/// ingest — `net_sync` calls this now, `socket_sync` reuses it, cloud
/// keeps its own (different batching/flush architecture, out of scope).
/// Transport differences live ABOVE (framing) and BELOW (none) this fn.
pub async fn apply_replicated_batch(
    db: Arc<Hako>,
    collection: String,
    ops: Vec<WalOp>,
    echo_cache: Arc<Mutex<HashMap<String, i64>>>,
) {
    // Local-only signal (inbound): a locally-scoped collection refuses
    // everything offered. Per-key marks do NOT filter inbound — a
    // genuinely newer remote put still resurrects (documented rule).
    if db.is_collection_local(&collection) {
        return;
    }
    let shard_arc = match db.get_shard(&collection) {
        Ok(s) => s,
        Err(e) => {
            // Serious: data arrived but the local shard is unreadable.
            eprintln!("[sync] CRITICAL: Cannot apply replication to {}. Shard error: {}", collection, e);
            return;
        }
    };
    let threshold = db.config.value_blob_threshold_bytes;

    let mut accepted_ops = Vec::new();
    let mut affected_keys = Vec::new();
    let mut index_puts = Vec::new();
    let mut blob_work_items = Vec::new();

    // 1. PHASE 1: PREPARE AND CONFLICT RESOLUTION
    {
        // Read lock first to check timestamps (LWW).
        let shard_read = shard_arc.read().unwrap();
        let echo_cache_clone = echo_cache.clone();

        for op in ops {
            let (key, mut doc, is_delete, remote_ts) = match op {
                WalOp::PutInlined { ref key, ref value } => {
                    if let Some(d) = HakoDoc::decode(value) {
                        let ts = d.get_logical_time();
                        (key.clone(), d, false, ts)
                    } else { continue; }
                }
                WalOp::Delete { ref key, timestamp } => {
                    (key.clone(), HakoDoc::default(), true, timestamp)
                }
                _ => continue,
            };

            // LWW: only apply when strictly newer than local.
            if let Some(local_ptr) = shard_read.index.get(&key) {
                let local_ts = match local_ptr {
                    Pointer::Deleted { timestamp } => *timestamp,
                    Pointer::Inlined(bytes) => i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0; 8])),
                    _ => {
                        shard_read.read_pointer_internal(local_ptr, false)
                            .ok().flatten()
                            .and_then(|b| HakoDoc::decode(&b))
                            .map(|d| d.get_logical_time())
                            .unwrap_or(0)
                    }
                };
                if remote_ts <= local_ts { continue; }
            }

            if is_delete {
                {
                    let mut cache = echo_cache_clone.lock().unwrap();
                    cache.insert(key.clone(), remote_ts);
                }
                accepted_ops.push(WalOp::Delete { key: key.clone(), timestamp: remote_ts });
                index_puts.push((key.clone(), None)); // None signals delete below
                // Deletes bump versions too: Hako::get serves from doc_cache
                // on version match, so an unbumped delete leaves the live
                // doc cached FOREVER (stale reads after replicated deletes).
                affected_keys.push(key.into());
            } else {
                let ts = doc.get_logical_time();
                {
                    let mut cache = echo_cache_clone.lock().unwrap();
                    cache.insert(key.clone(), ts);
                }
                // RE-EXTRACT BLOBS: sender sent a full doc but it's large —
                // extract locally on the receiver to save segment space.
                if let Some(bm) = &shard_read.blob_manager {
                    let extracted = bm.extract_blobs_raw(&collection, &key, &mut doc, threshold);
                    for b in extracted {
                        blob_work_items.push(b);
                    }
                }

                let skeleton_bytes = doc.encode();
                accepted_ops.push(WalOp::PutInlined { key: key.clone(), value: skeleton_bytes });
                index_puts.push((key.clone(), Some(doc)));
                affected_keys.push(key.into());
            }
        }
    } // Read lock dropped.

    if accepted_ops.is_empty() { return; }

    // 2. PHASE 2: PHYSICAL COMMIT (receiver shard).
    {
        let mut shard = shard_arc.write().unwrap();

        // A. WAL commit (remote: skip fsync).
        let tx_id = shard.next_tx_id;
        shard.next_tx_id += 1;
        let _ = shard.wal.append_batch_fast(tx_id, &accepted_ops, true);

        // B. Index update.
        for (key, doc_opt) in index_puts {
            if let Some(doc) = doc_opt {
                // Blob-extracted docs stay pending.
                let has_blob = blob_work_items.iter().any(|b| {
                    if let BlobWork::PutRaw { key: k, .. } = b { k == &key } else { false }
                });

                if has_blob {
                    shard.update_index_entry(key, Some(Pointer::BlobPending(Arc::new(doc))));
                } else {
                    shard.update_index_entry(key, Some(Pointer::Inlined(Arc::new(doc.encode()))));
                }
            } else {
                // Delete: find its timestamp from the accepted batch.
                let ts = accepted_ops.iter().find_map(|o| {
                    if let WalOp::Delete { key: k, timestamp } = o {
                        if k == &key { return Some(*timestamp); }
                    }
                    None
                }).unwrap_or(0);
                shard.update_index_entry(key, Some(Pointer::Deleted { timestamp: ts }));
            }
        }

        // C. Queue blobs for the receiver's blob worker.
        let mut total_bytes = 0;
        for b in blob_work_items {
            if let BlobWork::PutRaw { len, .. } = &b { total_bytes += *len as usize; }
            shard.blob_flush_queue.push_back(b);
        }
        shard.total_pending_blob_bytes.fetch_add(total_bytes, Ordering::Relaxed);

        // Wake the receiver's blob worker.
        db.trigger_blob_flush.store(true, Ordering::Release);
    }

    // 3. PHASE 3: NOTIFY LOCAL SYSTEM.
    db.bump_versions_by_keys(affected_keys);

    // 4. PHASE 4: hand to the indexer.
    let index_docs: Vec<(String, Arc<HakoDoc>)> = accepted_ops.iter().filter_map(|op| {
        if let WalOp::PutInlined { key, value } = op {
            // Naked ID from "col:id" form, else the key as-is.
            let doc_id = key.split_once(':')
                .map(|(_, id)| id.to_string())
                .unwrap_or_else(|| key.clone());

            // Decode and wrap in Arc immediately.
            HakoDoc::decode(value).map(|d| (doc_id, Arc::new(d)))
        } else {
            None
        }
    }).collect();

    if !index_docs.is_empty() {
        // Send the batch to the persistent index worker.
        let _ = db.index_tx.send(IndexOp::Update {
            collection: collection.clone(),
            puts: Arc::new(index_docs),
            deletes: vec![],
        });
    }

    // 5. PHASE 5: notify watchers (the tailer skips re-broadcast:
    // key/timestamp sits in the echo cache now).
    for op in &accepted_ops {
        let kind = match op {
            WalOp::PutInlined { .. } => ChangeKind::Put,
            WalOp::Delete { .. } => ChangeKind::Delete,
            _ => continue,
        };

        let event = ChangeEvent {
            path: Arc::from(op.get_key()),
            kind,
        };

        db.notify_watchers(&collection, event);
    }
}

/// Send-side blob inflation, shared: turn any WAL op into full wire bytes.
/// Segment/blob pointers resolve from disk; skeleton docs with BlobLinks
/// inflate via the blob manager (receivers can't use our file offsets).
/// Returns None when the bytes can't be materialized (caller skips, except
/// deletes which pass through). Ported from the mesh resolver — `net_sync`
/// calls this now, `socket_sync` reuses it.
pub fn resolve_send_bytes(
    shard_arc: &Arc<RwLock<StorageEngine>>,
    db: &Arc<Hako>,
    op: &WalOp,
) -> Option<Vec<u8>> {
    // 1. Raw bytes (skeleton) from the WAL op.
    let bytes = match op {
        WalOp::PutInlined { value, .. } => value.clone(),
        WalOp::Put { segment_id, segment_offset, len, .. } => {
            let ptr = Pointer::Segment { segment_id: *segment_id, offset: *segment_offset, len: *len };
            shard_arc.read().unwrap().read_pointer_internal(&ptr, false).ok().flatten()?
        }
        WalOp::PutBlob { offset, len, .. } => {
            let ptr = Pointer::Blob { offset: *offset, len: *len };
            shard_arc.read().unwrap().read_pointer_internal(&ptr, false).ok().flatten()?
        }
        _ => return None,
    };

    // 2. Decode to check for BlobLinks.
    if let Some(mut doc) = HakoDoc::decode(&bytes) {
        let has_links = doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }));

        if has_links {
            // INFLATE: file offsets become real bytes on the wire.
            let encryption_key = db.config.encryption_key.as_deref();
            if crate::engine::engine::resolve_doc_static(&mut doc, shard_arc, encryption_key).is_ok() {
                return Some(doc.encode_buffered());
            }
            return None;
        }
    }

    Some(bytes)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn op_time_decode_matches_tailers() {
        // PutInlined: magic, ver, LE time.
        let mut v = vec![3u8, 3u8];
        v.extend_from_slice(&12345678i64.to_le_bytes());
        v.extend_from_slice(&[9u8; 4]);
        let op = WalOp::PutInlined { key: "k".into(), value: v };
        assert_eq!(op_logical_time(&op), 12345678);
        // Short values fall through to 0 (never echo-matched).
        let op = WalOp::PutInlined { key: "k".into(), value: vec![1, 2, 3] };
        assert_eq!(op_logical_time(&op), 0);
        // Deletes carry their marker.
        let op = WalOp::Delete { key: "k".into(), timestamp: 999 };
        assert_eq!(op_logical_time(&op), 999);
        // Markers carry nothing.
        let op = WalOp::BeginTx { tx_id: 7 };
        assert_eq!(op_logical_time(&op), 0);
    }

    #[test]
    fn echo_take_removes_on_hit_only() {
        let cache = Mutex::new(HashMap::from([("k".to_string(), 42i64)]));
        assert!(echo_take(&cache, "k", 42));
        // Removed: second take misses.
        assert!(!echo_take(&cache, "k", 42));
        // Wrong timestamp: kept, misses.
        cache.lock().unwrap().insert("j".into(), 7);
        assert!(!echo_take(&cache, "j", 8));
        assert!(cache.lock().unwrap().contains_key("j"));
        // Unknown key: miss.
        assert!(!echo_take(&cache, "?", 0));
    }

    /// Shared ingest contract (guards net_sync's rewire + socket_sync):
    /// stale remote loses, newer wins, deletes tombstone, echo fills.
    #[tokio::test]
    async fn ingest_lww_delete_echo() {
        use crate::config::{DurabilityMode, HakoConfig};
        use crate::document::value::Value;

        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let dir = std::env::temp_dir().join(format!("hako-ingest-{nanos}"));
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = std::sync::Arc::new(Hako::open(&dir, cfg).expect("open"));

        fn doc_with_time(v: &str, ts: i64) -> Vec<u8> {
            let mut d = HakoDoc::default();
            d.insert("v", Value::String(v.into()));
            d._time = ts;
            d.encode()
        }

        // Local baseline via the normal path.
        let mut local = HakoDoc::default();
        local.insert("v", Value::String("local".into()));
        db.put_owned("c", "k", local).expect("put");
        let t0 = db.get("c", "k").expect("get").expect("doc").get_logical_time();

        let echo = Arc::new(Mutex::new(HashMap::new()));

        // Stale remote put loses (older than local).
        apply_replicated_batch(
            db.clone(),
            "c".into(),
            vec![WalOp::PutInlined { key: "k".into(), value: doc_with_time("stale", 1) }],
            echo.clone(),
        )
        .await;
        assert_eq!(db.get("c", "k").expect("get2").expect("doc2").get("v"), Some(&Value::String("local".into())));

        // Newer remote put wins.
        apply_replicated_batch(
            db.clone(),
            "c".into(),
            vec![WalOp::PutInlined { key: "k".into(), value: doc_with_time("new", i64::MAX) }],
            echo.clone(),
        )
        .await;
        let doc = db.get("c", "k").expect("get3").expect("doc3");
        assert_eq!(doc.get("v"), Some(&Value::String("new".into())));
        // Winner lands in the echo cache (tailers won't rebroadcast it).
        assert!(echo.lock().unwrap().contains_key("k"));

        // Stale delete loses to the live newer doc.
        apply_replicated_batch(
            db.clone(),
            "c".into(),
            vec![WalOp::Delete { key: "k".into(), timestamp: t0 }],
            echo.clone(),
        )
        .await;
        assert!(db.get("c", "k").expect("get4").is_some());

        // Newer delete tombstones (fresh key: local time is real, delete is MAX).
        let mut local2 = HakoDoc::default();
        local2.insert("v", Value::String("two".into()));
        db.put_owned("c", "k2", local2).expect("put2");
        apply_replicated_batch(
            db.clone(),
            "c".into(),
            vec![WalOp::Delete { key: "k2".into(), timestamp: i64::MAX }],
            echo.clone(),
        )
        .await;
        assert!(db.get("c", "k2").expect("get5").is_none());

        std::fs::remove_dir_all(&dir).ok();
    }
}
