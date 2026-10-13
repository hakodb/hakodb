//! socket_sync end-to-end (unix-only): two engines peer over a unix
//! socket — snapshot on connect plus live tail both directions, deletes
//! propagate, echo discipline holds (no ping-pong growth).
//!
//! Interval durability (not Manual): the live tailer reads the WAL *file*,
//! and only flushed bytes are visible there. Interval flushes on every
//! append past the 5ms group-commit window, so live phases are
//! deterministic under the 10s poll budget below.

#![cfg(all(unix, feature = "socket-sync"))]

use std::sync::Arc;
use std::time::Duration;

use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::socket_sync::SocketSync;

fn tmp(label: &str) -> std::path::PathBuf {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    std::env::temp_dir().join(format!("hako-sock-{label}-{nanos}-{}", std::process::id()))
}

fn open_db(dir: &std::path::Path) -> Arc<Hako> {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Interval;
    Arc::new(Hako::open(dir, cfg).unwrap())
}

fn put_kv(db: &Hako, col: &str, id: &str, v: &str) {
    let mut d = HakoDoc::default();
    d.insert("v", Value::String(v.into()));
    db.put_owned(col, id, d).unwrap();
}

async fn poll_until(label: &str, mut f: impl FnMut() -> bool) {
    for _ in 0..100 {
        if f() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timeout waiting for {label}");
}

#[tokio::test]
async fn socket_snapshot_and_live_both_directions() {
    let dir_a = tmp("a");
    let dir_b = tmp("b");
    let sock = tmp("sock").join("s.sock");
    std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
    let sock_str = sock.to_string_lossy().into_owned();

    let a = open_db(&dir_a);
    let b = open_db(&dir_b);

    // A holds data before peering; B starts empty. Flush so the file
    // tailer (not just the snapshot path) observes a deterministic state.
    put_kv(&a, "c", "k1", "one");
    put_kv(&a, "c", "k2", "two");
    a.flush().unwrap();

    let sa = SocketSync::new(a.clone(), vec![]);
    let sb = SocketSync::new(b.clone(), vec![]);
    sa.serve(&sock_str).unwrap();
    // Give the listener a moment (bind is sync, accept loop spawns async).
    tokio::time::sleep(Duration::from_millis(200)).await;
    sb.dial(&sock_str).await.unwrap();

    // Snapshot A -> B.
    poll_until("snapshot k1", || {
        b.get("c", "k1").ok().flatten().is_some()
    })
    .await;
    poll_until("snapshot k2", || {
        b.get("c", "k2").ok().flatten().is_some()
    })
    .await;
    poll_until("snapshot k2", || {
        b.get("c", "k2").ok().flatten().is_some()
    })
    .await;
    assert_eq!(
        b.get("c", "k1").unwrap().unwrap().get("v"),
        Some(&Value::String("one".into()))
    );

    // Live write B -> A (reverse direction over the same peering).
    put_kv(&b, "c", "k3", "three");
    b.flush().unwrap();
    poll_until("live k3", || {
        a.get("c", "k3").ok().flatten().is_some()
    })
    .await;

    // Live write A -> B.
    put_kv(&a, "c", "k4", "four");
    a.flush().unwrap();
    poll_until("live k4", || {
        b.get("c", "k4").ok().flatten().is_some()
    })
    .await;

    // Delete propagates (flushed: the live tailer reads the WAL file,
    // so the delete must be on disk, not just in the write buffer —
    // same visibility rule as production Interval mode).
    a.delete("c", "k1").unwrap();
    a.flush().unwrap();
    poll_until("delete k1", || {
        b.get("c", "k1").ok().flatten().is_none()
    })
    .await;

    // Echo discipline: no ping-pong duplicates (each key exactly once).
    let count_a = a.get("c", "k4").unwrap().is_some() as u8;
    let count_b = b.get("c", "k4").unwrap().is_some() as u8;
    assert_eq!((count_a, count_b), (1, 1));

    sa.stop();
    sb.stop();
    // Cleanup must never panic (a sync bug must fail in the phases above,
    // not here hiding as an unwrap).
    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
    if let Some(parent) = sock.parent() {
        std::fs::remove_dir_all(parent).ok();
    }
}

/// Issue #51: index defs converge over the socket plane (IndexDefs packet
/// after Hello). A creates a vector index; B dials; B's def appears and
/// vectors written on B afterwards search correctly.
#[tokio::test]
async fn socket_index_defs_converge_vector() {
    use hakodb::index::vector::{encode_f32s, Metric};

    let dir_a = tmp("defs-a");
    let dir_b = tmp("defs-b");
    let sock = tmp("defs-sock").join("s.sock");
    std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
    let sock_str = sock.to_string_lossy().into_owned();

    let a = open_db(&dir_a);
    let b = open_db(&dir_b);
    a.create_vector_index("pts", "emb", 2, Metric::Cosine).unwrap();

    let sa = SocketSync::new(a.clone(), vec![]);
    let sb = SocketSync::new(b.clone(), vec![]);
    sa.serve(&sock_str).unwrap();
    tokio::time::sleep(Duration::from_millis(200)).await;
    sb.dial(&sock_str).await.unwrap();

    poll_until("vector def on b", || {
        b.list_indexes(Some("pts"))
            .vector
            .get("pts")
            .is_some_and(|v| v.iter().any(|d| d.field == "emb" && d.dim == 2))
    })
    .await;

    for (id, p) in [("a", [1.0f32, 0.0]), ("b", [0.0, 1.0]), ("c", [1.0, 1.0])] {
        let mut doc = HakoDoc::default();
        doc.insert("emb", Value::Binary(encode_f32s(&p)));
        b.put("pts", id, &doc).unwrap();
    }
    assert!(b.await_quiescent(Duration::from_secs(60)));
    let hits = b.find_near("pts", "emb", &[1.0, 0.5], 2).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, "c");

    sa.stop();
    sb.stop();
    std::fs::remove_dir_all(&dir_a).ok();
    std::fs::remove_dir_all(&dir_b).ok();
    if let Some(parent) = sock.parent() {
        std::fs::remove_dir_all(parent).ok();
    }
}
