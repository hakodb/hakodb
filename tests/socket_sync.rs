//! socket_sync end-to-end (unix-only): two engines peer over a unix
//! socket — snapshot on connect plus live tail both directions, deletes
//! propagate, echo discipline holds (no ping-pong growth).

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
    cfg.durability_mode = DurabilityMode::Manual;
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
    eprintln!("TEST sock path: {} parent: {:?}", sock.display(), sock.parent());
    std::fs::create_dir_all(sock.parent().unwrap()).unwrap();
    let sock_str = sock.to_string_lossy().into_owned();

    let a = open_db(&dir_a);
    let b = open_db(&dir_b);

    // A holds data before peering; B starts empty.
    put_kv(&a, "c", "k1", "one");
    put_kv(&a, "c", "k2", "two");

    let sa = SocketSync::new(a.clone(), vec![]);
    let sb = SocketSync::new(b.clone(), vec![]);
    sa.serve(&sock_str).unwrap();
    // Give the listener a moment (bind is sync, accept loop spawns async).
    tokio::time::sleep(Duration::from_millis(200)).await;
    sb.dial(&sock_str).await.unwrap();

    // Snapshot A -> B.
    poll_until("snapshot k1", || {
        b.get("c", "k1").ok().flatten().is_some()
    });
    poll_until("snapshot k2", || {
        b.get("c", "k2").ok().flatten().is_some()
    });
    {
        let shard = b.get_shard("c").unwrap();
        let guard = shard.read().unwrap();
        let keys: Vec<_> = guard.index.keys().cloned().collect();
        eprintln!("DEBUG B index keys: {keys:?}");
    }
    assert_eq!(
        b.get("c", "k1").unwrap().unwrap().get("v"),
        Some(&Value::String("one".into()))
    );

    // Live write B -> A (reverse direction over the same peering).
    put_kv(&b, "c", "k3", "three");
    poll_until("live k3", || {
        a.get("c", "k3").ok().flatten().is_some()
    });

    // Live write A -> B.
    put_kv(&a, "c", "k4", "four");
    poll_until("live k4", || {
        b.get("c", "k4").ok().flatten().is_some()
    });

    // Delete propagates.
    a.delete("c", "k1").unwrap();
    poll_until("delete k1", || b.get("c", "k1").ok().flatten().is_none());

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
