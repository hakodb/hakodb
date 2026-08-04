//! End-to-end test: room-scoped cloud sync.
//!
//! Verifies that the server stores collections as `<room>_<collection>`, that
//! clients of the same (room_name, room_key) pair share data, that a client
//! using the same room name with a different security key is isolated into a
//! separate room (`<room>_1`), and that the internal room registry persists.
//!
//! Run with: cargo test --features cloud-sync --test cloud_sync_rooms

#![cfg(feature = "cloud-sync")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use firelite::cloud_sync::{CloudSync, CloudSyncMode, RoomRegistry};
use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;

fn temp_db(tag: &str) -> (Arc<FireLite>, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "firelite-synctest-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::OnCommit;
    let db = Arc::new(FireLite::open(&dir, cfg).unwrap());
    (db, dir)
}

fn free_port() -> u16 {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    port
}

async fn wait_until(what: &str, mut cond: impl FnMut() -> bool, timeout_ms: u64) {
    let deadline = std::time::Instant::now() + Duration::from_millis(timeout_ms);
    while std::time::Instant::now() < deadline {
        if cond() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    panic!("timed out waiting for: {what}");
}

fn make_doc(name: &str, owner: &str) -> FireLiteDoc {
    let mut doc = FireLiteDoc::default();
    doc.insert("name", Value::String(name.to_string()));
    doc.insert("owner", Value::String(owner.to_string()));
    doc
}

#[tokio::test]
async fn rooms_are_isolated_and_prefixed_on_server() {
    let port = free_port();
    let addr = format!("127.0.0.1:{}", port);

    let (srv_db, srv_dir) = temp_db("server");
    let server = CloudSync::new(
        srv_db.clone(),
        CloudSyncMode::Server,
        "srv",
        "",
        "srv-key",
        "tok",
    );
    server.start(&addr).await.unwrap();

    // Client A: room (alpha, k1)
    let (db_a, dir_a) = temp_db("client_a");
    let ca = CloudSync::new(db_a.clone(), CloudSyncMode::Client, "a", "alpha", "k1", "tok");
    ca.start(&format!("ws://{}", addr)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    // A writes a document; it must land on the server under "alpha_users".
    db_a.put("users", "doc1", &make_doc("alice", "alpha")).unwrap();
    wait_until(
        "server alpha_users/doc1",
        || srv_db.get("alpha_users", "doc1").unwrap().is_some(),
        6000,
    )
    .await;

    // Client B: same room (alpha, k1) must receive doc1 via catch-up.
    let (db_b, dir_b) = temp_db("client_b");
    let cb = CloudSync::new(db_b.clone(), CloudSyncMode::Client, "b", "alpha", "k1", "tok");
    cb.start(&format!("ws://{}", addr)).await.unwrap();
    wait_until(
        "client B receives doc1",
        || db_b.get("users", "doc1").unwrap().is_some(),
        8000,
    )
    .await;

    // Client C: same room NAME, different security key -> separate room.
    let (db_c, dir_c) = temp_db("client_c");
    let cc = CloudSync::new(db_c.clone(), CloudSyncMode::Client, "c", "alpha", "k2", "tok");
    cc.start(&format!("ws://{}", addr)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        db_c.get("users", "doc1").unwrap().is_none(),
        "client C (different key) must not see room alpha/k1 data"
    );

    // C writes its own doc -> server stores it under "alpha_1_users".
    db_c.put("users", "cdoc", &make_doc("carol", "alpha-k2"))
        .unwrap();
    wait_until(
        "server alpha_1_users/cdoc",
        || srv_db.get("alpha_1_users", "cdoc").unwrap().is_some(),
        6000,
    )
    .await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        db_b.get("users", "cdoc").unwrap().is_none(),
        "client B must not receive data from a different-key room"
    );

    // Server disk layout is room-prefixed.
    let cols = srv_db.list_collections().unwrap();
    assert!(
        cols.contains(&"alpha_users".to_string()),
        "expected alpha_users in server collections: {:?}",
        cols
    );
    assert!(
        cols.contains(&"alpha_1_users".to_string()),
        "expected alpha_1_users in server collections: {:?}",
        cols
    );

    // The internal registry resolves both rooms to the correct prefixes.
    let reg = RoomRegistry::new(srv_db.clone());
    let (_, p1) = reg.resolve("alpha", "k1").unwrap();
    let (_, p2) = reg.resolve("alpha", "k2").unwrap();
    assert_eq!(p1, "alpha");
    assert_eq!(p2, "alpha_1");

    server.stop();
    ca.stop();
    cb.stop();
    cc.stop();

    for d in [srv_dir, dir_a, dir_b, dir_c] {
        let _ = std::fs::remove_dir_all(d);
    }
}
