//! End-to-end test: room-scoped cloud sync.
//!
//! Verifies that the server stores collections as `<room>_<collection>`, that
//! clients of the same (room_name, room_key) pair share data, that a client
//! using the same room name with a different security key is isolated into a
//! separate room (`<room>_1`), that the internal room registry persists, that
//! two clients sharing a client_id in different rooms never clobber each other,
//! and that a client's data stays on the server it chose to sync with.
//!
//! Run with: cargo test --features cloud-sync --test cloud_sync_rooms

#![cfg(feature = "cloud-sync")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hakodb::cloud_sync::{CloudSync, CloudSyncMode, RoomRegistry};
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;

fn temp_db(tag: &str) -> (Arc<Hako>, PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "hakodb-synctest-{}-{}",
        std::process::id(),
        tag
    ));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::OnCommit;
    let db = Arc::new(Hako::open(&dir, cfg).unwrap());
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

fn make_doc(name: &str, owner: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
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

#[tokio::test]
async fn same_client_id_different_rooms_stay_isolated() {
    let port = free_port();
    let addr = format!("127.0.0.1:{}", port);

    let (srv_db, srv_dir) = temp_db("server_dup");
    let server = CloudSync::server(srv_db.clone(), "srv", "tok");
    server.start(&addr).await.unwrap();

    // Two clients share the SAME client_id but join different rooms. The server
    // must route sync per-room: the second connection must never clobber the
    // first one's peer entry.
    let (db_dup1, dir_dup1) = temp_db("dup1");
    let c1 = CloudSync::client(db_dup1.clone(), "dup", "alpha", "k1", "tok");
    c1.start(&format!("ws://{}", addr)).await.unwrap();

    let (db_dup2, dir_dup2) = temp_db("dup2");
    let c2 = CloudSync::client(db_dup2.clone(), "dup", "alpha", "k2", "tok");
    c2.start(&format!("ws://{}", addr)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    // Each client writes into its own room.
    db_dup1.put("users", "doc1", &make_doc("one", "alpha-k1"))
        .unwrap();
    db_dup2.put("users", "doc2", &make_doc("two", "alpha-k2"))
        .unwrap();
    wait_until(
        "server alpha_users/doc1",
        || srv_db.get("alpha_users", "doc1").unwrap().is_some(),
        6000,
    )
    .await;
    wait_until(
        "server alpha_1_users/doc2",
        || srv_db.get("alpha_1_users", "doc2").unwrap().is_some(),
        6000,
    )
    .await;

    // Server-side writes must reach the CORRECT room member even though both
    // clients share client_id "dup".
    srv_db.put("alpha_users", "srv1", &make_doc("s1", "server"))
        .unwrap();
    srv_db.put("alpha_1_users", "srv2", &make_doc("s2", "server"))
        .unwrap();

    wait_until(
        "dup1 receives srv1",
        || db_dup1.get("users", "srv1").unwrap().is_some(),
        8000,
    )
    .await;
    wait_until(
        "dup2 receives srv2",
        || db_dup2.get("users", "srv2").unwrap().is_some(),
        8000,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(
        db_dup1.get("users", "srv2").unwrap().is_none(),
        "dup1 must not receive room alpha/k2 server writes"
    );
    assert!(
        db_dup2.get("users", "srv1").unwrap().is_none(),
        "dup2 must not receive room alpha/k1 server writes"
    );
    assert!(
        db_dup1.get("users", "doc2").unwrap().is_none(),
        "dup1 must not receive room alpha/k2 client writes"
    );
    assert!(
        db_dup2.get("users", "doc1").unwrap().is_none(),
        "dup2 must not receive room alpha/k1 client writes"
    );

    server.stop();
    c1.stop();
    c2.stop();

    for d in [srv_dir, dir_dup1, dir_dup2] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test]
async fn clients_choose_which_server_to_sync_to() {
    let port1 = free_port();
    let port2 = free_port();
    let addr1 = format!("127.0.0.1:{}", port1);
    let addr2 = format!("127.0.0.1:{}", port2);

    let (db1, dir1) = temp_db("server_a");
    let s1 = CloudSync::server(db1.clone(), "srvA", "tok");
    s1.start(&addr1).await.unwrap();

    let (db2, dir2) = temp_db("server_b");
    let s2 = CloudSync::server(db2.clone(), "srvB", "tok");
    s2.start(&addr2).await.unwrap();

    // The client decides BOTH the room and the server it syncs with.
    let (c_db, c_dir) = temp_db("client_x");
    let cx = CloudSync::client(c_db.clone(), "x", "gamma", "k1", "tok");
    cx.start(&format!("ws://{}", addr1)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    c_db.put("users", "doc1", &make_doc("xavier", "gamma"))
        .unwrap();
    wait_until(
        "server A gamma_users/doc1",
        || db1.get("gamma_users", "doc1").unwrap().is_some(),
        6000,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1200)).await;

    // The OTHER server never saw the data.
    let cols2 = db2.list_collections().unwrap();
    assert!(
        !cols2.contains(&"gamma_users".to_string()),
        "server B must not receive room gamma data: {:?}",
        cols2
    );

    // A client on server B, same room, receives nothing from server A.
    let (d_db, d_dir) = temp_db("client_y");
    let cy = CloudSync::client(d_db.clone(), "y", "gamma", "k1", "tok");
    cy.start(&format!("ws://{}", addr2)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        d_db.get("users", "doc1").unwrap().is_none(),
        "client on server B must not see data synced to server A"
    );

    s1.stop();
    s2.stop();
    cx.stop();
    cy.stop();

    for d in [dir1, dir2, c_dir, d_dir] {
        let _ = std::fs::remove_dir_all(d);
    }
}
