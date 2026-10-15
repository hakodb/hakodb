//! End-to-end test: room-scoped cloud sync.
//!
//! Single-room cloud sync: the server is bound to one (room_name,
//! room_key) pair at construction; matching clients sync both ways with
//! collections stored under plain names, and wrong-room clients are
//! rejected at handshake before any data moves. No prefixes, no
//! registry, no multi-room fallback.
//!
//! Run with: cargo test --features cloud-sync --test cloud_sync_rooms

#![cfg(feature = "cloud-sync")]

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use hakodb::cloud_sync::{CloudSync, CloudSyncMode};
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
async fn room_match_syncs_plain_on_server() {
    let port = free_port();
    let addr = format!("127.0.0.1:{}", port);

    // Server bound to room (alpha, k1).
    let (srv_db, srv_dir) = temp_db("server");
    let server = CloudSync::server(srv_db.clone(), "srv", "tok", "alpha", "k1");
    server.start(&addr).await.unwrap();

    // Client A: matching room — writes land on the server under plain names.
    let (db_a, dir_a) = temp_db("client_a");
    let ca = CloudSync::new(db_a.clone(), CloudSyncMode::Client, "a", "alpha", "k1", "tok");
    ca.start(&format!("ws://{}", addr)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    db_a.put("users", "doc1", &make_doc("alice", "alpha")).unwrap();
    wait_until(
        "server users/doc1 (plain)",
        || srv_db.get("users", "doc1").unwrap().is_some(),
        6000,
    )
    .await;

    // Client B: same room — receives doc1 via catch-up.
    let (db_b, dir_b) = temp_db("client_b");
    let cb = CloudSync::new(db_b.clone(), CloudSyncMode::Client, "b", "alpha", "k1", "tok");
    cb.start(&format!("ws://{}", addr)).await.unwrap();
    wait_until(
        "client B receives doc1",
        || db_b.get("users", "doc1").unwrap().is_some(),
        8000,
    )
    .await;

    // Client C: same room NAME, different key — rejected at handshake.
    // Nothing flows either way, and nothing is stored for it.
    let (db_c, dir_c) = temp_db("client_c");
    let cc = CloudSync::new(db_c.clone(), CloudSyncMode::Client, "c", "alpha", "k2", "tok");
    cc.start(&format!("ws://{}", addr)).await.unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        db_c.get("users", "doc1").unwrap().is_none(),
        "client C (wrong key) must not see room data"
    );

    db_c.put("users", "cdoc", &make_doc("carol", "alpha-k2"))
        .unwrap();
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(
        srv_db.get("users", "cdoc").unwrap().is_none(),
        "server must not store data from a rejected room"
    );
    assert!(
        db_b.get("users", "cdoc").unwrap().is_none(),
        "client B must not receive wrong-room data"
    );
    assert!(
        db_a.get("users", "cdoc").unwrap().is_none(),
        "client A must not receive wrong-room data"
    );

    // Server disk layout is plain (no prefixes anywhere).
    let cols = srv_db.list_collections().unwrap();
    assert!(
        cols.contains(&"users".to_string()),
        "expected plain users in server collections: {:?}",
        cols
    );
    assert!(
        !cols.iter().any(|c| c.contains("alpha_")),
        "no prefixed collections may exist: {:?}",
        cols
    );

    server.stop();
    ca.stop();
    cb.stop();
    cc.stop();

    for d in [srv_dir, dir_a, dir_b, dir_c] {
        let _ = std::fs::remove_dir_all(d);
    }
}

/// Issue #51: index defs converge over the cloud plane with room-prefix
/// translation. A creates a vector index, the server lands it on
/// `alpha_pts`, and late-joining B (same room) converges bare `pts` from
/// the server reply — then vectors written on B search correctly.
#[tokio::test]
async fn cloud_index_defs_converge_vector() {
    use hakodb::index::vector::{encode_f32s, Metric};

    let port = free_port();
    let addr = format!("127.0.0.1:{}", port);

    let (srv_db, srv_dir) = temp_db("defs-server");
    let server = CloudSync::server(srv_db.clone(), "srv", "tok", "alpha", "k1");
    server.start(&addr).await.unwrap();

    let (db_a, dir_a) = temp_db("defs-a");
    db_a.create_vector_index("pts", "emb", 2, Metric::Cosine).unwrap();
    db_a.create_traverse_index("pts").unwrap();
    let ca = CloudSync::new(db_a.clone(), CloudSyncMode::Client, "a", "alpha", "k1", "tok");
    ca.start(&format!("ws://{}", addr)).await.unwrap();

    // Server converges the def on its own database (plain names)...
    wait_until("server pts def", || {
        srv_db
            .list_indexes(Some("pts"))
            .vector
            .get("pts")
            .is_some_and(|v| v.iter().any(|d| d.field == "emb" && d.dim == 2))
    }, 8000)
    .await;

    // ...and late-joining B converges the bare def from the reply.
    let (db_b, dir_b) = temp_db("defs-b");
    let cb = CloudSync::new(db_b.clone(), CloudSyncMode::Client, "b", "alpha", "k1", "tok");
    cb.start(&format!("ws://{}", addr)).await.unwrap();
    wait_until("client B pts def", || {
        db_b
            .list_indexes(Some("pts"))
            .vector
            .get("pts")
            .is_some_and(|v| v.iter().any(|d| d.field == "emb" && d.dim == 2))
    }, 8000)
    .await;
    wait_until("client B pts traverse", || {
        db_b.list_indexes(Some("pts")).traverse.contains(&"pts".to_string())
    }, 8000)
    .await;

    for (id, p) in [("a", [1.0f32, 0.0]), ("b", [0.0, 1.0]), ("c", [1.0, 1.0])] {
        let mut doc = HakoDoc::default();
        doc.insert("emb", Value::Binary(encode_f32s(&p)));
        db_b.put("pts", id, &doc).unwrap();
    }
    assert!(db_b.await_quiescent(Duration::from_secs(60)));
    let hits = db_b.find_near("pts", "emb", &[1.0, 0.5], 2).unwrap();
    assert_eq!(hits.len(), 2);
    assert_eq!(hits[0].0, "c");

    server.stop();
    ca.stop();
    cb.stop();
    for d in [srv_dir, dir_a, dir_b] {
        let _ = std::fs::remove_dir_all(d);
    }
}

#[tokio::test]
async fn same_client_id_reconnect_replaces() {
    let port = free_port();
    let addr = format!("127.0.0.1:{}", port);

    let (srv_db, srv_dir) = temp_db("server_dup");
    let server = CloudSync::server(srv_db.clone(), "srv", "tok", "alpha", "k1");
    server.start(&addr).await.unwrap();

    // Two connections share the SAME client_id: the second replaces the
    // first's peer entry (one room per server — no room suffix to
    // disambiguate by). Sync keeps working through the live connection.
    let (db_dup1, dir_dup1) = temp_db("dup1");
    let c1 = CloudSync::client(db_dup1.clone(), "dup", "alpha", "k1", "tok");
    c1.start(&format!("ws://{}", addr)).await.unwrap();

    let (db_dup2, dir_dup2) = temp_db("dup2");
    let c2 = CloudSync::client(db_dup2.clone(), "dup", "alpha", "k1", "tok");
    c2.start(&format!("ws://{}", addr)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;
    assert_eq!(
        server.status().active_clients,
        1,
        "one peer entry per client id"
    );

    // Each client writes; both land on the server under plain names.
    db_dup1.put("users", "doc1", &make_doc("one", "alpha"))
        .unwrap();
    db_dup2.put("users", "doc2", &make_doc("two", "alpha"))
        .unwrap();
    wait_until(
        "server users/doc1",
        || srv_db.get("users", "doc1").unwrap().is_some(),
        6000,
    )
    .await;
    wait_until(
        "server users/doc2",
        || srv_db.get("users", "doc2").unwrap().is_some(),
        6000,
    )
    .await;

    // Server-side writes reach the live connection.
    srv_db.put("users", "srv1", &make_doc("s1", "server"))
        .unwrap();
    wait_until(
        "live client receives srv1",
        || {
            db_dup1.get("users", "srv1").unwrap().is_some()
                || db_dup2.get("users", "srv1").unwrap().is_some()
        },
        8000,
    )
    .await;

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
    let s1 = CloudSync::server(db1.clone(), "srvA", "tok", "gamma", "k1");
    s1.start(&addr1).await.unwrap();

    let (db2, dir2) = temp_db("server_b");
    let s2 = CloudSync::server(db2.clone(), "srvB", "tok", "delta", "k9");
    s2.start(&addr2).await.unwrap();

    // The client decides BOTH the room and the server it syncs with.
    let (c_db, c_dir) = temp_db("client_x");
    let cx = CloudSync::client(c_db.clone(), "x", "gamma", "k1", "tok");
    cx.start(&format!("ws://{}", addr1)).await.unwrap();

    tokio::time::sleep(Duration::from_millis(800)).await;

    c_db.put("users", "doc1", &make_doc("xavier", "gamma"))
        .unwrap();
    wait_until(
        "server A users/doc1 (plain)",
        || db1.get("users", "doc1").unwrap().is_some(),
        6000,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(1200)).await;

    // The OTHER server never saw the data.
    let cols2 = db2.list_collections().unwrap();
    assert!(
        !cols2.contains(&"users".to_string()),
        "server B must not receive room gamma data: {:?}",
        cols2
    );

    // A client on server B, same room name but B serves another room:
    // handshake rejects it, so it sees nothing from server A.
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
