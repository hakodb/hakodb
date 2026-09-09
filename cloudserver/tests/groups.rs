//! Group management endpoint tests (admin-gated, key shown once).

use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::engine::FireLite;
use firelite_cloudserver::app::{build_router, AppState};
use firelite_cloudserver::auth::{upsert_user, Role};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_db() -> (FireLite, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-cs-groups-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    (FireLite::open(&dir, cfg).unwrap(), dir)
}

struct Resp {
    code: u16,
    body: serde_json::Value,
}

async fn req(
    addr: &std::net::SocketAddr,
    method: &str,
    path: &str,
    json: Option<&str>,
    cookie: Option<&str>,
) -> Resp {
    let (code, _, body) = raw_with_cookie(addr, method, path, json, cookie).await;
    Resp { code, body }
}

async fn raw_with_cookie(
    addr: &std::net::SocketAddr,
    method: &str,
    path: &str,
    json: Option<&str>,
    cookie: Option<&str>,
) -> (u16, Option<String>, serde_json::Value) {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let mut r = format!("{method} {path} HTTP/1.1\r\nHost: x\r\n");
    if let Some(j) = json {
        r.push_str("Content-Type: application/json\r\n");
        r.push_str(&format!("Content-Length: {}\r\n", j.len()));
    }
    if let Some(c) = cookie {
        r.push_str(&format!("Cookie: {c}\r\n"));
    }
    r.push_str("\r\n");
    if let Some(j) = json {
        r.push_str(j);
    }
    sock.write_all(r.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = sock.read(&mut tmp).await.unwrap();
        if n == 0 {
            break;
        }
        buf.extend_from_slice(&tmp[..n]);
        if let Some(hend) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&buf[..hend]).into_owned();
            let len: usize = header
                .lines()
                .find_map(|l| {
                    l.strip_prefix("content-length:")
                        .or_else(|| l.strip_prefix("Content-Length:"))
                        .map(|v| v.trim().parse().unwrap_or(0))
                })
                .unwrap_or(0);
            if buf.len() >= hend + 4 + len {
                break;
            }
        }
    }
    let text = String::from_utf8_lossy(&buf).into_owned();
    let code: u16 = text
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let set_cookie = text.lines().find_map(|l| {
        l.strip_prefix("set-cookie:")
            .or_else(|| l.strip_prefix("Set-Cookie:"))
            .map(|v| v.trim().split(';').next().unwrap_or("").to_string())
    });
    let body: serde_json::Value = serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap_or("{}"))
        .unwrap_or(serde_json::Value::Null);
    (code, set_cookie, body)
}

async fn admin_cookie(addr: &std::net::SocketAddr) -> String {
    let (_, set_cookie, _) = raw_with_cookie(
        addr,
        "POST",
        "/api/setup",
        Some(r#"{"username":"root","password":"root-password"}"#),
        None,
    )
    .await;
    set_cookie.expect("setup sets cookie")
}

#[tokio::test]
async fn group_crud_and_key_shown_once() {
    let (db, dir) = temp_db();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, build_router(Arc::new(AppState::new(Arc::new(db), false))))
            .await
            .unwrap();
    });
    let cookie = admin_cookie(&addr).await;

    // Create registered group: plaintext key returned exactly once.
    let r = req(
        &addr,
        "POST",
        "/api/groups",
        Some(r#"{"room_name":"game","mode":"registered"}"#),
        Some(&cookie),
    )
    .await;
    assert_eq!(r.code, 201, "create: {}", r.body);
    let key1 = r.body["api_key"].as_str().unwrap().to_string();
    assert_eq!(key1.len(), 64);

    // Duplicate create conflicts.
    let r = req(
        &addr,
        "POST",
        "/api/groups",
        Some(r#"{"room_name":"game","mode":"open"}"#),
        Some(&cookie),
    )
    .await;
    assert_eq!(r.code, 409);

    // List/get never expose key material.
    let r = req(&addr, "GET", "/api/groups", None, Some(&cookie)).await;
    assert_eq!(r.code, 200);
    assert_eq!(r.body["groups"][0]["room_name"], "game");
    assert!(r.body.to_string().find(&key1).is_none(), "key leaked in list");
    let r = req(&addr, "GET", "/api/groups/game", None, Some(&cookie)).await;
    assert_eq!(r.code, 200);
    assert_eq!(r.body["group"]["has_key"], true);
    assert!(r.body.to_string().find(&key1).is_none(), "key leaked in get");

    // Rotate replaces the key.
    let r = req(
        &addr,
        "POST",
        "/api/groups/game/rotate-key",
        None,
        Some(&cookie),
    )
    .await;
    assert_eq!(r.code, 200);
    let key2 = r.body["api_key"].as_str().unwrap().to_string();
    assert_ne!(key1, key2);

    // Members round-trip.
    let r = req(
        &addr,
        "POST",
        "/api/groups/game/members",
        Some(r#"{"client_id":"alice"}"#),
        Some(&cookie),
    )
    .await;
    assert_eq!(r.code, 200);
    assert_eq!(r.body["group"]["members"][0], "alice");
    let r = req(
        &addr,
        "DELETE",
        "/api/groups/game/members/alice",
        None,
        Some(&cookie),
    )
    .await;
    assert_eq!(r.code, 200);
    assert!(r.body["group"]["members"].as_array().unwrap().is_empty());

    // Delete group.
    let r = req(&addr, "DELETE", "/api/groups/game", None, Some(&cookie)).await;
    assert_eq!(r.code, 200);
    let r = req(&addr, "GET", "/api/groups/game", None, Some(&cookie)).await;
    assert_eq!(r.code, 404);

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn group_endpoints_require_admin_and_auth() {
    let (db, dir) = temp_db();
    upsert_user(&db, "op", "operator-pw", Role::Operator, false).unwrap();
    upsert_user(&db, "root", "root-password", Role::Admin, false).unwrap();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, build_router(Arc::new(AppState::new(Arc::new(db), false))))
            .await
            .unwrap();
    });

    // Anonymous rejected.
    let r = req(
        &addr,
        "POST",
        "/api/groups",
        Some(r#"{"room_name":"g","mode":"open"}"#),
        None,
    )
    .await;
    assert_eq!(r.code, 401);

    // Operator (non-admin) rejected.
    let (_, set_cookie, _) = raw_with_cookie(
        &addr,
        "POST",
        "/api/login",
        Some(r#"{"username":"op","password":"operator-pw"}"#),
        None,
    )
    .await;
    let op_cookie = set_cookie.unwrap();
    let r = req(
        &addr,
        "POST",
        "/api/groups",
        Some(r#"{"room_name":"g","mode":"open"}"#),
        Some(&op_cookie),
    )
    .await;
    assert_eq!(r.code, 403);

    // Admin passes; open group has no key.
    let (_, set_cookie, _) = raw_with_cookie(
        &addr,
        "POST",
        "/api/login",
        Some(r#"{"username":"root","password":"root-password"}"#),
        None,
    )
    .await;
    let root_cookie = set_cookie.unwrap();
    let r = req(
        &addr,
        "POST",
        "/api/groups",
        Some(r#"{"room_name":"g","mode":"open"}"#),
        Some(&root_cookie),
    )
    .await;
    assert_eq!(r.code, 201);
    assert!(r.body["api_key"].is_null());

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}
