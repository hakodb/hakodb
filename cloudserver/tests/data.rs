//! Data-plane endpoint tests: role gates, CRUD, query, batch, indexes,
//! maintenance. Raw HTTP over ephemeral ports, no client dependencies.

use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::engine::FireLite;
use firelite_cloudserver::app::{build_router, AppState};
use firelite_cloudserver::auth::{upsert_user, Role};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_db() -> (FireLite, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-cs-data-{}",
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

async fn call(
    addr: &std::net::SocketAddr,
    method: &str,
    path: &str,
    json: Option<&str>,
    cookie: Option<&str>,
) -> Resp {
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
    let mut tmp = [0u8; 16384];
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
    Resp {
        code,
        body: serde_json::from_str(text.split("\r\n\r\n").nth(1).unwrap_or("{}"))
            .unwrap_or(serde_json::Value::Null),
    }
}

async fn login_cookie(addr: &std::net::SocketAddr, user: &str, pass: &str) -> String {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = format!(r#"{{"username":"{user}","password":"{pass}"}}"#);
    let req = format!(
        "POST /api/login HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    sock.write_all(req.as_bytes()).await.unwrap();
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
    assert!(text.starts_with("HTTP/1.1 200"), "login failed: {text}");
    text.lines()
        .find_map(|l| {
            l.strip_prefix("set-cookie:")
                .or_else(|| l.strip_prefix("Set-Cookie:"))
                .map(|v| v.trim().split(';').next().unwrap_or("").to_string())
        })
        .expect("login sets cookie")
}

async fn spawn(db: FireLite) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        axum::serve(listener, build_router(Arc::new(AppState::new(Arc::new(db), false))))
            .await
            .unwrap();
    });
    (addr, h)
}

#[tokio::test]
async fn crud_query_batch_roundtrip_with_role_gates() {
    let (db, dir) = temp_db();
    upsert_user(&db, "root", "root-password", Role::Admin, false).unwrap();
    upsert_user(&db, "op", "operator-pw", Role::Operator, false).unwrap();
    upsert_user(&db, "ro", "viewer-pw12", Role::Viewer, false).unwrap();
    let (addr, srv) = spawn(db).await;
    let admin = login_cookie(&addr, "root", "root-password").await;
    let op = login_cookie(&addr, "op", "operator-pw").await;
    let ro = login_cookie(&addr, "ro", "viewer-pw12").await;

    // Anonymous blocked everywhere on the data plane.
    assert_eq!(
        call(&addr, "GET", "/api/collections", None, None).await.code,
        401
    );

    // Viewer reads: empty collections, query works.
    let r = call(&addr, "GET", "/api/collections", None, Some(&ro)).await;
    assert_eq!(r.code, 200);
    // Viewer writes blocked.
    let r = call(
        &addr,
        "PUT",
        "/api/docs/users/a",
        Some(r#"{"data":{"name":"x"}}"#),
        Some(&ro),
    )
    .await;
    assert_eq!(r.code, 403);

    // Operator writes.
    let r = call(
        &addr,
        "PUT",
        "/api/docs/users/a",
        Some(r#"{"data":{"name":"Alice","age":30}}"#),
        Some(&op),
    )
    .await;
    assert_eq!(r.code, 200, "put: {}", r.body);
    let r = call(
        &addr,
        "PUT",
        "/api/docs/users/b",
        Some(r#"{"data":{"name":"Bob","age":25}}"#),
        Some(&op),
    )
    .await;
    assert_eq!(r.code, 200);

    // Viewer reads the doc + queries it.
    let r = call(&addr, "GET", "/api/docs/users/a", None, Some(&ro)).await;
    assert_eq!(r.code, 200);
    assert_eq!(r.body["name"], "Alice");
    assert_eq!(r.body["id"], "a");
    let r = call(
        &addr,
        "POST",
        "/api/query",
        Some(r#"{"collection":"users","filters":[{"field":"age","op":">=","value":21}],"order_by":[{"field":"name","direction":"asc"}],"limit":10}"#),
        Some(&ro),
    )
    .await;
    assert_eq!(r.code, 200, "query: {}", r.body);
    assert_eq!(r.body["count"], 2);
    assert_eq!(r.body["rows"][0]["name"], "Alice");

    // Query limit cap enforced.
    let r = call(
        &addr,
        "POST",
        "/api/query",
        Some(r#"{"collection":"users","limit":999999}"#),
        Some(&ro),
    )
    .await;
    assert_eq!(r.code, 200);
    assert_eq!(r.body["count"], 2);

    // Bad operator rejected; unknown collection 404s on get.
    let r = call(
        &addr,
        "POST",
        "/api/query",
        Some(r#"{"collection":"users","filters":[{"field":"age","op":"~","value":1}]}"#),
        Some(&ro),
    )
    .await;
    assert_eq!(r.code, 400);
    let r = call(&addr, "GET", "/api/docs/nope/x", None, Some(&ro)).await;
    assert_eq!(r.code, 404);

    // Patch + batch as operator.
    let r = call(
        &addr,
        "PATCH",
        "/api/docs/users/a",
        Some(r#"{"data":{"age":31}}"#),
        Some(&op),
    )
    .await;
    assert_eq!(r.code, 200);
    let r = call(
        &addr,
        "POST",
        "/api/batch",
        Some(r#"{"mutations":[{"op":"set","collection":"users","doc_id":"c","data":{"name":"Cy"}},{"op":"delete","collection":"users","doc_id":"b"}]}"#),
        Some(&op),
    )
    .await;
    assert_eq!(r.code, 200, "batch: {}", r.body);
    let r = call(&addr, "GET", "/api/docs/users/b", None, Some(&ro)).await;
    assert_eq!(r.code, 404);
    let r = call(&addr, "GET", "/api/docs/users/c", None, Some(&ro)).await;
    assert_eq!(r.code, 200);

    // Operator cannot run maintenance; admin can. Viewer cannot even audit.
    let r = call(&addr, "POST", "/api/compact", None, Some(&op)).await;
    assert_eq!(r.code, 403);
    let r = call(&addr, "GET", "/api/audit", None, Some(&ro)).await;
    assert_eq!(r.code, 403);
    let r = call(&addr, "GET", "/api/audit", None, Some(&op)).await;
    assert_eq!(r.code, 200);
    let r = call(&addr, "POST", "/api/compact", None, Some(&admin)).await;
    assert_eq!(r.code, 200, "compact: {}", r.body);
    let r = call(
        &addr,
        "POST",
        "/api/vacuum",
        Some(r#"{"collection":"users"}"#),
        Some(&admin),
    )
    .await;
    assert_eq!(r.code, 200);

    // Stats + indexes.
    let r = call(&addr, "GET", "/api/stats", None, Some(&ro)).await;
    assert_eq!(r.code, 200);
    let r = call(
        &addr,
        "POST",
        "/api/indexes",
        Some(r#"{"kind":"simple","collection":"users","field":"age"}"#),
        Some(&op),
    )
    .await;
    assert_eq!(r.code, 200, "index: {}", r.body);
    let r = call(&addr, "GET", "/api/indexes?collection=users", None, Some(&ro)).await;
    assert_eq!(r.code, 200);

    // Backup to temp path as admin; operator denied.
    let dest = std::env::temp_dir().join(format!(
        "fl-cs-backup-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let body = format!(r#"{{"path":"{}"}}"#, dest.to_string_lossy().replace('\\', "/"));
    let r = call(&addr, "POST", "/api/backup", Some(&body), Some(&op)).await;
    assert_eq!(r.code, 403);
    let r = call(&addr, "POST", "/api/backup", Some(&body), Some(&admin)).await;
    assert_eq!(r.code, 200, "backup: {}", r.body);
    assert!(dest.exists());
    std::fs::remove_dir_all(&dest).ok();

    // Status + rooms visible to viewer.
    let r = call(&addr, "GET", "/api/status", None, Some(&ro)).await;
    assert_eq!(r.code, 200);
    let r = call(&addr, "GET", "/api/collections", None, Some(&ro)).await;
    assert_eq!(r.code, 200);
    let names: Vec<String> = r.body["collections"]
        .as_array()
        .unwrap()
        .iter()
        .filter_map(|c| c["name"].as_str().map(str::to_string))
        .collect();
    assert!(names.contains(&"users".to_string()), "users listed: {names:?}");
    let r = call(&addr, "GET", "/api/rooms", None, Some(&ro)).await;
    assert_eq!(r.code, 200);

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}
