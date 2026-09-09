//! Console surface tests: static UI serving, setup probe, user admin
//! endpoints incl. lockout guards. Raw HTTP, ephemeral ports.

use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::engine::FireLite;
use firelite_cloudserver::app::{build_router, AppState};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_db() -> (FireLite, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-cs-ui-{}",
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
    headers: std::collections::HashMap<String, String>,
    body: String,
    json: serde_json::Value,
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
            // Static files may arrive chunked; JSON APIs use content-length.
            if header.to_lowercase().contains("transfer-encoding: chunked") {
                // Read until terminal chunk.
                if buf.windows(7).any(|w| w == b"\r\n0\r\n\r\n") || buf.ends_with(b"0\r\n\r\n") {
                    break;
                }
                continue;
            }
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
    let mut parts = text.splitn(2, "\r\n\r\n");
    let head = parts.next().unwrap_or("");
    let mut body_raw = parts.next().unwrap_or("").to_string();
    // De-chunk when needed.
    if head.to_lowercase().contains("transfer-encoding: chunked") {
        let mut out = Vec::new();
        let mut rest = body_raw.as_bytes();
        loop {
            let Some(hend) = rest.windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let hexline = String::from_utf8_lossy(&rest[..hend]).into_owned();
            let Ok(n) = usize::from_str_radix(hexline.trim(), 16) else {
                break;
            };
            if n == 0 || rest.len() < hend + 2 + n {
                break;
            }
            out.extend_from_slice(&rest[hend + 2..hend + 2 + n]);
            rest = &rest[hend + 2 + n + 2..];
            if rest.is_empty() {
                break;
            }
        }
        body_raw = String::from_utf8_lossy(&out).into_owned();
    }
    let code: u16 = head
        .split_whitespace()
        .nth(1)
        .unwrap_or("0")
        .parse()
        .unwrap_or(0);
    let mut headers = std::collections::HashMap::new();
    for l in head.lines().skip(1) {
        if let Some((k, v)) = l.split_once(':') {
            headers.insert(k.trim().to_lowercase(), v.trim().to_string());
        }
    }
    let json: serde_json::Value =
        serde_json::from_str(&body_raw).unwrap_or(serde_json::Value::Null);
    Resp {
        code,
        headers,
        body: body_raw,
        json,
    }
}

async fn login_cookie(addr: &std::net::SocketAddr, user: &str, pass: &str) -> String {
    // Assumes the account exists; performs a raw login and returns the cookie.
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
    assert!(text.starts_with("HTTP/1.1 200"), "login: {text}");
    text.lines()
        .find_map(|l| {
            l.strip_prefix("set-cookie:")
                .or_else(|| l.strip_prefix("Set-Cookie:"))
                .map(|v| v.trim().split(';').next().unwrap_or("").to_string())
        })
        .expect("set-cookie")
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

async fn setup_admin(addr: &std::net::SocketAddr) -> String {
    // Fresh DB: probe reports setup required, then create the admin.
    let r = call(addr, "GET", "/api/setup/status", None, None).await;
    assert_eq!(r.code, 200);
    assert_eq!(r.json["setup_required"], true);
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = r#"{"username":"root","password":"root-password"}"#;
    let req = format!(
        "POST /api/setup HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
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
    assert!(text.starts_with("HTTP/1.1 200"), "setup: {text}");
    let cookie = text
        .lines()
        .find_map(|l| {
            l.strip_prefix("set-cookie:")
                .or_else(|| l.strip_prefix("Set-Cookie:"))
                .map(|v| v.trim().split(';').next().unwrap_or("").to_string())
        })
        .expect("set-cookie");
    // Probe flips off afterwards.
    let r = call(addr, "GET", "/api/setup/status", None, None).await;
    assert_eq!(r.json["setup_required"], false);
    cookie
}

#[tokio::test]
async fn static_console_served() {
    let (db, dir) = temp_db();
    let (addr, srv) = spawn(db).await;

    let r = call(&addr, "GET", "/", None, None).await;
    assert_eq!(r.code, 200);
    assert!(r.body.contains("FireLite Console"));
    assert_eq!(
        r.headers.get("content-type").map(String::as_str),
        Some("text/html; charset=utf-8")
    );
    let r = call(&addr, "GET", "/app.js", None, None).await;
    assert_eq!(r.code, 200);
    assert!(r.body.contains("EventSource"));
    let r = call(&addr, "GET", "/styles.css", None, None).await;
    assert_eq!(r.code, 200);
    assert!(r.body.contains("--bg"));
    // Deep link falls back to the shell.
    let r = call(&addr, "GET", "/dashboard", None, None).await;
    assert_eq!(r.code, 200);
    assert!(r.body.contains("FireLite Console"));
    // Unknown asset 404s.
    let r = call(&addr, "GET", "/nope.png", None, None).await;
    assert_eq!(r.code, 404);
    // Traversal rejected.
    let r = call(&addr, "GET", "/..%2fsecret", None, None).await;
    assert!(r.code == 400 || r.code == 404);

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn user_admin_crud_with_lockout_guards() {
    let (db, dir) = temp_db();
    let (addr, srv) = spawn(db).await;
    let root = setup_admin(&addr).await;

    // Create operator + viewer.
    let r = call(
        &addr,
        "POST",
        "/api/users",
        Some(r#"{"username":"op","password":"operator-pw","role":"operator"}"#),
        Some(&root),
    )
    .await;
    assert_eq!(r.code, 201, "create: {}", r.json);
    let r = call(&addr, "GET", "/api/users", None, Some(&root)).await;
    assert_eq!(r.code, 200);
    assert_eq!(r.json["users"].as_array().unwrap().len(), 2);
    // No password hashes leak through the listing.
    assert!(!r.body.contains("argon2"));

    // Operator cannot manage users.
    let op = login_cookie(&addr, "op", "operator-pw").await;
    let r = call(
        &addr,
        "POST",
        "/api/users",
        Some(r#"{"username":"x","password":"x-password","role":"viewer"}"#),
        Some(&op),
    )
    .await;
    assert_eq!(r.code, 403);

    // Disable operator, then re-enable + promote.
    let r = call(
        &addr,
        "PUT",
        "/api/users/op",
        Some(r#"{"disabled":true}"#),
        Some(&root),
    )
    .await;
    assert_eq!(r.code, 200);
    assert_eq!(r.json["user"]["disabled"], true);
    // Disabled login fails.
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    let body = r#"{"username":"op","password":"operator-pw"}"#;
    let req = format!(
        "POST /api/login HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n\r\n{}",
        body.len(),
        body
    );
    use tokio::io::AsyncWriteExt as _;
    sock.write_all(req.as_bytes()).await.unwrap();
    let mut buf = Vec::new();
    let mut tmp = [0u8; 1024];
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
    assert!(String::from_utf8_lossy(&buf).starts_with("HTTP/1.1 401"));

    // Self-delete refused even for root.
    let r = call(&addr, "DELETE", "/api/users/root", None, Some(&root)).await;
    assert_eq!(r.code, 400);

    // Promote op to second admin, then root can be removed by op.
    let r = call(
        &addr,
        "PUT",
        "/api/users/op",
        Some(r#"{"role":"admin","disabled":false}"#),
        Some(&root),
    )
    .await;
    assert_eq!(r.code, 200);
    let op2 = login_cookie(&addr, "op", "operator-pw").await;
    let r = call(&addr, "DELETE", "/api/users/root", None, Some(&op2)).await;
    assert_eq!(r.code, 200, "second admin removes first: {}", r.json);

    // Last admin standing: demote/disable/delete all refused.
    let r = call(
        &addr,
        "PUT",
        "/api/users/op",
        Some(r#"{"role":"viewer"}"#),
        Some(&op2),
    )
    .await;
    assert_eq!(r.code, 400, "self-demote of last admin blocked");
    let r = call(&addr, "DELETE", "/api/users/op", None, Some(&op2)).await;
    assert_eq!(r.code, 400, "self-delete blocked");

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}
