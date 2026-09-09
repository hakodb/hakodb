//! Auth flow tests against a live app instance (raw HTTP, ephemeral port).

use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::engine::FireLite;
use firelite_cloudserver::app::{build_router, AppState};
use firelite_cloudserver::auth::{upsert_user, Role};
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_db() -> (FireLite, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-cs-auth-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    (FireLite::open(&dir, cfg).unwrap(), dir)
}

struct Client {
    sock: tokio::net::TcpStream,
}

struct Resp {
    code: u16,
    set_cookie: Option<String>,
    body: serde_json::Value,
}

async fn connect(addr: &std::net::SocketAddr) -> Client {
    Client {
        sock: tokio::net::TcpStream::connect(addr).await.unwrap(),
    }
}

impl Client {
    async fn raw(&mut self, req: &str) -> Resp {
        self.sock.write_all(req.as_bytes()).await.unwrap();
        // New connection per request (server uses Connection: close semantics
        // only if we ask; simpler to read exactly one response: read until
        // headers end, then Content-Length bytes).
        let mut buf = Vec::new();
        let mut tmp = [0u8; 4096];
        loop {
            let n = self.sock.read(&mut tmp).await.unwrap();
            if n == 0 {
                break;
            }
            buf.extend_from_slice(&tmp[..n]);
            if let Some(hend) = find_subslice(&buf, b"\r\n\r\n") {
                let header: String = String::from_utf8_lossy(&buf[..hend]).into();
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
                .map(|v| v.trim().to_string())
        });
        let body_str = text.split("\r\n\r\n").nth(1).unwrap_or("{}");
        let body: serde_json::Value =
            serde_json::from_str(body_str).unwrap_or(serde_json::Value::Null);
        Resp {
            code,
            set_cookie,
            body,
        }
    }

    async fn post(&mut self, path: &str, json: &str, cookie: Option<&str>) -> Resp {
        let mut req = format!(
            "POST {path} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\n",
            json.len()
        );
        if let Some(c) = cookie {
            req.push_str(&format!("Cookie: {c}\r\n"));
        }
        req.push_str("\r\n");
        req.push_str(json);
        self.raw(&req).await
    }

    async fn get(&mut self, path: &str, cookie: Option<&str>) -> Resp {
        let mut req = format!("GET {path} HTTP/1.1\r\nHost: x\r\n");
        if let Some(c) = cookie {
            req.push_str(&format!("Cookie: {c}\r\n"));
        }
        req.push_str("\r\n");
        self.raw(&req).await
    }
}

fn find_subslice(hay: &[u8], needle: &[u8]) -> Option<usize> {
    hay.windows(needle.len()).position(|w| w == needle)
}

fn cookie_value(set_cookie: &str) -> String {
    set_cookie.split(';').next().unwrap_or("").to_string()
}

async fn spawn_app(db: FireLite) -> (std::net::SocketAddr, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let h = tokio::spawn(async move {
        axum::serve(listener, build_router(Arc::new(AppState::new(db, false))))
            .await
            .unwrap();
    });
    (addr, h)
}

#[tokio::test]
async fn setup_creates_admin_and_second_setup_forbidden() {
    let (db, dir) = temp_db();
    let (addr, srv) = spawn_app(db).await;
    let mut c = connect(&addr).await;

    let r = c
        .post(
            "/api/setup",
            r#"{"username":"root","password":"s3cret-pw!"}"#,
            None,
        )
        .await;
    assert_eq!(r.code, 200, "setup: {}", r.body);
    assert_eq!(r.body["username"], "root");
    assert_eq!(r.body["role"], "admin");
    let cookie = r.set_cookie.as_ref().map(|s| cookie_value(s)).unwrap();

    // Authenticated now (auto-login on setup).
    let mut c2 = connect(&addr).await;
    let me = c2.get("/api/me", Some(&cookie)).await;
    assert_eq!(me.code, 200);
    assert_eq!(me.body["username"], "root");

    // Second setup is a hard 403.
    let mut c3 = connect(&addr).await;
    let r2 = c3
        .post(
            "/api/setup",
            r#"{"username":"intruder","password":"s3cret-pw!2"}"#,
            None,
        )
        .await;
    assert_eq!(r2.code, 403);

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn login_logout_and_failures() {
    let (db, dir) = temp_db();
    upsert_user(&db, "op", "operator-pw", Role::Operator, false).unwrap();
    // setup gate must see a non-admin-only DB as still requiring setup;
    // that is intended (only an enabled admin closes the wizard).
    let (addr, srv) = spawn_app(db).await;

    // Wrong password and unknown user: identical 401s.
    let mut c = connect(&addr).await;
    assert_eq!(
        c.post(
            "/api/login",
            r#"{"username":"op","password":"nope"}"#,
            None
        )
        .await
        .code,
        401
    );
    let mut c = connect(&addr).await;
    assert_eq!(
        c.post(
            "/api/login",
            r#"{"username":"ghost","password":"whatever1"}"#,
            None
        )
        .await
        .code,
        401
    );

    // Success.
    let mut c = connect(&addr).await;
    let r = c
        .post(
            "/api/login",
            r#"{"username":"op","password":"operator-pw"}"#,
            None
        )
        .await;
    assert_eq!(r.code, 200);
    assert_eq!(r.body["role"], "operator");
    let cookie = cookie_value(r.set_cookie.as_ref().unwrap());
    assert!(r.set_cookie.as_ref().unwrap().contains("HttpOnly"));

    // Logout invalidates.
    let mut c = connect(&addr).await;
    assert_eq!(c.post("/api/logout", "{}", Some(&cookie)).await.code, 200);
    let mut c = connect(&addr).await;
    assert_eq!(c.get("/api/me", Some(&cookie)).await.code, 401);
    // And anonymous me is 401 too.
    let mut c = connect(&addr).await;
    assert_eq!(c.get("/api/me", None).await.code, 401);

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn disabled_user_cannot_login() {
    let (db, dir) = temp_db();
    upsert_user(&db, "old", "old-password", Role::Admin, true).unwrap();
    let (addr, srv) = spawn_app(db).await;
    let mut c = connect(&addr).await;
    assert_eq!(
        c.post(
            "/api/login",
            r#"{"username":"old","password":"old-password"}"#,
            None
        )
        .await
        .code,
        401
    );
    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn login_rate_limit_trips() {
    let (db, dir) = temp_db();
    upsert_user(&db, "u", "user-password", Role::Viewer, false).unwrap();
    let (addr, srv) = spawn_app(db).await;
    // 5 failures allowed per minute per IP; the 6th is 429.
    for i in 0..6 {
        let mut c = connect(&addr).await;
        let code = c
            .post("/api/login", r#"{"username":"u","password":"bad"}"#, None)
            .await
            .code;
        if i < 5 {
            assert_eq!(code, 401, "attempt {i}");
        } else {
            assert_eq!(code, 429, "attempt {i} should trip limiter");
        }
    }
    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}
