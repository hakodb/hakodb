//! SSE stream tests: snapshot frames, live doc events, auth gate.

use firelite::config::{DurabilityMode, FireLiteConfig};
use firelite::engine::FireLite;
use firelite_cloudserver::app::{build_router, AppState};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::io::{AsyncReadExt, AsyncWriteExt};

fn temp_db() -> (FireLite, std::path::PathBuf) {
    let dir = std::env::temp_dir().join(format!(
        "fl-cs-events-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ));
    let mut cfg = FireLiteConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    (FireLite::open(&dir, cfg).unwrap(), dir)
}

async fn read_response_head(
    sock: &mut tokio::net::TcpStream,
) -> (u16, HashMap<String, String>, Vec<u8>) {
    let mut buf = Vec::new();
    let mut tmp = [0u8; 4096];
    loop {
        let n = sock.read(&mut tmp).await.unwrap();
        assert!(n > 0, "connection closed early");
        buf.extend_from_slice(&tmp[..n]);
        if let Some(hend) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let header = String::from_utf8_lossy(&buf[..hend]).into_owned();
            let code: u16 = header
                .split_whitespace()
                .nth(1)
                .unwrap_or("0")
                .parse()
                .unwrap_or(0);
            let mut headers = HashMap::new();
            for l in header.lines().skip(1) {
                if let Some((k, v)) = l.split_once(':') {
                    headers.insert(k.trim().to_lowercase(), v.trim().to_string());
                }
            }
            let rest = buf[hend + 4..].to_vec();
            return (code, headers, rest);
        }
    }
}

/// Collect SSE frames until `want` event kinds seen (or timeout).
async fn collect_frames(
    sock: &mut tokio::net::TcpStream,
    pending: Vec<u8>,
    want: &[&str],
    timeout: std::time::Duration,
) -> Vec<(String, serde_json::Value)> {
    let mut frames = Vec::new();
    let mut raw = pending;
    let mut payload: Vec<u8> = Vec::new();
    let mut tmp = [0u8; 4096];
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        // 1. De-chunk: hyper streams SSE as <hex>\r\n<bytes>\r\n.
        loop {
            let Some(hend) = raw.windows(2).position(|w| w == b"\r\n") else {
                break;
            };
            let hexline = String::from_utf8_lossy(&raw[..hend]).into_owned();
            let Ok(n) = usize::from_str_radix(hexline.trim(), 16) else {
                break;
            };
            if n == 0 {
                break; // terminal chunk
            }
            if raw.len() < hend + 2 + n + 2 {
                break; // partial chunk, need more bytes
            }
            payload.extend_from_slice(&raw[hend + 2..hend + 2 + n]);
            raw.drain(..hend + 2 + n + 2);
        }
        // 2. Split payload into SSE frames on the blank line.
        while let Some(pos) = payload.windows(2).position(|w| w == b"\n\n") {
            let chunk: Vec<u8> = payload.drain(..pos + 2).collect();
            let text = String::from_utf8_lossy(&chunk).into_owned();
            let mut kind = String::new();
            let mut data = String::new();
            for line in text.lines() {
                if let Some(v) = line.strip_prefix("event:") {
                    kind = v.trim().to_string();
                } else if let Some(v) = line.strip_prefix("data:") {
                    if !data.is_empty() {
                        data.push('\n');
                    }
                    data.push_str(v.trim());
                }
            }
            if kind.is_empty() {
                continue; // comment / keep-alive
            }
            let v: serde_json::Value =
                serde_json::from_str(&data).unwrap_or(serde_json::Value::Null);
            frames.push((kind, v));
            if want.iter().all(|w| frames.iter().any(|(k, _)| k == w)) {
                return frames;
            }
        }
        if tokio::time::Instant::now() >= deadline {
            panic!("timeout waiting for {want:?}, got {frames:?}");
        }
        let left = deadline.saturating_duration_since(tokio::time::Instant::now());
        let n = tokio::time::timeout(left, sock.read(&mut tmp))
            .await
            .expect("read timeout")
            .unwrap();
        assert!(n > 0, "stream closed early: {frames:?}");
        raw.extend_from_slice(&tmp[..n]);
    }
}

async fn setup_cookie(addr: &std::net::SocketAddr) -> String {
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
    text.lines()
        .find_map(|l| {
            l.strip_prefix("set-cookie:")
                .or_else(|| l.strip_prefix("Set-Cookie:"))
                .map(|v| v.trim().split(';').next().unwrap_or("").to_string())
        })
        .expect("set-cookie")
}

async fn put_doc(addr: &std::net::SocketAddr, cookie: &str, col: &str, id: &str) {
    let mut sock = tokio::net::TcpStream::connect(addr).await.unwrap();
    let body = format!(r#"{{"data":{{"v":1}}}}"#);
    let req = format!(
        "PUT /api/docs/{col}/{id} HTTP/1.1\r\nHost: x\r\nContent-Type: application/json\r\nContent-Length: {}\r\nCookie: {cookie}\r\n\r\n{body}",
        body.len()
    );
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
    let text = String::from_utf8_lossy(&buf).into_owned();
    assert!(text.starts_with("HTTP/1.1 200"), "put: {text}");
}

#[tokio::test]
async fn events_require_auth() {
    let (db, dir) = temp_db();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, build_router(Arc::new(AppState::new(Arc::new(db), false))))
            .await
            .unwrap();
    });
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(b"GET /api/events HTTP/1.1\r\nHost: x\r\n\r\n")
        .await
        .unwrap();
    let (code, _, _) = read_response_head(&mut sock).await;
    assert_eq!(code, 401);
    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn snapshot_then_live_doc_event() {
    let (db, dir) = temp_db();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let srv = tokio::spawn(async move {
        axum::serve(listener, build_router(Arc::new(AppState::new(Arc::new(db), false))))
            .await
            .unwrap();
    });
    let cookie = setup_cookie(&addr).await;

    // Open the stream (watch only "watchme" to keep the test focused).
    let mut sock = tokio::net::TcpStream::connect(&addr).await.unwrap();
    sock.write_all(
        format!("GET /api/events?collections=watchme HTTP/1.1\r\nHost: x\r\nCookie: {cookie}\r\n\r\n").as_bytes(),
    )
    .await
    .unwrap();
    let (code, headers, rest) = read_response_head(&mut sock).await;
    assert_eq!(code, 200);
    assert!(
        headers
            .get("content-type")
            .map(|v| v.contains("text/event-stream"))
            .unwrap_or(false),
        "content-type: {headers:?}"
    );

    // Snapshot frames first.
    let frames = collect_frames(&mut sock, rest, &["peers", "versions"], std::time::Duration::from_secs(10)).await;
    assert!(frames.iter().any(|(k, _)| k == "peers"));
    assert!(frames.iter().any(|(k, _)| k == "versions"));

    // Let the watch pump subscribe, then write.
    tokio::time::sleep(std::time::Duration::from_millis(300)).await;
    put_doc(&addr, &cookie, "watchme", "a").await;

    // Live doc event arrives on the still-open stream.
    let frames = collect_frames(&mut sock, Vec::new(), &["doc"], std::time::Duration::from_secs(10)).await;
    let doc = frames.iter().find(|(k, _)| k == "doc").expect("doc frame");
    assert_eq!(doc.1["collection"], "watchme");
    assert_eq!(doc.1["id"], "a");
    assert_eq!(doc.1["kind"], "put");

    srv.abort();
    std::fs::remove_dir_all(&dir).ok();
}
