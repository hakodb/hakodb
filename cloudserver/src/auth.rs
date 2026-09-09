//! Admin authentication: `__users` credential store, Argon2id password
//! hashing, in-memory sessions over httpOnly cookies, login rate limiting.
//!
//! The `__users` collection never leaves the device (see
//! `firelite::engine::engine::is_sync_excluded`) and is hidden from
//! `list_collections` (underscore-prefixed), so credentials can't leak
//! through sync or the data browser.

use argon2::{
    password_hash::{rand_core::OsRng, PasswordHash, PasswordHasher, PasswordVerifier, SaltString},
    Argon2,
};
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;
use rand::RngCore;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

pub const USERS_COLLECTION: &str = "__users";
pub const SESSION_COOKIE: &str = "fl_admin_session";
pub const SESSION_TTL: Duration = Duration::from_secs(12 * 3600);
const MAX_LOGIN_ATTEMPTS: usize = 5;
const LOGIN_WINDOW: Duration = Duration::from_secs(60);

/// Stored user roles, weakest first (so `as_rank` comparisons read naturally).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum Role {
    Viewer,
    Operator,
    Admin,
}

impl Role {
    fn rank(self) -> u8 {
        match self {
            Role::Viewer => 0,
            Role::Operator => 1,
            Role::Admin => 2,
        }
    }

    pub fn at_least(self, need: Role) -> bool {
        self.rank() >= need.rank()
    }

    fn parse(s: &str) -> Option<Role> {
        match s {
            "viewer" => Some(Role::Viewer),
            "operator" => Some(Role::Operator),
            "admin" => Some(Role::Admin),
            _ => None,
        }
    }
}

#[derive(Debug, Clone)]
pub struct Session {
    pub username: String,
    pub role: Role,
    pub expires_at: Instant,
}

/// In-memory session table + per-IP login attempt log. Process-local by
/// design: a server restart logs everyone out (fail-closed, no stale
/// sessions surviving a compromise window).
#[derive(Debug, Default)]
pub struct AuthStore {
    sessions: Mutex<HashMap<String, Session>>,
    attempts: Mutex<HashMap<String, Vec<Instant>>>,
}

impl AuthStore {
    fn prune_sessions(&self) {
        if let Ok(mut s) = self.sessions.lock() {
            let now = Instant::now();
            s.retain(|_, sess| sess.expires_at > now);
        }
    }

    pub fn insert_session(&self, token: String, sess: Session) {
        self.prune_sessions();
        if let Ok(mut s) = self.sessions.lock() {
            s.insert(token, sess);
        }
    }

    pub fn lookup_session(&self, token: &str) -> Option<Session> {
        let s = self.sessions.lock().ok()?;
        let sess = s.get(token)?.clone();
        if sess.expires_at <= Instant::now() {
            drop(s);
            if let Ok(mut s) = self.sessions.lock() {
                s.remove(token);
            }
            return None;
        }
        Some(sess)
    }

    pub fn remove_session(&self, token: &str) {
        if let Ok(mut s) = self.sessions.lock() {
            s.remove(token);
        }
    }

    /// Fixed-window rate limit: at most MAX_LOGIN_ATTEMPTS recorded failures
    /// per IP per LOGIN_WINDOW. Returns false (reject) when exhausted.
    pub fn check_rate_limit(&self, ip: &str) -> bool {
        let now = Instant::now();
        let mut attempts = match self.attempts.lock() {
            Ok(a) => a,
            Err(_) => return false,
        };
        let log = attempts.entry(ip.to_string()).or_default();
        log.retain(|t| now.duration_since(*t) < LOGIN_WINDOW);
        log.len() < MAX_LOGIN_ATTEMPTS
    }

    pub fn record_failure(&self, ip: &str) {
        if let Ok(mut attempts) = self.attempts.lock() {
            attempts
                .entry(ip.to_string())
                .or_default()
                .push(Instant::now());
        }
    }
}

/// Argon2id with OWASP-recommended parameters (via the crate defaults).
pub fn hash_password(password: &str) -> Result<String, String> {
    let salt = SaltString::generate(&mut OsRng);
    Argon2::default()
        .hash_password(password.as_bytes(), &salt)
        .map(|h| h.to_string())
        .map_err(|e| format!("hash password: {e}"))
}

/// PHC-string verify (constant-time comparison inside the crate).
pub fn verify_password(password: &str, hash: &str) -> bool {
    let parsed = match PasswordHash::new(hash) {
        Ok(p) => p,
        Err(_) => return false,
    };
    Argon2::default()
        .verify_password(password.as_bytes(), &parsed)
        .is_ok()
}

/// 256-bit session token, hex-encoded.
pub fn new_session_token() -> String {
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn get_str(doc: &FireLiteDoc, field: &str) -> Option<String> {
    match doc.get(field) {
        Some(Value::String(s)) => Some(s.clone()),
        _ => None,
    }
}

/// Load a user row. Returns (password_hash, role, disabled).
pub fn load_user(db: &FireLite, username: &str) -> Option<(String, Role, bool)> {
    let doc = db.get(USERS_COLLECTION, username).ok()??;
    let hash = get_str(&doc, "password_hash")?;
    let role = doc
        .get("role")
        .and_then(|v| match v {
            Value::String(s) => Role::parse(s),
            _ => None,
        })
        .unwrap_or(Role::Viewer);
    let disabled = matches!(doc.get("disabled"), Some(Value::Bool(true)));
    Some((hash, role, disabled))
}

/// True when no usable admin path exists (fresh DB): either the collection
/// is empty or no enabled admin remains. Drives the setup-wizard gate.
pub fn setup_required(db: &FireLite) -> bool {
    match db.query(firelite::query::query::Query::new(USERS_COLLECTION)) {
        Ok(rows) => !rows.iter().any(|(_, doc)| {
            !matches!(doc.get("disabled"), Some(Value::Bool(true)))
                && doc.get("role").and_then(|v| match v {
                    Value::String(s) => Role::parse(s),
                    _ => None,
                }) == Some(Role::Admin)
        }),
        Err(_) => true,
    }
}

/// Insert or replace a user row. Passwords never persist in any other form.
pub fn upsert_user(
    db: &FireLite,
    username: &str,
    password: &str,
    role: Role,
    disabled: bool,
) -> Result<(), String> {
    if username.trim().is_empty() || username.len() > 64 {
        return Err("invalid username".into());
    }
    if password.len() < 8 {
        return Err("password must be at least 8 characters".into());
    }
    let role_str = match role {
        Role::Viewer => "viewer",
        Role::Operator => "operator",
        Role::Admin => "admin",
    };
    let mut doc = FireLiteDoc::default();
    doc.insert("password_hash", Value::String(hash_password(password)?));
    doc.insert("role", Value::String(role_str.to_string()));
    doc.insert("disabled", Value::Bool(disabled));
    doc.insert(
        "created_at",
        Value::Int(
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        ),
    );
    db.put(USERS_COLLECTION, username, &doc)
        .map(|_| ())
        .map_err(|e| format!("store user: {e}"))
}

/// Extract a session token from a Cookie header value.
pub fn token_from_cookie(header: &str) -> Option<String> {
    for part in header.split(';') {
        let mut kv = part.trim().splitn(2, '=');
        if kv.next()?.trim() == SESSION_COOKIE {
            let v = kv.next()?.trim().to_string();
            if !v.is_empty() {
                return Some(v);
            }
        }
    }
    None
}

/// Build a `Set-Cookie` value. `secure` flips on with TLS (phase 7); until
/// then the bind defaults to loopback, where Secure would only break logins.
pub fn set_cookie_value(token: &str, secure: bool, clear: bool) -> String {
    if clear {
        return format!("{SESSION_COOKIE}=; Path=/; HttpOnly; SameSite=Strict; Max-Age=0");
    }
    let mut c = format!("{SESSION_COOKIE}={token}; Path=/; HttpOnly; SameSite=Strict");
    if secure {
        c.push_str("; Secure");
    }
    c
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn password_roundtrip_and_wrong_password_rejected() {
        let h = hash_password("correct-horse-42").unwrap();
        assert!(verify_password("correct-horse-42", &h));
        assert!(!verify_password("wrong", &h));
        assert!(!verify_password("x", "not-a-valid-phc-string"));
    }

    #[test]
    fn session_lifecycle() {
        let store = AuthStore::default();
        let tok = new_session_token();
        assert_eq!(tok.len(), 64);
        assert!(store.lookup_session(&tok).is_none());
        store.insert_session(
            tok.clone(),
            Session {
                username: "u".into(),
                role: Role::Admin,
                expires_at: Instant::now() + SESSION_TTL,
            },
        );
        assert!(store.lookup_session(&tok).is_some());
        store.remove_session(&tok);
        assert!(store.lookup_session(&tok).is_none());
    }

    #[test]
    fn role_ordering() {
        assert!(Role::Admin.at_least(Role::Viewer));
        assert!(Role::Admin.at_least(Role::Admin));
        assert!(!Role::Viewer.at_least(Role::Operator));
        assert!(Role::parse("nope").is_none());
    }

    #[test]
    fn rate_limiter_trips_and_recovers() {
        let store = AuthStore::default();
        for _ in 0..MAX_LOGIN_ATTEMPTS {
            assert!(store.check_rate_limit("1.2.3.4"));
            store.record_failure("1.2.3.4");
        }
        assert!(!store.check_rate_limit("1.2.3.4"));
        assert!(store.check_rate_limit("5.6.7.8")); // other IPs unaffected
    }

    #[test]
    fn cookie_helpers() {
        assert_eq!(
            token_from_cookie("a=1; fl_admin_session=tok123; b=2"),
            Some("tok123".to_string())
        );
        assert_eq!(token_from_cookie("a=1"), None);
        assert_eq!(token_from_cookie("fl_admin_session=; a=1"), None);
        let set = set_cookie_value("tok", false, false);
        assert!(set.contains("HttpOnly") && set.contains("SameSite=Strict"));
        assert!(!set.contains("Secure"));
        assert!(set_cookie_value("tok", true, false).contains("Secure"));
        assert!(set_cookie_value("tok", false, true).contains("Max-Age=0"));
    }
}
