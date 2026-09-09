//! HTTP application: shared state, routes, handlers.

use axum::{
    async_trait,
    extract::{ConnectInfo, FromRef, FromRequestParts, State},
    http::{header, request::Parts, HeaderMap, StatusCode},
    response::{IntoResponse, Response},
    routing::{get, post},
    Json, Router,
};
use firelite::engine::FireLite;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::net::SocketAddr;
use std::sync::Arc;

use crate::auth::{
    hash_password, load_user, new_session_token, set_cookie_value, setup_required,
    token_from_cookie, upsert_user, verify_password, AuthStore, Role, Session, SESSION_TTL,
};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<FireLite>,
    pub auth: Arc<AuthStore>,
    pub secure_cookies: bool,
}

impl AppState {
    pub fn new(db: FireLite, secure_cookies: bool) -> Self {
        Self {
            db: Arc::new(db),
            auth: Arc::new(AuthStore::default()),
            secure_cookies,
        }
    }
}

/// Authenticated caller, extracted from the session cookie. Rejects with
/// 401 when missing, unknown, or expired.
pub struct AuthedUser {
    pub username: String,
    pub role: Role,
}

#[async_trait]
impl<S> FromRequestParts<S> for AuthedUser
where
    S: Send + Sync,
    Arc<AppState>: FromRef<S>,
{
    type Rejection = (StatusCode, Json<Value>);

    async fn from_request_parts(parts: &mut Parts, state: &S) -> Result<Self, Self::Rejection> {
        let app: Arc<AppState> = Arc::from_ref(state);
        let err = || {
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "authentication required"})),
            )
        };
        let token = parts
            .headers
            .get(header::COOKIE)
            .and_then(|v| v.to_str().ok())
            .and_then(token_from_cookie)
            .ok_or_else(err)?;
        app.auth
            .lookup_session(&token)
            .map(|s| AuthedUser {
                username: s.username,
                role: s.role,
            })
            .ok_or_else(err)
    }
}

// `Arc<AppState>: FromRef<S>` needs this import path to resolve.

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    let collections = state.db.list_collections().map(|c| c.len()).unwrap_or(0);
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "collections": collections,
    }))
}

#[derive(Debug, Deserialize)]
struct SetupBody {
    username: String,
    password: String,
}

/// First-run bootstrap: creates the initial admin. Open only while no
/// enabled admin exists; afterwards it is a hard 403. Success also logs
/// the new admin in (sets the session cookie).
async fn setup(
    State(state): State<Arc<AppState>>,
    Json(body): Json<SetupBody>,
) -> Response {
    if !setup_required(&state.db) {
        return (
            StatusCode::FORBIDDEN,
            Json(json!({"error": "server already initialized"})),
        )
            .into_response();
    }
    if body.username.trim().is_empty() || body.username.len() > 64 {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "invalid username"})),
        )
            .into_response();
    }
    // Re-check after validation to narrow the TOCTOU window (single-node
    // server: two racing setups both pass the first check, but the second
    // write just overwrites the same admin row — no privilege split).
    if let Err(e) = upsert_user(&state.db, body.username.trim(), &body.password, Role::Admin, false)
    {
        return (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response();
    }
    issue_session(&state, body.username.trim(), Role::Admin).into_response()
}

#[derive(Debug, Deserialize)]
struct LoginBody {
    username: String,
    password: String,
}

fn client_ip(parts_ip: Option<ConnectInfo<SocketAddr>>) -> String {
    parts_ip
        .map(|ConnectInfo(addr)| addr.ip().to_string())
        .unwrap_or_else(|| "unknown".into())
}

async fn login(
    State(state): State<Arc<AppState>>,
    ip: Option<ConnectInfo<SocketAddr>>,
    Json(body): Json<LoginBody>,
) -> Response {
    let ip_str = client_ip(ip);
    if !state.auth.check_rate_limit(&ip_str) {
        return (
            StatusCode::TOO_MANY_REQUESTS,
            Json(json!({"error": "too many attempts, try again later"})),
        )
            .into_response();
    }
    // Generic failure for missing/disabled/wrong — no user enumeration.
    let ok = match load_user(&state.db, body.username.trim()) {
        Some((hash, role, disabled)) if !disabled && verify_password(&body.password, &hash) => {
            Some(role)
        }
        _ => None,
    };
    match ok {
        Some(role) => issue_session(&state, body.username.trim(), role).into_response(),
        None => {
            state.auth.record_failure(&ip_str);
            // Hash once to flatten timing between known/unknown users.
            let _ = hash_password("dummy-timing-flatten");
            (
                StatusCode::UNAUTHORIZED,
                Json(json!({"error": "invalid credentials"})),
            )
                .into_response()
        }
    }
}

/// Create session + Set-Cookie header + caller identity body.
fn issue_session(state: &Arc<AppState>, username: &str, role: Role) -> (StatusCode, HeaderMap, Json<Value>) {
    let token = new_session_token();
    state.auth.insert_session(
        token.clone(),
        Session {
            username: username.to_string(),
            role,
            expires_at: std::time::Instant::now() + SESSION_TTL,
        },
    );
    let mut headers = HeaderMap::new();
    headers.insert(
        header::SET_COOKIE,
        set_cookie_value(&token, state.secure_cookies, false)
            .parse()
            .unwrap(),
    );
    let role_str = match role {
        Role::Viewer => "viewer",
        Role::Operator => "operator",
        Role::Admin => "admin",
    };
    (
        StatusCode::OK,
        headers,
        Json(json!({"username": username, "role": role_str})),
    )
}

async fn logout(State(state): State<Arc<AppState>>, headers: HeaderMap) -> impl IntoResponse {
    if let Some(token) = headers
        .get(header::COOKIE)
        .and_then(|v| v.to_str().ok())
        .and_then(token_from_cookie)
    {
        state.auth.remove_session(&token);
    }
    let mut out = HeaderMap::new();
    out.insert(
        header::SET_COOKIE,
        set_cookie_value("", state.secure_cookies, true)
            .parse()
            .unwrap(),
    );
    (StatusCode::OK, out, Json(json!({"ok": true})))
}

#[derive(Debug, Serialize)]
struct MeBody {
    username: String,
    role: Role,
}

async fn me(user: AuthedUser) -> Json<MeBody> {
    Json(MeBody {
        username: user.username,
        role: user.role,
    })
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/setup", post(setup))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .with_state(state)
}
