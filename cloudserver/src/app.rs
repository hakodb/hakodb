//! HTTP application: shared state, routes, handlers.

use axum::{
    async_trait,
    extract::{ConnectInfo, FromRef, FromRequestParts, Path, State},
    http::{header, request::Parts, HeaderMap, StatusCode},
    middleware::{self, Next},
    response::{IntoResponse, Response},
    routing::{delete, get, post, put},
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
use crate::groups::{    add_member, create_group, delete_group, get_group, list_groups, remove_member,
    rotate_group_key, set_group_mode, GroupMode,
};

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<FireLite>,
    pub auth: Arc<AuthStore>,
    pub secure_cookies: bool,
    /// Sync plane handle (None in tests / before boot).
    pub sync: Option<Arc<firelite::cloud_sync::CloudSync>>,
    /// Resolved server config snapshot for display (None in tests).
    pub config: Option<crate::config::ServerConfig>,
}

impl AppState {
    pub fn new(db: Arc<FireLite>, secure_cookies: bool) -> Self {
        Self {
            db,
            auth: Arc::new(AuthStore::default()),
            secure_cookies,
            sync: None,
            config: None,
        }
    }

    pub fn with_sync(mut self, sync: Arc<firelite::cloud_sync::CloudSync>) -> Self {
        self.sync = Some(sync);
        self
    }

    pub fn with_config(mut self, config: crate::config::ServerConfig) -> Self {
        self.config = Some(config);
        self
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

/// Public probe driving the setup wizard: true only while no enabled
/// admin exists. No auth (it must work before any account exists); reveals
/// nothing beyond a single bit.
async fn setup_status(State(state): State<Arc<AppState>>) -> Json<Value> {
    Json(json!({ "setup_required": setup_required(&state.db) }))
}

/// Non-sensitive server facts for the console config view. The sync token
/// and any secret are deliberately never serialized here.
async fn config_view(State(state): State<Arc<AppState>>, user: AuthedUser) -> Response {
    if let Err(e) = require_role(&user, Role::Operator) {
        return e;
    }
    let (admin_bind, log_level, server_id) = match &state.config {
        Some(c) => (
            c.admin_bind.clone(),
            c.log_level.clone(),
            c.server_id.clone(),
        ),
        None => ("unknown".into(), "unknown".into(), "unknown".into()),
    };
    Json(json!({
        "version": env!("CARGO_PKG_VERSION"),
        "admin_bind": admin_bind,
        "log_level": log_level,
        "server_id": server_id,
    }))
    .into_response()
}

#[derive(rust_embed::RustEmbed)]
#[folder = "static/"]
struct StaticAssets;

fn content_type(path: &str) -> &'static str {
    if path.ends_with(".html") {
        "text/html; charset=utf-8"
    } else if path.ends_with(".css") {
        "text/css; charset=utf-8"
    } else if path.ends_with(".js") {
        "text/javascript; charset=utf-8"
    } else if path.ends_with(".json") {
        "application/json"
    } else if path.ends_with(".png") {
        "image/png"
    } else {
        "application/octet-stream"
    }
}

async fn static_file(Path(path): Path<String>) -> Response {
    let key = path.trim_start_matches('/');
    // index + SPA fallback: unknown non-asset paths serve the shell so
    // hash-routing deep links work when opened directly.
    let key = if key.is_empty() { "index.html" } else { key };
    if key.contains("..") {
        return (StatusCode::BAD_REQUEST, "bad path").into_response();
    }
    match StaticAssets::get(key) {
        Some(f) => (
            [(header::CONTENT_TYPE, content_type(key))],
            f.data.into_owned(),
        )
            .into_response(),
        None if !key.contains('.') => match StaticAssets::get("index.html") {
            Some(f) => (
                [(header::CONTENT_TYPE, "text/html; charset=utf-8")],
                f.data.into_owned(),
            )
                .into_response(),
            None => (StatusCode::NOT_FOUND, "no ui").into_response(),
        },
        None => (StatusCode::NOT_FOUND, "not found").into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct CreateUserBody {
    username: String,
    password: String,
    #[serde(default = "viewer_default")]
    role: String,
}

fn viewer_default() -> String {
    "viewer".to_string()
}

fn parse_role(s: &str) -> Result<Role, Response> {
    Role::parse(s.trim().to_lowercase().as_str()).ok_or_else(|| {
        (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": "role must be viewer|operator|admin"})),
        )
            .into_response()
    })
}

async fn list_users_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    Json(json!({ "users": crate::users::list_users(&state.db) })).into_response()
}

async fn create_user_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Json(body): Json<CreateUserBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    let role = match parse_role(&body.role) {
        Ok(r) => r,
        Err(e) => return e,
    };
    match crate::users::create_user(&state.db, body.username.trim(), &body.password, role) {
        Ok(u) => (StatusCode::CREATED, Json(json!({ "user": u }))).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct UpdateUserBody {
    role: Option<String>,
    disabled: Option<bool>,
    password: Option<String>,
}

async fn update_user_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
    Json(body): Json<UpdateUserBody>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    let role = match body.role {
        Some(ref r) => match parse_role(r) {
            Ok(role) => Some(role),
            Err(e) => return e,
        },
        None => None,
    };
    match crate::users::update_user(
        &state.db,
        &user.username,
        &name,
        role,
        body.disabled,
        body.password.as_deref(),
    ) {
        Ok(u) => Json(json!({ "user": u })).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

async fn delete_user_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
) -> Response {
    if let Err(e) = require_role(&user, Role::Admin) {
        return e;
    }
    match crate::users::delete_user(&state.db, &user.username, &name) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

fn require_admin(user: &AuthedUser) -> Result<(), Response> {
    require_role(user, Role::Admin)
}

pub(crate) fn require_role(user: &AuthedUser, need: Role) -> Result<(), Response> {
    if user.role.at_least(need) {
        Ok(())
    } else {
        Err((
            StatusCode::FORBIDDEN,
            Json(json!({"error": "insufficient role"})),
        )
            .into_response())
    }
}

#[derive(Debug, Deserialize)]
struct CreateGroupBody {
    room_name: String,
    mode: GroupMode,
}

async fn create_group_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Json(body): Json<CreateGroupBody>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match create_group(&state.db, body.room_name.trim(), body.mode, new_session_token) {
        Ok((view, key)) => (
            StatusCode::CREATED,
            Json(json!({"group": view, "api_key": key})),
        )
            .into_response(),
        Err(e) => (
            StatusCode::CONFLICT,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

async fn list_groups_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    Json(json!({"groups": list_groups(&state.db)})).into_response()
}

async fn get_group_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match get_group(&state.db, &name) {
        Some(g) => Json(json!({"group": g})).into_response(),
        None => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": "unknown group"})),
        )
            .into_response(),
    }
}

async fn rotate_key_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match rotate_group_key(&state.db, &name, new_session_token) {
        Ok(key) => Json(json!({"api_key": key})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct SetModeBody {
    mode: GroupMode,
}

async fn set_mode_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
    Json(body): Json<SetModeBody>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match set_group_mode(&state.db, &name, body.mode) {
        Ok(g) => Json(json!({"group": g})).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

#[derive(Debug, Deserialize)]
struct MemberBody {
    client_id: String,
}

async fn add_member_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
    Json(body): Json<MemberBody>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match add_member(&state.db, &name, &body.client_id) {
        Ok(g) => Json(json!({"group": g})).into_response(),
        Err(e) => (
            StatusCode::BAD_REQUEST,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

async fn remove_member_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path((name, client_id)): Path<(String, String)>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match remove_member(&state.db, &name, &client_id) {
        Ok(g) => Json(json!({"group": g})).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

async fn delete_group_route(
    State(state): State<Arc<AppState>>,
    user: AuthedUser,
    Path(name): Path<String>,
) -> Response {
    if let Err(e) = require_admin(&user) {
        return e;
    }
    match delete_group(&state.db, &name) {
        Ok(()) => Json(json!({"ok": true})).into_response(),
        Err(e) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error": e})),
        )
            .into_response(),
    }
}

/// Defense-in-depth response headers for every response (API + UI).
/// No `Server` version leak, no framing/sniffing, no embedding. HSTS only
/// makes sense over TLS, so it rides on the Secure-cookie flag.
async fn security_headers(
    State(state): State<Arc<AppState>>,
    req: axum::http::Request<axum::body::Body>,
    next: Next,
) -> Response {
    let hsts = state.secure_cookies;
    let mut res = next.run(req).await;
    let h = res.headers_mut();
    h.insert("x-content-type-options", "nosniff".parse().unwrap());
    h.insert("x-frame-options", "DENY".parse().unwrap());
    h.insert("referrer-policy", "no-referrer".parse().unwrap());
    h.insert(
        "content-security-policy",
        "default-src 'self'; style-src 'self' 'unsafe-inline'; script-src 'self'; connect-src 'self'; img-src 'self' data:; object-src 'none'; base-uri 'none'; frame-ancestors 'none'"
            .parse()
            .unwrap(),
    );
    h.remove(header::SERVER);
    if hsts {
        h.insert(
            "strict-transport-security",
            "max-age=31536000; includeSubDomains".parse().unwrap(),
        );
    }
    res
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .route("/api/setup", post(setup))
        .route("/api/login", post(login))
        .route("/api/logout", post(logout))
        .route("/api/me", get(me))
        .route("/api/setup/status", get(setup_status))
        .route("/api/config", get(config_view))
        .route("/", get(|| async { static_file(Path("index.html".to_string())).await }))
        .route("/*path", get(static_file))
        .route("/api/users", post(create_user_route).get(list_users_route))
        .route(
            "/api/users/:name",
            put(update_user_route).delete(delete_user_route),
        )
        .route("/api/groups", post(create_group_route).get(list_groups_route))
        .route("/api/groups/:name", get(get_group_route).delete(delete_group_route))
        .route("/api/groups/:name/rotate-key", post(rotate_key_route))
        .route("/api/groups/:name/mode", put(set_mode_route))
        .route("/api/groups/:name/members", post(add_member_route))
        .route(
            "/api/groups/:name/members/:client_id",
            delete(remove_member_route),
        )
        .route("/api/events", get(crate::events::events))
        .merge(crate::data::data_routes())
        .layer(middleware::from_fn_with_state(
            state.clone(),
            security_headers,
        ))
        .with_state(state)
}
