//! HTTP application: shared state, routes, handlers.

use axum::{extract::State, routing::get, Json, Router};
use firelite::engine::FireLite;
use serde_json::{json, Value};
use std::sync::Arc;

#[derive(Clone)]
pub struct AppState {
    pub db: Arc<FireLite>,
}

impl AppState {
    pub fn new(db: FireLite) -> Self {
        Self { db: Arc::new(db) }
    }
}

async fn health(State(state): State<Arc<AppState>>) -> Json<Value> {
    let collections = state.db.list_collections().map(|c| c.len()).unwrap_or(0);
    Json(json!({
        "status": "ok",
        "version": env!("CARGO_PKG_VERSION"),
        "collections": collections,
    }))
}

pub fn build_router(state: Arc<AppState>) -> Router {
    Router::new()
        .route("/api/health", get(health))
        .with_state(state)
}
