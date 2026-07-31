use axum::{
    extract::{Path, State},
    http::StatusCode,
    routing::{get, post},
    Json, Router,
};
use reqwest::Client as HttpClient;
use serde_json::Value;
use std::sync::{
    atomic::{AtomicUsize, Ordering},
    Arc,
};

mod node_manager;
mod ring;

use node_manager::NodeManager;
use ring::ConsistentHashRing;

#[derive(Clone)]
pub struct ControllerState {
    pub http_client: HttpClient,
    pub hash_ring: ConsistentHashRing,
    pub node_manager: NodeManager,
    pub read_rr_counter: Arc<AtomicUsize>,
}

#[tokio::main]
async fn main() {
    let nodes = vec![
        "http://127.0.0.1:8081".to_string(),
        "http://127.0.0.1:8082".to_string(),
        "http://127.0.0.1:8083".to_string(),
    ];

    let http_client = HttpClient::new();
    let node_manager = NodeManager::new(nodes.clone());

    // Spawn background health checker
    let nm_clone = node_manager.clone();
    let client_clone = http_client.clone();
    tokio::spawn(async move {
        nm_clone.start_health_check_loop(client_clone).await;
    });

    let state = ControllerState {
        http_client,
        hash_ring: ConsistentHashRing::new(nodes.clone(), 10),
        node_manager,
        read_rr_counter: Arc::new(AtomicUsize::new(0)),
    };

    let app = Router::new()
        // Deterministic Write Routes -> Owner node derived via HashRing
        .route("/api/v1/:collection/doc/:id", post(handle_write))
        // Load Balanced Read Routes -> Round Robin distribution
        .route("/api/v1/:collection/doc/:id", get(handle_read))
        .with_state(state);

    let addr = "0.0.0.0:8000";
    let listener = tokio::net::TcpListener::bind(addr).await.unwrap();
    println!("🚀 FireLite Cloud Controller listening on http://{}", addr);
    axum::serve(listener, app).await.unwrap();
}

async fn handle_write(
    State(state): State<ControllerState>,
    Path((collection, doc_id)): Path<(String, String)>,
    Json(payload): Json<Value>,
) -> Result<Json<Value>, StatusCode> {
    let target_node = state
        .hash_ring
        .get_owner_node(&doc_id)
        .ok_or(StatusCode::SERVICE_UNAVAILABLE)?;

    let target_url = format!("{}/api/v1/{}/doc/{}", target_node, collection, doc_id);

    let response = state
        .http_client
        .post(&target_url)
        .json(&payload)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    if response.status().is_success() {
        let res_json = response
            .json::<Value>()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(Json(res_json))
    } else {
        Err(StatusCode::from_u16(response.status().as_u16()).unwrap_or(StatusCode::INTERNAL_SERVER_ERROR))
    }
}

async fn handle_read(
    State(state): State<ControllerState>,
    Path((collection, doc_id)): Path<(String, String)>,
) -> Result<Json<Value>, StatusCode> {
    let active_nodes = state.node_manager.active_nodes.read().await;
    if active_nodes.is_empty() {
        return Err(StatusCode::SERVICE_UNAVAILABLE);
    }

    let idx = state.read_rr_counter.fetch_add(1, Ordering::Relaxed) % active_nodes.len();
    let target_node = &active_nodes[idx];
    let target_url = format!("{}/api/v1/{}/doc/{}", target_node, collection, doc_id);

    let response = state
        .http_client
        .get(&target_url)
        .send()
        .await
        .map_err(|_| StatusCode::BAD_GATEWAY)?;

    if response.status().is_success() {
        let res_json = response
            .json::<Value>()
            .await
            .map_err(|_| StatusCode::INTERNAL_SERVER_ERROR)?;
        Ok(Json(res_json))
    } else {
        Err(StatusCode::NOT_FOUND)
    }
}