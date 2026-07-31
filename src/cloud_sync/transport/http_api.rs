use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, HeaderMap, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use crate::cloud_sync::{
    protocol::{HandshakeRequest, HandshakeResponse, HeartbeatFrame, SyncBatch, SyncResponse, VectorClock},
    CloudSyncEngine,
};

pub fn build_cloud_router(engine: Arc<CloudSyncEngine>) -> Router {
    Router::new()
        .route("/api/v1/cloud/handshake", post(handle_handshake))
        .route("/api/v1/cloud/push", post(handle_push))
        .route("/api/v1/cloud/clock", get(handle_clock))
        .route("/api/v1/cloud/ws", get(handle_ws))
        .with_state(engine)
}

async fn handle_handshake(
    State(engine): State<Arc<CloudSyncEngine>>,
    Json(req): Json<HandshakeRequest>,
) -> Result<Json<HandshakeResponse>, StatusCode> {
    if !engine.auth.validate_token(&req.auth_token) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    let clock = engine.vector_clock.read().await;
    Ok(Json(HandshakeResponse {
        server_id: engine.config.node_id.clone(),
        accepted: true,
        server_clock: clock.clone(),
    }))
}

async fn handle_push(
    State(engine): State<Arc<CloudSyncEngine>>,
    headers: HeaderMap,
    Json(batch): Json<SyncBatch>,
) -> Result<Json<SyncResponse>, StatusCode> {
    let token = headers
        .get("authorization")
        .and_then(|h| h.to_str().ok())
        .unwrap_or_default();

    if !engine.auth.validate_token(token) {
        return Err(StatusCode::UNAUTHORIZED);
    }

    match engine.apply_sync_batch(batch).await {
        Ok(res) => Ok(Json(res)),
        Err(_) => Err(StatusCode::INTERNAL_SERVER_ERROR),
    }
}

async fn handle_clock(State(engine): State<Arc<CloudSyncEngine>>) -> Json<VectorClock> {
    let clock = engine.vector_clock.read().await;
    Json(clock.clone())
}

async fn handle_ws(ws: WebSocketUpgrade, State(engine): State<Arc<CloudSyncEngine>>) -> Response {
    ws.on_upgrade(move |socket| handle_websocket_loop(socket, engine))
}

/// Multiplexed WebSocket Loop: Micro-batch broadcasts + Ping/Pong Catch-up
async fn handle_websocket_loop(socket: WebSocket, engine: Arc<CloudSyncEngine>) {
    let (mut sender, mut receiver) = socket.split();
    let mut broadcast_rx = engine.broadcaster.subscribe();

    // Task 1: Stream micro-batched broadcasts to the client
    let mut send_task = tokio::spawn(async move {
        while let Ok(ops) = broadcast_rx.recv().await {
            if let Ok(bytes) = bincode::serialize(&ops) {
                if sender.send(Message::Binary(bytes)).await.is_err() {
                    break;
                }
            }
        }
    });

    // Task 2: Handle incoming client batches & Ping/Pong Heartbeat Catch-ups
    let engine_ref = engine.clone();
    let mut recv_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = receiver.next().await {
            if let Message::Binary(bytes) = msg {
                // A. Check for Heartbeat Ping Frame
                if let Ok(HeartbeatFrame::Ping { clock: client_clock, .. }) = bincode::deserialize(&bytes) {
                    let server_clock = engine_ref.vector_clock.read().await.clone();
                    let missed_ops = engine_ref.get_deltas_since(&client_clock).await;

                    let pong = HeartbeatFrame::Pong { server_clock, missed_ops };
                    let _ = bincode::serialize(&pong);
                    continue;
                }

                // B. Check for SyncBatch Frame
                if let Ok(batch) = bincode::deserialize::<SyncBatch>(&bytes) {
                    let _ = engine_ref.apply_sync_batch(batch).await;
                }
            }
        }
    });

    tokio::select! {
        _ = &mut send_task => recv_task.abort(),
        _ = &mut recv_task => send_task.abort(),
    }
}