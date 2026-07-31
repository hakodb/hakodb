use axum::{
    extract::{ws::{Message, WebSocket, WebSocketUpgrade}, HeaderMap, State},
    http::StatusCode,
    response::Response,
    routing::{get, post},
    Json, Router,
};
use futures::{SinkExt, StreamExt};
use std::sync::Arc;
use tokio::sync::mpsc;
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

/// FIXED: Multiplexed WebSocket Loop with an MPSC channel to safely route Pong frames back to clients
async fn handle_websocket_loop(socket: WebSocket, engine: Arc<CloudSyncEngine>) {
    let (mut ws_sender, mut ws_receiver) = socket.split();
    let mut broadcast_rx = engine.broadcaster.subscribe();

    // Outbound channel to funnel both broadcasts and direct Pong responses to ws_sender
    let (outbound_tx, mut outbound_rx) = mpsc::channel::<Vec<u8>>(100);

    // Task 1: Dedicated Writer task to forward raw bytes to WebSocket
    let mut writer_task = tokio::spawn(async move {
        while let Some(bytes) = outbound_rx.recv().await {
            if ws_sender.send(Message::Binary(bytes)).await.is_err() {
                break;
            }
        }
    });

    // Task 2: Broadcast Subscriber task
    let tx_for_broadcast = outbound_tx.clone();
    let mut broadcast_task = tokio::spawn(async move {
        while let Ok(ops) = broadcast_rx.recv().await {
            if let Ok(bytes) = bincode::serialize(&ops) {
                if tx_for_broadcast.send(bytes).await.is_err() {
                    break;
                }
            }
        }
    });

    // Task 3: Reader task handling incoming Client Batches and Pings
    let engine_ref = engine.clone();
    let mut reader_task = tokio::spawn(async move {
        while let Some(Ok(msg)) = ws_receiver.next().await {
            if let Message::Binary(bytes) = msg {
                // A. Check for Heartbeat Ping Frame
                if let Ok(HeartbeatFrame::Ping { clock: client_clock, .. }) = bincode::deserialize(&bytes) {
                    let server_clock = engine_ref.vector_clock.read().await.clone();
                    let missed_ops = engine_ref.get_deltas_since(&client_clock).await;

                    let pong = HeartbeatFrame::Pong { server_clock, missed_ops };
                    if let Ok(pong_bytes) = bincode::serialize(&pong) {
                        // FIXED: Transmit Pong back down the outbound channel
                        let _ = outbound_tx.send(pong_bytes).await;
                    }
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
        _ = &mut writer_task => { reader_task.abort(); broadcast_task.abort(); },
        _ = &mut broadcast_task => { reader_task.abort(); writer_task.abort(); },
        _ = &mut reader_task => { writer_task.abort(); broadcast_task.abort(); },
    }
}