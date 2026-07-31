use axum::{
    extract::{ws::WebSocket, State, WebSocketUpgrade},
    response::IntoResponse,
    routing::{get, post},
    Json, Router,
};
use clap::Parser;
use std::net::SocketAddr;
use std::sync::Arc;
use tracing::{info, error};

mod node_manager;
mod ring;

use node_manager::{ClusterConfig, ClusterManager};

#[derive(Parser, Debug)]
#[command(author, version, about = "FireLite Cloud Controller Daemon")]
struct Args {
    #[arg(short, long, default_value = "config.json")]
    config: String,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt::init();

    let args = Args::parse();
    let config = ClusterConfig::load_or_default(&args.config)?;
    
    info!("Starting FireLite Controller with {} shards...", config.num_instances);
    let manager = Arc::new(ClusterManager::new(config.clone()).await?);

    // Launch dedicated TCP Listener in background
    let tcp_manager = Arc::clone(&manager);
    let tcp_port = config.tcp_port;
    tokio::spawn(async move {
        if let Err(e) = tcp_manager.start_tcp_listener(tcp_port).await {
            error!("TCP server error: {}", e);
        }
    });

    // HTTP & WebSocket API Gateway
    let app = Router::new()
        .route("/health", get(|| async { "OK" }))
        .route("/ws/sync", get(ws_handler))
        .route("/api/v1/query", post(api_query_handler))
        .with_state(manager);

    let addr: SocketAddr = format!("0.0.0.0:{}", config.http_port).parse()?;
    info!("FireLite Controller HTTP/WS listening on http://{}", addr);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn ws_handler(
    ws: WebSocketUpgrade,
    State(manager): State<Arc<ClusterManager>>,
) -> impl IntoResponse {
    ws.on_upgrade(|socket| async move {
        if let Err(e) = manager.handle_websocket(socket).await {
            error!("WebSocket stream error: {}", e);
        }
    })
}

async fn api_query_handler(
    State(manager): State<Arc<ClusterManager>>,
    Json(payload): Json<serde_json::Value>,
) -> impl IntoResponse {
    match manager.route_query(payload).await {
        Ok(res) => (axum::http::StatusCode::OK, Json(res)),
        Err(e) => (
            axum::http::StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({ "error": e.to_string() })),
        ),
    }
}