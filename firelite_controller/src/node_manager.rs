use std::path::PathBuf;
use std::sync::Arc;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;
use serde::{Deserialize, Serialize};
use tracing::{info, warn, error};
use axum::extract::ws::{WebSocket, Message};
use futures::{SinkExt, StreamExt};

use crate::ring::ConsistentHashRing;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClusterConfig {
    pub num_instances: usize, // Default = 8
    pub base_data_dir: PathBuf,
    pub http_port: u16,
    pub tcp_port: u16,
    pub auth_token: String,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            num_instances: 8,
            base_data_dir: PathBuf::from("./firelite_cluster_data"),
            http_port: 8080,
            tcp_port: 9090,
            auth_token: "change_me_in_production".to_string(),
        }
    }
}

impl ClusterConfig {
    pub fn load_or_default(path: &str) -> Result<Self, Box<dyn std::error::Error>> {
        if std::path::Path::new(path).exists() {
            let content = std::fs::read_to_string(path)?;
            Ok(serde_json::from_str(&content)?)
        } else {
            let cfg = Self::default();
            std::fs::write(path, serde_json::to_string_pretty(&cfg)?)?;
            Ok(cfg)
        }
    }
}

pub struct ClusterManager {
    config: ClusterConfig,
    hash_ring: ConsistentHashRing,
    // Represents process sharding engines or instances handles
    instances: Vec<Arc<RwLock<()>>>, 
}

impl ClusterManager {
    pub async fn new(config: ClusterConfig) -> Result<Self, Box<dyn std::error::Error>> {
        std::fs::create_dir_all(&config.base_data_dir)?;

        let mut hash_ring = ConsistentHashRing::new();
        let mut instances = Vec::with_capacity(config.num_instances);

        for i in 0..config.num_instances {
            let shard_path = config.base_data_dir.join(format!("shard_{}", i));
            std::fs::create_dir_all(&shard_path)?;

            // Initialize or bind embedded FireLite Engine instance for shard_i
            instances.push(Arc::new(RwLock::new(())));
            hash_ring.add_node(i);
            info!("Initialized Shard Engine [{}] at {:?}", i, shard_path);
        }

        Ok(Self {
            config,
            hash_ring,
            instances,
        })
    }

    pub async fn start_tcp_listener(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let listener = TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        info!("Dedicated TLS/TCP Sync Port open on 0.0.0.0:{}", port);

        loop {
            let (stream, addr) = listener.accept().await?;
            let auth_token = self.config.auth_token.clone();
            tokio::spawn(async move {
                if let Err(e) = Self::handle_raw_tcp(stream, auth_token).await {
                    warn!("TCP Socket connection closed with error from {}: {}", addr, e);
                }
            });
        }
    }

    async fn handle_raw_tcp(mut stream: TcpStream, _auth_token: String) -> Result<(), Box<dyn std::error::Error>> {
        // Authenticate client frame and perform replication frame parsing
        Ok(())
    }

    pub async fn handle_websocket(&self, mut socket: WebSocket) -> Result<(), Box<dyn std::error::Error>> {
        while let Some(msg) = socket.recv().await {
            let msg = msg?;
            match msg {
                Message::Binary(bytes) => {
                    // Frame processing & routing to internal shard
                    socket.send(Message::Binary(bytes)).await?;
                }
                Message::Close(_) => break,
                _ => {}
            }
        }
        Ok(())
    }

    pub async fn route_query(&self, payload: serde_json::Value) -> Result<serde_json::Value, Box<dyn std::error::Error>> {
        let doc_id = payload.get("id").and_then(|v| v.as_str()).unwrap_or("default");
        let shard_id = self.hash_ring.get_node(doc_id);
        
        info!("Routing document [{}] to Shard Engine [{}]", doc_id, shard_id);
        // Execute routed operation on targeted shard
        Ok(serde_json::json!({ "status": "ok", "shard": shard_id }))
    }
}