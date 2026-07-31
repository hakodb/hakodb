use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TransportMode {
    TcpDedicated {
        endpoint: String, // e.g., "cloud.firelite.db:9090"
        use_tls: bool,
    },
    WebSocketApi {
        ws_url: String,   // e.g., "wss://cloud.firelite.db/ws/sync"
        api_url: String,  // e.g., "https://cloud.firelite.db/api/v1"
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CloudSyncConfig {
    pub mode: TransportMode,
    pub auth_token: String,
    pub client_id: String,
    pub auto_reconnect: bool,
    pub sync_interval_ms: u64,
}

impl Default for CloudSyncConfig {
    fn default() -> Self {
        Self {
            mode: TransportMode::WebSocketApi {
                ws_url: "ws://127.0.0.1:8080/ws/sync".to_string(),
                api_url: "http://127.0.0.1:8080/api/v1".to_string(),
            },
            auth_token: String::new(),
            client_id: format!("node-{}", rand::random::<u32>()),
            auto_reconnect: true,
            sync_interval_ms: 5000,
        }
    }
}