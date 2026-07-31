use std::sync::Arc;
use tokio::sync::mpsc;
use tracing::{info, error};

use crate::cloud_sync::config::{CloudSyncConfig, TransportMode};
use crate::cloud_sync::transport::tcp_tls::TcpTransport;
use crate::cloud_sync::transport::http_api::WebSocketTransport;

pub struct CloudSyncClient {
    config: CloudSyncConfig,
    tx_outbound: mpsc::Sender<Vec<u8>>,
}

impl CloudSyncClient {
    pub async fn start(config: CloudSyncConfig) -> Result<Self, Box<dyn std::error::Error + Send + Sync>> {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(1024);
        let client_cfg = config.clone();

        tokio::spawn(async move {
            loop {
                info!("Initializing CloudSync transport loop...");
                let result = match &client_cfg.mode {
                    TransportMode::TcpDedicated { endpoint, use_tls } => {
                        TcpTransport::run(endpoint, *use_tls, &client_cfg, rx).await
                    }
                    TransportMode::WebSocketApi { ws_url, .. } => {
                        WebSocketTransport::run(ws_url, &client_cfg, rx).await
                    }
                };

                if let Err(e) = result {
                    error!("CloudSync Connection lost: {}. Retrying...", e);
                }

                if !client_cfg.auto_reconnect {
                    break;
                }
                tokio::time::sleep(tokio::time::Duration::from_secs(3)).await;
            }
        });

        Ok(Self {
            config,
            tx_outbound: tx,
        })
    }

    pub async fn push_delta(&self, delta_bytes: Vec<u8>) -> Result<(), Box<dyn std::error::Error>> {
        self.tx_outbound.send(delta_bytes).await?;
        Ok(())
    }
}