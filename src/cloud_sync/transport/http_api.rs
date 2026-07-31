use crate::cloud_sync::config::CloudSyncConfig;
use crate::cloud_sync::protocol::{ClientHandshake, SyncFrame};
use futures::{SinkExt, StreamExt};
use reqwest::Client as HttpClient;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio_tungstenite::{
    connect_async,
    tungstenite::protocol::Message,
};
use tracing::{debug, error, info, warn};

pub struct WebSocketTransport;

impl WebSocketTransport {
    /// Connects to the server over WebSocket, exchanges handshakes, and processes frames.
    pub async fn run(
        ws_url: &str,
        config: &CloudSyncConfig,
        mut rx: mpsc::Receiver<Vec<u8>>,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        info!("Connecting to WebSocket endpoint at {}", ws_url);

        // Build WebSocket connection request with custom headers
        let request = http::Request::builder()
            .uri(ws_url)
            .header("Authorization", format!("Bearer {}", config.auth_token))
            .header("X-Client-ID", &config.client_id)
            .body(())?;

        let (ws_stream, response) = connect_async(request).await?;
        debug!("WebSocket handshake status: {}", response.status());

        let (mut write, mut read) = ws_stream.split();

        // 1. Send Initial Authentication Handshake Frame
        let handshake = ClientHandshake {
            client_id: config.client_id.clone(),
            auth_token: config.auth_token.clone(),
            protocol_version: 1,
        };
        let handshake_bytes = bincode::serialize(&handshake)?;
        write.send(Message::Binary(handshake_bytes)).await?;

        // Heartbeat timer interval
        let mut ping_interval = tokio::time::interval(Duration::from_secs(15));

        loop {
            tokio::select! {
                // Outbound local delta updates from WAL
                Some(payload) = rx.recv() => {
                    let frame = SyncFrame::Delta {
                        sender_id: config.client_id.clone(),
                        payload,
                    };
                    let bytes = bincode::serialize(&frame)?;
                    write.send(Message::Binary(bytes)).await?;
                }

                // Inbound remote frames from Cloud Controller
                incoming = read.next() => {
                    match incoming {
                        Some(Ok(msg)) => match msg {
                            Message::Binary(bytes) => {
                                Self::handle_inbound_frame(&bytes)?;
                            }
                            Message::Text(text) => {
                                debug!("Received text message from server: {}", text);
                            }
                            Message::Ping(payload) => {
                                write.send(Message::Pong(payload)).await?;
                            }
                            Message::Pong(_) => {
                                debug!("Received heartbeat ACK from cloud");
                            }
                            Message::Close(reason) => {
                                warn!("Server closed WebSocket connection: {:?}", reason);
                                break;
                            }
                            _ => {}
                        },
                        Some(Err(e)) => {
                            error!("WebSocket stream read error: {}", e);
                            return Err(Box::new(e));
                        }
                        None => {
                            warn!("WebSocket connection stream ended");
                            break;
                        }
                    }
                }

                // Periodic ping/heartbeat to keep connection active
                _ = ping_interval.tick() => {
                    write.send(Message::Ping(vec![])).await?;
                }
            }
        }

        Ok(())
    }

    /// Process incoming byte frames sent from the server.
    fn handle_inbound_frame(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let frame: SyncFrame = bincode::deserialize(bytes)?;
        match frame {
            SyncFrame::Delta { sender_id, payload } => {
                debug!("Received delta frame ({}) bytes from node: {}", payload.len(), sender_id);
                // Insert delta into local LWW/HLK engine or local FireLite instance
            }
            SyncFrame::Ack { sequence_number } => {
                debug!("Server acknowledged transaction sequence: {}", sequence_number);
            }
            SyncFrame::Error { message } => {
                error!("Cloud server returned error frame: {}", message);
            }
            _ => {}
        }
        Ok(())
    }

    /// Optional REST API Query execution for non-streaming sync operations.
    pub async fn send_rest_query(
        api_url: &str,
        config: &CloudSyncConfig,
        payload: serde_json::Value,
    ) -> Result<serde_json::Value, Box<dyn std::error::Error + Send + Sync>> {
        let client = HttpClient::new();
        let res = client
            .post(format!("{}/query", api_url))
            .bearer_auth(&config.auth_token)
            .json(&payload)
            .send()
            .await?
            .json::<serde_json::Value>()
            .await?;

        Ok(res)
    }
}