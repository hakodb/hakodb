use tokio::time::{interval, Duration};
use std::sync::Arc;
use crate::cloud_sync::{
    protocol::{HeartbeatFrame, SyncBatch},
    CloudSyncEngine,
};

/// Client background worker handling periodic Heartbeat Pings and outbound pushes
pub struct CloudSyncClient {
    engine: Arc<CloudSyncEngine>,
    target_url: String,
}

impl CloudSyncClient {
    pub fn new(engine: Arc<CloudSyncEngine>, target_url: impl Into<String>) -> Self {
        Self {
            engine,
            target_url: target_url.into(),
        }
    }

    pub async fn start_heartbeat_loop(&self, ping_interval_secs: u64) {
        let mut timer = interval(Duration::from_secs(ping_interval_secs));
        let engine = self.engine.clone();

        tokio::spawn(async move {
            loop {
                timer.tick().await;

                let current_clock = engine.vector_clock.read().await.clone();
                let ping = HeartbeatFrame::Ping {
                    client_id: engine.config.node_id.clone(),
                    clock: current_clock,
                };

                let _bytes = bincode::serialize(&ping).unwrap();
                // Send _bytes over active WebSocket connection to firelite_controller
            }
        });
    }
}