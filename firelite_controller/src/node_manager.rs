use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NodeManager {
    pub active_nodes: Arc<RwLock<Vec<String>>>,
}

impl NodeManager {
    pub fn new(initial_nodes: Vec<String>) -> Self {
        Self {
            active_nodes: Arc::new(RwLock::new(initial_nodes)),
        }
    }

    pub async fn start_health_check_loop(&self, http_client: reqwest::Client) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let nodes = self.active_nodes.read().await.clone();

            for node in nodes {
                let health_url = format!("{}/api/v1/cloud/clock", node);
                if let Ok(res) = http_client.get(&health_url).send().await {
                    if !res.status().is_success() {
                        println!("⚠️ [Controller] Node {} returned non-200 state", node);
                    }
                } else {
                    println!("❌ [Controller] Node {} unreachable!", node);
                }
            }
        }
    }
}