use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Clone)]
pub struct NodeManager {
    pub active_nodes: Arc<RwLock<Vec<String>>>,
    pub known_nodes: Vec<String>,
}

impl NodeManager {
    pub fn new(initial_nodes: Vec<String>) -> Self {
        Self {
            active_nodes: Arc::new(RwLock::new(initial_nodes.clone())),
            known_nodes: initial_nodes,
        }
    }

    /// FIXED: Periodically checks health and prunes unreachable nodes from active_nodes
    pub async fn start_health_check_loop(&self, http_client: reqwest::Client) {
        loop {
            tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;
            let mut healthy_nodes = Vec::new();

            for node in &self.known_nodes {
                let health_url = format!("{}/api/v1/cloud/clock", node);
                if let Ok(res) = http_client.get(&health_url).send().await {
                    if res.status().is_success() {
                        healthy_nodes.push(node.clone());
                        continue;
                    }
                }
                println!("❌ [Controller] Node {} failed health check and was pruned!", node);
            }

            let mut active = self.active_nodes.write().await;
            *active = healthy_nodes;
        }
    }
}