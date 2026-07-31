use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Vector Clock state tracking (Collection Name -> Sequence ID / High-Water Mark)
pub type VectorClock = HashMap<String, u64>;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BlobData {
    pub hash: String,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeRequest {
    pub node_id: String,
    pub auth_token: String,
    pub vector_clock: VectorClock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandshakeResponse {
    pub server_id: String,
    pub accepted: bool,
    pub server_clock: VectorClock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncOp {
    Upsert {
        collection: String,
        doc_id: String,
        version: u64,
        payload_bytes: Vec<u8>,        // Raw FireLiteDoc v5 binary
        attached_blobs: Vec<BlobData>, // Attached offloaded large binaries
    },
    Delete {
        collection: String,
        doc_id: String,
        version: u64,
    },
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncBatch {
    pub sender_id: String,
    pub ops: Vec<SyncOp>,
    pub vector_clock: VectorClock,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SyncResponse {
    pub success: bool,
    pub ops_applied: usize,
    pub updated_clock: VectorClock,
}

/// Heartbeat frame: keeps connections alive AND reconciles missing updates
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum HeartbeatFrame {
    Ping {
        client_id: String,
        clock: VectorClock,
    },
    Pong {
        server_clock: VectorClock,
        missed_ops: Vec<SyncOp>,
    },
}