use serde::{Deserialize, Serialize};

/// Handshake frame sent upon initiating connection
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ClientHandshake {
    pub client_id: String,
    pub auth_token: String,
    pub protocol_version: u32,
}

/// Server Handshake response
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ServerHandshakeResponse {
    pub success: bool,
    pub server_version: String,
    pub assigned_shard: usize,
    pub message: Option<String>,
}

/// Binary wire frames streaming across client & cloud controller
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum SyncFrame {
    /// Authentication handshake
    Handshake(ClientHandshake),
    
    /// Write-Ahead Log (WAL) or CRDT state delta payload
    Delta {
        sender_id: String,
        payload: Vec<u8>,
    },
    
    /// Sequence acknowledgement
    Ack {
        sequence_number: u64,
    },
    
    /// Synchronization request for a range of logical timestamps
    FetchMissing {
        since_sequence: u64,
    },

    /// Server error frame
    Error {
        message: String,
    },
}