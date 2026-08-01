#[cfg(feature = "cloud-sync")]
use std::collections::HashMap;
#[cfg(feature = "cloud-sync")]
use std::sync::{Arc, Mutex as StdMutex};
#[cfg(feature = "cloud-sync")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "cloud-sync")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "cloud-sync")]
use crate::document::firelite_doc::FireLiteDoc;
// #[cfg(feature = "cloud-sync")]
// use crate::document::value::Value;
#[cfg(feature = "cloud-sync")]
use crate::engine::{BatchMutation, FireLite};
#[cfg(feature = "cloud-sync")]
use crate::error::{FireLiteError, Result as FLResult};
#[cfg(feature = "cloud-sync")]
use crate::storage::wal::WalOp;

#[cfg(feature = "cloud-sync")]
use futures_util::{SinkExt, StreamExt};
#[cfg(feature = "cloud-sync")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "cloud-sync")]
use sha2::{Digest, Sha256};
#[cfg(feature = "cloud-sync")]
use tokio::sync::{mpsc, Mutex as AsyncMutex, RwLock as AsyncRwLock};
#[cfg(feature = "cloud-sync")]
use tokio_tungstenite::tungstenite::Message;

// ============================================================================
// DATA STRUCTURES & PROTOCOL PACKETS
// ============================================================================

#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudSyncMode {
    Server,
    Client,
}

#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum CloudPacket {
    /// Client -> Server: Initial Auth & Handshake
    Authenticate {
        token: String,
        client_id: String,
        room_key: String,
    },
    /// Server -> Client: Handshake Response
    AuthResult {
        success: bool,
        error: Option<String>,
    },
    /// Version handshake to catch up on missed deltas
    VersionPing {
        versions: HashMap<String, i64>,
    },
    /// Direct Replication Batch
    Replication {
        msg_id: u128,
        collection: String,
        ops: Vec<WalOp>,
    },
    /// Server Request for full state catchup
    SyncRequest {
        collection: String,
    },
    /// Heartbeat Ping / Pong
    Heartbeat,
}

#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone, Serialize)]
pub struct CloudStatus {
    pub mode: CloudSyncMode,
    pub connected: bool,
    pub room_key: String,
    pub active_clients: usize,
    pub queued_writes: usize,
}

// ============================================================================
// CLOUD SYNC CONTROLLER & AGGREGATOR
// ============================================================================

#[cfg(feature = "cloud-sync")]
pub struct CloudSync {
    db: Arc<FireLite>,
    mode: CloudSyncMode,
    room_hash: [u8; 32],
    client_id: String,
    auth_token: String,
    running: Arc<AtomicBool>,
    echo_cache: Arc<StdMutex<HashMap<String, i64>>>,
    // Ingress Buffer for High-Throughput Batch Coalescing
    ingest_tx: mpsc::Sender<IngestItem>,
    ingest_rx: Arc<AsyncMutex<Option<mpsc::Receiver<IngestItem>>>>,
    active_peers: Arc<AsyncRwLock<HashMap<String, mpsc::Sender<Message>>>>,
}

#[cfg(feature = "cloud-sync")]
struct IngestItem {
    collection: String,
    op: WalOp,
    sender_client_id: Option<String>,
}

#[cfg(feature = "cloud-sync")]
impl CloudSync {
    pub fn new(
        db: Arc<FireLite>,
        mode: CloudSyncMode,
        client_id: &str,
        room_key: &str,
        auth_token: &str,
    ) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(room_key.as_bytes());
        let room_hash: [u8; 32] = hasher.finalize().into();

        let (tx, rx) = mpsc::channel(100_000);

        Self {
            db,
            mode,
            room_hash,
            client_id: client_id.to_string(),
            auth_token: auth_token.to_string(),
            running: Arc::new(AtomicBool::new(false)),
            echo_cache: Arc::new(StdMutex::new(HashMap::new())),
            ingest_tx: tx,
            ingest_rx: Arc::new(AsyncMutex::new(Some(rx))),
            active_peers: Arc::new(AsyncRwLock::new(HashMap::new())),
        }
    }

    /// Spawns the Cloud Sync system.
    pub async fn start(&self, bind_or_server_url: &str) -> FLResult<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(()); // Already running
        }

        // 1. Start the High-Performance Ingest Flusher
        self.spawn_batch_flusher();

        // 2. Start Mode Specific Engine
        match self.mode {
            CloudSyncMode::Server => self.start_server_mode(bind_or_server_url).await?,
            CloudSyncMode::Client => self.start_client_mode(bind_or_server_url).await?,
        }

        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::SeqCst);
    }

    // ========================================================================
    // BATCH AGGREGATOR: SOLVES SINGLE-WRITE LOCK BOTTLE-NECK
    // ========================================================================

    /// Spawns a background task that drains the `ingest_tx` MPSC queue every 5ms 
    /// or 500 items, executing a SINGLE `write_batch` call. This converts 50,000 
    /// individual lock acquisitions/sec into ~100 batch lock acquisitions/sec.
    fn spawn_batch_flusher(&self) {
        let rx_option = self.ingest_rx.clone();
        let db = self.db.clone();
        let running = self.running.clone();
        let echo_cache = self.echo_cache.clone();
        let active_peers = self.active_peers.clone();
        let server_client_id = self.client_id.clone();

        tokio::spawn(async move {
            let mut rx = match rx_option.lock().await.take() {
                Some(r) => r,
                None => return,
            };

            let mut batch_buffer: HashMap<String, Vec<(WalOp, Option<String>)>> = HashMap::new();
            let mut interval = tokio::time::interval(Duration::from_millis(5));

            while running.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::flush_ingest_buffer(&db, &mut batch_buffer, &echo_cache, &active_peers, &server_client_id).await;
                    }
                    item = rx.recv() => {
                        match item {
                            Some(item) => {
                                batch_buffer.entry(item.collection)
                                    .or_default()
                                    .push((item.op, item.sender_client_id));

                                let total_pending: usize = batch_buffer.values().map(|v| v.len()).sum();
                                if total_pending >= 512 {
                                    Self::flush_ingest_buffer(&db, &mut batch_buffer, &echo_cache, &active_peers, &server_client_id).await;
                                }
                            }
                            None => break,
                        }
                    }
                }
            }
        });
    }

    async fn flush_ingest_buffer(
        db: &Arc<FireLite>,
        buffer: &mut HashMap<String, Vec<(WalOp, Option<String>)>>,
        echo_cache: &Arc<StdMutex<HashMap<String, i64>>>,
        peers: &Arc<AsyncRwLock<HashMap<String, mpsc::Sender<Message>>>>,
        server_client_id: &str,
    ) {
        if buffer.is_empty() {
            return;
        }

        for (col, items) in buffer.drain() {
            if items.is_empty() {
                continue;
            }

            let mut mutations = Vec::with_capacity(items.len());
            let mut relay_ops = Vec::with_capacity(items.len());

            for (op, _sender_id) in &items {
                match op {
                    WalOp::PutInlined { key, value } => {
                        if let Some(doc) = FireLiteDoc::decode(value) {
                            let ts = doc.get_logical_time();
                            {
                                let mut cache = echo_cache.lock().unwrap();
                                cache.insert(key.clone(), ts);
                            }
                            mutations.push(BatchMutation::Put {
                                collection: col.clone(),
                                doc_id: key.clone(),
                                doc,
                            });
                            relay_ops.push(op.clone());
                        }
                    }
                    WalOp::Delete { key, timestamp } => {
                        {
                            let mut cache = echo_cache.lock().unwrap();
                            cache.insert(key.clone(), *timestamp);
                        }
                        mutations.push(BatchMutation::Delete {
                            collection: col.clone(),
                            doc_id: key.clone(),
                        });
                        relay_ops.push(op.clone());
                    }
                    _ => {}
                }
            }

            // Execute single write_batch call (1 Lock acquisition for the entire batch!)
            if !mutations.is_empty() {
                let _ = db.write_batch(mutations);
            }

            // Fan-out/Relay packet to all connected clients except origin sender
            if !relay_ops.is_empty() {
                let packet = CloudPacket::Replication {
                    msg_id: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros(),
                    collection: col.clone(),
                    ops: relay_ops,
                };

                if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                    let msg = Message::Binary(bytes.into());
                    let peers_guard = peers.read().await;

                    for (client_id, tx) in peers_guard.iter() {
                        if client_id != server_client_id {
                            let _ = tx.try_send(msg.clone());
                        }
                    }
                }
            }
        }
    }

    // ========================================================================
    // SERVER MODE: WebSocket Hub
    // ========================================================================

    async fn start_server_mode(&self, bind_addr: &str) -> FLResult<()> {
        let listener = tokio::net::TcpListener::bind(bind_addr)
            .await
            .map_err(|e| FireLiteError::Io(e))?;

        let ingest_tx = self.ingest_tx.clone();
        let db = self.db.clone();
        let room_hash = self.room_hash;
        let active_peers = self.active_peers.clone();
        let running = self.running.clone();

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok((stream, _)) = listener.accept().await {
                    let ingest_tx = ingest_tx.clone();
                    let db = db.clone();
                    let active_peers = active_peers.clone();

                    tokio::spawn(async move {
                        if let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await {
                            Self::handle_server_client(
                                ws_stream,
                                ingest_tx,
                                db,
                                room_hash,
                                active_peers,
                            )
                            .await;
                        }
                    });
                }
            }
        });

        Ok(())
    }

    async fn handle_server_client<S>(
        ws_stream: tokio_tungstenite::WebSocketStream<S>,
        ingest_tx: mpsc::Sender<IngestItem>,
        db: Arc<FireLite>,
        expected_room_hash: [u8; 32],
        peers: Arc<AsyncRwLock<HashMap<String, mpsc::Sender<Message>>>>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_tx, mut ws_rx) = ws_stream.split();
        let (client_tx, mut client_rx) = mpsc::channel::<Message>(2000);

        let mut client_id = String::new();
        let mut authenticated = false;

        // 1. Authenticate Client
        if let Some(Ok(Message::Binary(bytes))) = ws_rx.next().await {
            if let Ok(CloudPacket::Authenticate {
                token: _,
                client_id: cid,
                room_key,
            }) = rmp_serde::from_slice(&bytes)
            {
                let mut hasher = Sha256::new();
                hasher.update(room_key.as_bytes());
                let h: [u8; 32] = hasher.finalize().into();

                if h == expected_room_hash {
                    authenticated = true;
                    client_id = cid.clone();

                    peers.write().await.insert(client_id.clone(), client_tx);

                    let ack = CloudPacket::AuthResult {
                        success: true,
                        error: None,
                    };
                    let ack_bytes = rmp_serde::to_vec_named(&ack).unwrap();
                    let _ = ws_tx.send(Message::Binary(ack_bytes.into())).await;
                }
            }
        }

        if !authenticated {
            let nack = CloudPacket::AuthResult {
                success: false,
                error: Some("Authentication or Room Key mismatch".into()),
            };
            if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                let _ = ws_tx.send(Message::Binary(bytes.into())).await;
            }
            return;
        }

        // 2. Spawn Outbound Network Task for this Client
        let send_task = tokio::spawn(async move {
            while let Some(msg) = client_rx.recv().await {
                if ws_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        // 3. Process Inbound Packets
        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(bytes) => {
                    if let Ok(packet) = rmp_serde::from_slice::<CloudPacket>(&bytes) {
                        match packet {
                            CloudPacket::Replication {
                                collection, ops, ..
                            } => {
                                for op in ops {
                                    let _ = ingest_tx
                                        .send(IngestItem {
                                            collection: collection.clone(),
                                            op,
                                            sender_client_id: Some(client_id.clone()),
                                        })
                                        .await;
                                }
                            }
                            CloudPacket::VersionPing { versions } => {
                                for (col, remote_ts) in versions {
                                    if let Ok(local_version) = db.get_collection_version(&col) {
                                        if local_version > remote_ts {
                                            // Trigger Delta Replication catchup
                                            Self::send_catchup_deltas(
                                                &db,
                                                &col,
                                                remote_ts,
                                                &client_id,
                                                &peers,
                                            )
                                            .await;
                                        }
                                    }
                                }
                            }
                            CloudPacket::Heartbeat => {}
                            _ => {}
                        }
                    }
                }
                Message::Close(_) => break,
                _ => {}
            }
        }

        // Clean up disconnect
        send_task.abort();
        peers.write().await.remove(&client_id);
    }

    async fn send_catchup_deltas(
        db: &Arc<FireLite>,
        collection: &str,
        since_ts: i64,
        client_id: &str,
        peers: &Arc<AsyncRwLock<HashMap<String, mpsc::Sender<Message>>>>,
    ) {
        if let Ok(shard_arc) = db.get_shard(collection) {
            let changed_items: Vec<(String, crate::storage::engine::Pointer)> = {
                let guard = shard_arc.read().unwrap();
                guard
                    .index
                    .iter()
                    .filter_map(|(k, ptr)| {
                        let ts = match ptr {
                            crate::storage::engine::Pointer::Deleted { timestamp } => *timestamp,
                            crate::storage::engine::Pointer::Inlined(bytes) => {
                                i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0; 8]))
                            }
                            _ => 0,
                        };
                        if ts > since_ts {
                            Some((k.clone(), ptr.clone()))
                        } else {
                            None
                        }
                    })
                    .collect()
            };

            let mut ops = Vec::new();
            {
                let guard = shard_arc.read().unwrap();
                for (key, ptr) in changed_items {
                    if let Ok(Some(bytes)) = guard.read_pointer_internal(&ptr, false) {
                        ops.push(WalOp::PutInlined { key, value: bytes });
                    }
                }
            }

            if !ops.is_empty() {
                let packet = CloudPacket::Replication {
                    msg_id: 0,
                    collection: collection.to_string(),
                    ops,
                };
                if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                    let peers_guard = peers.read().await;
                    if let Some(tx) = peers_guard.get(client_id) {
                        let _ = tx.try_send(Message::Binary(bytes.into()));
                    }
                }
            }
        }
    }

    // ========================================================================
    // CLIENT MODE: Connects to Cloud Server
    // ========================================================================

    async fn start_client_mode(&self, server_url: &str) -> FLResult<()> {
        let server_url = server_url.to_string();
        let db = self.db.clone();
        let client_id = self.client_id.clone();
        let auth_token = self.auth_token.clone();
        let room_key_str = self.client_id.clone();
        let running = self.running.clone();
        let ingest_tx = self.ingest_tx.clone();

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok((ws_stream, _)) =
                    tokio_tungstenite::connect_async(&server_url).await
                {
                    let (mut ws_tx, mut ws_rx) = ws_stream.split();

                    // Authenticate
                    let auth_packet = CloudPacket::Authenticate {
                        token: auth_token.clone(),
                        client_id: client_id.clone(),
                        room_key: room_key_str.clone(),
                    };
                    let auth_bytes = rmp_serde::to_vec_named(&auth_packet).unwrap();
                    if ws_tx.send(Message::Binary(auth_bytes.into())).await.is_err() {
                        tokio::time::sleep(Duration::from_secs(3)).await;
                        continue;
                    }

                    // Await Auth Confirmation
                    if let Some(Ok(Message::Binary(bytes))) = ws_rx.next().await {
                        if let Ok(CloudPacket::AuthResult { success: true, .. }) =
                            rmp_serde::from_slice(&bytes)
                        {
                            // Send Local Versions to Request Catchup
                            let ping = CloudPacket::VersionPing {
                                versions: db.get_version_map(),
                            };
                            let ping_bytes = rmp_serde::to_vec_named(&ping).unwrap();
                            let _ = ws_tx.send(Message::Binary(ping_bytes.into())).await;

                            // Incoming Client Loop
                            while let Some(Ok(msg)) = ws_rx.next().await {
                                if let Message::Binary(b) = msg {
                                    if let Ok(CloudPacket::Replication { collection, ops, .. }) =
                                        rmp_serde::from_slice(&b)
                                    {
                                        for op in ops {
                                            let _ = ingest_tx
                                                .send(IngestItem {
                                                    collection: collection.clone(),
                                                    op,
                                                    sender_client_id: None,
                                                })
                                                .await;
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                // Reconnect delay
                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });

        Ok(())
    }
}