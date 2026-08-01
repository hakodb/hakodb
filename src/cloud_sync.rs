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
#[cfg(feature = "cloud-sync")]
use crate::document::value::Value;
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
    /// Version handshake exchanged symmetrically to catch up on missed deltas
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
    room_key: String,
    client_id: String,
    auth_token: String,
    running: Arc<AtomicBool>,
    echo_cache: Arc<StdMutex<HashMap<String, i64>>>,
    seen_messages: Arc<AsyncMutex<Vec<u128>>>,
    // Ingress Buffer for High-Throughput Batch Coalescing
    ingest_tx: mpsc::Sender<IngestItem>,
    ingest_rx: Arc<AsyncMutex<Option<mpsc::Receiver<IngestItem>>>>,
    active_peers: Arc<AsyncRwLock<HashMap<String, mpsc::Sender<Message>>>>,
    // Outbound client queue (Client mode -> Server)
    outbound_tx: Arc<AsyncMutex<Option<mpsc::Sender<CloudPacket>>>>,
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
            room_key: room_key.to_string(),
            client_id: client_id.to_string(),
            auth_token: auth_token.to_string(),
            running: Arc::new(AtomicBool::new(false)),
            echo_cache: Arc::new(StdMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(Vec::with_capacity(1000))),
            ingest_tx: tx,
            ingest_rx: Arc::new(AsyncMutex::new(Some(rx))),
            active_peers: Arc::new(AsyncRwLock::new(HashMap::new())),
            outbound_tx: Arc::new(AsyncMutex::new(None)),
        }
    }

    /// Spawns the Cloud Sync system.
    pub async fn start(&self, bind_or_server_url: &str) -> FLResult<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
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
    // BATCH AGGREGATOR: SOLVES SINGLE-WRITE LOCK BOTTLENECK
    // ========================================================================

    fn spawn_batch_flusher(&self) {
        let rx_option = self.ingest_rx.clone();
        let db = self.db.clone();
        let running = self.running.clone();
        let echo_cache = self.echo_cache.clone();
        let active_peers = self.active_peers.clone();

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
                        Self::flush_ingest_buffer(&db, &mut batch_buffer, &echo_cache, &active_peers).await;
                    }
                    item = rx.recv() => {
                        match item {
                            Some(item) => {
                                batch_buffer.entry(item.collection)
                                    .or_default()
                                    .push((item.op, item.sender_client_id));

                                let total_pending: usize = batch_buffer.values().map(|v| v.len()).sum();
                                if total_pending >= 512 {
                                    Self::flush_ingest_buffer(&db, &mut batch_buffer, &echo_cache, &active_peers).await;
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
    ) {
        if buffer.is_empty() {
            return;
        }

        // Auto-prune echo_cache (Entries older than 5 mins when > 10k items)
        {
            let mut cache = echo_cache.lock().unwrap();
            if cache.len() > 10_000 {
                let cutoff = (SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() as i64) - 300_000_000;
                cache.retain(|_, &mut ts| ts > cutoff);
            }
        }

        for (col, items) in buffer.drain() {
            if items.is_empty() {
                continue;
            }

            let mut mutations = Vec::with_capacity(items.len());
            let mut sender_relays: HashMap<Option<String>, Vec<WalOp>> = HashMap::new();

            for (op, sender_id) in &items {
                match op {
                    WalOp::PutInlined { key, value } => {
                        if let Some(doc) = FireLiteDoc::decode(value) {
                            let ts = doc.get_logical_time();

                            // LWW Check
                            if let Ok(Some(existing_doc)) = db.get(&col, key) {
                                if existing_doc.get_logical_time() >= ts {
                                    continue;
                                }
                            }

                            {
                                let mut cache = echo_cache.lock().unwrap();
                                cache.insert(key.clone(), ts);
                            }
                            mutations.push(BatchMutation::Put {
                                collection: col.clone(),
                                doc_id: key.clone(),
                                doc,
                            });
                            sender_relays
                                .entry(sender_id.clone())
                                .or_default()
                                .push(op.clone());
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
                        sender_relays
                            .entry(sender_id.clone())
                            .or_default()
                            .push(op.clone());
                    }
                    _ => {}
                }
            }

            if !mutations.is_empty() {
                let db_clone = db.clone();
                let _ = tokio::task::spawn_blocking(move || db_clone.write_batch(mutations)).await;
            }

            // Relay packet to all connected room members EXCEPT origin sender
            for (origin_sender, ops) in sender_relays {
                if ops.is_empty() {
                    continue;
                }

                let packet = CloudPacket::Replication {
                    msg_id: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros(),
                    collection: col.clone(),
                    ops,
                };

                if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                    let msg = Message::Binary(bytes.into());
                    let peers_guard = peers.read().await;

                    for (client_id, tx) in peers_guard.iter() {
                        if Some(client_id) != origin_sender.as_ref() {
                            let _ = tx.try_send(msg.clone());
                        }
                    }
                }
            }
        }
    }

    // ========================================================================
    // SERVER MODE
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
        let seen_messages = self.seen_messages.clone();
        let echo_cache = self.echo_cache.clone();

        // 1. Spawn Server Local WAL Tailer (Broadcasts server-side writes to all connected clients)
        self.spawn_server_outbound_tailer(echo_cache);

        // 2. Accept Incoming WebSocket Clients
        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok((stream, _)) = listener.accept().await {
                    let ingest_tx = ingest_tx.clone();
                    let db = db.clone();
                    let active_peers = active_peers.clone();
                    let seen_messages = seen_messages.clone();

                    tokio::spawn(async move {
                        if let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await {
                            Self::handle_server_client(
                                ws_stream,
                                ingest_tx,
                                db,
                                room_hash,
                                active_peers,
                                seen_messages,
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
        seen_messages: Arc<AsyncMutex<Vec<u128>>>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_tx, mut ws_rx) = ws_stream.split();
        let (client_tx, mut client_rx) = mpsc::channel::<Message>(2000);

        let mut client_id = String::new();
        let mut authenticated = false;

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

        let send_task = tokio::spawn(async move {
            while let Some(msg) = client_rx.recv().await {
                if ws_tx.send(msg).await.is_err() {
                    break;
                }
            }
        });

        while let Some(Ok(msg)) = ws_rx.next().await {
            match msg {
                Message::Binary(bytes) => {
                    if let Ok(packet) = rmp_serde::from_slice::<CloudPacket>(&bytes) {
                        match packet {
                            CloudPacket::Replication {
                                msg_id,
                                collection,
                                ops,
                            } => {
                                if msg_id != 0 {
                                    let mut seen = seen_messages.lock().await;
                                    if seen.contains(&msg_id) {
                                        continue;
                                    }
                                    seen.push(msg_id);
                                    if seen.len() > 1000 {
                                        seen.remove(0);
                                    }
                                }

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
                            CloudPacket::VersionPing { versions: client_versions } => {
                                // 1. SYMMETRICAL REPLY: Send Server's VersionMap back to Client
                                let server_versions = db.get_version_map();
                                let reply = CloudPacket::VersionPing {
                                    versions: server_versions.clone(),
                                };
                                if let Ok(reply_bytes) = rmp_serde::to_vec_named(&reply) {
                                    let peers_guard = peers.read().await;
                                    if let Some(tx) = peers_guard.get(&client_id) {
                                        let _ = tx.try_send(Message::Binary(reply_bytes.into()));
                                    }
                                }

                                // 2. Send Deltas for collections where Server is ahead of Client
                                for (col, remote_ts) in client_versions {
                                    if let Ok(local_version) = db.get_collection_version(&col) {
                                        if local_version > remote_ts {
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

    /// Tails local WAL changes on the SERVER and broadcasts server-side writes to all connected clients.
    fn spawn_server_outbound_tailer(&self, echo_cache: Arc<StdMutex<HashMap<String, i64>>>) {
        let db = self.db.clone();
        let active_peers = self.active_peers.clone();
        let running = self.running.clone();

        tokio::task::spawn_blocking(move || {
            let mut offsets: HashMap<String, u64> = HashMap::new();

            while running.load(Ordering::Relaxed) {
                let cols = db.list_collections().unwrap_or_default();

                for col in cols {
                    if let Ok(shard_arc) = db.get_shard(&col) {
                        let last_pos = *offsets.get(&col).unwrap_or(&0);
                        let tail_res = {
                            let guard = shard_arc.read().unwrap();
                            guard.wal.tail(last_pos)
                        };

                        if let Ok((ops, new_pos)) = tail_res {
                            if !ops.is_empty() {
                                let mut to_send = Vec::new();

                                for op in ops {
                                    let key = op.get_key();
                                    let wal_ts = match &op {
                                        WalOp::PutInlined { value, .. } => {
                                            i64::from_le_bytes(value[2..10].try_into().unwrap_or([0; 8]))
                                        }
                                        WalOp::Delete { timestamp, .. } => *timestamp,
                                        _ => 0,
                                    };

                                    // Skip writes originating from client WebSockets (already handled)
                                    let is_echo = {
                                        let mut cache = echo_cache.lock().unwrap();
                                        if let Some(&cached_ts) = cache.get(key) {
                                            if cached_ts == wal_ts {
                                                cache.remove(key);
                                                true
                                            } else {
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    };

                                    if !is_echo {
                                        let final_op = match op {
                                            WalOp::PutInlined { ref key, ref value } => {
                                                if let Some(mut doc) = FireLiteDoc::decode(value) {
                                                    let has_blobs = doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }));
                                                    if has_blobs {
                                                        let enc_key = db.config.encryption_key.as_deref();
                                                        if crate::engine::engine::resolve_doc_static(&mut doc, &shard_arc, enc_key).is_ok() {
                                                            WalOp::PutInlined { key: key.clone(), value: doc.encode_buffered() }
                                                        } else {
                                                            op
                                                        }
                                                    } else {
                                                        op
                                                    }
                                                } else {
                                                    op
                                                }
                                            }
                                            _ => op,
                                        };

                                        to_send.push(final_op);
                                    }
                                }

                                if !to_send.is_empty() {
                                    let packet = CloudPacket::Replication {
                                        msg_id: SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_micros(),
                                        collection: col.clone(),
                                        ops: to_send,
                                    };

                                    if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                                        let msg = Message::Binary(bytes.into());
                                        let rt = tokio::runtime::Handle::current();
                                        let peers_ptr = active_peers.clone();
                                        rt.block_on(async move {
                                            let peers_guard = peers_ptr.read().await;
                                            for tx in peers_guard.values() {
                                                let _ = tx.try_send(msg.clone());
                                            }
                                        });
                                    }
                                }

                                offsets.insert(col, new_pos);
                            }
                        }
                    }
                }

                std::thread::sleep(Duration::from_millis(150));
            }
        });
    }

    // ========================================================================
    // CLIENT MODE
    // ========================================================================

    async fn start_client_mode(&self, server_url: &str) -> FLResult<()> {
        let mut ws_url = server_url.trim().to_string();
        if ws_url.starts_with("https://") {
            ws_url = ws_url.replacen("https://", "wss://", 1);
        } else if ws_url.starts_with("http://") {
            ws_url = ws_url.replacen("http://", "ws://", 1);
        }
        if !ws_url.starts_with("ws://") && !ws_url.starts_with("wss://") {
            ws_url = format!("wss://{}", ws_url);
        }

        let db = self.db.clone();
        let client_id = self.client_id.clone();
        let auth_token = self.auth_token.clone();
        let room_key_str = self.room_key.clone();
        let running = self.running.clone();
        let ingest_tx = self.ingest_tx.clone();
        let echo_cache = self.echo_cache.clone();
        let seen_messages = self.seen_messages.clone();

        let (outbound_tx, mut outbound_rx) = mpsc::channel::<CloudPacket>(10_000);
        *self.outbound_tx.lock().await = Some(outbound_tx.clone());

        self.spawn_client_outbound_tailer(echo_cache);

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok((ws_stream, _)) =
                    tokio_tungstenite::connect_async(&ws_url).await
                {
                    let (mut ws_tx, mut ws_rx) = ws_stream.split();

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

                    if let Some(Ok(Message::Binary(bytes))) = ws_rx.next().await {
                        if let Ok(CloudPacket::AuthResult { success: true, .. }) =
                            rmp_serde::from_slice(&bytes)
                        {
                            let ping = CloudPacket::VersionPing {
                                versions: db.get_version_map(),
                            };
                            let ping_bytes = rmp_serde::to_vec_named(&ping).unwrap();
                            let _ = ws_tx.send(Message::Binary(ping_bytes.into())).await;

                            loop {
                                tokio::select! {
                                    Some(packet) = outbound_rx.recv() => {
                                        if let Ok(b) = rmp_serde::to_vec_named(&packet) {
                                            if ws_tx.send(Message::Binary(b.into())).await.is_err() {
                                                break;
                                            }
                                        }
                                    }
                                    Some(Ok(msg)) = ws_rx.next() => {
                                        if let Message::Binary(b) = msg {
                                            if let Ok(packet) = rmp_serde::from_slice::<CloudPacket>(&b) {
                                                match packet {
                                                    CloudPacket::VersionPing { versions: server_versions } => {
                                                        let client_versions = db.get_version_map();
                                                        for (col, client_ts) in client_versions {
                                                            let server_ts = server_versions.get(&col).copied().unwrap_or(0);
                                                            if client_ts > server_ts {
                                                                Self::push_client_deltas_upstream(&db, &col, server_ts, &outbound_tx).await;
                                                            }
                                                        }
                                                    }
                                                    CloudPacket::Replication { msg_id, collection, ops, .. } => {
                                                        if msg_id != 0 {
                                                            let mut seen = seen_messages.lock().await;
                                                            if seen.contains(&msg_id) {
                                                                continue;
                                                            }
                                                            seen.push(msg_id);
                                                            if seen.len() > 1000 {
                                                                seen.remove(0);
                                                            }
                                                        }

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
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                    else => break,
                                }
                            }
                        }
                    }
                }

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });

        Ok(())
    }

    async fn push_client_deltas_upstream(
        db: &Arc<FireLite>,
        collection: &str,
        server_ts: i64,
        outbound_tx: &mpsc::Sender<CloudPacket>,
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
                        if ts > server_ts {
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
                    msg_id: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros(),
                    collection: collection.to_string(),
                    ops,
                };
                let _ = outbound_tx.send(packet).await;
            }
        }
    }

    fn spawn_client_outbound_tailer(&self, echo_cache: Arc<StdMutex<HashMap<String, i64>>>) {
        let db = self.db.clone();
        let outbound_tx_option = self.outbound_tx.clone();
        let running = self.running.clone();

        tokio::task::spawn_blocking(move || {
            let mut offsets: HashMap<String, u64> = HashMap::new();

            while running.load(Ordering::Relaxed) {
                let outbound_guard = outbound_tx_option.blocking_lock();
                let Some(ref tx) = *outbound_guard else {
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                };

                let cols = db.list_collections().unwrap_or_default();

                for col in cols {
                    if let Ok(shard_arc) = db.get_shard(&col) {
                        let last_pos = *offsets.get(&col).unwrap_or(&0);
                        let tail_res = {
                            let guard = shard_arc.read().unwrap();
                            guard.wal.tail(last_pos)
                        };

                        if let Ok((ops, new_pos)) = tail_res {
                            if !ops.is_empty() {
                                let mut to_send = Vec::new();

                                for op in ops {
                                    let key = op.get_key();
                                    let wal_ts = match &op {
                                        WalOp::PutInlined { value, .. } => {
                                            i64::from_le_bytes(value[2..10].try_into().unwrap_or([0; 8]))
                                        }
                                        WalOp::Delete { timestamp, .. } => *timestamp,
                                        _ => 0,
                                    };

                                    let is_echo = {
                                        let mut cache = echo_cache.lock().unwrap();
                                        if let Some(&cached_ts) = cache.get(key) {
                                            if cached_ts == wal_ts {
                                                cache.remove(key);
                                                true
                                            } else {
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    };

                                    if !is_echo {
                                        let final_op = match op {
                                            WalOp::PutInlined { ref key, ref value } => {
                                                if let Some(mut doc) = FireLiteDoc::decode(value) {
                                                    let has_blobs = doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }));
                                                    if has_blobs {
                                                        let enc_key = db.config.encryption_key.as_deref();
                                                        if crate::engine::engine::resolve_doc_static(&mut doc, &shard_arc, enc_key).is_ok() {
                                                            WalOp::PutInlined { key: key.clone(), value: doc.encode_buffered() }
                                                        } else {
                                                            op
                                                        }
                                                    } else {
                                                        op
                                                    }
                                                } else {
                                                    op
                                                }
                                            }
                                            _ => op,
                                        };

                                        to_send.push(final_op);
                                    }
                                }

                                if !to_send.is_empty() {
                                    let packet = CloudPacket::Replication {
                                        msg_id: SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_micros(),
                                        collection: col.clone(),
                                        ops: to_send,
                                    };
                                    let _ = tx.blocking_send(packet);
                                }

                                offsets.insert(col, new_pos);
                            }
                        }
                    }
                }

                std::thread::sleep(Duration::from_millis(150));
            }
        });
    }
}