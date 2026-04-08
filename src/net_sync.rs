#[cfg(feature = "net-sync")]
use crate::engine::FireLite;
#[cfg(feature = "net-sync")]
use crate::document::value::Value;
#[cfg(feature = "net-sync")]
use crate::document::firelite_doc::FireLiteDoc;
#[cfg(feature = "net-sync")]
use crate::storage::wal::WalOp;
#[cfg(feature = "net-sync")]
use crate::storage::engine::Pointer;
#[cfg(feature = "net-sync")]
use std::sync::{Arc, Mutex};
#[cfg(feature = "net-sync")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "net-sync")]
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
#[cfg(feature = "net-sync")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "net-sync")]
use tokio::sync::{Mutex as AsyncMutex, watch};
#[cfg(feature = "net-sync")]
use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};
#[cfg(feature = "net-sync")]
use sha2::{Sha256, Digest};
#[cfg(feature = "net-sync")]
use tokio::net::{TcpStream, tcp::OwnedWriteHalf};
// #[cfg(feature = "net-sync")]
// use std::net::SocketAddr;

// --- Data Structures ---

#[cfg(feature = "net-sync")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus { Idle, Connected, Syncing }

#[cfg(feature = "net-sync")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct NetworkStatus {
    pub status: SyncStatus,
    pub self_id: String,
    pub peer_count: usize,
    pub known_peers: Vec<String>,
}

#[cfg(feature = "net-sync")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum NetPacket {
    /// Handshake: Includes a hash of the room key to verify membership without leaking the secret
    Identify { 
        id: String, 
        room_hash: [u8; 32], 
        db_name: String 
    },
    SyncRequest,
    Replication { 
        msg_id: u128, 
        origin_id: String, 
        collection: String, 
        ops: Vec<WalOp> 
    },
    
}

// --- NetSyncer Engine ---

#[cfg(feature = "net-sync")]
pub struct NetSyncer {
    db: Arc<FireLite>,
    self_id: String,
    room_hash: [u8; 32],
    excluded_collections: HashSet<String>,
    service_type: String,
    status_tx: watch::Sender<NetworkStatus>,
    status_rx: watch::Receiver<NetworkStatus>,
    peers: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, 
    seen_messages: Arc<AsyncMutex<HashSet<u128>>>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    shard_offsets: Arc<Mutex<HashMap<String, u64>>>,
}

#[cfg(feature = "net-sync")]
impl NetSyncer {
    pub fn new(db: Arc<FireLite>, name: &str, room_key: &str, mut excluded: Vec<String>) -> Self {
        let (tx, rx) = watch::channel(NetworkStatus {
            status: SyncStatus::Idle,
            self_id: name.to_string(),
            peer_count: 0,
            known_peers: Vec::new(),
        });

        // Pre-compute room hash for fast verification
        let mut hasher = Sha256::new();
        hasher.update(room_key.as_bytes());
        let room_hash: [u8; 32] = hasher.finalize().into();

        // Load persisted offsets from DB
        let mut shard_offsets = HashMap::new();
        if let Ok(Some(doc)) = db.get("__firelite_system", "sync_checkpoint") {
            if let Some(Value::Map(fields)) = doc.get("offsets") {
                for (name, val) in fields {
                    if let Value::Int(off) = val { shard_offsets.insert(name.to_string(), *off as u64); }
                }
            }
        }

        excluded.push("__firelite_system".to_string());
        excluded.push("__firelite_security".to_string());
        excluded.push("audit_log".to_string());

        let excluded_collections: HashSet<String> = excluded.into_iter().collect();

        Self {
            db: db.clone(),
            self_id: name.to_string(),
            room_hash,
            excluded_collections,
            service_type: format!("_{}._tcp.local.", db.db_name().to_lowercase().replace('.', "_")),
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(AsyncMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(HashSet::with_capacity(1000))),
            tasks: Arc::new(Mutex::new(Vec::new())),
            shard_offsets: Arc::new(Mutex::new(shard_offsets)),
        }
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        self.stop(); 
        
        
        // 1. Seen-message Pruner
        let seen_messages = self.seen_messages.clone();
        let handle_pruner = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                seen_messages.lock().await.retain(|&id| id > now - 300_000_000);
            }
        });
        self.tasks.lock().unwrap().push(handle_pruner);
        
        // ip address
        let my_ip = local_ip_address::local_ip().map(|ip| ip.to_string()).unwrap_or_else(|_| "127.0.0.1".to_string());
        let beacon_port = 8118;

        // 1. SEEN-MESSAGE PRUNER (RAM Safety)
        let seen_messages = self.seen_messages.clone();
        let handle_pruner = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(60));
            loop {
                interval.tick().await;
                let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                seen_messages.lock().await.retain(|&id| id > now - 300_000_000);
            }
        });
        self.tasks.lock().unwrap().push(handle_pruner);

        // 2. mDNS DISCOVERY SETUP
        let mdns = ServiceDaemon::new()?;
        let hostname = gethostname::gethostname().to_string_lossy().into_owned() + ".local.";
        let service_info = ServiceInfo::new(&self.service_type, &self.self_id, &hostname, &my_ip, port, None)?;
        mdns.register(service_info)?;

        // 3. UDP BEACON SETUP (Socket Reuse Optimization)
        let udp_addr: std::net::SocketAddr = format!("0.0.0.0:{}", beacon_port).parse()?;
        let raw_socket = socket2::Socket::new(socket2::Domain::IPV4, socket2::Type::DGRAM, Some(socket2::Protocol::UDP))?;
        raw_socket.set_reuse_address(true)?; 
        #[cfg(not(windows))] raw_socket.set_reuse_port(true);
        raw_socket.bind(&udp_addr.into())?;
        raw_socket.set_broadcast(true)?;
        let udp_socket = Arc::new(tokio::net::UdpSocket::from_std(raw_socket.into())?);

        // 4. BROADCAST BEACON (TX)
        let beacon_msg = format!("FL_BEACON:{}:{}:{}", self.self_id, my_ip, port);
        let socket_tx = udp_socket.clone();
        let handle_udp_tx = tokio::spawn(async move {
            let mut interval = tokio::time::interval(Duration::from_secs(5));
            loop {
                interval.tick().await;
                let _ = socket_tx.send_to(beacon_msg.as_bytes(), format!("255.255.255.255:{}", beacon_port)).await;
            }
        });
        self.tasks.lock().unwrap().push(handle_udp_tx);

        // 5. UNIFIED DISCOVERY LISTENER (Combines UDP and mDNS)
        let db_disc = self.db.clone();
        let peers_disc = self.peers.clone();
        let seen_disc = self.seen_messages.clone();
        let status_tx_disc = self.status_tx.clone();
        let self_id_disc = self.self_id.clone();
        let my_hash_disc = self.room_hash;
        let excluded_disc = self.excluded_collections.clone();
        
        let socket_rx = udp_socket.clone();
        let browser = mdns.browse(&self.service_type)?;

        let handle_discovery = tokio::spawn(async move {
            let mut udp_buf = [0u8; 1024];
            loop {
                tokio::select! {
                    // A. Hear UDP Beacon
                    Ok((len, _)) = socket_rx.recv_from(&mut udp_buf) => {
                        let msg = String::from_utf8_lossy(&udp_buf[..len]);
                        if msg.starts_with("FL_BEACON:") {
                            let parts: Vec<&str> = msg.split(':').collect();
                            if parts.len() == 4 {
                                let (p_id, p_ip, p_port) = (parts[1], parts[2], parts[3]);
                                if p_id != self_id_disc {
                                    attempt_connect(p_id, &format!("{}:{}", p_ip, p_port), &db_disc, &peers_disc, &seen_disc, &status_tx_disc, &self_id_disc, my_hash_disc, &excluded_disc).await;
                                }
                            }
                        }
                    }
                    // B. Hear mDNS Event
                    Ok(event) = browser.recv_async() => {
                        if let ServiceEvent::ServiceResolved(info) = event {
                            let p_name = info.get_fullname().split('.').next().unwrap_or("");
                            if p_name != self_id_disc {
                                if let Some(addr) = info.get_addresses().iter().next() {
                                    attempt_connect(p_name, &format!("{}:{}", addr, info.get_port()), &db_disc, &peers_disc, &seen_disc, &status_tx_disc, &self_id_disc, my_hash_disc, &excluded_disc).await;
                                }
                            }
                        }
                    }
                }
            }
        });
        self.tasks.lock().unwrap().push(handle_discovery);

        // 6. TCP LISTENER (Server)
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        let db_inc = self.db.clone();
        let peers_inc = self.peers.clone();
        let seen_inc = self.seen_messages.clone();
        let status_tx_inc = self.status_tx.clone();
        let self_id_inc = self.self_id.clone();
        let my_hash_inc = self.room_hash;
        let excluded_inc = self.excluded_collections.clone();

        let handle_listener = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                handle_incoming_peer(stream, db_inc.clone(), peers_inc.clone(), seen_inc.clone(), status_tx_inc.clone(), self_id_inc.clone(), my_hash_inc, excluded_inc.clone()).await;
            }
        });
        self.tasks.lock().unwrap().push(handle_listener);

        // 5. Log-Following Tailer
        let db_tail = self.db.clone();
        let offsets_tail = self.shard_offsets.clone();
        let self_id_tail = self.self_id.clone();
        let peers_tail = self.peers.clone();
        let excluded_tail = self.excluded_collections.clone();

        let handle_tailer = tokio::task::spawn_blocking(move || {
            let mut last_checkpoint_save = Instant::now();
            loop {
                let peers_count = peers_tail.blocking_lock().len(); // CHECK PEER COUNT
                
                if peers_count == 0 {
                    std::thread::sleep(Duration::from_millis(500)); // Sleep longer if alone
                    continue; // DON'T tail the log if no one is listening
                }

                let collections = db_tail.list_collections().unwrap_or_default();
                let mut changed = false;

                let mut offsets = offsets_tail.lock().unwrap();
                for col in collections {
                    if excluded_tail.contains(&col) { continue; }
                    let shard_arc = db_tail.get_shard(&col);
                    let shard = shard_arc.read().unwrap();
                    let last_pos = *offsets.get(&col).unwrap_or(&0);

                    if let Ok((ops, new_pos)) = shard.wal.tail(last_pos) {
                        if !ops.is_empty() {
                            let mut logical_ops = Vec::with_capacity(ops.len());
                            for op in ops {
                                match op {
                                    WalOp::Put { key, segment_id, segment_offset, len } => {
                                        let ptr = Pointer::Segment { segment_id, offset: segment_offset, len };
                                        if let Ok(Some(data)) = shard.read_pointer_uncached(&ptr) {
                                            logical_ops.push(WalOp::PutInlined { key, value: data });
                                        }
                                    }
                                    WalOp::PutBlob { key, offset, len } => {
                                        let ptr = Pointer::Blob { offset, len };
                                        if let Ok(Some(data)) = shard.read_pointer_uncached(&ptr) {
                                            logical_ops.push(WalOp::PutInlined { key, value: data });
                                        }
                                    }
                                    _ => logical_ops.push(op),
                                }
                            }
                            broadcast_batch_replication(&peers_tail, &self_id_tail, col.clone(), logical_ops);
                            offsets.insert(col, new_pos);
                            changed = true;
                        }
                    }
                }
                
                if changed && last_checkpoint_save.elapsed() > Duration::from_secs(5) {
                    let mut doc = FireLiteDoc::default();
                    let map = offsets.iter().map(|(k,v)| (Arc::from(k.as_str()), Value::Int(*v as i64))).collect();
                    doc.insert("offsets", Value::Map(map));
                    let _ = db_tail.put("__firelite_system", "sync_checkpoint", &doc);
                    last_checkpoint_save = Instant::now();
                }
                std::thread::sleep(Duration::from_millis(250));
            }
        });
        self.tasks.lock().unwrap().push(handle_tailer);

        Ok(())
    }

    pub fn stop(&self) {
        let offsets = self.shard_offsets.lock().unwrap().clone();
        let mut doc = FireLiteDoc::default();
        let map = offsets.iter().map(|(k,v)| (Arc::from(k.as_str()), Value::Int(*v as i64))).collect();
        doc.insert("offsets", Value::Map(map));
        let _ = self.db.put("__firelite_system", "sync_checkpoint", &doc);

        let mut task_guard = self.tasks.lock().unwrap();
        for handle in task_guard.drain(..) { handle.abort(); }
    }

    pub fn status(&self) -> NetworkStatus {
        let stats = self.status_rx.borrow().clone();
        let live_peers = if let Ok(guard) = self.peers.try_lock() { guard.keys().cloned().collect() } else { stats.known_peers };
        NetworkStatus { status: stats.status, self_id: self.self_id.clone(), peer_count: live_peers.len(), known_peers: live_peers }
    }

    
}


// Helper to prevent duplicate connection attempts
async fn attempt_connect(
    peer_id: &str, 
    addr: &str,
    db: &Arc<FireLite>,
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    seen: &Arc<AsyncMutex<HashSet<u128>>>,
    status_tx: &watch::Sender<NetworkStatus>,
    self_id: &str,
    my_hash: [u8; 32],
    excluded: &HashSet<String>
) {
    let already_connected = { peers.lock().await.contains_key(peer_id) };
    if !already_connected {
        if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(addr)).await {
            handle_incoming_peer(stream, db.clone(), peers.clone(), seen.clone(), status_tx.clone(), self_id.to_string(), my_hash, excluded.clone()).await;
        }
    }
}
// --- Logic Helpers ---

async fn handle_incoming_peer(
    stream: TcpStream, 
    db: Arc<FireLite>, 
    peers_map: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    seen_cache: Arc<AsyncMutex<HashSet<u128>>>, 
    status_tx: watch::Sender<NetworkStatus>,
    self_id: String, 
    my_hash: [u8; 32],
    excluded: HashSet<String>, 
) {
    let (mut reader, mut writer) = stream.into_split();
    
    // 1. Handshake
    let handshake = NetPacket::Identify { id: self_id.clone(), room_hash: my_hash, db_name: db.db_name() };
    if let Ok(hello) = bincode::serialize(&handshake) {
        if send_raw(&mut writer, &hello).await.is_err() { return; }

        let payload = match tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut reader)).await {
            Ok(Ok(p)) => p,
            _ => return,
        };

        let (peer_id, peer_hash) = if let Ok(NetPacket::Identify { id, room_hash, .. }) = bincode::deserialize::<NetPacket>(&payload) { (id, room_hash) } else { return; };

        // SECURITY CHECK: Verify Room Key Match
        if peer_hash != my_hash {
            return; // Key mismatch, drop connection silently
        }

        peers_map.lock().await.insert(peer_id.clone(), writer);
        
        // Trigger Sync Request immediately
        let _ = send_raw(&mut peers_map.lock().await.get_mut(&peer_id).unwrap(), &bincode::serialize(&NetPacket::SyncRequest).unwrap()).await;

        status_tx.send_modify(|s| {
            if !s.known_peers.contains(&peer_id) { s.known_peers.push(peer_id.clone()); }
            s.peer_count = s.known_peers.len();
            s.status = SyncStatus::Connected;
        });

        loop {
            let p_bytes = match recv_raw(&mut reader).await { Ok(p) => p, Err(_) => break };
            let packet: NetPacket = match bincode::deserialize(&p_bytes) { Ok(p) => p, Err(_) => continue };

            match packet {
                NetPacket::SyncRequest => { handle_bootstrap_request(&db, &peers_map, &peer_id, &excluded, &self_id).await; }
                NetPacket::Replication { msg_id, collection, ops, .. } => {
                    // SECURITY GATE: Never allow remote peers to touch system shards
                    if collection.starts_with("__firelite_") {
                        continue; 
                    }
                    if msg_id != 0 && !seen_cache.lock().await.insert(msg_id) { continue; }
                    if !excluded.contains(&collection) {
                        apply_replication_batch(&db, collection, ops).await;
                    }
                }
                _ => {}
            }
        }
        peers_map.lock().await.remove(&peer_id);
    }
}

async fn handle_bootstrap_request(db: &Arc<FireLite>, peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, peer_id: &str, excluded: &HashSet<String>, self_id: &str) {
    let collections = db.list_collections().unwrap_or_default();
    for col in collections {
        if excluded.contains(&col) { continue; }
        let shard_arc = db.get_shard(&col);

        let index_snapshot: Vec<(String, Pointer)> = {
            let shard = shard_arc.read().unwrap();
            shard.index.iter().map(|(k, p)| (k.clone(), p.clone())).collect()
        };

        for (key, pointer) in index_snapshot {
            let ops = match pointer {
                Pointer::Deleted { timestamp } => vec![WalOp::Delete { key: key.clone(), timestamp }],
                _ => {
                    if let Ok(Some(value)) = shard_arc.read().unwrap().read_pointer_internal(&pointer, false) {
                        vec![WalOp::PutInlined { key: key.clone(), value }]
                    } else { continue; }
                }
            };

            let packet = NetPacket::Replication { msg_id: 0, origin_id: self_id.to_string(), collection: col.clone(), ops };
            if let Ok(payload) = bincode::serialize(&packet) {
                let mut guard = peers.lock().await;
                if let Some(w) = guard.get_mut(peer_id) { let _ = send_raw(w, &payload).await; }
            }
        }
    }
}

async fn apply_replication_batch(db: &Arc<FireLite>, collection: String, ops: Vec<WalOp>) {
    let shard_arc = db.get_shard(&collection);
    let mut shard = shard_arc.write().unwrap();

    let mut filtered_ops = Vec::new();

    for op in ops {
        // 1. Extract Metadata (Key and Timestamp) for LWW check
        let (key, remote_ts) = match &op {
            WalOp::PutInlined { key, value } => {
                if let Some(doc) = FireLiteDoc::decode(value) {
                    (key, doc.get_logical_time())
                } else { continue; }
            }
            WalOp::Delete { key, timestamp } => (key, *timestamp),
            _ => continue,
        };

        // 2. Conflict Resolution (Document OR Tombstone)
        if let Some(local_ptr) = shard.index.get(key) {
            let local_ts = match local_ptr {
                Pointer::Deleted { timestamp } => *timestamp,
                _ => {
                    shard.read_pointer_internal(local_ptr, false)
                        .ok().flatten()
                        .and_then(|b| FireLiteDoc::decode(&b))
                        .map(|d| d.get_logical_time())
                        .unwrap_or(0)
                }
            };

            // Skip if local is strictly newer or equal
            if remote_ts <= local_ts {
                continue;
            }
        }

        // 3. If we passed the check, keep the operation
        filtered_ops.push(op);
    }

    // 4. Commit accepted operations to local storage
    if !filtered_ops.is_empty() {
        if shard.apply_replicated_ops(&filtered_ops).is_ok() {
            let index_puts: Vec<_> = filtered_ops.into_iter().filter_map(|o| {
                if let WalOp::PutInlined { key, value } = o {
                    let id = key.split(':').last()?.to_string();
                    let doc = FireLiteDoc::decode(&value)?;
                    Some((id, doc))
                } else { None }
            }).collect();

            if !index_puts.is_empty() {
                db.inject_replication_to_indexer(collection, Arc::new(index_puts));
            }
        }
    }
}

fn broadcast_batch_replication(peers_map: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, self_id: &str, collection: String, ops: Vec<WalOp>) {
    let msg_id = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
    let packet = NetPacket::Replication { msg_id, origin_id: self_id.to_string(), collection, ops };
    
    if let Ok(payload) = bincode::serialize(&packet) {
        let header = (payload.len() as u32).to_le_bytes();
        let peers_clone = peers_map.clone();
        tokio::spawn(async move {
            let mut p_guard = peers_clone.lock().await;
            let mut dead = Vec::new();
            for (id, writer) in p_guard.iter_mut() {
                if writer.write_all(&header).await.is_err() || writer.write_all(&payload).await.is_err() { dead.push(id.clone()); }
            }
            for id in dead { p_guard.remove(&id); }
        });
    }
}

async fn send_raw<W: AsyncWriteExt + Unpin>(w: &mut W, data: &[u8]) -> tokio::io::Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes()).await?;
    w.write_all(data).await
}

async fn recv_raw<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<Vec<u8>> {
    let mut len_b = [0u8; 4];
    r.read_exact(&mut len_b).await?;
    let len = u32::from_le_bytes(len_b) as usize;
    let mut data = vec![0u8; len];
    r.read_exact(&mut data).await?;
    Ok(data)
}