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
use std::sync::{Arc, Mutex, RwLock};
#[cfg(feature = "net-sync")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "net-sync")]
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
#[cfg(feature = "net-sync")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "net-sync")]
use tokio::sync::{Mutex as AsyncMutex, watch};
#[cfg(feature = "net-sync")]
use sha2::{Sha256, Digest};
#[cfg(feature = "net-sync")]
use tokio::net::{TcpStream, tcp::OwnedWriteHalf};
#[cfg(feature = "net-sync")]
use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};

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
    Identify { id: String, room_hash: [u8; 32] },
    SyncRequest,
    Replication { msg_id: u128, collection: String, ops: Vec<WalOp> },
}

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
    pub enable_relay: bool, 
}

#[cfg(feature = "net-sync")]
impl NetSyncer {
    pub fn new(db: Arc<FireLite>, name: &str, room_key: &str, mut excluded: Vec<String>) -> Self {
        let (tx, rx) = watch::channel(NetworkStatus {
            status: SyncStatus::Idle, self_id: name.to_string(), peer_count: 0, known_peers: Vec::new(),
        });

        let mut hasher = Sha256::new();
        hasher.update(room_key.as_bytes());
        let room_hash: [u8; 32] = hasher.finalize().into();

        let mut shard_offsets = HashMap::new();
        if let Ok(Some(doc)) = db.get("__firelite_system", "sync_checkpoint") {
            if let Some(Value::Map(fields)) = doc.get("offsets") {
                for (name, val) in fields {
                    if let Value::Int(off) = val { shard_offsets.insert(name.to_string(), *off as u64); }
                }
            }
        }

        excluded.extend(vec!["__firelite_system".into(), "__firelite_security".into(), "audit_log".into()]);

        Self {
            db: db.clone(), 
            self_id: name.to_string(), 
            room_hash,
            excluded_collections: excluded.into_iter().collect(),
            service_type: format!("_{}._tcp.local.", db.db_name().to_lowercase().replace('.', "_")),
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(AsyncMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(HashSet::with_capacity(1000))),
            tasks: Arc::new(Mutex::new(Vec::new())),
            shard_offsets: Arc::new(Mutex::new(shard_offsets)),
            enable_relay: false
        }
    }

    pub fn with_relay(mut self, enable: bool) -> Self {
        self.enable_relay = enable;
        self
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        self.stop();
        let my_ip = local_ip_address::local_ip().map(|ip| ip.to_string()).unwrap_or_else(|_| "127.0.0.1".to_string());

        // 1. TCP Listener (Same as before)
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        let db_ptr = self.db.clone();
        let peers_ptr = self.peers.clone();
        let seen_ptr = self.seen_messages.clone();
        let stx_ptr = self.status_tx.clone();
        let sid = self.self_id.clone();
        let hash = self.room_hash;
        let excl_srv = self.excluded_collections.clone();

        let relay_enabled = self.enable_relay; 

        let handle_srv = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                handle_peer(stream, db_ptr.clone(), peers_ptr.clone(), seen_ptr.clone(), stx_ptr.clone(), sid.clone(), hash, excl_srv.clone(), relay_enabled).await;
            }
        });
        self.tasks.lock().unwrap().push(handle_srv);

        // 3. mDNS Discovery (The Working Part)
        let mdns = ServiceDaemon::new()?;
        let hostname = gethostname::gethostname().to_string_lossy().into_owned() + ".local.";
        let service_info = ServiceInfo::new(&self.service_type, &self.self_id, &hostname, &my_ip, port, None)?;
        mdns.register(service_info)?;
        let browser = mdns.browse(&self.service_type)?;

        let db_rx = self.db.clone();
        let peers_rx = self.peers.clone();
        let seen_rx = self.seen_messages.clone();
        let stx_rx = self.status_tx.clone();
        let sid_rx = self.self_id.clone();
        let excl_disc = self.excluded_collections.clone();

        let handle_discovery = tokio::spawn(async move {
            while let Ok(event) = browser.recv_async().await {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let p_name = info.get_fullname().split('.').next().unwrap_or("");
                    
                    // TIE-BREAKING: Only the larger ID connects
                    if p_name > sid_rx.as_str() {
                        if let Some(addr) = info.get_addresses().iter().next() {
                            let p_addr = format!("{}:{}", addr, info.get_port());
                            let already_connected = { peers_rx.lock().await.contains_key(p_name) };
                            
                            if !already_connected {
                                if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(3), TcpStream::connect(&p_addr)).await {
                                    handle_peer(stream, db_rx.clone(), peers_rx.clone(), seen_rx.clone(), stx_rx.clone(), sid_rx.clone(), hash, excl_disc.clone(), relay_enabled).await;
                                }
                            }
                        }
                    }
                }
            }
        });
        self.tasks.lock().unwrap().push(handle_discovery);

        // 3. Tailer
        let db_tail = self.db.clone();
        let peers_tail = self.peers.clone();
        let offsets_tail = self.shard_offsets.clone();
        let excl_tail = self.excluded_collections.clone();
        
        let handle_tailer = tokio::task::spawn_blocking(move || {
            let mut last_checkpoint_save = Instant::now(); // Use Instant for timing
            loop {
                if peers_tail.blocking_lock().is_empty() {
                    std::thread::sleep(Duration::from_millis(1000));
                    continue;
                }
                
                let mut overall_changed = false;
                let mut offsets = offsets_tail.lock().unwrap();
                
                for col in db_tail.list_collections().unwrap_or_default() {
                    if excl_tail.contains(&col) { continue; }
                    let shard = db_tail.get_shard(&col);
                    let last_pos = *offsets.get(&col).unwrap_or(&0);
                    
                    let tail_data = {
                        let guard = shard.read().unwrap();
                        guard.wal.tail(last_pos)
                    };

                    if let Ok((ops, new_pos)) = tail_data {
                        if !ops.is_empty() {
                            let mut logical_ops = Vec::new();
                            for op in ops {
                                if let Some(bytes) = resolve_op_to_bytes(&shard, &db_tail, &op) {
                                    logical_ops.push(WalOp::PutInlined { key: op.get_key().to_string(), value: bytes });
                                } else if matches!(op, WalOp::Delete { .. }) {
                                    logical_ops.push(op);
                                }
                            }
                            if !logical_ops.is_empty() {
                                broadcast_mesh(&peers_tail, &col, logical_ops, 0);
                            }
                            offsets.insert(col, new_pos);
                            overall_changed = true;
                        }
                    };
                }

                // --- RESTORED: Periodically save offsets to the database ---
                if overall_changed && last_checkpoint_save.elapsed() > Duration::from_secs(5) {
                    let mut doc = FireLiteDoc::default();
                    let map: Vec<(Arc<str>, Value)> = offsets.iter()
                        .map(|(k, v)| (Arc::from(k.as_str()), Value::Int(*v as i64)))
                        .collect();
                    
                    doc.insert("offsets", Value::Map(map));
                    let _ = db_tail.put("__firelite_system", "sync_checkpoint", &doc);
                    last_checkpoint_save = Instant::now();
                }

                std::thread::sleep(Duration::from_millis(500));
            }
        });
        self.tasks.lock().unwrap().push(handle_tailer);

        Ok(())
    }

    pub fn stop(&self) {
        let mut guard = self.tasks.lock().unwrap();
        for h in guard.drain(..) { h.abort(); }
    }

    pub fn status(&self) -> NetworkStatus {
        self.status_rx.borrow().clone()
    }
}

// --- Logic Helpers ---

async fn handle_peer(
    stream: TcpStream, db: Arc<FireLite>, peers_map: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    seen_cache: Arc<AsyncMutex<HashSet<u128>>>, status_tx: watch::Sender<NetworkStatus>,
    self_id: String, my_hash: [u8; 32], excluded: HashSet<String>,
    enable_relay: bool,
) {
    let (mut reader, mut writer) = stream.into_split();
    let handshake = bincode::serialize(&NetPacket::Identify { id: self_id.clone(), room_hash: my_hash }).unwrap();
    if send_raw(&mut writer, &handshake).await.is_err() { return; }
    
    let p_bytes = match tokio::time::timeout(Duration::from_secs(2), recv_raw(&mut reader)).await {
        Ok(Ok(b)) => b, _ => return,
    };
    
    if let Ok(NetPacket::Identify { id: peer_id, room_hash: p_hash }) = bincode::deserialize::<NetPacket>(&p_bytes) {
        if p_hash != my_hash { return; }
        peers_map.lock().await.insert(peer_id.clone(), writer);
        update_status(&status_tx, &peers_map).await;
        let _ = send_raw(peers_map.lock().await.get_mut(&peer_id).unwrap(), &bincode::serialize(&NetPacket::SyncRequest).unwrap()).await;

        loop {
            let raw = match recv_raw(&mut reader).await { Ok(b) => b, _ => break };
            if let Ok(packet) = bincode::deserialize::<NetPacket>(&raw) {
                match packet {
                    NetPacket::SyncRequest => handle_bootstrap(&db, &peers_map, &peer_id, &excluded).await,
                    NetPacket::Replication { msg_id, collection, ops } => {
                        if msg_id != 0 && !seen_cache.lock().await.insert(msg_id) { continue; }
                        apply_replication_batch(&db, collection, ops).await;
                        // relay_mesh(&peers_map, &raw, &peer_id).await;
                        if enable_relay {
                            relay_mesh(&peers_map, &raw, &peer_id).await;
                        }
                    }
                    _ => {}
                }
            }
        }
        peers_map.lock().await.remove(&peer_id);
        update_status(&status_tx, &peers_map).await;
    }
}

fn resolve_op_to_bytes(shard_arc: &Arc<RwLock<crate::storage::engine::StorageEngine>>, db: &Arc<FireLite>, op: &WalOp) -> Option<Vec<u8>> {
    // 1. Resolve the raw bytes from the WAL op
    let bytes = match op {
        // If it's a pointer to a segment or a finalized blob on disk
        WalOp::Put { segment_id, segment_offset, len, .. } => {
            let ptr = Pointer::Segment { segment_id: *segment_id, offset: *segment_offset, len: *len };
            shard_arc.read().unwrap().read_pointer_internal(&ptr, false).ok().flatten()?
        }
        WalOp::PutBlob { offset, len, .. } => {
            let ptr = Pointer::Blob { offset: *offset, len: *len };
            shard_arc.read().unwrap().read_pointer_internal(&ptr, false).ok().flatten()?
        }
        // If it's already inlined in the WAL (This is where our skeletons live!)
        WalOp::PutInlined { value, .. } => value.clone(),
        
        _ => return None, // Ignore markers like BeginTx/CommitTx
    };

    // 2. Decode the document to see if it's a skeleton
    let mut doc = FireLiteDoc::decode(&bytes)?;
    
    // 3. Check for any BlobLinks
    let has_links = doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }));

    if has_links {
        let encryption_key = db.config.encryption_key.as_deref();
        
        if let Err(_e) = crate::engine::engine::resolve_doc_static(&mut doc, shard_arc, encryption_key) {
            return None;
        }
        
        return Some(doc.encode());
    }

    // If no links were found, it's a standard document, send as-is
    Some(bytes)
}

async fn handle_bootstrap(db: &Arc<FireLite>, peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, peer_id: &str, excluded: &HashSet<String>) {
    let encryption_key = db.config.encryption_key.as_deref();

    for col in db.list_collections().unwrap_or_default() {
        if excluded.contains(&col) { continue; }
        let shard_arc = db.get_shard(&col);
        
        // 1. Snapshot the index (Short lock)
        let snapshot: Vec<(String, Pointer)> = {
            let guard = shard_arc.read().unwrap();
            guard.index.iter().map(|(k,p)| (k.clone(), p.clone())).collect()
        };

        for (key, ptr) in snapshot {
            // 2. Read and Inflate data in an isolated scope
            // This ensures the RwLockReadGuard is dropped BEFORE the .await below
            let bytes_to_send = {
                let guard = shard_arc.read().unwrap();
                match guard.read_pointer_internal(&ptr, false) {
                    Ok(Some(bytes)) => {
                        let mut final_bytes = bytes;
                        // Check if we need to inflate the skeleton
                        if let Some(mut doc) = FireLiteDoc::decode(&final_bytes) {
                            if doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. })) {
                                // Drop this specific read guard because resolve_doc_static will acquire its own
                                drop(guard); 
                                let _ = crate::engine::engine::resolve_doc_static(&mut doc, &shard_arc, encryption_key);
                                final_bytes = doc.encode();
                            }
                        }
                        Some(final_bytes)
                    }
                    _ => None,
                }
            }; // <--- All Shard Locks are 100% dropped here.

            if let Some(value) = bytes_to_send {
                let packet = NetPacket::Replication { 
                    msg_id: 0, 
                    collection: col.clone(), 
                    ops: vec![WalOp::PutInlined { key, value }] 
                };

                if let Ok(payload) = bincode::serialize(&packet) {
                    // 3. Now we can safely await the async Mutex for peers
                    let mut guard = peers.lock().await;
                    if let Some(w) = guard.get_mut(peer_id) {
                        let _ = send_raw(w, &payload).await;
                    }
                }
            }
        }
    }
}


async fn apply_replication_batch(db: &Arc<FireLite>, collection: String, ops: Vec<WalOp>) {
    let shard_arc = db.get_shard(&collection);
    let mut filtered_ops = Vec::new();
    let mut index_puts = Vec::new();
    let mut replication_blob_work = Vec::new();
    let threshold = db.config.value_blob_threshold_bytes;

    let mut shard = shard_arc.write().unwrap();
    for op in ops {
        let (key, mut doc, is_delete, delete_ts) = match op {
            WalOp::PutInlined { key, value } => {
                if let Some(d) = FireLiteDoc::decode(&value) { (key, d, false, 0) } else { continue; }
            }
            WalOp::Delete { key, timestamp } => (key, FireLiteDoc::default(), true, timestamp),
            _ => continue,
        };

        let remote_ts = if is_delete { delete_ts } else { doc.get_logical_time() };
        if let Some(local_ptr) = shard.index.get(&key) {
            let local_ts = match local_ptr {
                Pointer::Deleted { timestamp } => *timestamp,
                _ => shard.read_pointer_internal(local_ptr, false).ok().flatten()
                        .and_then(|b| FireLiteDoc::decode(&b)).map(|d| d.get_logical_time()).unwrap_or(0),
            };
            if remote_ts <= local_ts { continue; }
        }

        if !is_delete {
            // Updated call: now returns work instead of blocking on file write
            let work = shard
                .blob_manager
                .as_ref()
                .map(|bm| db.process_doc_blobs(&collection, &mut doc, bm, threshold, remote_ts))
                .unwrap_or_default();
            replication_blob_work.extend(work);
            
            filtered_ops.push(WalOp::PutInlined { key: key.clone(), value: doc.encode() });
            if let Some((_, doc_id)) = key.split_once(':') { 
                index_puts.push((doc_id.to_string(), doc)); 
            }
        } else {
            filtered_ops.push(WalOp::Delete { key, timestamp: delete_ts });
        }
    }

    // Commit to local shard
    if !filtered_ops.is_empty() && shard.apply_replicated_ops(&filtered_ops).is_ok() {
        if !index_puts.is_empty() { 
            db.inject_replication_to_indexer(collection, Arc::new(index_puts)); 
        }
    }

    // DISPATCH: Offload remote blobs to the same background pool as local writes
    for w in replication_blob_work {
        let _ = db.blob_tx.try_send(w);
    }
}

async fn relay_mesh(peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, data: &[u8], sender_id: &str) {
    let mut guard = peers.lock().await;
    for (id, writer) in guard.iter_mut() {
        if id != sender_id { let _ = send_raw(writer, data).await; }
    }
}

fn broadcast_mesh(peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, col: &str, ops: Vec<WalOp>, msg_id: u128) {
    let id = if msg_id == 0 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() } else { msg_id };
    let packet = NetPacket::Replication { msg_id: id, collection: col.to_string(), ops };
    if let Ok(payload) = bincode::serialize(&packet) {
        let p_ptr = peers.clone();
        tokio::spawn(async move {
            let mut guard = p_ptr.lock().await;
            for writer in guard.values_mut() { let _ = send_raw(writer, &payload).await; }
        });
    }
}

async fn update_status(tx: &watch::Sender<NetworkStatus>, peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>) {
    let guard = peers.lock().await;
    let list: Vec<String> = guard.keys().cloned().collect();
    tx.send_modify(|s| { 
        s.peer_count = list.len(); 
        s.known_peers = list; 
        s.status = if s.peer_count > 0 { SyncStatus::Connected } else { SyncStatus::Idle }; 
    });
}

async fn send_raw<W: AsyncWriteExt + Unpin>(w: &mut W, data: &[u8]) -> tokio::io::Result<()> {
    w.write_all(&(data.len() as u32).to_le_bytes()).await?;
    w.write_all(data).await
}

async fn recv_raw<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<Vec<u8>> {
    let mut len_b = [0u8; 4];
    r.read_exact(&mut len_b).await?;
    let mut data = vec![0u8; u32::from_le_bytes(len_b) as usize];
    r.read_exact(&mut data).await?;
    Ok(data)
}
