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
use std::sync::atomic::Ordering;
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
#[cfg(feature = "net-sync")]
use crate::storage::blob::BlobWork;

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
    Ping { 
        versions: HashMap<String, i64>, 
        indexes: crate::engine::engine::IndexList,
    },
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
    seen_messages: Arc<AsyncMutex<Vec<u128>>>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
    shard_offsets: Arc<Mutex<HashMap<String, u64>>>,
    pub enable_relay: bool, 
    last_mesh_ping: Arc<Mutex<Instant>>,
    echo_cache: Arc<Mutex<HashMap<String, i64>>>,
    mdns: Arc<Mutex<Option<ServiceDaemon>>>,
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

        excluded.extend(vec!["__firelite_system".into()]);

        Self {
            db: db.clone(), 
            self_id: name.to_string(), 
            room_hash,
            excluded_collections: excluded.into_iter().collect(),
            // service_type: format!("_{}._tcp.local.", db.db_name().to_lowercase().replace('.', "_")),
            service_type: "_firelite._tcp.local.".to_string(),
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(AsyncMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(Vec::with_capacity(1000))),
            tasks: Arc::new(Mutex::new(Vec::new())),
            shard_offsets: Arc::new(Mutex::new(shard_offsets)),
            enable_relay: false,
            last_mesh_ping: Arc::new(Mutex::new(Instant::now())),
            echo_cache: Arc::new(Mutex::new(HashMap::new())),
            mdns: Arc::new(Mutex::new(None)),
        }
    }

    pub fn with_relay(mut self, enable: bool) -> Self {
        self.enable_relay = enable;
        self
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        self.stop();
        let my_ip = local_ip_address::local_ip().map(|ip| ip.to_string()).unwrap_or_else(|_| "127.0.0.1".to_string());
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        let db_ptr = self.db.clone();
        let peers_ptr = self.peers.clone();
        let seen_ptr = self.seen_messages.clone();
        let stx_ptr = self.status_tx.clone();
        let sid = self.self_id.clone();
        let hash = self.room_hash;
        let excl_srv = self.excluded_collections.clone();
        let last_ping_ptr = self.last_mesh_ping.clone();
        let echo_cache_clone = self.echo_cache.clone();

        let relay_enabled = self.enable_relay; 

        let handle_srv = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let db_c = db_ptr.clone();
                let peers_c = peers_ptr.clone();
                let seen_c = seen_ptr.clone();
                let stx_c = stx_ptr.clone();
                let sid_c = sid.clone();
                let hash_c = hash;
                let excl_c = excl_srv.clone();
                let lp_c = last_ping_ptr.clone();
                let echo_c = echo_cache_clone.clone();
                
                // SPAWN the handler so the loop can continue accepting other peers
                tokio::spawn(async move {
                    handle_peer(
                        stream, db_c, peers_c, seen_c, stx_c, 
                        sid_c, hash_c, excl_c, relay_enabled, 
                        lp_c, echo_c
                    ).await;
                });
            }
        });
        self.tasks.lock().unwrap().push(handle_srv);

        // 3. mDNS Discovery (The Working Part)
        let mdns = ServiceDaemon::new().expect("Failed to create mDNS");
        * self.mdns.lock().unwrap() = Some(mdns.clone());
        
        let hostname = gethostname::gethostname().to_string_lossy().into_owned() + ".local.";
        let service_info = ServiceInfo::new(&self.service_type, &self.self_id, &hostname, &my_ip, port, None)?;
        mdns.register(service_info)?;

        // 1. Create the browser once
        let browser = mdns.browse(&self.service_type)?;

        // 2. Prepare all clones needed for the background task
        let db_rx = self.db.clone();
        let peers_rx = self.peers.clone();
        let seen_rx = self.seen_messages.clone();
        let stx_rx = self.status_tx.clone();
        let sid_rx = self.self_id.clone();
        let excl_disc = self.excluded_collections.clone();
        let lp_disc = self.last_mesh_ping.clone();
        let hash_disc = self.room_hash;
        let relay_disc = self.enable_relay;
        let echo_cache_clone = self.echo_cache.clone();

        // 3. SINGLE Unified Discovery & Reconnection Task
        let handle_discovery = tokio::spawn(async move {
            // Map of Name -> Last Known Address
            let mut discovery_cache: HashMap<String, String> = HashMap::new();
            let mut retry_interval = tokio::time::interval(Duration::from_secs(15));
            let mut connecting: HashSet<String> = HashSet::new();
            
            // We own 'browser' here and use it exclusively in this loop
            loop {
                tokio::select! {
                    // Branch A: Listen for NEW peers via mDNS
                    event_res = browser.recv_async() => {
                        match event_res {
                            Ok(ServiceEvent::ServiceResolved(info)) => {
                                let p_name = info.get_fullname().split('.').next().unwrap_or("").to_string();
                                if p_name == sid_rx { continue; }

                                // if let Some(addr) = info.get_addresses().iter().next() {
                                //     let p_addr = format!("{}:{}", addr, info.get_port());
                                //     discovery_cache.insert(p_name, p_addr);
                                // }
                                let p_addr = info.get_addresses()
                                    .iter()
                                    .filter_map(|addr| match addr {
                                        std::net::IpAddr::V4(v4) if !v4.is_loopback() => Some(*v4),
                                        _ => None,
                                    })
                                    .max_by_key(|ip| {
                                        let o = ip.octets();
                                        // prioritize LAN ranges
                                        if o[0] == 192 && o[1] == 168 { 3 }
                                        else if o[0] == 10 { 2 }
                                        else if o[0] == 172 && (16..=31).contains(&o[1]) { 1 }
                                        else { 0 }
                                    })
                                    .map(|ip| format!("{}:{}", ip, info.get_port()));

                                if let Some(p_addr) = p_addr {
                                    discovery_cache.insert(p_name, p_addr);
                                }
                            }
                            Ok(ServiceEvent::ServiceRemoved(_type, name)) => {
                                let p_name = name.split('.').next().unwrap_or("");
                                discovery_cache.remove(p_name);
                                connecting.remove(p_name);
                            }
                            _ => { }
                        }
                    }

                    // Branch B: PERIODICALLY try to connect to peers in the cache
                    _ = retry_interval.tick() => {
                        let active_peer_names = {
                            let guard = peers_rx.lock().await;
                            guard.keys().cloned().collect::<HashSet<String>>()
                        };

                        connecting.retain(|name| !active_peer_names.contains(name));

                        for (p_name, p_addr) in &discovery_cache {
                            // TIE-BREAKING: Only higher ID initiates to prevent double-connections
                            if p_name > &sid_rx && !active_peer_names.contains(p_name) {
                            // if !active_peer_names.contains(p_name) {
                                let db_c = db_rx.clone();
                                let peers_c = peers_rx.clone();
                                let seen_c = seen_rx.clone();
                                let stx_c = stx_rx.clone();
                                let sid_c = sid_rx.clone();
                                let excl_c = excl_disc.clone();
                                let lp_c = lp_disc.clone();
                                let addr_c = p_addr.clone();
                                let echo_cache = echo_cache_clone.clone();

                                tokio::spawn(async move {
                                    // Short timeout so a single dead peer doesn't hang the loop
                                    if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr_c)).await {
                                        handle_peer(
                                            stream, db_c, peers_c, seen_c, stx_c, 
                                            sid_c, hash_disc, excl_c, relay_disc, lp_c,
                                            echo_cache
                                        ).await;
                                    }
                                });
                            }
                        }
                    }
                }
            }
        });

        // 4. Push the single handle to the tasks list
        self.tasks.lock().unwrap().push(handle_discovery);

        // 3. Tailer
        let db_tail = self.db.clone();
        let peers_tail = self.peers.clone();
        let offsets_tail = self.shard_offsets.clone();
        let excl_tail = self.excluded_collections.clone();
        let echo_cache_clone = self.echo_cache.clone();
        
        let handle_tailer = tokio::task::spawn_blocking(move || {
            let mut last_checkpoint_save = Instant::now(); // Use Instant for timing
            loop {
                if peers_tail.blocking_lock().is_empty() {
                    std::thread::sleep(Duration::from_millis(1000));
                    continue;
                }
                
                let mut overall_changed = false;
                let mut offsets = offsets_tail.lock().unwrap();

                // Adding hidden internal firelite security collection into sync
                let mut cols = db_tail.list_collections().unwrap_or_default();
                cols.extend(vec!["__firelite_security".to_string()]);

                for col in cols {
                    if excl_tail.contains(&col) { continue; }
                    // let shard = db_tail.get_shard(&col);
                    let shard = match db_tail.get_shard(&col) {
                        Ok(s) => s,
                        Err(_) => {
                            // Collection is likely encrypted and we don't have the key.
                            // Skip silently or log once to avoid spamming the console.
                            continue; 
                        }
                    };
                    let last_pos = *offsets.get(&col).unwrap_or(&0);
                    
                    let tail_data = {
                        let guard = shard.read().unwrap();
                        guard.wal.tail(last_pos)
                    };

                    if let Ok((ops, new_pos)) = tail_data {
                        if !ops.is_empty() {
                            let mut logical_ops = Vec::new();
                            for op in ops {

                                let key = op.get_key();
                                        
                                // 1. Get the timestamp from the WAL record
                                let wal_timestamp = match &op {
                                    WalOp::PutInlined { value, .. } => {
                                        // Version 3: Magic(1), Ver(1), Time(8)
                                        i64::from_le_bytes(value[2..10].try_into().unwrap_or([0;8]))
                                    }
                                    WalOp::Delete { timestamp, .. } => *timestamp,
                                    _ => 0,
                                };

                                // 2. CHECK & REMOVE (The Core Fix)
                                let is_echo = {
                                    let echo_cache = echo_cache_clone.clone();
                                    let mut cache = echo_cache.lock().unwrap();
                                    if let Some(&cached_ts) = cache.get(key) {
                                        if cached_ts == wal_timestamp {
                                            // Match found! Remove it so it doesn't linger
                                            cache.remove(key);
                                            true
                                        } else {
                                            false
                                        }
                                    } else {
                                        false
                                    }
                                };

                                if is_echo {
                                    continue; // Skip this one, it was a remote write
                                }

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

        // Inside NetSyncer::start
        let db_audit = self.db.clone();
        let peers_audit = self.peers.clone();
        let last_ping_ptr = self.last_mesh_ping.clone();

        let handle_periodic_ping = tokio::spawn(async move {
            let base_interval = Duration::from_secs(300); // 5 minutes audit
            
            loop {
                tokio::time::sleep(base_interval).await;

                // Randomized Jitter to prevent "Thundering Herd"
                let jitter = {
                    use rand::Rng;
                    rand::thread_rng().gen_range(10..500)
                };
                tokio::time::sleep(Duration::from_millis(jitter)).await;

                // Only ping if NO ONE in the mesh has pinged in the last 5 minutes
                let should_ping = {
                    let lp = last_ping_ptr.lock().unwrap();
                    lp.elapsed() >= base_interval
                };

                if should_ping {
                    let my_versions = db_audit.get_version_map();
                    let my_indexes = db_audit.list_indexes(None);
                    let packet = NetPacket::Ping { 
                        versions: my_versions, 
                        indexes: my_indexes
                    };
                    
                    if let Ok(payload) = bincode::serialize(&packet) {
                        let mut guard = peers_audit.lock().await;
                        for writer in guard.values_mut() {
                            let _ = send_raw(writer, &payload).await;
                        }
                        // Reset local timer
                        let mut lp = last_ping_ptr.lock().unwrap();
                        *lp = Instant::now();
                    }
                }
            }
        });
        self.tasks.lock().unwrap().push(handle_periodic_ping);

        Ok(())
    }

    pub fn stop(&self) {
        let mut guard = self.tasks.lock().unwrap();
        for h in guard.drain(..) { h.abort(); }
        if let Some(mdns) = self.mdns.lock().unwrap().take() {
            let _ = mdns.shutdown();
        }
        // 4. clear peers (close sockets)
        let peers = self.peers.clone();
        // Only spawn on a running Tokio runtime. FFI callers from other
        // languages (Go / Node / Pascal / C) have no ambient runtime, so
        // fall back to dropping the peer write halves inline - the sockets
        // are closed anyway once the last Arc reference is dropped.
        if tokio::runtime::Handle::try_current().is_ok() {
            tokio::spawn(async move {
                peers.lock().await.clear();
            });
        }
    }

    pub fn status(&self) -> NetworkStatus {
        self.status_rx.borrow().clone()
    }
}

// --- Logic Helpers ---
async fn handle_peer(
    stream: TcpStream, 
    db: Arc<FireLite>, 
    peers_map: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    seen_cache: Arc<AsyncMutex<Vec<u128>>>, 
    status_tx: watch::Sender<NetworkStatus>,
    self_id: String, 
    my_hash: [u8; 32], 
    excluded: HashSet<String>,
    enable_relay: bool, 
    last_ping: Arc<Mutex<Instant>>,
    echo_cache: Arc<Mutex<HashMap<String, i64>>>,
) {
    let (mut reader, mut writer) = stream.into_split();

    // 1. Identify Handshake
    let handshake = NetPacket::Identify { id: self_id.clone(), room_hash: my_hash };
    if let Ok(bytes) = bincode::serialize(&handshake) {
        if send_raw(&mut writer, &bytes).await.is_err() { return; }
    }

    let p_bytes = match tokio::time::timeout(Duration::from_secs(5), recv_raw(&mut reader)).await {
        Ok(Ok(b)) => b,
        _ => return,
    };

    let peer_id = match bincode::deserialize::<NetPacket>(&p_bytes) {
        Ok(NetPacket::Identify { id, room_hash }) => {
            if room_hash != my_hash || id == self_id { return; }
            id
        }
        _ => return,
    };

    // 2. Register Peer
    {
        let mut guard = peers_map.lock().await;
        guard.insert(peer_id.clone(), writer);
    }
    update_status(&status_tx, &peers_map).await;

    // 3. Initial Sync Trigger
    let my_versions = db.get_version_map();
    let my_indexes = db.list_indexes(None);
    {
        let mut guard = peers_map.lock().await;
        if let Some(w) = guard.get_mut(&peer_id) {
            let _ = send_packet(w, NetPacket::Ping { 
                versions: my_versions, 
                indexes: my_indexes
            }).await;
            let _ = send_packet(w, NetPacket::SyncRequest).await;
        }
    }

    // 4. Loop
    let echo_cache_clone = echo_cache.clone();
    loop {
        let raw = match recv_raw(&mut reader).await { Ok(b) => b, _ => break };

        if let Ok(packet) = bincode::deserialize::<NetPacket>(&raw) {
            match packet {
                NetPacket::Ping { versions, indexes } => {
                    if let Ok(mut lp) = last_ping.lock() { *lp = Instant::now(); }
                    
                    for (col, fields) in indexes.secondary {
                        for field in fields {
                            // db.create_index is internal-idempotent (it won't recreate if exists)
                            let _ = db.create_index(&col, &field);
                        }
                    }

                    // 2. Sync FTS Indexes
                    for (col, fields) in indexes.fts {
                        for field in fields {
                            let _ = db.create_fts_index(&col, &field);
                        }
                    }

                    // 3. Sync Composite Indexes
                    use crate::index::composite::definition::SortDirection;
                    for comp in indexes.composite {
                        let fields: Vec<(String, SortDirection)> = comp.fields.into_iter().map(|f| {
                            let dir = if f.direction == "desc" { SortDirection::Desc } else { SortDirection::Asc };
                            (f.field, dir)
                        }).collect();
                        
                        // This registers the index and starts background backfilling
                        let _ = db.create_composite_index(&comp.collection, fields);
                    }
                    
                    for (col, remote_time) in versions {
                        if excluded.contains(&col) { continue; }
                        // if db.get_collection_version(&col) > remote_time {
                        //     handle_delta_send(&db, &peers_map, &peer_id, &col, remote_time).await;
                        // }
                        if let Ok(local_version) = db.get_collection_version(&col) {
                            if local_version > remote_time {
                                handle_delta_send(&db, &peers_map, &peer_id, &col, remote_time).await;
                            }
                        } else {
                            // Log that we couldn't check this collection due to an error
                            crate::util::log::info(&format!("[sync] Skipping collection {}: Shard could not be opened.", col));
                        }
                    }
                }
                NetPacket::SyncRequest => {
                    handle_bootstrap(&db, &peers_map, &peer_id, &excluded).await;
                }
                NetPacket::Replication { msg_id, collection, ops } => {
                    if excluded.contains(&collection) {continue;}
                    // Check Cache
                    if msg_id != 0 {
                        let mut cache = seen_cache.lock().await;
                        if cache.contains(&msg_id) { continue; }
                        cache.push(msg_id);
                        if cache.len() > 1000 { cache.remove(0); }
                    }

                    // Apply (Lock is released here)
                    apply_replication_batch(db.clone(), collection, ops, echo_cache_clone.clone()).await; 

                    if msg_id != 0 && enable_relay { 
                        relay_mesh(&peers_map, &raw, &peer_id).await; 
                    }
                }
                _ => {}
            }
        }
    }

    // 5. Cleanup
    peers_map.lock().await.remove(&peer_id);
    update_status(&status_tx, &peers_map).await;
}


fn resolve_op_to_bytes(shard_arc: &Arc<RwLock<crate::storage::engine::StorageEngine>>, db: &Arc<FireLite>, op: &WalOp) -> Option<Vec<u8>> {
    // 1. Resolve the raw bytes (Skeleton) from the WAL op
    let bytes = match op {
        WalOp::PutInlined { value, .. } => value.clone(),
        WalOp::Put { segment_id, segment_offset, len, .. } => {
            let ptr = Pointer::Segment { segment_id: *segment_id, offset: *segment_offset, len: *len };
            shard_arc.read().unwrap().read_pointer_internal(&ptr, false).ok().flatten()?
        }
        WalOp::PutBlob { offset, len, .. } => {
            let ptr = Pointer::Blob { offset: *offset, len: *len };
            shard_arc.read().unwrap().read_pointer_internal(&ptr, false).ok().flatten()?
        }
        _ => return None,
    };

    // 2. Decode to check for BlobLinks
    if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
        let has_links = doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }));
        
        if has_links {
            // INFLATE: Replace file offsets with actual binary data for the wire
            // Blobs are not encrypted/compressed, so we read them raw from disk
            let encryption_key = db.config.encryption_key.as_deref();
            if crate::engine::engine::resolve_doc_static(&mut doc, shard_arc, encryption_key).is_ok() {
                return Some(doc.encode_buffered()); // Encode the now-full document
            }
            return None;
        }
    }

    Some(bytes)
}

async fn handle_bootstrap(db: &Arc<FireLite>, peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, peer_id: &str, excluded: &HashSet<String>) {
    let encryption_key = db.config.encryption_key.as_deref();

    let mut cols = db.list_collections().unwrap_or_default();
    cols.extend(vec!["__firelite_security".to_string()]);

    for col in cols {
        if excluded.contains(&col) { continue; }
        // let shard_arc = db.get_shard(&col);
        let shard_arc = match db.get_shard(&col) {
            Ok(s) => s,
            Err(e) => {
                eprintln!("[sync] Bootstrap skipped collection {}: {}", col, e);
                continue;
            }
        };
        
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

#[cfg(feature = "net-sync")]
async fn handle_delta_send(
    db: &Arc<FireLite>, 
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, 
    peer_id: &str, 
    collection: &str, 
    since_time: i64
) {
    // let shard_arc = db.get_shard(collection);
    let shard_arc = match db.get_shard(collection) {
        Ok(s) => s,
        Err(e) => {
            eprintln!("[sync] Delta send failed for {}: {}", collection, e);
            return;
        }
    };
    let encryption_key = db.config.encryption_key.as_deref();

    // 1. SCAN PHASE (RAM-only)
    // Identify which keys changed without touching the disk yet.
    let changed_items: Vec<(String, Pointer)> = {
        let guard = shard_arc.read().unwrap();
        guard.index.iter()
            .filter_map(|(k, ptr)| {
                let ts = match ptr {
                    Pointer::Inlined(bytes) => {
                        // Extract _time from version 3 header [2..10]
                        i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8]))
                    }
                    Pointer::BlobPending(doc) => doc.get_logical_time(),
                    Pointer::Deleted { timestamp } => *timestamp,
                    Pointer::BlobPendingData { data: _, skeleton } => {
                        i64::from_le_bytes(skeleton[2..10].try_into().unwrap_or([0;8]))
                    }
                    _ => 0,
                };
                
                if ts > since_time {
                    Some((k.clone(), ptr.clone()))
                } else {
                    None
                }
            })
            .collect()
    };

    if changed_items.is_empty() { return; }

    // 2. INFLATION & TRANSMISSION PHASE
    let mut batch_ops = Vec::with_capacity(50);

    for (key, ptr) in changed_items {
        match ptr {
            Pointer::Deleted { timestamp } => {
                batch_ops.push(WalOp::Delete { key, timestamp });
            }
            _ => {
                // Read the skeleton/data from local storage
                let raw_res = {
                    let guard = shard_arc.read().unwrap();
                    guard.read_pointer_internal(&ptr, false).ok().flatten()
                };

                if let Some(bytes) = raw_res {
                    if let Some(mut doc) = FireLiteDoc::decode(&bytes) {
                        // INFLATE: If document has blobs, resolve them.
                        // resolve_doc_static will look in the RAM queue before hitting blobs.dat
                        let has_blobs = doc.fields.iter().any(|(_, v)| matches!(v, Value::BlobLink { .. }));
                        
                        let finalized_bytes = if has_blobs {
                            if crate::engine::engine::resolve_doc_static(&mut doc, &shard_arc, encryption_key).is_ok() {
                                doc.encode_buffered()
                            } else {
                                continue; // Skip if inflation fails to prevent sending corrupted docs
                            }
                        } else {
                            bytes // No blobs, send original bytes
                        };

                        batch_ops.push(WalOp::PutInlined { key, value: finalized_bytes });
                    }
                }
            }
        }

        // 3. BATCH SENDING
        if batch_ops.len() >= 50 {
            send_replication_packet(peers, peer_id, collection, std::mem::take(&mut batch_ops)).await;
        }
    }

    // Send the final remaining items
    if !batch_ops.is_empty() {
        send_replication_packet(peers, peer_id, collection, batch_ops).await;
    }
}

/// Helper to serialize and send a batch of operations to a specific peer.
/// msg_id is set to 0 for bootstrap/delta syncs to prevent mesh relay loops.
async fn send_replication_packet(
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, 
    peer_id: &str, 
    collection: &str, 
    ops: Vec<WalOp>
) {
    let packet = NetPacket::Replication { 
        msg_id: 0, 
        collection: collection.to_string(), 
        ops 
    };

    // Serialize the packet using bincode
    match bincode::serialize(&packet) {
        Ok(payload) => {
            // Acquire the async lock for the peer map
            let mut guard = peers.lock().await;
            
            // Look up the specific peer's TCP write half
            if let Some(writer) = guard.get_mut(peer_id) {
                // Send using your existing send_raw utility (length-prefix + data)
                if let Err(e) = send_raw(writer, &payload).await {
                    eprintln!("[net_sync] Failed to send packet to {}: {}", peer_id, e);
                }
            }
        }
        Err(e) => {
            eprintln!("[net_sync] Serialization error for peer {}: {}", peer_id, e);
        }
    }
}

#[cfg(feature = "net-sync")]
async fn apply_replication_batch(db: Arc<FireLite>, collection: String, ops: Vec<WalOp>, echo_cache: Arc<Mutex<HashMap<String, i64>>>,) {
    // let shard_arc = db.get_shard(&collection);
    let shard_arc = match db.get_shard(&collection) {
        Ok(s) => s,
        Err(e) => {
            // This is a serious error: we received data but cannot write it
            // because the local shard is locked/unreadable.
            eprintln!("[sync] CRITICAL: Cannot apply replication to {}. Shard error: {}", collection, e);
            return; 
        }
    };
    let threshold = db.config.value_blob_threshold_bytes;
    
    let mut accepted_ops = Vec::new();
    let mut affected_keys = Vec::new();
    let mut index_puts = Vec::new();
    let mut blob_work_items = Vec::new();

    // 1. PHASE 1: PREPARE AND CONFLICT RESOLUTION
    {
        // We take a read lock first to check timestamps (LWW)
        let shard_read = shard_arc.read().unwrap();
        let echo_cache_clone = echo_cache.clone();
        
        for op in ops {
            let (key, mut doc, is_delete, remote_ts) = match op {
                WalOp::PutInlined { ref key, ref value } => {
                    if let Some(d) = FireLiteDoc::decode(value) { 
                        let ts = d.get_logical_time();
                        (key.clone(), d, false, ts) 
                    } else { continue; }
                }
                WalOp::Delete { ref key, timestamp } => {
                    (key.clone(), FireLiteDoc::default(), true, timestamp)
                }
                _ => continue,
            };

            // Conflict Resolution: Only apply if the remote timestamp is newer than local
            if let Some(local_ptr) = shard_read.index.get(&key) {
                let local_ts = match local_ptr {
                    Pointer::Deleted { timestamp } => *timestamp,
                    Pointer::Inlined(bytes) => i64::from_le_bytes(bytes[2..10].try_into().unwrap_or([0;8])),
                    _ => {
                        // Fast path: if the pointer is in-memory (Inlined/Pending), get time directly
                        // otherwise decode the disk header.
                        shard_read.read_pointer_internal(local_ptr, false)
                            .ok().flatten()
                            .and_then(|b| FireLiteDoc::decode(&b))
                            .map(|d| d.get_logical_time())
                            .unwrap_or(0)
                    }
                };
                if remote_ts <= local_ts { continue; }
            }

            if is_delete {
                {
                    let mut cache = echo_cache_clone.lock().unwrap();
                    cache.insert(key.clone(), remote_ts);
                }
                accepted_ops.push(WalOp::Delete { key: key.clone(), timestamp: remote_ts });
                index_puts.push((key, None)); // None signals delete in our local loop
            } else {
                let ts = doc.get_logical_time();
                {
                    let mut cache = echo_cache_clone.lock().unwrap();
                    cache.insert(key.clone(), ts);
                }
                // RE-EXTRACT BLOBS: If the sender sent a full doc but it's large,
                // we extract blobs locally on the receiver to save segment space.
                if let Some(bm) = &shard_read.blob_manager {
                    let extracted = bm.extract_blobs_raw(&collection, &key, &mut doc, threshold);
                    for b in extracted {
                        blob_work_items.push(b);
                    }
                }
                
                let skeleton_bytes = doc.encode();
                accepted_ops.push(WalOp::PutInlined { key: key.clone(), value: skeleton_bytes });
                index_puts.push((key.clone(), Some(doc)));
                affected_keys.push(key.into());
            }
        }
    } // Read lock dropped

    if accepted_ops.is_empty() { return; }

    // 2. PHASE 2: PHYSICAL COMMIT (Receiver Shard)
    {
        let mut shard = shard_arc.write().unwrap();
        
        // A. WAL Commit
        let tx_id = shard.next_tx_id;
        shard.next_tx_id += 1;
        // Use the fast batch appender
        let _ = shard.wal.append_batch_fast(tx_id, &accepted_ops, true); // true = remote (skip fsync)

        // B. Index Update
        for (key, doc_opt) in index_puts {
            if let Some(doc) = doc_opt {
                // If we extracted blobs, mark as Pending
                let has_blob = blob_work_items.iter().any(|b| {
                    if let BlobWork::PutRaw { key: k, .. } = b { k == &key } else { false }
                });

                if has_blob {
                    shard.update_index_entry(key, Some(Pointer::BlobPending(Arc::new(doc))));
                } else {
                    shard.update_index_entry(key, Some(Pointer::Inlined(Arc::new(doc.encode()))));
                }
            } else {
                // It was a delete
                let ts = accepted_ops.iter().find_map(|o| {
                    if let WalOp::Delete { key: k, timestamp } = o {
                        if k == &key { return Some(*timestamp); }
                    }
                    None
                }).unwrap_or(0);
                shard.update_index_entry(key, Some(Pointer::Deleted { timestamp: ts }));
            }
        }

        // C. Queue Blobs for Receiver's Blob Worker
        let mut total_bytes = 0;
        for b in blob_work_items {
            if let BlobWork::PutRaw { len, .. } = &b { total_bytes += *len as usize; }
            shard.blob_flush_queue.push_back(b);
        }
        shard.total_pending_blob_bytes.fetch_add(total_bytes, Ordering::Relaxed);
        
        // Wake up receiver's blob worker
        db.trigger_blob_flush.store(true, Ordering::Release);
    }

    // 3. PHASE 3: NOTIFY LOCAL SYSTEM
    db.bump_versions_by_keys(affected_keys);
    
    // 4. PHASE 4: Hand to Indexer
    // update search indexes.
    let index_docs: Vec<(String, Arc<FireLiteDoc>)> = accepted_ops.iter().filter_map(|op| {
        if let WalOp::PutInlined { key, value } = op {
            // Attempt to extract naked ID if using "col:id" format, else use key as is
            let doc_id = key.split_once(':')
                .map(|(_, id)| id.to_string())
                .unwrap_or_else(|| key.clone());

            // Decode the bytes and wrap the resulting document in an Arc immediately
            FireLiteDoc::decode(value).map(|d| (doc_id, Arc::new(d)))
        } else { 
            None 
        }
    }).collect();

    if !index_docs.is_empty() {
        // Send the batch to the persistent index worker
        let _ = db.index_tx.send(crate::engine::engine::IndexOp::Update { 
            collection: collection.clone(), 
            // Wrap the whole vector in an Arc as required by the Enum definition
            puts: Arc::new(index_docs), 
            deletes: vec![] 
        });
    }

    // 5. PHASE 5. Notify Watcher (change event)
    for op in &accepted_ops {
        let kind = match op {
            WalOp::PutInlined { .. } => crate::engine::ChangeKind::Put,
            WalOp::Delete { .. } => crate::engine::ChangeKind::Delete,
            _ => continue,
        };

let event = crate::engine::ChangeEvent {
path: Arc::from(op.get_key()),
kind,
};

        // Notify local watchers (Tauri frontend, etc.)
        // This triggers the UI but the 'Tailer' will skip re-broadcasting 
        // because the key/timestamp is in the echo_cache.
        db.notify_watchers(&collection, event);
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
            for writer in guard.values_mut() { 
                // let peer = writer.peer_addr().unwrap().ip().to_string();
                let _ = send_raw(writer, &payload).await; 

            }
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

/// Helper to send typed packets safely
async fn send_packet(writer: &mut OwnedWriteHalf, packet: NetPacket) -> tokio::io::Result<()> {
    if let Ok(payload) = bincode::serialize(&packet) {
        send_raw(writer, &payload).await?;
    }
    Ok(())
}
