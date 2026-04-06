#[cfg(feature = "net-sync")]
use crate::engine::engine::ReplicationEvent;
#[cfg(feature = "net-sync")]
use crate::engine::FireLite;
#[cfg(feature = "net-sync")]
use crate::document::value::Value;
#[cfg(feature = "net-sync")]
use crate::document::firelite_doc::FireLiteDoc;
#[cfg(feature = "net-sync")]
use std::sync::{Arc, RwLock, Mutex};
#[cfg(feature = "net-sync")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "net-sync")]
use std::time::UNIX_EPOCH;
#[cfg(feature = "net-sync")]
use tokio::net::{TcpStream, tcp::OwnedWriteHalf};
#[cfg(feature = "net-sync")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "net-sync")]
use tokio::sync::{Mutex as AsyncMutex, watch};
#[cfg(feature = "net-sync")]
use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};

// --- Data Structures ---

#[cfg(feature = "net-sync")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus { Idle, Searching, Connected, Syncing }

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
    Identify { id: String, is_authority: bool, db_name: String },
    SyncRequest,
    Replication { msg_id: u128, origin_id: String, event: ReplicationEvent },
}

// --- NetSyncer Engine ---

#[cfg(feature = "net-sync")]
pub struct NetSyncer {
    db: Arc<FireLite>,
    self_id: String,
    leader_id: Arc<RwLock<String>>, 
    excluded_collections: HashSet<String>,
    is_authority: bool,
    service_type: String,
    status_tx: watch::Sender<NetworkStatus>,
    status_rx: watch::Receiver<NetworkStatus>,
    peers: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, 
    seen_messages: Arc<AsyncMutex<HashSet<u128>>>,
    auth_cache: Arc<RwLock<HashMap<String, String>>>,
    tasks: Arc<Mutex<Vec<tokio::task::JoinHandle<()>>>>,
}

#[cfg(feature = "net-sync")]
impl NetSyncer {
    pub fn new(db: Arc<FireLite>, name: &str, excluded: Vec<String>, is_authority: bool) -> Self {
        let (tx, rx) = watch::channel(NetworkStatus {
            status: SyncStatus::Idle,
            self_id: name.to_string(),
            peer_count: 0,
            known_peers: Vec::new(),
        });

        // Try to load current leader from DB config if it exists
        let initial_leader = match db.get("__firelite_security", "config") {
            Ok(Some(doc)) => doc.get("current_leader")
                .and_then(|v| if let Value::String(s) = v { Some(s.clone()) } else { None })
                .unwrap_or_default(),
            _ => String::new(),
        };

        Self {
            db: db.clone(),
            self_id: name.to_string(),
            leader_id: Arc::new(RwLock::new(initial_leader)),
            excluded_collections: excluded.into_iter().collect(),
            is_authority,
            service_type: format!("_{}._tcp.local.", db.db_name().to_lowercase().replace('.', "_")),
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(AsyncMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(HashSet::with_capacity(1000))),
            auth_cache: Arc::new(RwLock::new(HashMap::new())),
            tasks: Arc::new(Mutex::new(Vec::new())),
        }
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        // 1. CLEAR OLD TASKS (if any)
        self.stop(); 

        // FIX: Do NOT define 'let mut task_guard = self.tasks.lock().unwrap();' here.
        // If you define it here, it stays alive until the end of the function,
        // crashing every .await point below.

        // 2. DETECT REAL LAN IP
        let my_ip = local_ip_address::local_ip()
            .map(|ip| ip.to_string())
            .unwrap_or_else(|_| "127.0.0.1".to_string());

        // 3. START SECURITY MONITOR
        let monitor_handle = Self::start_security_monitor(
            self.db.clone(), 
            self.auth_cache.clone(), 
            self.leader_id.clone()
        ).await;
        
        // Lock, push, and release immediately
        self.tasks.lock().unwrap().push(monitor_handle);

        // 4. mDNS REGISTRATION
        let mdns = ServiceDaemon::new()?;
        let hostname = format!("{}.local.", gethostname::gethostname().to_string_lossy());
        let service_info = ServiceInfo::new(&self.service_type, &self.self_id, &hostname, &my_ip, port, None)?;
        mdns.register(service_info)?;

        // 5. TASK 1: INCOMING PEER LISTENER
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        let db_inc = self.db.clone();
        let peers_inc = self.peers.clone();
        let seen_inc = self.seen_messages.clone();
        let status_tx_inc = self.status_tx.clone();
        let auth_cache_inc = self.auth_cache.clone();
        let leader_id_inc = self.leader_id.clone();
        let self_id_inc = self.self_id.clone();
        let excluded_inc = self.excluded_collections.clone();
        let is_auth = self.is_authority;

        let handle_listener = tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                handle_incoming_peer(
                    stream, db_inc.clone(), peers_inc.clone(), seen_inc.clone(),
                    status_tx_inc.clone(), self_id_inc.clone(), leader_id_inc.clone(),
                    excluded_inc.clone(), is_auth, auth_cache_inc.clone()
                ).await;
            }
        });
        // Lock, push, and release immediately
        self.tasks.lock().unwrap().push(handle_listener);

        // 6. TASK 2: OUTGOING PEER BROWSER (Discovery)
        let browser = mdns.browse(&self.service_type)?;
        let db_brw = self.db.clone();
        let peers_brw = self.peers.clone();
        let seen_brw = self.seen_messages.clone();
        let status_tx_brw = self.status_tx.clone();
        let auth_cache_brw = self.auth_cache.clone();
        let leader_id_brw = self.leader_id.clone();
        let self_id_brw = self.self_id.clone();
        let excluded_brw = self.excluded_collections.clone();

        let handle_browser = tokio::spawn(async move {
            while let Ok(event) = browser.recv() {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let peer_name = info.get_fullname().split('.').next().unwrap_or("");
                    if peer_name == self_id_brw { continue; }
                    
                    let already_connected = { peers_brw.lock().await.contains_key(peer_name) };
                    if !already_connected {
                        if let Some(addr) = info.get_addresses().iter().next() {
                            let addr_str = format!("{}:{}", addr, info.get_port());
                            if let Ok(Ok(stream)) = tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&addr_str)).await {
                                handle_incoming_peer(
                                    stream, db_brw.clone(), peers_brw.clone(), seen_brw.clone(),
                                    status_tx_brw.clone(), self_id_brw.clone(), leader_id_brw.clone(),
                                    excluded_brw.clone(), is_auth, auth_cache_brw.clone()
                                ).await;
                            }
                        }
                    }
                }
            }
        });
        // Lock, push, and release immediately
        self.tasks.lock().unwrap().push(handle_browser);

        // 7. TASK 3: REPLICATION BROADCASTER
        let local_rx = self.db.subscribe_replication();
        let peers_obs = self.peers.clone();
        let auth_obs = self.auth_cache.clone();
        let leader_id_obs = self.leader_id.clone();
        let self_id_obs = self.self_id.clone();

        let handle_broadcaster = tokio::spawn(async move {
            while let Ok(event) = local_rx.recv() {
                let is_security = event.0 == "__firelite_security";
                let msg_id = std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros();
                let packet = NetPacket::Replication { msg_id, origin_id: self_id_obs.clone(), event };
                
                if let Ok(payload) = bincode::serialize(&packet) {
                    let header = (payload.len() as u32).to_le_bytes();
                    let mut p_guard = peers_obs.lock().await;
                    let mut dead = Vec::new();

                    for (id, writer) in p_guard.iter_mut() {
                        let is_allowed = {
                            let cache = auth_obs.read().unwrap();
                            let current_leader = leader_id_obs.read().unwrap();
                            is_security || cache.get(id).map(|s| s == "allowed").unwrap_or(false) || id == &*current_leader
                        };

                        if is_allowed {
                            if writer.write_all(&header).await.is_err() || writer.write_all(&payload).await.is_err() {
                                dead.push(id.clone());
                            }
                        }
                    }
                    for id in dead { p_guard.remove(&id); }
                }
            }
        });
        // Lock, push, and release immediately
        self.tasks.lock().unwrap().push(handle_broadcaster);

        Ok(())
    }

    // 8. THE STOP FUNCTION (Instant Cleanup)
    pub fn stop(&self) {
        let mut task_guard = self.tasks.lock().unwrap();
        for handle in task_guard.drain(..) {
            handle.abort(); // Immediately kills the task and releases resources/ports
        }
        // Set status to Idle for UI
        self.status_tx.send_modify(|s| {
            s.status = SyncStatus::Idle;
            s.peer_count = 0;
            s.known_peers.clear();
        });
    }

    async fn start_security_monitor(
        db: Arc<FireLite>, 
        cache: Arc<RwLock<HashMap<String, String>>>, 
        leader_id_ref: Arc<RwLock<String>>
    ) -> tokio::task::JoinHandle<()> {
        {
            let shard_arc = db.get_shard("__firelite_security");
            let shard = shard_arc.read().unwrap();
            if let Ok(docs) = shard.scan_prefix("peers:") {
                let mut guard = cache.write().unwrap();
                for (key, bytes) in docs {
                    if let Some(doc) = FireLiteDoc::decode(&bytes) {
                        if let Some(Value::String(status)) = doc.get("status") {
                            let peer_id = key.replace("peers:", "");
                            guard.insert(peer_id, status.clone());
                        }
                    }
                }
            }
        }

        // 2. Return the JoinHandle from tokio::spawn
        tokio::spawn(async move {
            let rx = db.watch_collection("__firelite_security");
            while let Ok(event) = rx.recv() {
                if event.path.starts_with("peers:") {
                    let peer_id = event.path.replace("peers:", "");
                    let shard_arc = db.get_shard("__firelite_security");
                    let shard_guard = shard_arc.read().unwrap();
                    if let Ok(Some(bytes)) = shard_guard.get(&event.path) {
                        if let Some(doc) = FireLiteDoc::decode(&bytes) {
                            if let Some(Value::String(status)) = doc.get("status") {
                                cache.write().unwrap().insert(peer_id, status.clone());
                            }
                        }
                    }
                }
                if event.path == "config" {
                    let shard_arc = db.get_shard("__firelite_security");
                    let shard_guard = shard_arc.read().unwrap();
                    if let Ok(Some(bytes)) = shard_guard.get("config") {
                        if let Some(doc) = FireLiteDoc::decode(&bytes) {
                            if let Some(Value::String(new_leader)) = doc.get("current_leader") {
                                let mut lid = leader_id_ref.write().unwrap();
                                *lid = new_leader.clone();
                            }
                        }
                    }
                }
            }
        })
    }

    pub fn status(&self) -> NetworkStatus {
        // Calling .borrow() on the watch::Receiver counts as a "read"
        let stats = self.status_rx.borrow().clone();
        
        let live_peers = if let Ok(guard) = self.peers.try_lock() {
            guard.keys().cloned().collect()
        } else {
            stats.known_peers
        };

        NetworkStatus {
            status: stats.status,
            self_id: self.self_id.clone(),
            peer_count: live_peers.len(),
            known_peers: live_peers,
        }
    }
}

async fn handle_incoming_peer(
    stream: TcpStream, 
    db: Arc<FireLite>, 
    peers_map: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    seen_cache: Arc<AsyncMutex<HashSet<u128>>>, 
    status_tx: watch::Sender<NetworkStatus>,
    self_id: String, 
    leader_id: Arc<RwLock<String>>, 
    excluded: HashSet<String>, 
    is_authority: bool,
    auth_cache: Arc<RwLock<HashMap<String, String>>>,
) {
    let (mut reader, writer) = stream.into_split();
    
    let handshake = NetPacket::Identify { id: self_id, is_authority, db_name: db.db_name() };
    if let Ok(hello) = bincode::serialize(&handshake) {
        let mut w_temp = writer;
        let _ = send_raw(&mut w_temp, &hello).await;

        tokio::spawn(async move {
            let payload = match tokio::time::timeout(std::time::Duration::from_secs(2), recv_raw(&mut reader)).await {
                Ok(Ok(p)) => p,
                _ => return,
            };
            let peer_id = if let Ok(NetPacket::Identify { id, .. }) = bincode::deserialize::<NetPacket>(&payload) { id } else { return; };

            if is_authority {
                let key = format!("peers:{}", peer_id);
                let shard_arc = db.get_shard("__firelite_security");
                let exists = { shard_arc.read().unwrap().get(&key).map(|r| r.is_some()).unwrap_or(false) };
                if !exists {
                    let mut doc = FireLiteDoc::default();
                    doc.insert("status", Value::String("pending".to_string()));
                    doc.insert("name", Value::String(format!("Device {}", &peer_id[..4])));
                    let _ = db.put("__firelite_security", &key, &doc);
                }
            }

            peers_map.lock().await.insert(peer_id.clone(), w_temp);
            status_tx.send_modify(|s| {
                if !s.known_peers.contains(&peer_id) { s.known_peers.push(peer_id.clone()); }
                s.peer_count = s.known_peers.len();
                s.status = SyncStatus::Connected;
            });

            loop {
                let p_bytes = match recv_raw(&mut reader).await { Ok(p) => p, Err(_) => break };
                let packet: NetPacket = match bincode::deserialize(&p_bytes) { Ok(p) => p, Err(_) => continue };

                match packet {
                    NetPacket::Identify { .. } => {},
                    NetPacket::SyncRequest => {
                        let allowed = is_peer_allowed(&auth_cache, &peer_id);
                        let is_lid = { leader_id.read().unwrap().eq(&peer_id) };
                        // Accept bootstrap request if peer is authorized or is the authority
                        if allowed || is_lid || is_authority {
                            handle_bootstrap_request(&db, &peers_map, &peer_id, &excluded).await;
                        }
                    }
                    NetPacket::Replication { msg_id, origin_id, event } => {
                        {
                            let mut seen = seen_cache.lock().await;
                            if seen.contains(&msg_id) { continue; }
                            seen.insert(msg_id);
                        }

                        let is_security = event.0 == "__firelite_security";
                        let can_sync = {
                            let cache = auth_cache.read().unwrap();
                            let current_leader = leader_id.read().unwrap();
                            // RECEIVE GATE:
                            // 1. Security lane is ALWAYS accepted.
                            // 2. Data lane only from Allowed Peers or the Leader.
                            is_security || 
                            cache.get(&origin_id).map(|s| s == "allowed").unwrap_or(false) ||
                            origin_id == *current_leader
                        };

                        if can_sync && !excluded.contains(&event.0) {
                            apply_replication(&db, event).await;
                        }
                    }
                }
            }
            peers_map.lock().await.remove(&peer_id);
            status_tx.send_modify(|s| {
                s.known_peers.retain(|p| p != &peer_id);
                s.peer_count = s.known_peers.len();
                if s.peer_count == 0 { s.status = SyncStatus::Searching; }
            });
        });
    }
}

async fn apply_replication(db: &Arc<FireLite>, event: ReplicationEvent) {
    let (col, ops, docs, _blobs) = event;
    let shard_arc = db.get_shard(&col);
    let mut shard = shard_arc.write().unwrap();
    
    for (id, doc) in docs.iter() {
        let key = format!("{}:{}", col, id);
        let mut should_apply = true;
        if let Ok(Some(local)) = shard.get(&key) {
            if let Some(local_doc) = FireLiteDoc::decode(&local) {
                if get_logical_timestamp(doc) <= get_logical_timestamp(&local_doc) {
                    should_apply = false;
                }
            }
        }
        if should_apply {
            if let Some(op) = ops.iter().find(|o| o.get_key() == key) {
                let _ = shard.apply_replicated_ops(&[op.clone()]);
                db.inject_replication_to_indexer(col.clone(), Arc::new(vec![(id.clone(), doc.clone())]));
            }
        }
    }
}

#[cfg(feature = "net-sync")]
async fn handle_bootstrap_request(db: &Arc<FireLite>, peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, peer_id: &str, excluded: &HashSet<String>) {
    let collections = db.list_collections().unwrap_or_default();
    for col in collections {
        if excluded.contains(&col) { continue; }
        let all_data = {
            let shard_arc = db.get_shard(&col);
            let shard_guard = shard_arc.read().unwrap();
            shard_guard.scan_prefix("").unwrap_or_default()
        }; 

        for chunk in all_data.chunks(100) {
            let docs: Vec<_> = chunk.iter().filter_map(|(k, v)| {
                FireLiteDoc::decode(v).map(|d| (k.split(':').last().unwrap().to_string(), d))
            }).collect();
            
            let event = (col.clone(), Vec::new(), Arc::new(docs), Vec::new());
            let packet = NetPacket::Replication { msg_id: 0, origin_id: "system".to_string(), event };
            
            if let Ok(payload) = bincode::serialize(&packet) {
                let mut guard = peers.lock().await;
                if let Some(w) = guard.get_mut(peer_id) { 
                    let _ = send_raw(w, &payload).await; 
                }
            }
        }
    }
}

fn is_peer_allowed(cache: &Arc<RwLock<HashMap<String, String>>>, id: &str) -> bool {
    cache.read().unwrap().get(id).map(|s| s == "allowed").unwrap_or(false)
}

fn get_logical_timestamp(doc: &FireLiteDoc) -> i64 {
    doc.fields.iter().filter_map(|(_, v)| if let Value::Timestamp(t) = v { Some(*t) } else { None }).max().unwrap_or(0)
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