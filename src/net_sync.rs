#[cfg(feature = "net-sync")]
use crate::engine::engine::ReplicationEvent;
#[cfg(feature = "net-sync")]
use crate::engine::FireLite;
#[cfg(feature = "net-sync")]
use crate::document::value::Value;
#[cfg(feature = "net-sync")]
use crate::document::firelite_doc::FireLiteDoc;
#[cfg(feature = "net-sync")]
use std::sync::{Arc, RwLock};
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
        }
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        Self::start_security_monitor(self.db.clone(), self.auth_cache.clone(), self.leader_id.clone()).await;
        
        let mdns = ServiceDaemon::new()?;
        let hostname = format!("{}.local.", gethostname::gethostname().to_string_lossy());
        let service_info = ServiceInfo::new(&self.service_type, &self.self_id, &hostname, "0.0.0.0", port, None)?;
        mdns.register(service_info)?;

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        
        let db_shr = self.db.clone();
        let peers_shr = self.peers.clone();
        let seen_shr = self.seen_messages.clone();
        let status_tx_shr = self.status_tx.clone();
        let auth_cache_shr = self.auth_cache.clone();
        let leader_id_shr = self.leader_id.clone();
        let self_id = self.self_id.clone();
        let excluded = self.excluded_collections.clone();
        let is_authority = self.is_authority;

        // TASK 1: Handle Incoming Connections
        let (db_inc, peers_inc, seen_inc, status_inc, auth_inc, lid_inc) = 
            (db_shr.clone(), peers_shr.clone(), seen_shr.clone(), status_tx_shr.clone(), auth_cache_shr.clone(), leader_id_shr.clone());
        let sid_inc = self_id.clone();
        let excl_inc = excluded.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                handle_incoming_peer(stream, db_inc.clone(), peers_inc.clone(), seen_inc.clone(), 
                    status_inc.clone(), sid_inc.clone(), lid_inc.clone(), excl_inc.clone(), is_authority, auth_inc.clone()).await;
            }
        });

        // TASK 2: Service Browser (Auto-discovery)
        let browser = mdns.browse(&self.service_type)?;
        let (db_brw, peers_brw, seen_brw, status_brw, auth_brw, lid_brw) = 
            (db_shr.clone(), peers_shr.clone(), seen_shr.clone(), status_tx_shr.clone(), auth_cache_shr.clone(), leader_id_shr.clone());
        let sid_brw = self_id.clone();
        let excl_brw = excluded.clone();
        tokio::spawn(async move {
            while let Ok(event) = browser.recv() {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let peer_name = info.get_fullname().split('.').next().unwrap_or("");
                    if peer_name == sid_brw { continue; }
                    
                    if !peers_brw.lock().await.contains_key(peer_name) {
                        if let Some(addr) = info.get_addresses().iter().next() {
                            if let Ok(Ok(stream)) = tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(format!("{}:{}", addr, info.get_port()))).await {
                                handle_incoming_peer(stream, db_brw.clone(), peers_brw.clone(), seen_brw.clone(), 
                                    status_brw.clone(), sid_brw.clone(), lid_brw.clone(), excl_brw.clone(), is_authority, auth_brw.clone()).await;
                            }
                        }
                    }
                }
            }
        });

        // TASK 3: Replication Broadcaster
        let local_rx = self.db.subscribe_replication();
        let (peers_obs, auth_obs, lid_obs) = (peers_shr.clone(), auth_cache_shr.clone(), leader_id_shr.clone());
        let sid_obs = self_id.clone();
        tokio::spawn(async move {
            while let Ok(event) = local_rx.recv() {
                let is_security = event.0 == "__firelite_security";
                let packet = NetPacket::Replication { 
                    msg_id: std::time::SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros(), 
                    origin_id: sid_obs.clone(), 
                    event 
                };
                let payload = bincode::serialize(&packet).unwrap();
                let header = (payload.len() as u32).to_le_bytes();

                let mut p_guard = peers_obs.lock().await;
                let mut dead = Vec::new();

                for (id, writer) in p_guard.iter_mut() {
                    let is_allowed = {
                        let cache = auth_obs.read().unwrap();
                        let current_leader = lid_obs.read().unwrap();
                        let status = cache.get(id).map(|s| s.as_str()).unwrap_or("pending");
                        // Broadcaster Gate: 
                        // Security collection is broadcast to EVERYONE.
                        // Other collections only go to allowed peers or the current leader.
                        is_security || status == "allowed" || id == &*current_leader
                    };

                    if is_allowed {
                        if writer.write_all(&header).await.is_err() || writer.write_all(&payload).await.is_err() {
                            dead.push(id.clone());
                        }
                    }
                }
                for id in dead { p_guard.remove(&id); }
            }
        });

        Ok(())
    }

    async fn start_security_monitor(db: Arc<FireLite>, cache: Arc<RwLock<HashMap<String, String>>>, leader_id_ref: Arc<RwLock<String>>) {
        let rx = db.watch_collection("__firelite_security");
        tokio::spawn(async move {
            while let Ok(event) = rx.recv() {
                if event.path.starts_with("peers:") {
                    let peer_id = event.path.replace("peers:", "");
                    let shard_arc = db.get_shard("__firelite_security");
                    let shard_guard = shard_arc.read().unwrap();
                    if let Ok(Some(bytes)) = shard_guard.get(&event.path) {
                        if let Some(doc) = FireLiteDoc::decode(&bytes) {
                            if let Some(Value::String(s)) = doc.get("status") {
                                cache.write().unwrap().insert(peer_id, s.clone());
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
        });
    }

    pub fn status(&self) -> NetworkStatus {
        let stats = self.status_rx.borrow().clone();
        
        // We need to get the actual keys from the live peers map
        // Since status() is usually called by the UI, we can use try_lock 
        // or a quick lock to avoid blocking replication.
        let live_peers = if let Ok(guard) = self.peers.try_lock() {
            guard.keys().cloned().collect()
        } else {
            stats.known_peers // Fallback to last known if locked
        };

        NetworkStatus {
            status: stats.status,
            self_id: self.self_id.clone(),
            peer_count: live_peers.len(),
            known_peers: live_peers, // These are the people physically "here"
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