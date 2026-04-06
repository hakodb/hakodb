#[cfg(feature = "net-sync")]
use crate::engine::engine::ReplicationEvent;
#[cfg(feature = "net-sync")]
use crate::engine::FireLite;
#[cfg(feature = "net-sync")]
use crate::document::value::Value;
#[cfg(feature = "net-sync")]
use crate::document::firelite_doc::FireLiteDoc;
#[cfg(feature = "net-sync")]
use std::sync::Arc;
#[cfg(feature = "net-sync")]
use std::sync::atomic::Ordering;
#[cfg(feature = "net-sync")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "net-sync")]
use tokio::net::TcpStream;
#[cfg(feature = "net-sync")]
use tokio::net::tcp::OwnedWriteHalf;
#[cfg(feature = "net-sync")]
use tokio::io::{AsyncReadExt, AsyncWriteExt};
#[cfg(feature = "net-sync")]
use tokio::sync::{Mutex, watch};
#[cfg(feature = "net-sync")]
use mdns_sd::{ServiceDaemon, ServiceInfo, ServiceEvent};
#[cfg(feature = "net-sync")]
use local_ip_address::list_afinet_netifas;

// --- Data Structures ---

#[cfg(feature = "net-sync")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SyncStatus {
    Idle,
    Searching,
    Connected,
    Syncing,
}

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
    Replication { 
        msg_id: u128, 
        origin_id: String, 
        event: ReplicationEvent 
    },
    Ping,
    Pong,
}

// --- NetSyncer Engine ---

#[cfg(feature = "net-sync")]
pub struct NetSyncer {
    db: Arc<FireLite>,
    self_id: String,
    leader_id: String,
    excluded_collections: HashSet<String>,
    is_authority: bool,
    service_type: String, // Dynamic service type
    status_tx: watch::Sender<NetworkStatus>,
    status_rx: watch::Receiver<NetworkStatus>,
    peers: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>, 
    seen_messages: Arc<Mutex<HashSet<u128>>>,
}

#[cfg(feature = "net-sync")]
impl NetSyncer {
    pub fn new(
        db: Arc<FireLite>, 
        name: &str, 
        leader_id: &str, 
        excluded: Vec<String>,
        is_authority: bool
    ) -> Self {
        let (tx, rx) = watch::channel(NetworkStatus {
            status: SyncStatus::Idle,
            self_id: name.to_string(),
            peer_count: 0,
            known_peers: Vec::new(),
        });

        // Use DB name to make service type unique (Namespace Isolation)
        let db_slug = db.db_name().to_lowercase().replace('.', "_");
        let service_type = format!("_{}._tcp.local.", db_slug);

        Self {
            db,
            self_id: name.to_string(),
            leader_id: leader_id.to_string(),
            excluded_collections: excluded.into_iter().collect(),
            is_authority,
            service_type,
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(Mutex::new(HashMap::new())),
            seen_messages: Arc::new(Mutex::new(HashSet::with_capacity(1000))),
        }
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let mdns = ServiceDaemon::new()?;
        
        // 1. GATHER REAL IPs (Fixes unreachable 0.0.0.0 bug)
        let mut my_ips = HashSet::new();
        if let Ok(network_interfaces) = list_afinet_netifas() {
            for (_name, ip) in network_interfaces {
                if ip.is_ipv4() && !ip.is_loopback() {
                    my_ips.insert(ip.to_string());
                }
            }
        }
        if my_ips.is_empty() {
            if let Ok(ip) = local_ip_address::local_ip() { my_ips.insert(ip.to_string()); }
        }
        let ip_list = my_ips.iter().cloned().collect::<Vec<_>>().join(",");

        // 2. PREPARE SHARED DATA
        let self_id = self.self_id.clone();
        let leader_id = self.leader_id.clone();
        let excluded = self.excluded_collections.clone();
        let hostname = format!("{}.local.", gethostname::gethostname().to_string_lossy());
        let service_type = self.service_type.clone();
        
        let service_info = ServiceInfo::new(
            &service_type, 
            &self_id, 
            &hostname, 
            &ip_list,
            port, 
            None,
        )?;
        mdns.register(service_info)?;

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        
        // --- TASK 1: INCOMING PEER HANDLER ---
        let db_inc = Arc::clone(&self.db);
        let peers_inc = Arc::clone(&self.peers);
        let seen_inc = Arc::clone(&self.seen_messages);
        let status_inc = self.status_tx.clone();
        let self_id_inc = self_id.clone();
        let leader_id_inc = leader_id.clone();
        let excluded_inc = excluded.clone();
        let is_auth = self.is_authority;

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                handle_incoming_peer(
                    stream, db_inc.clone(), peers_inc.clone(), seen_inc.clone(), 
                    status_inc.clone(), self_id_inc.clone(), leader_id_inc.clone(),
                    excluded_inc.clone(), is_auth
                ).await;
            }
        });

        // --- TASK 2: OUTGOING PEER BROWSER ---
        let browser = mdns.browse(&service_type)?;
        let db_brw = Arc::clone(&self.db);
        let peers_brw = Arc::clone(&self.peers);
        let seen_brw = Arc::clone(&self.seen_messages);
        let status_brw = self.status_tx.clone();
        let self_id_brw = self_id.clone();
        let leader_id_brw = leader_id.clone();
        let excluded_brw = excluded.clone();

        tokio::spawn(async move {
            status_brw.send_modify(|s| s.status = SyncStatus::Searching);
            while let Ok(event) = browser.recv() {
                if let ServiceEvent::ServiceResolved(info) = event {
                    // Extract instance name from fullname safely
                    let peer_name = info.get_fullname().split('.').next().unwrap_or("");
                    
                    if peer_name == self_id_brw { continue; }

                    let addr = info.get_addresses().iter().next().unwrap();
                    let connect_addr = format!("{}:{}", addr, info.get_port());

                    let already_connected = { 
                        let guard = peers_brw.lock().await;
                        guard.contains_key(peer_name) 
                    };

                    if !already_connected {
                        if let Ok(Ok(stream)) = tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&connect_addr)).await {
                            handle_incoming_peer(
                                stream, db_brw.clone(), peers_brw.clone(), seen_brw.clone(), 
                                status_brw.clone(), self_id_brw.clone(), leader_id_brw.clone(),
                                excluded_brw.clone(), is_auth
                            ).await;
                        }
                    }
                }
            }
        });

        // --- TASK 3: REPLICATION BROADCASTER ---
        let local_rx = self.db.subscribe_replication();
        let peers_obs = Arc::clone(&self.peers);
        let self_id_obs = self_id.clone();

        tokio::spawn(async move {
            while let Ok(event) = local_rx.recv() {
                let msg_id = std::time::SystemTime::now().duration_since(std::time::UNIX_EPOCH).unwrap().as_micros();
                let packet = NetPacket::Replication { msg_id, origin_id: self_id_obs.clone(), event };
                let payload = bincode::serialize(&packet).unwrap();
                let mut guard = peers_obs.lock().await;
                
                let size_header = (payload.len() as u32).to_le_bytes();
                let mut disconnected = Vec::new();
                for (id, writer) in guard.iter_mut() {
                    if writer.write_all(&size_header).await.is_err() || writer.write_all(&payload).await.is_err() {
                        disconnected.push(id.clone());
                    }
                }
                for id in disconnected { guard.remove(&id); }
            }
        });

        Ok(())
    }

    pub fn status(&self) -> NetworkStatus {
        self.status_rx.borrow().clone()
    }
}

async fn handle_incoming_peer(
    stream: TcpStream,
    db: Arc<FireLite>,
    peers_map: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>,
    seen_cache: Arc<Mutex<HashSet<u128>>>,
    status_tx: watch::Sender<NetworkStatus>,
    self_id: String,
    leader_id: String,
    excluded: HashSet<String>,
    is_authority: bool,
) {
    let (mut reader, mut writer) = stream.into_split();
    
    // 1. Handshake Identity
    let hello = bincode::serialize(&NetPacket::Identify { 
        id: self_id.clone(), 
        is_authority, 
        db_name: db.db_name() 
    }).unwrap();
    
    if send_raw(&mut writer, &hello).await.is_err() { return; }

    tokio::spawn(async move {
        // 2. Performance Safe Identify check with 2s timeout
        let (peer_id, _peer_db_name) = match tokio::time::timeout(
            std::time::Duration::from_secs(2), 
            recv_raw(&mut reader)
        ).await {
            Ok(Ok(payload)) => {
                if let Ok(NetPacket::Identify { id, db_name, .. }) = bincode::deserialize::<NetPacket>(&payload) {
                    if db_name != db.db_name() { return; }
                    (id, db_name)
                } else { return; }
            }
            _ => return,
        };

        // 3. Prevent Duplicates
        {
            let mut guard = peers_map.lock().await;
            if guard.contains_key(&peer_id) { return; }
            guard.insert(peer_id.clone(), writer);
        }

        status_tx.send_modify(|s| {
            s.status = SyncStatus::Connected;
            if !s.known_peers.contains(&peer_id) { s.known_peers.push(peer_id.clone()); }
            s.peer_count = s.known_peers.len();
        });

        // 4. Replication Loop
        loop {
            match recv_raw(&mut reader).await {
                Ok(payload) => {
                    if let Ok(packet) = bincode::deserialize::<NetPacket>(&payload) {
                        match packet {
                            NetPacket::Replication { msg_id, origin_id, event } => {
                                {
                                    let mut seen = seen_cache.lock().await;
                                    if seen.contains(&msg_id) { continue; }
                                    seen.insert(msg_id);
                                    if seen.len() > 2000 { seen.clear(); }
                                }

                                let col = event.0.clone();
                                if excluded.contains(&col) { continue; }

                                if col == "__firelite_security" {
                                    if origin_id != leader_id { continue; }
                                } else {
                                    if !is_authorized(&db, &origin_id).await { continue; }
                                }

                                status_tx.send_modify(|s| s.status = SyncStatus::Syncing);
                                
                                let ops = event.1.clone();
                                let docs_arc = event.2.clone();
                                let raw_blobs = event.3.clone();

                                let shard_arc = db.get_shard(&col);
                                {
                                    let mut shard = shard_arc.write().unwrap();
                                    let mut local_blob_offsets = Vec::new();
                                    
                                    if let Some(ref file) = shard.blob_file {
                                        let mut current_offset = shard.blob_size.load(Ordering::Acquire);
                                        for data in &raw_blobs {
                                            let data_len = data.len() as u32;
                                            #[cfg(unix)] {
                                                use std::os::unix::fs::FileExt;
                                                let _ = file.write_all_at(data, current_offset);
                                            }
                                            #[cfg(windows)] {
                                                use std::os::windows::fs::FileExt;
                                                let _ = file.seek_write(data, current_offset);
                                            }
                                            local_blob_offsets.push((current_offset, data_len));
                                            current_offset += data_len as u64;
                                        }
                                        shard.blob_size.store(current_offset, Ordering::Release);
                                    }

                                    let mut incoming_batch = (*docs_arc).clone();
                                    let mut blob_ptr_idx = 0;
                                    for (_, doc) in &mut incoming_batch {
                                        for (_, value) in &mut doc.fields {
                                            if let Value::BlobLink { .. } = value {
                                                if let Some((new_off, new_len)) = local_blob_offsets.get(blob_ptr_idx) {
                                                    *value = Value::BlobLink { offset: *new_off, len: *new_len };
                                                    blob_ptr_idx += 1;
                                                }
                                            }
                                        }
                                    }

                                    let mut final_docs_to_index = Vec::with_capacity(incoming_batch.len());
                                    let mut ops_to_apply = Vec::with_capacity(ops.len());

                                    for (doc_id, incoming_doc) in incoming_batch {
                                        let key = format!("{}:{}", col, doc_id);
                                        let mut should_apply = true;

                                        if let Ok(Some(local_bytes)) = shard.get(&key) {
                                            if let Some(local_doc) = FireLiteDoc::decode(&local_bytes) {
                                                let incoming_ts = get_logical_timestamp(&incoming_doc);
                                                let local_ts = get_logical_timestamp(&local_doc);
                                                if incoming_ts <= local_ts { should_apply = false; }
                                            }
                                        }

                                        if should_apply {
                                            if let Some(op) = ops.iter().find(|o| o.get_key() == key) {
                                                ops_to_apply.push(op.clone());
                                            }
                                            final_docs_to_index.push((doc_id, incoming_doc));
                                        }
                                    }

                                    if !ops_to_apply.is_empty() {
                                        let _ = shard.apply_replicated_ops(&ops_to_apply);
                                        db.inject_replication_to_indexer(col, Arc::new(final_docs_to_index));
                                    }
                                }
                                status_tx.send_modify(|s| s.status = SyncStatus::Connected);
                            }
                            _ => {}
                        }
                    }
                }
                Err(_) => break, 
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

// --- Helpers ---

#[cfg(feature = "net-sync")]
async fn is_authorized(db: &Arc<FireLite>, peer_id: &str) -> bool {
    let auth_key = format!("peers:{}", peer_id);
    let shard = db.get_shard("__firelite_security");
    if let Ok(Some(bytes)) = shard.read().unwrap().get(&auth_key) {
        if let Some(doc) = FireLiteDoc::decode(&bytes) {
            return doc.get("status") == Some(&Value::String("allowed".to_string()));
        }
    }
    false
}

#[cfg(feature = "net-sync")]
fn get_logical_timestamp(doc: &FireLiteDoc) -> i64 {
    doc.fields.iter()
        .filter_map(|(_, v)| {
            if let Value::Timestamp(t) = v { Some(*t) } else { None }
        })
        .max()
        .unwrap_or(0)
}

async fn send_raw<W: AsyncWriteExt + Unpin>(writer: &mut W, data: &[u8]) -> tokio::io::Result<()> {
    writer.write_all(&(data.len() as u32).to_le_bytes()).await?;
    writer.write_all(data).await?;
    Ok(())
}

async fn recv_raw<R: AsyncReadExt + Unpin>(reader: &mut R) -> tokio::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    reader.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    let mut data = vec![0u8; len];
    reader.read_exact(&mut data).await?;
    Ok(data)
}