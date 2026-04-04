#[cfg(feature = "net-sync")]
use crate::engine::engine::ReplicationEvent;
#[cfg(feature = "net-sync")]
use crate::engine::FireLite;
#[cfg(feature = "net-sync")]
use crate::document::value::Value;
#[cfg(feature = "net-sync")]
use std::sync::Arc;
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

// --- 1. Networking Data Structures ---

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
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct PeerInfo {
    pub id: String,
    pub is_authority: bool,
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
impl Default for NetworkStatus {
    fn default() -> Self {
        Self {
            status: SyncStatus::Idle,
            self_id: String::new(),
            peer_count: 0,
            known_peers: Vec::new(),
        }
    }
}

#[cfg(feature = "net-sync")]
#[derive(Debug, serde::Serialize, serde::Deserialize)]
pub enum NetPacket {
    Identify { id: String, is_authority: bool },
    Replication { 
        msg_id: u128, 
        origin_id: String, 
        event: ReplicationEvent 
    },
    Ping,
    Pong,
}

// --- 2. The NetSyncer Engine ---

#[cfg(feature = "net-sync")]
pub struct NetSyncer {
    db: Arc<FireLite>,
    self_id: String,
    is_authority: bool,
    service_type: &'static str,
    status_tx: watch::Sender<NetworkStatus>,
    status_rx: watch::Receiver<NetworkStatus>,
    peers: Arc<Mutex<HashMap<String, OwnedWriteHalf>>>, 
    seen_messages: Arc<Mutex<HashSet<u128>>>,
}

#[cfg(feature = "net-sync")]
impl NetSyncer {
    pub fn new(db: Arc<FireLite>, name: &str, is_authority: bool) -> Self {
        let (tx, rx) = watch::channel(NetworkStatus {
            status: SyncStatus::Idle,
            self_id: name.to_string(),
            peer_count: 0,
            known_peers: Vec::new(),
        });

        Self {
            db,
            self_id: name.to_string(),
            is_authority,
            service_type: "_firelite_pos_mesh._tcp.local.",
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(Mutex::new(HashMap::new())),
            seen_messages: Arc::new(Mutex::new(HashSet::with_capacity(1000))),
        }
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        let mdns = ServiceDaemon::new()?;
        let self_id = self.self_id.clone();
        
        let hostname = format!("{}.local.", gethostname::gethostname().to_string_lossy());
        let service_info = ServiceInfo::new(
            self.service_type, &self_id, &hostname, "0.0.0.0", port, None,
        )?;
        mdns.register(service_info)?;

        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        let db_srv = Arc::clone(&self.db);
        let peers_srv = Arc::clone(&self.peers);
        let seen_srv = Arc::clone(&self.seen_messages);
        let status_srv = self.status_tx.clone();
        let self_id_srv = self_id.clone();
        let is_auth = self.is_authority;

        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                handle_incoming_peer(stream, db_srv.clone(), peers_srv.clone(), seen_srv.clone(), status_srv.clone(), self_id_srv.clone(), is_auth).await;
            }
        });

        let browser = mdns.browse(self.service_type)?;
        let db_brw = Arc::clone(&self.db);
        let peers_brw = Arc::clone(&self.peers);
        let seen_brw = Arc::clone(&self.seen_messages);
        let status_brw = self.status_tx.clone();
        let self_id_brw = self_id.clone();

        tokio::spawn(async move {
            status_brw.send_modify(|s| s.status = SyncStatus::Searching);
            while let Ok(event) = browser.recv() {
                if let ServiceEvent::ServiceResolved(info) = event {
                    let peer_fullname = info.get_fullname();
                    if peer_fullname.contains(&self_id_brw) { continue; }

                    let addr = info.get_addresses().iter().next().unwrap();
                    let connect_addr = format!("{}:{}", addr, info.get_port());

                    let already_connected = { peers_brw.lock().await.contains_key(info.get_fullname()) };
                    if !already_connected {
                        if let Ok(Ok(stream)) = tokio::time::timeout(std::time::Duration::from_secs(3), TcpStream::connect(&connect_addr)).await {
                            handle_incoming_peer(stream, db_brw.clone(), peers_brw.clone(), seen_brw.clone(), status_brw.clone(), self_id_brw.clone(), is_auth).await;
                        }
                    }
                }
            }
        });

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
    is_authority: bool,
) {
    let (mut reader, mut writer) = stream.into_split();

    // 1. Initial Identity Exchange (Send our ID)
    let hello = bincode::serialize(&NetPacket::Identify { id: self_id, is_authority }).unwrap();
    if send_raw(&mut writer, &hello).await.is_err() { return; }

    tokio::spawn(async move {
        // --- PHASE 1: IDENTIFICATION ---
        // We wait for the peer to identify themselves BEFORE entering the loop
        let peer_id = match recv_raw(&mut reader).await {
            Ok(payload) => {
                if let Ok(NetPacket::Identify { id, .. }) = bincode::deserialize::<NetPacket>(&payload) {
                    let id_clone = id.clone();
                    // Move writer into map (Consumers the writer)
                    peers_map.lock().await.insert(id_clone.clone(), writer);
                    status_tx.send_modify(|s| {
                        s.status = SyncStatus::Connected;
                        if !s.known_peers.contains(&id_clone) { s.known_peers.push(id_clone.clone()); }
                        s.peer_count = s.known_peers.len();
                    });
                    id_clone
                } else { return; }
            }
            Err(_) => return,
        };

        // --- PHASE 2: REPLICATION LOOP ---
        // writer is now gone (moved into map), we only work with reader here
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

                                if (event.0 == "roles" || event.0 == "licenses") && origin_id != "MainPC" {
                                    continue;
                                }

                                status_tx.send_modify(|s| s.status = SyncStatus::Syncing);
                                
                                let col = event.0.clone();
                                let ops = event.1.clone();
                                let docs_arc = event.2.clone();
                                let raw_blobs = event.3.clone();

                                let shard_arc = db.get_shard(&col);
                                {
                                    let mut shard = shard_arc.write().unwrap();
                                    let mut local_blob_offsets = Vec::new();
                                    
                                    if let Some(ref file) = shard.blob_file {
                                        // 1. Get current file length as the starting append point
                                        let mut current_offset = file.metadata().unwrap().len();

                                        for data in &raw_blobs {
                                            let data_len = data.len() as u32;

                                            // 2. Perform Positional Write (Atomic on Unix, Batch-safe on Windows)
                                            #[cfg(unix)] {
                                                use std::os::unix::fs::FileExt;
                                                file.write_all_at(data, current_offset).unwrap();
                                            }
                                            #[cfg(windows)] {
                                                use std::os::windows::fs::FileExt;
                                                file.seek_write(data, current_offset).unwrap();
                                            }

                                            local_blob_offsets.push((current_offset, data_len));
                                            
                                            // 3. Increment offset for the next blob in the batch
                                            current_offset += data_len as u64;
                                        }
                                    }

                                    let mut docs = (*docs_arc).clone();
                                    let mut blob_ptr_idx = 0;
                                    for (_, doc) in &mut docs {
                                        for (_, value) in &mut doc.fields {
                                            if let Value::BlobLink { .. } = value {
                                                if let Some((new_off, new_len)) = local_blob_offsets.get(blob_ptr_idx) {
                                                    *value = Value::BlobLink { offset: *new_off, len: *new_len };
                                                    blob_ptr_idx += 1;
                                                }
                                            }
                                        }
                                    }

                                    let _ = shard.apply_replicated_ops(&ops);
                                    db.inject_replication_to_indexer(col, Arc::new(docs));
                                }

                                status_tx.send_modify(|s| s.status = SyncStatus::Connected);
                            }
                            _ => {}
                        }
                    }
                }
                Err(_) => break, // Disconnect
            }
        }

        // Cleanup
        peers_map.lock().await.remove(&peer_id);
        status_tx.send_modify(|s| {
            s.known_peers.retain(|p| p != &peer_id);
            s.peer_count = s.known_peers.len();
            if s.peer_count == 0 { s.status = SyncStatus::Searching; }
        });
    });
}

// --- IO Helpers ---

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