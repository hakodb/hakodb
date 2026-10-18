#[cfg(feature = "net-sync")]
use crate::engine::Hako;
#[cfg(feature = "net-sync")]
use crate::document::value::Value;
#[cfg(feature = "net-sync")]
use crate::document::hako_doc::HakoDoc;
#[cfg(feature = "net-sync")]
use crate::storage::wal::WalOp;
#[cfg(feature = "net-sync")]
use crate::storage::engine::Pointer;
#[cfg(feature = "net-sync")]
use std::sync::{Arc, Mutex, RwLock};
#[cfg(feature = "net-sync")]
use std::sync::atomic::{AtomicU8, Ordering};
#[cfg(feature = "net-sync")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "net-sync")]
use std::time::{Duration, Instant, UNIX_EPOCH, SystemTime};
#[cfg(feature = "net-sync")]
use crate::sync_frame::{recv_frame, send_frame};
#[cfg(feature = "net-sync")]
use tokio::sync::{Mutex as AsyncMutex, watch};
#[cfg(feature = "net-sync")]
use sha2::{Sha256, Digest};
#[cfg(feature = "net-sync")]
use tokio::net::{TcpStream, tcp::OwnedWriteHalf};
#[cfg(feature = "net-sync")]
use crate::sync_guard::{self, CapsMap, PeerCaps};
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
    Ping {
        versions: HashMap<String, i64>,
        indexes: crate::engine::engine::IndexList,
    },
    SyncRequest,
    Replication { msg_id: u128, collection: String, ops: Vec<WalOp> },
    /// Capability advertisement for the encryption fail-closed rules (see
    /// `crate::sync_guard`). Appended LAST so old peers keep decoding every
    /// earlier variant; unknown trailing variants fail decode and are
    /// silently skipped by the receive loop (no disconnect, no breakage).
    SyncCaps {
        key_fp: [u8; 32],
        encrypted_cols: Vec<String>,
    },
    /// LAN relay coordination (#72 phase 1). Both appended LAST so old
    /// peers keep decoding every earlier variant; unknown trailing
    /// variants fail decode and are silently skipped (the SyncCaps rule).
    ///
    /// NOTE ON THE DESIGN TEXT: the issue sketched the lease as an
    /// additive field inside `Ping`. Under bincode that would BREAK old
    /// peers decoding the Ping variant itself (all version exchange would
    /// die in a mixed mesh). New variants keep the compat property the
    /// design actually needs; senders announce at the same cadence as
    /// their ping.
    /// A peer announces it holds (or recently held) the cloud relay role:
    /// `last_exchange_ts` is its last successful cloud round-trip (ms
    /// epoch). Observers track freshness per client_id for the lag meter
    /// and the phase-2 GRACE eligibility rule.
    CloudLease {
        client_id: String,
        last_exchange_ts: i64,
    },
    /// Sent by a peer the moment it takes over the relay role. Observers
    /// use it for fast convergence (no need to wait for the next
    /// CloudLease) and logging; it carries no lease state.
    RelayTake {
        ts: i64,
        client_id: String,
    },
}

// --- UDP broadcast discovery (mobile path; desktop opt-in) ---
//
// Android's WiFi stack filters inbound *multicast* unless the app holds a
// MulticastLock (Java/Kotlin side) — so mDNS browsing silently hears nothing.
// Subnet *broadcast* is not subject to that filter: no lock, no new
// permission beyond INTERNET, pure Rust. Same broadcast domain as mDNS, so
// no reach is lost. Desktop runs mDNS by default and enables broadcast via
// DiscoveryMode::Both/Broadcast for mixed groups; the beacon tasks below are
// plain logic so the desktop test suite exercises them over loopback.
#[cfg(feature = "net-sync")]
const BEACON_PORT: u16 = 5354; // one above mDNS 5353
#[cfg(feature = "net-sync")]
const BEACON_MAGIC: u32 = 0x464C4252; // "FLBR"
#[cfg(feature = "net-sync")]
const BEACON_INTERVAL: Duration = Duration::from_secs(5);
#[cfg(feature = "net-sync")]
const BEACON_EXPIRY: Duration = Duration::from_secs(45); // ~9 missed beacons

/// Beacon payload. The receiver takes the sender's address from the UDP
/// packet source (src_ip:tcp_port), never from a self-reported IP — Android
/// devices routinely have several interfaces and the "default" one may not
/// be the WiFi the mesh lives on. `known` gossips membership so finding one
/// peer bootstraps the group (the TCP handshake itself carries no peer list).
#[cfg(feature = "net-sync")]
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
struct BeaconPacket {
    magic: u32,
    id: String,
    room_hash: [u8; 32],
    tcp_port: u16,
    known: Vec<(String, String)>,
}

#[cfg(feature = "net-sync")]
fn encode_beacon(p: &BeaconPacket) -> Vec<u8> {
    bincode::serialize(p).unwrap_or_default()
}

#[cfg(feature = "net-sync")]
fn decode_beacon(bytes: &[u8]) -> Option<BeaconPacket> {
    let p: BeaconPacket = bincode::deserialize(bytes).ok()?;
    if p.magic != BEACON_MAGIC {
        return None;
    }
    Some(p)
}

/// Merge one beacon into the discovery cache. Pure function of (cache,
/// packet, source ip) — unit-tested, no sockets. Returns true if the cache
/// changed. Rules: wrong room or self → ignore; otherwise upsert sender +
/// gossiped peers with a fresh last-seen timestamp.
#[cfg(feature = "net-sync")]
fn merge_beacon(
    cache: &mut HashMap<String, (String, Instant)>,
    pkt: &BeaconPacket,
    src_ip: std::net::IpAddr,
    self_id: &str,
    room_hash: &[u8; 32],
    now: Instant,
) -> bool {
    if pkt.room_hash != *room_hash || pkt.id == self_id {
        return false;
    }
    let mut changed = false;
    let mut upsert = |name: &str, addr: &str| {
        if name == self_id {
            return;
        }
        match cache.get(name) {
            Some((a, _)) if a == addr => {}
            _ => changed = true,
        }
        cache.insert(name.to_string(), (addr.to_string(), now));
    };
    upsert(&pkt.id, &format!("{}:{}", src_ip, pkt.tcp_port));
    for (name, addr) in &pkt.known {
        upsert(name, addr);
    }
    changed
}

/// Drop entries unheard-from for longer than BEACON_EXPIRY. Broadcast has no
/// ServiceRemoved event; without pruning, dead peers get dialed forever
/// (battery on mobile, timeout spam everywhere).
#[cfg(feature = "net-sync")]
fn prune_stale_peers(cache: &mut HashMap<String, (String, Instant)>, now: Instant) {
    cache.retain(|_, (_, seen)| now.duration_since(*seen) < BEACON_EXPIRY);
}

/// Beacon sender. `target` is 255.255.255.255:BEACON_PORT in production;
/// loopback in tests. Spawned only when the discovery mode enables it.
#[cfg(feature = "net-sync")]
async fn beacon_sender_task(
    cache: Arc<Mutex<HashMap<String, (String, Instant)>>>,
    self_id: String,
    room_hash: [u8; 32],
    tcp_port: u16,
    target: std::net::SocketAddr,
) {
    let sock = match tokio::net::UdpSocket::bind("0.0.0.0:0").await {
        Ok(s) => s,
        Err(_) => return,
    };
    let _ = sock.set_broadcast(true);
    loop {
        // Gossip: advertise everyone we know so one heard beacon bootstraps
        // the whole group (the TCP handshake carries no peer list).
        let known: Vec<(String, String)> = {
            let guard = cache.lock().unwrap();
            guard.iter().map(|(n, (a, _))| (n.clone(), a.clone())).collect()
        };
        let pkt = BeaconPacket {
            magic: BEACON_MAGIC,
            id: self_id.clone(),
            room_hash,
            tcp_port,
            known,
        };
        let _ = sock.send_to(&encode_beacon(&pkt), target).await;
        tokio::time::sleep(BEACON_INTERVAL).await;
    }
}

/// Beacon listener. Binds the wildcard on BEACON_PORT; a second instance on
/// the same device gets a bind error and silently skips discovery.
#[cfg(feature = "net-sync")]
async fn beacon_listener_task(
    cache: Arc<Mutex<HashMap<String, (String, Instant)>>>,
    self_id: String,
    room_hash: [u8; 32],
    bind_addr: std::net::SocketAddr,
) {
    let sock = match tokio::net::UdpSocket::bind(bind_addr).await {
        Ok(s) => s,
        Err(_) => return,
    };
    let mut buf = vec![0u8; 2048];
    loop {
        let (len, src) = match sock.recv_from(&mut buf).await {
            Ok(x) => x,
            Err(_) => continue,
        };
        if let Some(pkt) = decode_beacon(&buf[..len]) {
            let mut guard = cache.lock().unwrap();
            merge_beacon(&mut guard, &pkt, src.ip(), &self_id, &room_hash, Instant::now());
        }
    }
}

#[cfg(feature = "net-sync")]
pub struct NetSyncer {
    db: Arc<Hako>,
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
    /// Observed cloud-relay leases per peer client_id (#72): client_id ->
    /// (last_exchange_ms_epoch, seen_at). Fed by CloudLease/RelayTake
    /// packets; read by the lag meter and the phase-2 GRACE rule.
    cloud_leases: Arc<Mutex<HashMap<String, (i64, Instant)>>>,
    mdns: Arc<Mutex<Option<ServiceDaemon>>>,
    /// Static peer addresses (tcp://host:port or host:port) for server
    /// deployments where discovery cannot run: containers/k8s list exact
    /// addresses; mDNS/broadcast does not cross those boundaries. Each
    /// address gets one dedicated dial task (connect → handle_peer until
    /// the connection drops → retry) — no tie-breaking, the configured
    /// side dials by determinism.
    static_peers: Vec<String>,
    // ponytail: atomic so the FFI setter can change the mode on a shared
    // (Arc'd) handle; start() reads it once at boot.
    discovery: AtomicU8,
}

/// Which discovery transports a mesh node runs. mDNS is the desktop sweet
/// spot and stays the default there; mobile defaults to broadcast (no
/// MulticastLock needed). Mixed groups need at least one common channel —
/// i.e. a desktop joining Android/iOS peers must opt into `Both`.
#[cfg(feature = "net-sync")]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
#[repr(u8)]
pub enum DiscoveryMode {
    /// mDNS register + browse only (desktop default; historic behavior).
    #[default]
    Mdns = 0,
    /// UDP broadcast beacons only (mobile default; no multicast).
    Broadcast = 1,
    /// Both transports at once (mixed groups, debugging).
    Both = 2,
}

#[cfg(feature = "net-sync")]
impl DiscoveryMode {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0 => Some(DiscoveryMode::Mdns),
            1 => Some(DiscoveryMode::Broadcast),
            2 => Some(DiscoveryMode::Both),
            _ => None,
        }
    }
}

/// Platform default: broadcast where multicast is hostile, mDNS elsewhere.
#[cfg(feature = "net-sync")]
fn default_discovery_mode() -> DiscoveryMode {
    #[cfg(target_os = "android")]
    {
        DiscoveryMode::Broadcast
    }
    // ponytail: iOS gets Broadcast until the Bonjour shim lands; raw
    // multicast needs an Apple entitlement the library must not assume.
    #[cfg(target_os = "ios")]
    {
        DiscoveryMode::Broadcast
    }
    #[cfg(not(any(target_os = "android", target_os = "ios")))]
    {
        DiscoveryMode::Mdns
    }
}

/// Pure transport matrix for a mode — unit-tested, so the spawn sites below
/// stay trivially reviewable.
#[cfg(feature = "net-sync")]
fn transports_for(mode: DiscoveryMode) -> (bool, bool) {
    match mode {
        DiscoveryMode::Mdns => (true, false),
        DiscoveryMode::Broadcast => (false, true),
        DiscoveryMode::Both => (true, true),
    }
}

#[cfg(feature = "net-sync")]
impl NetSyncer {
    pub fn new(db: Arc<Hako>, name: &str, room_key: &str, mut excluded: Vec<String>) -> Self {
        let (tx, rx) = watch::channel(NetworkStatus {
            status: SyncStatus::Idle, self_id: name.to_string(), peer_count: 0, known_peers: Vec::new(),
        });

        let mut hasher = Sha256::new();
        hasher.update(room_key.as_bytes());
        let room_hash: [u8; 32] = hasher.finalize().into();

        let mut shard_offsets = HashMap::new();
        if let Ok(Some(doc)) = db.get("__hako_system", "sync_checkpoint") {
            if let Some(Value::Map(fields)) = doc.get("offsets") {
                for (name, val) in fields {
                    if let Value::Int(off) = val { shard_offsets.insert(name.to_string(), *off as u64); }
                }
            }
        }

        excluded.extend(
            crate::engine::engine::SYNC_EXCLUDED_COLLECTIONS
                .iter()
                .map(|s| s.to_string()),
        );
        // Per-deployment extras ride along too, so mesh honors the same
        // explicit plane as cloud sync.
        excluded.extend(db.config.sync_excluded.iter().cloned());

        Self {
            db: db.clone(), 
            self_id: name.to_string(), 
            room_hash,
            excluded_collections: excluded.into_iter().collect(),
            // service_type: format!("_{}._tcp.local.", db.db_name().to_lowercase().replace('.', "_")),
            service_type: "_hakodb._tcp.local.".to_string(),
            status_tx: tx,
            status_rx: rx,
            peers: Arc::new(AsyncMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(Vec::with_capacity(1000))),
            tasks: Arc::new(Mutex::new(Vec::new())),
            shard_offsets: Arc::new(Mutex::new(shard_offsets)),
            enable_relay: false,
            last_mesh_ping: Arc::new(Mutex::new(Instant::now())),
            echo_cache: Arc::new(Mutex::new(HashMap::new())),
            cloud_leases: Arc::new(Mutex::new(HashMap::new())),
            mdns: Arc::new(Mutex::new(None)),
            static_peers: Vec::new(),
            discovery: AtomicU8::new(default_discovery_mode() as u8),
        }
    }

    /// Static mesh peers to dial directly (see field docs). Replaces the
    /// list; takes effect at [`Self::start`].
    pub fn with_static_peers(mut self, addrs: Vec<String>) -> Self {
        self.static_peers = addrs;
        self
    }

    pub fn with_relay(mut self, enable: bool) -> Self {
        self.enable_relay = enable;
        self
    }

    /// Choose discovery transports (default: mDNS on desktop, broadcast on
    /// mobile). Mixed-platform groups need a common channel — set `Both` on
    /// the desktop side to meet broadcast-only mobile peers.
    pub fn with_discovery(self, mode: DiscoveryMode) -> Self {
        self.discovery.store(mode as u8, Ordering::Relaxed);
        self
    }

    /// FFI-facing setter: same as with_discovery, on a shared handle. Takes
    /// effect at the next start().
    pub fn set_discovery(&self, mode: DiscoveryMode) {
        self.discovery.store(mode as u8, Ordering::Relaxed);
    }

    /// Active discovery mode (defaults are platform-dependent).
    pub fn discovery_mode(&self) -> DiscoveryMode {
        DiscoveryMode::from_u8(self.discovery.load(Ordering::Relaxed))
            .unwrap_or(DiscoveryMode::Mdns)
    }

    pub async fn start(&self, port: u16) -> Result<(), Box<dyn std::error::Error>> {
        self.stop();
        let listener = tokio::net::TcpListener::bind(format!("0.0.0.0:{}", port)).await?;
        // Peer capability announcements for the encryption fail-closed
        // rules. Fresh per start(): reconnects re-announce, so nothing
        // stale survives a restart.
        let caps_map: Arc<CapsMap> = Arc::new(CapsMap::default());
        let db_ptr = self.db.clone();
        let peers_ptr = self.peers.clone();
        let seen_ptr = self.seen_messages.clone();
        let stx_ptr = self.status_tx.clone();
        let sid = self.self_id.clone();
        let hash = self.room_hash;
        let excl_srv = self.excluded_collections.clone();
        let last_ping_ptr = self.last_mesh_ping.clone();
        let echo_cache_clone = self.echo_cache.clone();
        let lease_srv = self.cloud_leases.clone();
        let caps_srv = caps_map.clone();
        let caps_static = caps_map.clone();

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
                let caps_c = caps_srv.clone();
                let lease_c = lease_srv.clone();
                
                // SPAWN the handler so the loop can continue accepting other peers
                tokio::spawn(async move {
                    handle_peer(
                        stream, db_c, peers_c, seen_c, stx_c, 
                        sid_c, hash_c, excl_c, relay_enabled, 
                        lp_c, echo_c, caps_c, lease_c
                    ).await;
                });
            }
        });
        self.tasks.lock().unwrap().push(handle_srv);

        // Discovery transports for this node (mode chosen via with_discovery /
        // set_discovery; default is mDNS on desktop, broadcast on mobile).
        let (mdns_on, bcast_on) = transports_for(self.discovery_mode());

        // 3. mDNS Discovery — skipped entirely under Broadcast-only mode.
        let browser = if mdns_on {
            let mdns = ServiceDaemon::new().expect("Failed to create mDNS");
            * self.mdns.lock().unwrap() = Some(mdns.clone());

            let my_ip = local_ip_address::local_ip().map(|ip| ip.to_string()).unwrap_or_else(|_| "127.0.0.1".to_string());
            let hostname = gethostname::gethostname().to_string_lossy().into_owned() + ".local.";
            let service_info = ServiceInfo::new(&self.service_type, &self.self_id, &hostname, &my_ip, port, None)?;
            mdns.register(service_info)?;

            // 1. Create the browser once
            Some(mdns.browse(&self.service_type)?)
        } else {
            None
        };

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
        let lease_disc = self.cloud_leases.clone();

        // Shared membership view: Name -> (Last Known Address, last-seen).
        // Written by whichever transports the mode enables (mDNS and/or
        // broadcast); the dial loop reads it uniformly.
        let discovery_cache: Arc<Mutex<HashMap<String, (String, Instant)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let caps_disc = caps_map.clone();

        // 5. UDP broadcast discovery — runs when the mode enables it
        // (mobile default; desktop opt-in via with_discovery(Both/Broadcast)
        // for mixed groups). Handles join self.tasks so stop() aborts them.
        if bcast_on {
            use std::net::SocketAddr;
            let bcast_target = SocketAddr::from(([255, 255, 255, 255], BEACON_PORT));
            let bcast_bind = SocketAddr::from(([0, 0, 0, 0], BEACON_PORT));
            let bc_cache = discovery_cache.clone();
            let bc_id = self.self_id.clone();
            let bc_hash = self.room_hash;
            self.tasks.lock().unwrap().push(tokio::spawn(beacon_sender_task(
                bc_cache, bc_id, bc_hash, port, bcast_target,
            )));
            let bl_cache = discovery_cache.clone();
            let bl_id = self.self_id.clone();
            let bl_hash = self.room_hash;
            self.tasks.lock().unwrap().push(tokio::spawn(beacon_listener_task(
                bl_cache, bl_id, bl_hash, bcast_bind,
            )));
        }

        // 3. SINGLE Unified Discovery & Reconnection Task
        // Map of Name -> (Last Known Address, last-seen). Shared with the
        // Android broadcast tasks (mDNS stays the only writer on desktop).
        let handle_discovery = tokio::spawn(async move {
            let mut retry_interval = tokio::time::interval(Duration::from_secs(15));
            let mut connecting: HashSet<String> = HashSet::new();
            
            // We own 'browser' here and use it exclusively in this loop
            loop {
                tokio::select! {
                    // Branch A: Listen for NEW peers via mDNS (parked forever
                    // under Broadcast-only mode — None has no browser).
                    event_res = async {
                        match &browser {
                            Some(b) => b.recv_async().await,
                            None => std::future::pending().await,
                        }
                    } => {
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
                                    discovery_cache.lock().unwrap()
                                        .insert(p_name, (p_addr, Instant::now()));
                                }
                            }
                            Ok(ServiceEvent::ServiceRemoved(_type, name)) => {
                                let p_name = name.split('.').next().unwrap_or("");
                                discovery_cache.lock().unwrap().remove(p_name);
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

                        // Snapshot + prune under one short lock; dial outside it.
                        // Pruning also covers broadcast-discovered peers, which
                        // have no ServiceRemoved event.
                        let dial_list: Vec<(String, String)> = {
                            let mut guard = discovery_cache.lock().unwrap();
                            prune_stale_peers(&mut guard, Instant::now());
                            guard.iter().map(|(n, (a, _))| (n.clone(), a.clone())).collect()
                        };

                        for (p_name, p_addr) in &dial_list {
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
                                let caps_c = caps_disc.clone();
                                let lease_c = lease_disc.clone();

                                tokio::spawn(async move {
                                    // Short timeout so a single dead peer doesn't hang the loop
                                    if let Ok(Ok(stream)) = tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr_c)).await {
                                        handle_peer(
                                            stream, db_c, peers_c, seen_c, stx_c, 
                                            sid_c, hash_disc, excl_c, relay_disc, lp_c,
                                            echo_cache, caps_c, lease_c
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

        // Static peer dialers (server deployments; see field docs). One
        // task per address, guarded by a per-address in-flight set so a
        // healthy connection is never re-dialed — retry happens only after
        // the previous connection fully drops. 15s cadence matches the
        // discovery reconncetions above.
        let static_addrs = self.static_peers.clone();
        if !static_addrs.is_empty() {
            let db_s = self.db.clone();
            let peers_s = self.peers.clone();
            let seen_s = self.seen_messages.clone();
            let stx_s = self.status_tx.clone();
            let sid_s = self.self_id.clone();
            let excl_s = self.excluded_collections.clone();
            let lp_s = self.last_mesh_ping.clone();
            let echo_s = self.echo_cache.clone();
            let lease_s = self.cloud_leases.clone();
            let relay_s = relay_enabled;
            let hash_s = hash;
            let in_flight: Arc<std::sync::Mutex<HashSet<String>>> =
                Arc::new(std::sync::Mutex::new(HashSet::new()));
            let caps_st = caps_static.clone();
            let handle_static = tokio::spawn(async move {
                let mut tick = tokio::time::interval(Duration::from_secs(15));
                tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
                loop {
                    tick.tick().await;
                    for addr in static_addrs.iter() {
                        if !in_flight.lock().unwrap().insert(addr.clone()) {
                            continue;
                        }
                        let db_c = db_s.clone();
                        let peers_c = peers_s.clone();
                        let seen_c = seen_s.clone();
                        let stx_c = stx_s.clone();
                        let sid_c = sid_s.clone();
                        let excl_c = excl_s.clone();
                        let lp_c = lp_s.clone();
                        let echo_c = echo_s.clone();
                        let caps_c = caps_st.clone();
                        let lease_c = lease_s.clone();
                        let conn = in_flight.clone();
                        let addr_c = addr.clone();
                        tokio::spawn(async move {
                            let dial =
                                tokio::time::timeout(Duration::from_secs(5), TcpStream::connect(&addr_c)).await;
                            if let Ok(Ok(stream)) = dial {
                                handle_peer(
                                    stream, db_c, peers_c, seen_c, stx_c, sid_c, hash_s, excl_c,
                                    relay_s, lp_c, echo_c, caps_c, lease_c,
                                )
                                .await;
                            }
                            conn.lock().unwrap().remove(&addr_c);
                        });
                    }
                }
            });
            self.tasks.lock().unwrap().push(handle_static);
        }

        // 3. Tailer
        let db_tail = self.db.clone();
        let peers_tail = self.peers.clone();
        let offsets_tail = self.shard_offsets.clone();
        let excl_tail = self.excluded_collections.clone();
        let echo_cache_clone = self.echo_cache.clone();
        let caps_tail = caps_static.clone();
        
        let handle_tailer = tokio::task::spawn_blocking(move || {
            let mut last_checkpoint_save = Instant::now(); // Use Instant for timing
            loop {
                if peers_tail.blocking_lock().is_empty() {
                    std::thread::sleep(Duration::from_millis(1000));
                    continue;
                }
                
                let mut overall_changed = false;
                let mut offsets = offsets_tail.lock().unwrap();

                // Sync enumeration (hidden included, excluded dropped).
                // `__hako_security` rides along by enumeration now —
                // policies replicate — no manual re-add.
                let cols = db_tail.sync_collections().unwrap_or_default();

                // Encryption fail-closed set for this pass: collections WE
                // encrypt at rest. Empty in unencrypted deployments, in which
                // case every send below takes the legacy broadcast path.
                let local_fp = sync_guard::local_fingerprint(
                    db_tail.config.encryption_key.as_deref(),
                );
                let enc_cols: HashSet<String> = cols
                    .iter()
                    .filter(|c| db_tail.is_collection_encrypted(c))
                    .cloned()
                    .collect();

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
                                // (sync_core: shared decode, all tailers).
                                let wal_timestamp = crate::sync_core::op_logical_time(&op);

                                // 2. CHECK & REMOVE (The Core Fix)
                                let is_echo = crate::sync_core::echo_take(
                                    &echo_cache_clone,
                                    key,
                                    wal_timestamp,
                                );

                                if is_echo {
                                    continue; // Skip this one, it was a remote write
                                }

                                // Local-only signal: never broadcast marked ops.
                                if db_tail.is_local_only(&col, key) {
                                    continue;
                                }

                                if let Some(bytes) = resolve_op_to_bytes(&shard, &db_tail, &op) {
                                    logical_ops.push(WalOp::PutInlined { key: op.get_key().to_string(), value: bytes });
                                } else if matches!(op, WalOp::Delete { .. }) {
                                    logical_ops.push(op);
                                }
                            }
                            if !logical_ops.is_empty() {
                                if enc_cols.contains(&col) {
                                    // Encrypted room: fan out only to
                                    // fingerprint-matched peers (same msg_id
                                    // for all, preserving dedup semantics).
                                    send_filtered_mesh(
                                        &peers_tail,
                                        &caps_tail,
                                        &col,
                                        logical_ops,
                                        local_fp,
                                    );
                                } else {
                                    broadcast_mesh(&peers_tail, &col, logical_ops, 0);
                                }
                            }
                            offsets.insert(col, new_pos);
                            overall_changed = true;
                        }
                    };
                }

                // --- RESTORED: Periodically save offsets to the database ---
                if overall_changed && last_checkpoint_save.elapsed() > Duration::from_secs(5) {
                    let mut doc = HakoDoc::default();
                    let map: Vec<(Arc<str>, Value)> = offsets.iter()
                        .map(|(k, v)| (Arc::from(k.as_str()), Value::Int(*v as i64)))
                        .collect();
                    
                    doc.insert("offsets", Value::Map(map));
                    let _ = db_tail.put("__hako_system", "sync_checkpoint", &doc);
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
                        for (peer_id, writer) in guard.iter_mut() {
                            if let Err(e) = send_frame(writer, &payload).await {
                                net_send_warn(
                                    &format!("ping-{peer_id}"),
                                    format!("[sync] periodic ping to peer '{peer_id}' failed ({} bytes): {e}", payload.len()),
                                );
                            }
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

/// This node's capability advertisement: key fingerprint + the collections
/// it encrypts at rest (sync enumeration already includes the hidden
/// security collection, so no manual re-add).
#[cfg(feature = "net-sync")]
fn local_caps(db: &Arc<Hako>) -> PeerCaps {
    let cols: Vec<String> = db
        .sync_collections()
        .unwrap_or_default()
        .into_iter()
        .filter(|c| db.is_collection_encrypted(c))
        .collect();
    PeerCaps {
        key_fp: sync_guard::local_fingerprint(db.config.encryption_key.as_deref()),
        encrypted_cols: cols,
    }
}

/// Warn text shared by sender/receiver drops. Names the peer, collection,
/// and both fingerprints so an operator can tell "wrong key" from "no key
/// / old peer" at a glance.
#[cfg(feature = "net-sync")]
fn caps_warn_msg(dir: &str, peer: &str, col: &str, local_fp: [u8; 32]) -> String {
    format!(
        "{dir} encrypted collection '{col}' for peer '{peer}' (local key {local}, peer unverified — peer needs the same encryption key; upgrade old peers). Wire stays silent instead of leaking plaintext.",
        local = sync_guard::fp_short(&local_fp),
    )
}

/// Sender-side rule for one (peer, collection): true when the collection
/// may leave this node toward that peer. Warns (throttled) on refusal.
#[cfg(feature = "net-sync")]
fn peer_may_send(
    db: &Arc<Hako>,
    caps: &Arc<CapsMap>,
    peer_id: &str,
    col: &str,
    local_fp: [u8; 32],
) -> bool {
    let enc = db.is_collection_encrypted(col);
    if sync_guard::caps_allow(enc, local_fp, caps.get(&peer_id).as_ref()) {
        return true;
    }
    caps.warn(
        peer_id,
        col,
        &caps_warn_msg("skipping send of", peer_id, col, local_fp),
    );
    false
}

async fn handle_peer(
    stream: TcpStream, 
    db: Arc<Hako>, 
    peers_map: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    seen_cache: Arc<AsyncMutex<Vec<u128>>>, 
    status_tx: watch::Sender<NetworkStatus>,
    self_id: String, 
    my_hash: [u8; 32], 
    excluded: HashSet<String>,
    enable_relay: bool, 
    last_ping: Arc<Mutex<Instant>>,
    echo_cache: Arc<Mutex<HashMap<String, i64>>>,
    caps: Arc<CapsMap>,
    cloud_leases: Arc<Mutex<HashMap<String, (i64, Instant)>>>,
) {
    let (mut reader, mut writer) = stream.into_split();

    // 1. Identify Handshake
    let handshake = NetPacket::Identify { id: self_id.clone(), room_hash: my_hash };
    if let Ok(bytes) = bincode::serialize(&handshake) {
        if send_frame(&mut writer, &bytes).await.is_err() { return; }
    }

    let p_bytes = match tokio::time::timeout(Duration::from_secs(5), recv_frame(&mut reader)).await {
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

    // 1b. Announce our encryption capabilities immediately (before any
    // data flows). Old peers fail to decode the new variant and skip it
    // silently — no disconnect, no breakage; they simply never present
    // caps and are treated as unverified for encrypted rooms.
    {
        let ours = local_caps(&db);
        let packet = NetPacket::SyncCaps {
            key_fp: ours.key_fp,
            encrypted_cols: ours.encrypted_cols,
        };
        if let Ok(bytes) = bincode::serialize(&packet) {
            if send_frame(&mut writer, &bytes).await.is_err() {
                return;
            }
        }
    }

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
        let raw = match recv_frame(&mut reader).await { Ok(b) => b, _ => break };

        if let Ok(packet) = bincode::deserialize::<NetPacket>(&raw) {
            match packet {
                NetPacket::Ping { versions, indexes } => {
                    if let Ok(mut lp) = last_ping.lock() { *lp = Instant::now(); }

                    // Index defs advertised by the peer converge locally
                    // (issue #51). Shared helper in sync_core so it
                    // unit-tests without networking.
                    crate::sync_core::apply_peer_index_defs(&db, &indexes);
                    
                    for (col, remote_time) in versions {
                        if excluded.contains(&col) || db.is_sync_withheld(&col) { continue; }
                        // if db.get_collection_version(&col) > remote_time {
                        //     handle_delta_send(&db, &peers_map, &peer_id, &col, remote_time).await;
                        // }
                        if let Ok(local_version) = db.get_collection_version(&col) {
                            if local_version > remote_time {
                                handle_delta_send(&db, &peers_map, &peer_id, &col, remote_time, &caps).await;
                            }
                        } else {
                            // Log that we couldn't check this collection due to an error
                            crate::util::log::info(&format!("[sync] Skipping collection {}: Shard could not be opened.", col));
                        }
                    }
                }
                NetPacket::SyncRequest => {
                    handle_bootstrap(&db, &peers_map, &peer_id, &excluded, &caps).await;
                }
                NetPacket::Replication { msg_id, collection, ops } => {
                    if excluded.contains(&collection) || db.is_sync_withheld(&collection) {continue;}
                    // Check Cache
                    if msg_id != 0 {
                        let mut cache = seen_cache.lock().await;
                        if cache.contains(&msg_id) { continue; }
                        cache.push(msg_id);
                        if cache.len() > 1000 { cache.remove(0); }
                    }

                    // Receiver rule (fail-closed): drop the batch when the
                    // collection is encrypted locally and the sender never
                    // proved the same key. Sender-side filtering is the
                    // primary guard; this is defense in depth (notably
                    // against old senders, which announce nothing).
                    let peer_caps = caps.get(&peer_id);
                    let enc_local = db.is_collection_encrypted(&collection);
                    let local_fp = sync_guard::local_fingerprint(
                        db.config.encryption_key.as_deref(),
                    );
                    if !sync_guard::caps_allow(enc_local, local_fp, peer_caps.as_ref()) {
                        caps.warn(
                            &peer_id,
                            &collection,
                            &caps_warn_msg("dropping", &peer_id, &collection, local_fp),
                        );
                        continue;
                    }

                    // Apply (Lock is released here)
                    apply_replication_batch(db.clone(), collection, ops, echo_cache_clone.clone()).await; 

                    if msg_id != 0 && enable_relay { 
                        relay_mesh(&peers_map, &caps, &raw, &peer_id).await; 
                    }
                }
                NetPacket::SyncCaps { key_fp, encrypted_cols } => {
                    // Record the announcement (clears that peer's warn
                    // history), then re-request versions: ops the peer
                    // skipped for lack of caps are still behind our version
                    // vector and get picked up by the delta that follows.
                    caps.set(
                        &peer_id,
                        PeerCaps {
                            key_fp,
                            encrypted_cols: encrypted_cols.clone(),
                        },
                    );                    crate::util::log::info(&format!(
                        "[sync] peer '{peer_id}' announced encryption caps (key {}, {} encrypted cols)",
                        sync_guard::fp_short(&key_fp),
                        encrypted_cols.len(),
                    ));
                    let my_versions = db.get_version_map();
                    let my_indexes = db.list_indexes(None);
                    let mut guard = peers_map.lock().await;
                    if let Some(w) = guard.get_mut(&peer_id) {
                        let _ = send_packet(
                            w,
                            NetPacket::Ping {
                                versions: my_versions,
                                indexes: my_indexes,
                            },
                        )
                        .await;
                        let _ = send_packet(w, NetPacket::SyncRequest).await;
                    }
                }
                NetPacket::CloudLease { client_id, last_exchange_ts } => {
                    // #72 phase 1: record the relay's cloud freshness for
                    // the lag meter / phase-2 GRACE rule. Best-effort book
                    // keeping — a bad clock just shows a big lag.
                    let mut leases = cloud_leases.lock().unwrap();
                    leases.insert(client_id, (last_exchange_ts, Instant::now()));
                }
                NetPacket::RelayTake { ts, client_id } => {
                    // Same book-keeping; the take-over moment is itself
                    // proof of a live relay (assume fresh until the next
                    // CloudLease refines it).
                    let mut leases = cloud_leases.lock().unwrap();
                    leases.insert(client_id.clone(), (ts, Instant::now()));
                    crate::util::log::info(&format!(
                        "[sync] peer '{peer_id}' took the cloud relay role (client '{client_id}')"
                    ));
                }
                _ => {}
            }
        }
    }

    // 5. Cleanup
    peers_map.lock().await.remove(&peer_id);
    caps.remove(&peer_id);
    update_status(&status_tx, &peers_map).await;
}


fn resolve_op_to_bytes(shard_arc: &Arc<RwLock<crate::storage::engine::StorageEngine>>, db: &Arc<Hako>, op: &WalOp) -> Option<Vec<u8>> {
    // 0.9.0: shared send-side inflation (sync_core) — one blob path for
    // all transports. Body moved verbatim; this stays as the call edge.
    crate::sync_core::resolve_send_bytes(shard_arc, db, op)
}

// Stage B (#69): bootstrap paging + pacing + send hygiene.
/// Byte budget per bootstrap/delta page — same unit as the cloud pager;
/// sits far below the mesh hard cap `sync_core::MAX_FRAME` (256 MiB),
/// which must stay unchanged.
const BOOT_PAGE_MAX_BYTES: usize = 8 * 1024 * 1024;
/// Doc-count ceiling per page: payload size varies wildly, so both
/// budgets apply (docs bound the LWW-lock window, bytes bound the frame).
const BOOT_PAGE_MAX_DOCS: usize = 1000;
/// Bootstrap pacing floor per the measured WAN goodput in #69: a joining
/// peer may not monopolize the host. ponytail: arithmetic sleep pacing
/// for one serial sender; upgrade to a true token bucket if two joins
/// ever contend measurably at this scale.
const BOOTSTRAP_PACE_BPS: u64 = 4 * 1024 * 1024;

/// Sleep owed before the next page, keeping bootstrap traffic under the
/// pacing floor (static per-peer; concurrent joins each get their own).
fn bootstrap_pace(payload_bytes: usize) -> Duration {
    Duration::from_secs_f64(payload_bytes as f64 / BOOTSTRAP_PACE_BPS as f64)
}

/// Throttled failure log for sends that would otherwise drop silently:
/// one line per key per window. Real failures surface with the peer and
/// packet size; peer flaps do not firehose stderr. Returns true when a
/// line was emitted (test tripwire).
fn net_send_warn(key: &str, msg: String) -> bool {
    const WINDOW: Duration = Duration::from_secs(10);
    let due = {
        let mut g = NET_WARN_LAST.lock().unwrap();
        let due = g
            .get(key)
            .map(|t| t.elapsed() >= WINDOW)
            .unwrap_or(true);
        if due { g.insert(key.to_string(), Instant::now()); }
        due
    };
    if due { eprintln!("{msg}"); }
    due
}

static NET_WARN_LAST: std::sync::LazyLock<std::sync::Mutex<HashMap<String, Instant>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

async fn handle_bootstrap(db: &Arc<Hako>, peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, peer_id: &str, excluded: &HashSet<String>, caps: &Arc<CapsMap>) {
    let encryption_key = db.config.encryption_key.as_deref();

    // Sync enumeration covers hidden collections; the excluded plane is
    // dropped by the enumerator, `excluded` double-checks per instance.
    let cols = db.sync_collections().unwrap_or_default();

    // Sender rule, evaluated once per collection (before any disk reads,
    // so refused rooms also skip the inflation work).
    let local_fp = sync_guard::local_fingerprint(db.config.encryption_key.as_deref());

    for col in cols {
        if excluded.contains(&col) || db.is_sync_withheld(&col) { continue; }
        if !peer_may_send(db, caps, peer_id, &col, local_fp) {
            continue;
        }
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

        // 2-3. Paged replay stream (#69 Stage B): pages of both budgets —
        // `BOOT_PAGE_MAX_DOCS` bounds the receiver's apply window,
        // `BOOT_PAGE_MAX_BYTES` bounds the frame (payload sizes vary
        // wildly, so doc count alone would lie). The read lock drops
        // BEFORE each page send. Pace after each page so a joining peer
        // cannot monopolize the host.
        let mut page: Vec<WalOp> = Vec::new();
        let mut page_bytes = 0usize;

        for (key, ptr) in snapshot {
            // Inflate in an isolated scope: the RwLockReadGuard must drop
            // before the async send below (and before blob resolution,
            // which takes its own writes).
            let bytes_to_send = {
                let guard = shard_arc.read().unwrap();
                match guard.read_pointer_internal(&ptr, false) {
                    Ok(Some(bytes)) => {
                        let mut final_bytes = bytes;
                        // Check if we need to inflate the skeleton
                        if let Some(mut doc) = HakoDoc::decode(&final_bytes) {
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
                let op_bytes = key.len() + value.len();
                if !page.is_empty()
                    && (page.len() >= BOOT_PAGE_MAX_DOCS
                        || page_bytes + op_bytes > BOOT_PAGE_MAX_BYTES)
                {
                    let payload_len = send_bootstrap_page(peers, peer_id, &col, std::mem::take(&mut page)).await;
                    tokio::time::sleep(bootstrap_pace(payload_len)).await;
                    page_bytes = 0;
                }
                page.push(WalOp::PutInlined { key, value });
                page_bytes += op_bytes;
            }
        }
        // Final partial page (or the whole thing for small collections).
        let payload_len = send_bootstrap_page(peers, peer_id, &col, page).await;
        let _ = payload_len;
    }
}

/// Serialize and send ONE bootstrap page to one peer. Returns the payload
/// size for pacing; failures are loud (throttled) — a silently dropped
/// bootstrap page means a silently divergent peer, the exact failure mode
/// this stage exists to remove.
async fn send_bootstrap_page(
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    peer_id: &str,
    col: &str,
    ops: Vec<WalOp>,
) -> usize {
    if ops.is_empty() { return 0; }
    let packet = NetPacket::Replication {
        msg_id: 0,
        collection: col.to_string(),
        ops,
    };
    match bincode::serialize(&packet) {
        Ok(payload) => {
            let len = payload.len();
            let mut guard = peers.lock().await;
            if let Some(w) = guard.get_mut(peer_id) {
                if let Err(e) = send_frame(w, &payload).await {
                    net_send_warn(
                        &format!("boot-{peer_id}"),
                        format!("[sync] bootstrap page to peer '{peer_id}' for collection '{col}' failed ({len} bytes): {e}"),
                    );
                }
            }
            len
        }
        Err(e) => {
            net_send_warn(
                &format!("boot-ser-{col}"),
                format!("[sync] bootstrap page serialization error for collection '{col}': {e}"),
            );
            0
        }
    }
}


#[cfg(feature = "net-sync")]
/// Rejoin/bootstrap catch-up builder shared by the delta sender: every index
/// entry (puts AND tombstones) newer than `since_time`, minus local-only
/// marks. Tombstones replay as `WalOp::Delete` (timestamped, so the
/// receiver's LWW stays sound). The handshake must not leak what the live
/// tailer withholds — the pre-v0.7.7 code sent local tombstones here.
#[cfg(feature = "net-sync")]
fn collect_delta_ops(
    db: &Arc<Hako>,
    collection: &str,
    since_time: i64,
) -> Vec<WalOp> {
    // Local-only collection: nothing leaves, not even on rejoin.
    if db.is_collection_local(collection) {
        return Vec::new();
    }
    let shard_arc = match db.get_shard(collection) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let encryption_key = db.config.encryption_key.as_deref();

    // 1. SCAN PHASE (RAM-only)
    let changed_items: Vec<(String, Pointer)> = {
        let guard = shard_arc.read().unwrap();
        guard.index.iter()
            .filter_map(|(k, ptr)| {
                if db.is_local_only(collection, k) {
                    return None;
                }
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

    // 2. INFLATION PHASE (tombstones pass through directly)
    let mut batch_ops = Vec::with_capacity(changed_items.len());
    for (key, ptr) in changed_items {
        match ptr {
            Pointer::Deleted { timestamp } => {
                batch_ops.push(WalOp::Delete { key, timestamp });
            }
            _ => {
                let raw_res = {
                    let guard = shard_arc.read().unwrap();
                    guard.read_pointer_internal(&ptr, false).ok().flatten()
                };

                if let Some(bytes) = raw_res {
                    if let Some(mut doc) = HakoDoc::decode(&bytes) {
                        // INFLATE: If document has blobs, resolve them.
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
    }
    batch_ops
}

async fn handle_delta_send(
    db: &Arc<Hako>,
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    peer_id: &str,
    collection: &str,
    since_time: i64,
    caps: &Arc<CapsMap>,
) {
    // Sender rule first: refused rooms skip the scan/inflation entirely.
    let local_fp = sync_guard::local_fingerprint(db.config.encryption_key.as_deref());
    if !peer_may_send(db, caps, peer_id, collection, local_fp) {
        return;
    }
    let batch_ops = collect_delta_ops(db, collection, since_time);
    if batch_ops.is_empty() { return; }

    // 3. BATCH SENDING (50 ops per packet; msg_id 0 = no mesh relay)
    for chunk in batch_ops.chunks(50) {
        send_replication_packet(peers, peer_id, collection, chunk.to_vec()).await;
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
                // Send using the shared sync_frame utility (length-prefix + data)
                if let Err(e) = send_frame(writer, &payload).await {
                    net_send_warn(
                        &format!("delta-{peer_id}"),
                        format!("[net_sync] Failed to send packet to {}: {}", peer_id, e),
                    );
                }
            }
        }
        Err(e) => {
            eprintln!("[net_sync] Serialization error for peer {}: {}", peer_id, e);
        }
    }
}

#[cfg(feature = "net-sync")]
async fn apply_replication_batch(db: Arc<Hako>, collection: String, ops: Vec<WalOp>, echo_cache: Arc<Mutex<HashMap<String, i64>>>,) {
    // 0.9.0: shared ingest (sync_core) — one LWW/echo/commit path for all
    // transports. Body moved verbatim; this stays as the call edge.
    crate::sync_core::apply_replicated_batch(db, collection, ops, echo_cache).await
}

async fn relay_mesh(
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    caps: &Arc<CapsMap>,
    data: &[u8],
    sender_id: &str,
) {
    // Peek the collection so origin-marked encrypted rooms are only
    // forwarded to fingerprint-matched peers. Unknown origin (old sender)
    // relays as before — receivers enforce locally. The ORIGINAL bytes go
    // out: no re-encode, no extra crypto.
    let encrypted_origin: Option<PeerCaps> = match bincode::deserialize::<NetPacket>(data) {
        Ok(NetPacket::Replication { collection, .. }) => caps
            .get(sender_id)
            .filter(|o| o.encrypted_cols.iter().any(|c| c == &collection)),
        _ => None,
    };
    let mut guard = peers.lock().await;
    for (id, writer) in guard.iter_mut() {
        if id == sender_id {
            continue;
        }
        if let Some(ref origin) = encrypted_origin {
            let matched = caps.get(id).map(|rc| rc.key_fp == origin.key_fp);
            if matched != Some(true) {
                continue;
            }
        }
        if let Err(e) = send_frame(writer, data).await {
            net_send_warn(
                &format!("relay-{id}"),
                format!("[sync] mesh relay to peer '{id}' failed ({} bytes): {e}", data.len()),
            );
        }
    }
}

fn broadcast_mesh(peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>, col: &str, ops: Vec<WalOp>, msg_id: u128) {
    let id = if msg_id == 0 { SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_micros() } else { msg_id };
    let packet = NetPacket::Replication { msg_id: id, collection: col.to_string(), ops };
    if let Ok(payload) = bincode::serialize(&packet) {
        let p_ptr = peers.clone();
        let col_s = col.to_string();
        tokio::spawn(async move {
            let mut guard = p_ptr.lock().await;
            for (peer_id, writer) in guard.iter_mut() {
                if let Err(e) = send_frame(writer, &payload).await {
                    net_send_warn(
                        &format!("mesh-{peer_id}"),
                        format!("[sync] mesh broadcast to peer '{peer_id}' for collection '{col_s}' failed ({} bytes): {e}", payload.len()),
                    );
                }
            }
        });
    }
}

/// Tailer fan-out for an ENCRYPTED collection: same packet (same msg_id,
/// preserving dedup semantics) goes only to fingerprint-matched peers.
/// Everyone else is skipped with a throttled loud warning — never silently.
/// Plaintext collections keep using `broadcast_mesh` (zero behavior delta).
#[cfg(feature = "net-sync")]
fn send_filtered_mesh(
    peers: &Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>>,
    caps: &Arc<CapsMap>,
    col: &str,
    ops: Vec<WalOp>,
    local_fp: [u8; 32],
) {
    let id = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_micros();
    let packet = NetPacket::Replication {
        msg_id: id,
        collection: col.to_string(),
        ops,
    };
    if let Ok(payload) = bincode::serialize(&packet) {
        let p_ptr = peers.clone();
        let c_ptr = caps.clone();
        let col_s = col.to_string();
        tokio::spawn(async move {
            let mut guard = p_ptr.lock().await;
            for (peer_id, writer) in guard.iter_mut() {
                let peer = c_ptr.get(peer_id);
                if !sync_guard::caps_allow(true, local_fp, peer.as_ref()) {
                    c_ptr.warn(
                        peer_id,
                        &col_s,
                        &caps_warn_msg("skipping send of", peer_id, &col_s, local_fp),
                    );
                    continue;
                }
                if let Err(e) = send_frame(writer, &payload).await {
                    net_send_warn(
                        &format!("meshf-{peer_id}"),
                        format!("[sync] filtered mesh send to peer '{peer_id}' for collection '{col_s}' failed ({} bytes): {e}", payload.len()),
                    );
                }
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

/// Helper to send typed packets safely
async fn send_packet(writer: &mut OwnedWriteHalf, packet: NetPacket) -> tokio::io::Result<()> {
    if let Ok(payload) = bincode::serialize(&packet) {
        send_frame(writer, &payload).await?;
    }
    Ok(())
}

#[cfg(all(test, feature = "net-sync"))]
mod tests {
    use super::*;
    use crate::config::{DurabilityMode, HakoConfig};

    fn temp_db(tag: &str) -> (Arc<Hako>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "hakodb-netsync-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Arc::new(Hako::open(&dir, cfg).unwrap());
        (db, dir)
    }

    fn put_simple(db: &Arc<Hako>, col: &str, id: &str) {
        let mut doc = HakoDoc::default();
        doc.insert("v", Value::Int(1));
        db.put(col, id, &doc).unwrap();
    }

    #[test]
    fn delta_send_withholds_local_only_tombstones() {
        let (db, _dir) = temp_db("delta-local");
        put_simple(&db, "c", "keep");
        put_simple(&db, "c", "gone-normal");
        put_simple(&db, "c", "gone-local");
        db.delete("c", "gone-normal").unwrap();
        db.delete_local("c", "gone-local").unwrap();

        let ops = collect_delta_ops(&db, "c", 0);
        let mut puts = 0;
        let mut dels = Vec::new();
        for op in &ops {
            match op {
                WalOp::PutInlined { .. } => puts += 1,
                WalOp::Delete { key, .. } => dels.push(key.clone()),
                _ => {}
            }
        }
        assert_eq!(puts, 1, "expected only the live doc, got {ops:?}");
        assert_eq!(dels, vec!["gone-normal".to_string()], "local tombstone leaked: {ops:?}");
    }

    #[test]
    fn delta_send_empty_for_local_collection() {
        let (db, _dir) = temp_db("delta-col");
        put_simple(&db, "c", "a");
        db.set_collection_local("c", true);

        assert!(collect_delta_ops(&db, "c", 0).is_empty());
    }

    /// Issue #51: a peer's advertised defs converge locally — secondary
    /// arm unchanged, vector def lands, repeated Pings are no-ops, and
    /// vectors written after convergence search correctly. Covers the
    /// shared sync_core helper for all planes.
    #[test]
    fn peer_index_defs_converge_vector() {
        use crate::index::vector::{encode_f32s, Metric};
        use crate::sync_core::apply_peer_index_defs;

        let (a, _da) = temp_db("ping-a");
        let (b, _db) = temp_db("ping-b");
        a.create_vector_index("pts", "emb", 2, Metric::Cosine).unwrap();
        a.create_index("pts", "grp").unwrap();
        a.create_traverse_index("pts").unwrap();

        let advertised = a.list_indexes(None);
        apply_peer_index_defs(&b, &advertised);

        let listed = b.list_indexes(Some("pts"));
        assert!(
            listed.vector.get("pts").is_some_and(|v| v
                .iter()
                .any(|d| d.field == "emb" && d.dim == 2 && d.metric == "cosine")),
            "vector def must converge"
        );
        assert!(
            listed.secondary.get("pts").is_some_and(|v| v.contains(&"grp".to_string())),
            "secondary arm must keep working"
        );
        assert!(
            listed.traverse.contains(&"pts".to_string()),
            "traverse targets must converge (issue #53)"
        );

        // Second Ping: guard skips the known def (no error, no dup).
        apply_peer_index_defs(&b, &advertised);
        let relisted = b.list_indexes(Some("pts"));
        assert_eq!(
            relisted.vector.get("pts").map(|v| v.len()),
            Some(1),
            "repeated Ping must not duplicate defs"
        );

        // Vectors written post-convergence index + search (quiescence
        // first — same rule as engine tests: Near fail-closes early).
        for (id, p) in [("a", [1.0f32, 0.0]), ("b", [0.0, 1.0]), ("c", [1.0, 1.0])] {
            let mut doc = HakoDoc::default();
            doc.insert("emb", Value::Binary(encode_f32s(&p)));
            b.put("pts", id, &doc).unwrap();
        }
        assert!(b.await_quiescent(Duration::from_secs(60)));
        let hits = b.find_near("pts", "emb", &[1.0, 0.5], 2).unwrap();
        assert_eq!(hits.len(), 2);
        assert_eq!(hits[0].0, "c");
        assert_eq!(hits[1].0, "a");
    }

    fn beacon_pkt(id: &str, room: &[u8; 32], port: u16) -> BeaconPacket {
        BeaconPacket {
            magic: BEACON_MAGIC,
            id: id.to_string(),
            room_hash: *room,
            tcp_port: port,
            known: vec![],
        }
    }

    #[test]
    fn beacon_roundtrip_and_rejects_garbage() {
        let room = [7u8; 32];
        let p = beacon_pkt("node-a", &room, 7070);
        let back = decode_beacon(&encode_beacon(&p)).expect("roundtrip");
        assert_eq!(back.id, "node-a");
        assert_eq!(back.tcp_port, 7070);

        assert!(decode_beacon(b"not a beacon").is_none());
        let mut bad = encode_beacon(&p);
        bad[0] ^= 0xFF; // corrupt magic
        assert!(decode_beacon(&bad).is_none());
    }

    #[test]
    fn beacon_merge_rules() {
        use std::net::IpAddr;
        let room_a = [1u8; 32];
        let room_b = [2u8; 32];
        let now = Instant::now();
        let mut cache: HashMap<String, (String, Instant)> = HashMap::new();
        let src: IpAddr = "192.168.1.20".parse().unwrap();

        // Wrong room and self are ignored (no insert, returns false).
        let mut foreign = beacon_pkt("node-x", &room_b, 7070);
        assert!(!merge_beacon(&mut cache, &foreign, src, "node-me", &room_a, now));
        foreign.room_hash = room_a;
        foreign.id = "node-me".to_string();
        assert!(!merge_beacon(&mut cache, &foreign, src, "node-me", &room_a, now));
        assert!(cache.is_empty());

        // Sender upsert uses the PACKET SOURCE ip, never a self-reported one.
        let p = beacon_pkt("node-x", &room_a, 7070);
        assert!(merge_beacon(&mut cache, &p, src, "node-me", &room_a, now));
        assert_eq!(cache["node-x"].0, "192.168.1.20:7070");

        // Same content re-announced: no change reported...
        assert!(!merge_beacon(&mut cache, &p, src, "node-me", &room_a, now));
        // ...but a changed port updates the entry.
        let p2 = beacon_pkt("node-x", &room_a, 8080);
        assert!(merge_beacon(&mut cache, &p2, src, "node-me", &room_a, now));
        assert_eq!(cache["node-x"].0, "192.168.1.20:8080");

        // Gossip: known peers merge in; self inside gossip is skipped.
        let mut g = beacon_pkt("node-x", &room_a, 8080);
        g.known = vec![
            ("node-y".to_string(), "192.168.1.30:7070".to_string()),
            ("node-me".to_string(), "192.168.1.99:9999".to_string()),
        ];
        assert!(merge_beacon(&mut cache, &g, src, "node-me", &room_a, now));
        assert_eq!(cache["node-y"].0, "192.168.1.30:7070");
        assert!(!cache.contains_key("node-me"));
    }

    #[test]
    fn stale_peers_pruned() {
        let now = Instant::now();
        let old = now - BEACON_EXPIRY - Duration::from_secs(1);
        let mut cache: HashMap<String, (String, Instant)> = HashMap::from([
            ("fresh".to_string(), ("1.2.3.4:1".to_string(), now)),
            ("gone".to_string(), ("1.2.3.5:1".to_string(), old)),
        ]);
        prune_stale_peers(&mut cache, now);
        assert!(cache.contains_key("fresh"));
        assert!(!cache.contains_key("gone"));
    }

    #[test]
    fn transports_for_matrix() {
        assert_eq!(transports_for(DiscoveryMode::Mdns), (true, false));
        assert_eq!(transports_for(DiscoveryMode::Broadcast), (false, true));
        assert_eq!(transports_for(DiscoveryMode::Both), (true, true));
    }

    #[test]
    fn platform_default_discovery() {
        // Desktop keeps historic behavior (mDNS only); mobile gets the
        // multicast-free default. Mixed groups opt into Both explicitly.
        #[cfg(any(target_os = "android", target_os = "ios"))]
        assert_eq!(default_discovery_mode(), DiscoveryMode::Broadcast);
        #[cfg(not(any(target_os = "android", target_os = "ios")))]
        assert_eq!(default_discovery_mode(), DiscoveryMode::Mdns);
    }

    #[test]
    fn with_discovery_overrides_default() {
        let (db, _dir) = temp_db("mode");
        let s = NetSyncer::new(db, "n", "k", vec![]).with_discovery(DiscoveryMode::Both);
        assert_eq!(s.discovery_mode(), DiscoveryMode::Both);
        let d = NetSyncer::new(temp_db("mode2").0, "n", "k", vec![]);
        assert_eq!(d.discovery_mode(), default_discovery_mode());
    }

    #[tokio::test]
    async fn beacon_sender_reaches_listener_over_loopback() {
        use std::net::SocketAddr;
        // Fixed high port, loopback only: proves the socket tasks interoperate.
        // Production uses 255.255.255.255 (broadcast) instead of 127.0.0.1.
        let port: u16 = 45354;
        let room = [9u8; 32];
        let cache_a: Arc<Mutex<HashMap<String, (String, Instant)>>> =
            Arc::new(Mutex::new(HashMap::new()));
        let cache_b: Arc<Mutex<HashMap<String, (String, Instant)>>> =
            Arc::new(Mutex::new(HashMap::new()));

        let h_listen = tokio::spawn(beacon_listener_task(
            cache_b.clone(),
            "node-b".to_string(),
            room,
            SocketAddr::from(([127, 0, 0, 1], port)),
        ));
        let h_send = tokio::spawn(beacon_sender_task(
            cache_a.clone(),
            "node-a".to_string(),
            room,
            7070,
            SocketAddr::from(([127, 0, 0, 1], port)),
        ));
        tokio::time::sleep(Duration::from_millis(300)).await;
        h_send.abort();
        h_listen.abort();

        let guard = cache_b.lock().unwrap();
        let (addr, _) = guard.get("node-a").expect("node-b must hear node-a");
        assert_eq!(addr, "127.0.0.1:7070");
    }

    fn temp_enc_db(tag: &str, key: &str) -> (Arc<Hako>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "hakodb-netsync-enc-{tag}-{}",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.encryption_key = Some(key.to_string());
        // No encrypted_cols list: encryption is global on this node.
        let db = Arc::new(Hako::open(&dir, cfg).unwrap());
        (db, dir)
    }

    #[test]
    fn sync_caps_roundtrip_and_old_peers_skip_it() {
        // New <-> new: the appended variant survives bincode both ways.
        let pkt = NetPacket::SyncCaps {
            key_fp: [7u8; 32],
            encrypted_cols: vec!["notes".to_string()],
        };
        let bytes = bincode::serialize(&pkt).unwrap();
        match bincode::deserialize::<NetPacket>(&bytes).expect("decode own caps") {
            NetPacket::SyncCaps {
                key_fp,
                encrypted_cols,
            } => {
                assert_eq!(key_fp, [7u8; 32]);
                assert_eq!(encrypted_cols, vec!["notes".to_string()]);
            }
            other => panic!("wrong variant: {other:?}"),
        }
        // Old peers (which only know indices 0..=3) fail to decode ANY
        // trailing-variant packet and skip it via `if let Ok` — the exact
        // property that keeps mixed-version meshes connected. Simulate by
        // decoding out-of-range variant index bytes.
        let mut unknown = vec![99u8, 0, 0, 0];
        unknown.extend_from_slice(&[0u8; 8]);
        assert!(
            bincode::deserialize::<NetPacket>(&unknown).is_err(),
            "unknown variants must fail (and therefore be skipped, never fatal)"
        );
        // Pre-existing variants keep their indices (Identify = 0).
        let id = NetPacket::Identify {
            id: "n".to_string(),
            room_hash: [1u8; 32],
        };
        let id_bytes = bincode::serialize(&id).unwrap();
        assert_eq!(&id_bytes[..4], &[0u8, 0, 0, 0]);
    }

    #[test]
    fn local_caps_reflects_at_rest_config() {
        let (plain_db, _d1) = temp_db("caps-plain");
        let plain = local_caps(&plain_db);
        assert_eq!(plain.key_fp, [0u8; 32]);
        assert!(plain.encrypted_cols.is_empty());

        let (enc_db, _d2) = temp_enc_db("caps-enc", "s3cret");
        assert!(enc_db.is_collection_encrypted("notes"));
        assert!(!plain_db.is_collection_encrypted("notes"));
        let caps = local_caps(&enc_db);
        assert_eq!(
            caps.key_fp,
            crate::sync_guard::key_fingerprint("s3cret")
        );
        // A written collection shows up once encrypted at rest.
        put_simple(&enc_db, "notes", "a");
        let caps = local_caps(&enc_db);
        assert!(caps.encrypted_cols.contains(&"notes".to_string()));
    }

    #[test]
    fn sender_rule_end_to_end_per_peer() {
        let (enc_db, _d) = temp_enc_db("sender-rule", "s3cret");
        put_simple(&enc_db, "notes", "a");
        let caps_map: Arc<CapsMap> = Arc::new(CapsMap::default());
        let fp = crate::sync_guard::local_fingerprint(Some("s3cret"));

        // Unknown peer (old / silent): refused, warned once per window.
        assert!(!peer_may_send(&enc_db, &caps_map, "old-peer", "notes", fp));
        // Wrong-key peer: refused.
        caps_map.set(
            "other-peer",
            crate::sync_guard::PeerCaps {
                key_fp: [9u8; 32],
                encrypted_cols: vec!["notes".to_string()],
            },
        );
        assert!(!peer_may_send(&enc_db, &caps_map, "other-peer", "notes", fp));
        // Same-key peer: allowed.
        caps_map.set(
            "good-peer",
            crate::sync_guard::PeerCaps {
                key_fp: fp,
                encrypted_cols: vec!["notes".to_string()],
            },
        );
        assert!(peer_may_send(&enc_db, &caps_map, "good-peer", "notes", fp));

        // Plaintext collection on the same node: everyone allowed.
        let (plain_db, _d2) = temp_db("sender-plain");
        put_simple(&plain_db, "open", "a");
        let fp_none = crate::sync_guard::local_fingerprint(None);
        assert!(peer_may_send(&plain_db, &caps_map, "old-peer", "open", fp_none));
        assert!(peer_may_send(&plain_db, &caps_map, "good-peer", "open", fp_none));
    }

    /// Stage B (#69) send hygiene: the throttled warn fires on the first
    /// failure for a key, then holds the window (no firehose during peer
    /// flaps), then fires again once it expires.
    #[test]
    fn net_send_warn_throttles_per_key() {
        let tag = format!("tw-{}", std::process::id());
        assert!(net_send_warn(&tag, "first".into()), "first failure is loud");
        assert!(!net_send_warn(&tag, "second".into()), "inside the window: quiet");
        // Different key fires independently.
        assert!(net_send_warn(&format!("{tag}-b"), "other peer".into()));
    }

    /// Stage B (#69): the pacing floor is arithmetic — 4 MB/s.
    #[test]
    fn bootstrap_pace_matches_floor() {
        assert_eq!(bootstrap_pace(0), Duration::ZERO);
        assert_eq!(bootstrap_pace(BOOTSTRAP_PACE_BPS as usize), Duration::from_secs(1));
        assert_eq!(bootstrap_pace(BOOTSTRAP_PACE_BPS as usize / 2), Duration::from_millis(500));
    }

    /// Stage B (#69) core: a mesh peer joining a >16 MB node receives the
    /// whole collection in byte-budgeted pages (never one giant frame like
    /// the cloud plane bug), each page within both budgets, every key
    /// delivered exactly once.
    #[tokio::test]
    async fn bootstrap_pages_large_collection_over_loopback() {
        let (db, _dir) = temp_db("boot-page");
        // 96 x 256 KiB = 24 MB node: far over the 16 MiB transport class
        // that silently dropped data on the cloud plane before Stage A.
        let chunk = vec![7u8; 256 * 1024];
        for i in 0..96 {
            let mut doc = HakoDoc::default();
            doc.insert("b", Value::Binary(chunk.clone()));
            db.put("big", &format!("k{i:03}"), &doc).unwrap();
        }

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let client = tokio::net::TcpStream::connect(addr).await.unwrap();
        let (server, _) = listener.accept().await.unwrap();
        let (mut c_read, _c_write) = client.into_split();
        let (_s_read, s_write) = server.into_split();

        let peers: Arc<AsyncMutex<HashMap<String, OwnedWriteHalf>>> = Arc::new(
            AsyncMutex::new(HashMap::from([("u1".to_string(), s_write)])),
        );
        let caps: Arc<CapsMap> = Arc::new(CapsMap::default());
        let db2 = db.clone();
        let peers2 = peers.clone();
        let sender = tokio::spawn(async move {
            handle_bootstrap(&db2, &peers2, "u1", &HashSet::new(), &caps).await;
        });

        let mut seen: HashMap<String, usize> = HashMap::new();
        let mut pages = 0usize;
        let mut max_page_bytes = 0usize;
        let deadline = Instant::now() + Duration::from_secs(90);
        while seen.len() < 96 {
            assert!(Instant::now() < deadline, "bootstrap never converged (seen {})", seen.len());
            let frame = tokio::time::timeout(Duration::from_secs(30), recv_frame(&mut c_read)).await;
            match frame {
                Ok(Ok(bytes)) => {
                    if let Ok(NetPacket::Replication { collection, ops, .. }) =
                        bincode::deserialize::<NetPacket>(&bytes)
                    {
                        assert_eq!(collection, "big");
                        assert!(ops.len() <= BOOT_PAGE_MAX_DOCS, "page over doc budget: {}", ops.len());
                        // Framing overhead is a few bytes; budget holds.
                        max_page_bytes = max_page_bytes.max(bytes.len());
                        assert!(
                            bytes.len() <= BOOT_PAGE_MAX_BYTES + 64,
                            "page {} bytes over budget", bytes.len()
                        );
                        pages += 1;
                        for op in ops {
                            if let WalOp::PutInlined { key, value } = op {
                                seen.insert(key, value.len());
                            }
                        }
                    }
                }
                other => panic!("bootstrap stream ended early: {other:?}"),
            }
        }
        sender.abort();
        assert_eq!(seen.len(), 96, "every key must arrive");
        assert!(pages >= 3, ">16 MB node must split into multiple pages, got {pages}");
        assert!(max_page_bytes <= BOOT_PAGE_MAX_BYTES + 64);
        // Payloads survived the pager intact (256 KiB docs; the wire value
        // is the encoded HakoDoc, a few header bytes over the raw chunk).
        assert_eq!(seen["k000"], chunk.len() + 19);
        assert_eq!(seen["k095"], chunk.len() + 19);
    }

    /// #72 phase 1: the relay carriers survive the wire and keep the
    /// mixed-version mesh property — earlier variant indices unchanged,
    /// unknown trailing variants skipped by old peers (decode failure,
    /// never a disconnect).
    #[test]
    fn relay_carriers_roundtrip_and_old_peers_skip_them() {
        let lease = NetPacket::CloudLease {
            client_id: "relay-1".to_string(),
            last_exchange_ts: 1_760_000_000_000,
        };
        let take = NetPacket::RelayTake {
            ts: 1_760_000_000_000,
            client_id: "relay-1".to_string(),
        };
        for pkt in [lease, take] {
            let bytes = bincode::serialize(&pkt).unwrap();
            match bincode::deserialize::<NetPacket>(&bytes).expect("decode own relay packet") {
                NetPacket::CloudLease { client_id, last_exchange_ts } => {
                    assert_eq!(client_id, "relay-1");
                    assert_eq!(last_exchange_ts, 1_760_000_000_000);
                }
                NetPacket::RelayTake { ts, client_id } => {
                    assert_eq!(ts, 1_760_000_000_000);
                    assert_eq!(client_id, "relay-1");
                }
                other => panic!("wrong variant: {other:?}"),
            }
        }
        // Older variants keep their wire positions (Identify = 0).
        let id = NetPacket::Identify { id: "n".to_string(), room_hash: [1u8; 32] };
        assert_eq!(&bincode::serialize(&id).unwrap()[..4], &[0u8, 0, 0, 0]);
        // Old peers (which only know the pre-relay variants) fail decode
        // on ANY trailing-variant packet and skip it — simulate with an
        // out-of-range variant index, the exact mixed-version rule.
        let mut unknown = vec![99u8, 0, 0, 0];
        unknown.extend_from_slice(&[0u8; 8]);
        assert!(bincode::deserialize::<NetPacket>(&unknown).is_err());
    }
}
