#[cfg(feature = "cloud-sync")]
use std::collections::{HashMap, HashSet};
#[cfg(feature = "cloud-sync")]
use std::sync::atomic::{AtomicBool, Ordering};
#[cfg(feature = "cloud-sync")]
use std::sync::{Arc, Mutex as StdMutex, RwLock as StdRwLock};
#[cfg(feature = "cloud-sync")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "cloud-sync")]
use crate::document::firelite_doc::FireLiteDoc;
#[cfg(feature = "cloud-sync")]
use crate::document::value::Value;
#[cfg(feature = "cloud-sync")]
use crate::engine::engine::IndexOp;
#[cfg(feature = "cloud-sync")]
use crate::engine::{BatchMutation, ChangeEvent, ChangeKind, FireLite};
#[cfg(feature = "cloud-sync")]
use crate::error::{FireLiteError, Result as FLResult};
#[cfg(feature = "cloud-sync")]
use crate::query::query::Query;
#[cfg(feature = "cloud-sync")]
use crate::storage::engine::Pointer;
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
fn init_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Server-internal collection that stores the room registry. It is
/// `_`-prefixed so it stays hidden from `list_collections()`, the WAL
/// tailers and the version maps, exactly like `__firelite_security`.
#[cfg(feature = "cloud-sync")]
pub const INTERNAL_ROOMS_COLLECTION: &str = "__firelite_rooms";

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
    /// Client -> Server: Initial Auth & Handshake.
    /// `api_key` / `client_version` are additive (serde defaults): old
    /// peers omit them and still authenticate against open groups.
    /// `enc_fp` / `enc_cols` carry the same capability advertisement as the
    /// mesh `SyncCaps` packet (see `crate::sync_guard`): the sender's key
    /// fingerprint plus the collections it encrypts at rest. Absent on old
    /// peers, which are therefore treated as unverified for encrypted
    /// rooms (fail closed).
    Authenticate {
        token: String,
        client_id: String,
        room_name: String,
        room_key: String,
        #[serde(default)]
        api_key: Option<String>,
        #[serde(default)]
        client_version: Option<String>,
        #[serde(default)]
        enc_fp: Option<[u8; 32]>,
        #[serde(default)]
        enc_cols: Vec<String>,
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
    pub room_name: String,
    pub room_key: String,
    pub active_clients: usize,
    pub queued_writes: usize,
    /// Server mode only: number of distinct rooms currently hosted.
    pub hosted_rooms: usize,
}

// ============================================================================
// ROOM REGISTRY
// ============================================================================
//
// A "room" is uniquely identified by the pair (room_name, room_key). Clients
// that share a room name but use a different security key are considered to be
// in different groups/rooms. On the server, every room owns a storage prefix:
//   - first distinct (name, key) for a name  -> "roomname"
//   - each additional distinct key           -> "roomname_1", "roomname_2", ...
// Client collections are stored server-side as "<prefix>_<collection>" and are
// presented back to clients as just "<collection>", so data never mixes across
// rooms even when clients use the same collection names.
//
// The registry is persisted in the internal collection and mirrored into an
// in-memory cache. The disk is only consulted on cache misses (first connect
// after start / cache cold), and a periodic re-read keeps the cache realtime.

#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone)]
struct RoomEntry {
    room_id: String,
    prefix: String,
}

#[cfg(feature = "cloud-sync")]
#[derive(Default)]
struct RoomRegistryState {
    cache: HashMap<String, RoomEntry>,
    prefixes: HashSet<String>,
}

#[cfg(feature = "cloud-sync")]
pub struct RoomRegistry {
    db: Arc<FireLite>,
    state: StdRwLock<RoomRegistryState>,
    alloc_lock: StdMutex<()>,
    loaded: std::sync::atomic::AtomicBool,
}

#[cfg(feature = "cloud-sync")]
fn hash_room_id(room_name: &str, room_key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(room_name.as_bytes());
    hasher.update(b"\x00");
    hasher.update(room_key.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for b in digest {
        hex.push_str(&format!("{:02x}", b));
    }
    hex
}

/// Group policy store: a `__groups` doc per room name (admin-managed).
/// Absent row or `mode: "open"` admits everyone (historic behavior);
/// `mode: "registered"` admits only valid API keys (+ listed members when
/// the members list is non-empty). This collection never syncs.
#[cfg(feature = "cloud-sync")]
pub const GROUPS_COLLECTION: &str = "__groups";

/// SHA-256 hex of an API key. Keys are 256-bit random (unbounded entropy),
/// so a fast hash + timing-safe compare is the right tool — no KDF needed.
#[cfg(feature = "cloud-sync")]
pub fn hash_api_key(key: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(key.as_bytes());
    let digest = hasher.finalize();
    let mut hex = String::with_capacity(64);
    for b in digest {
        hex.push_str(&format!("{:02x}", b));
    }
    hex
}

#[cfg(feature = "cloud-sync")]
fn timing_safe_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b.iter()).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

/// Admission decision for one handshake. Pure function of (db row, presented
/// key, client id) — unit-tested without sockets.
#[cfg(feature = "cloud-sync")]
pub(crate) fn check_group_access(
    db: &FireLite,
    room_name: &str,
    api_key: Option<&str>,
    client_id: &str,
) -> Result<(), String> {
    let doc = match db.get(GROUPS_COLLECTION, room_name) {
        Ok(Some(d)) => d,
        _ => return Ok(()), // no policy row: open group, historic behavior
    };
    let mode = doc.get("mode").and_then(value_to_string);
    match mode.as_deref() {
        None | Some("open") => Ok(()),
        Some("registered") => {
            let presented = api_key.unwrap_or("");
            let expected = doc.get("api_key_hash").and_then(value_to_string).unwrap_or_default();
            if expected.is_empty() || !timing_safe_eq(&hash_api_key(presented), &expected) {
                return Err("group requires a valid API key".to_string());
            }
            let members: Vec<String> = match doc.get("members") {
                Some(Value::Array(items)) => items
                    .iter()
                    .filter_map(|v| match v {
                        Value::String(s) => Some(s.clone()),
                        _ => None,
                    })
                    .collect(),
                _ => Vec::new(),
            };
            if !members.is_empty() && !members.iter().any(|m| m == client_id) {
                return Err("client not registered for group".to_string());
            }
            Ok(())
        }
        Some(other) => Err(format!("unknown group mode '{other}'")),
    }
}

#[cfg(feature = "cloud-sync")]
fn sanitize_room_name(name: &str) -> String {    let mut out = String::new();
    for c in name.chars() {
        if c.is_ascii_alphanumeric() || c == '_' || c == '-' {
            out.push(c);
        } else if !out.is_empty() && !out.ends_with('_') {
            out.push('_');
        }
    }
    while out.ends_with('_') {
        out.pop();
    }
    if out.is_empty() {
        "room".to_string()
    } else {
        out.chars().take(40).collect()
    }
}
#[cfg(feature = "cloud-sync")]
fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Client-facing collections are stored server-side as "<prefix>_<collection>".
/// When the prefix is empty (client-local ingest), the collection keeps its
/// plain name instead of gaining a leading "_".
#[cfg(feature = "cloud-sync")]
fn storage_col_name(prefix: &str, plain_col: &str) -> String {
    if prefix.is_empty() {
        plain_col.to_string()
    } else {
        format!("{}_{}", prefix, plain_col)
    }
}

/// Uniform key used by the outbound tailers and the ingest flusher to suppress
/// echoes (writes that originated locally and were only re-applied on arrival).
#[cfg(feature = "cloud-sync")]
fn echo_key(prefix: &str, plain_col: &str, key: &str) -> String {
    format!("{}:{}:{}", prefix, plain_col, key)
}

#[cfg(feature = "cloud-sync")]
impl RoomRegistry {
    pub fn new(db: Arc<FireLite>) -> Self {
        Self {
            db,
            state: StdRwLock::new(RoomRegistryState::default()),
            alloc_lock: StdMutex::new(()),
            loaded: std::sync::atomic::AtomicBool::new(false),
        }
    }

    fn load_all(&self) {
        let mut state = self.state.write().unwrap();
        state.cache.clear();
        state.prefixes.clear();
        if let Ok(hits) = self.db.query(Query::new(INTERNAL_ROOMS_COLLECTION)) {
            for (id, doc) in hits {
                let prefix = doc
                    .get("prefix")
                    .and_then(value_to_string)
                    .unwrap_or_default();
                if prefix.is_empty() {
                    continue;
                }
                state.cache.insert(
                    id.clone(),
                    RoomEntry {
                        room_id: id.clone(),
                        prefix: prefix.clone(),
                    },
                );
                state.prefixes.insert(prefix);
            }
        }
        self.loaded.store(true, Ordering::SeqCst);
    }

    /// Resolves a room to its storage prefix, allocating + persisting a new
    /// prefix when the room is seen for the first time. This is the ONLY path
    /// that touches the internal collection on the hot path, and only on a
    /// cache miss.
    pub fn resolve(&self, room_name: &str, room_key: &str) -> FLResult<(String, String)> {
        let room_id = hash_room_id(room_name, room_key);
        let _guard = self.alloc_lock.lock().unwrap();

        if !self.loaded.load(Ordering::SeqCst) {
            self.load_all();
        }

        // 1. In-memory cache hit.
        if let Some(entry) = self.state.read().unwrap().cache.get(&room_id) {
            return Ok((entry.room_id.clone(), entry.prefix.clone()));
        }

        // 2. Durable registry hit (single-doc read).
        if let Ok(Some(doc)) = self.db.get(INTERNAL_ROOMS_COLLECTION, &room_id) {
            if let Some(prefix) = doc.get("prefix").and_then(value_to_string) {
                let mut state = self.state.write().unwrap();
                state.cache.insert(
                    room_id.clone(),
                    RoomEntry {
                        room_id: room_id.clone(),
                        prefix: prefix.clone(),
                    },
                );
                state.prefixes.insert(prefix.clone());
                return Ok((room_id, prefix));
            }
        }

        // 3. Allocate a globally unique prefix.
        let base = sanitize_room_name(room_name);
        let prefix = {
            let prefixes = self.state.read().unwrap();
            if !prefixes.prefixes.contains(&base) {
                base.clone()
            } else {
                let mut n = 1;
                loop {
                    let candidate = format!("{}_{}", base, n);
                    if !prefixes.prefixes.contains(&candidate) {
                        break candidate;
                    }
                    n += 1;
                }
            }
        };

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let mut doc = FireLiteDoc::default();
        doc.insert("room_name", Value::String(room_name.to_string()));
        doc.insert("key_hash", Value::String(hash_room_id(room_name, room_key)));
        doc.insert("prefix", Value::String(prefix.clone()));
        doc.insert("created_at", Value::Int(now));
        doc.insert("last_seen", Value::Int(now));
        self.db.write_batch(vec![BatchMutation::Put {
            collection: INTERNAL_ROOMS_COLLECTION.to_string(),
            doc_id: room_id.clone(),
            doc,
        }])?;

        let mut state = self.state.write().unwrap();
        state.cache.insert(
            room_id.clone(),
            RoomEntry {
                room_id: room_id.clone(),
                prefix: prefix.clone(),
            },
        );
        state.prefixes.insert(prefix.clone());

        Ok((room_id, prefix))
    }

    /// All currently-known storage prefixes.
    pub fn prefixes(&self) -> Vec<String> {
        self.state.read().unwrap().prefixes.iter().cloned().collect()
    }

    /// Longest-prefix match: maps a server-side storage collection name back to
    /// `(room_prefix, client_collection_name)`. Returns `None` when the
    /// collection does not belong to any registered room.
    pub fn prefix_of(&self, storage_col: &str) -> Option<(String, String)> {
        let prefixes = self.state.read().unwrap();
        let mut best: Option<(&String, &str)> = None;
        for p in prefixes.prefixes.iter() {
            let marker = format!("{}_", p);
            if let Some(rest) = storage_col.strip_prefix(&marker) {
                if rest.is_empty() {
                    continue;
                }
                if best.map_or(true, |(bp, _)| p.len() > bp.len()) {
                    best = Some((p, rest));
                }
            }
        }
        best.map(|(p, c)| (p.clone(), c.to_string()))
    }

    /// Re-reads the durable registry so the cache stays realtime.
    pub fn refresh(&self) {
        self.load_all();
    }
}

// ============================================================================
// CLOUD SYNC CONTROLLER & AGGREGATOR
// ============================================================================

#[cfg(feature = "cloud-sync")]
pub struct CloudSync {
    db: Arc<FireLite>,
    mode: CloudSyncMode,
    room_name: String,
    room_key: String,
    client_id: String,
    auth_token: String,
    /// Optional group API key presented at handshake (see `set_api_key`).
    /// Interior mutability so FFI/SDK handles (`Arc<CloudSync>`) can set it
    /// after construction; read once per (re)connect.
    api_key: Arc<StdMutex<Option<String>>>,
    running: Arc<AtomicBool>,
    echo_cache: Arc<StdMutex<HashMap<String, i64>>>,
    seen_messages: Arc<AsyncMutex<Vec<u128>>>,
    // Ingress Buffer for High-Throughput Batch Coalescing
    ingest_tx: mpsc::Sender<IngestItem>,
    ingest_rx: Arc<AsyncMutex<Option<mpsc::Receiver<IngestItem>>>>,
    active_peers: Arc<AsyncRwLock<HashMap<String, PeerInfo>>>,
    // Outbound client queue (Client mode -> Server)
    outbound_tx: Arc<AsyncMutex<Option<mpsc::Sender<CloudPacket>>>>,
    // Server mode: room registry so `status()` can report hosted rooms.
    room_registry: Option<Arc<RoomRegistry>>,
    /// Peer capability announcements for the encryption fail-closed rules.
    /// Server: keyed by peer_key, set at auth, cleared on disconnect.
    /// Client: the server's caps live under the fixed key `"server"`.
    caps: Arc<crate::sync_guard::CapsMap>,
}

#[cfg(feature = "cloud-sync")]
struct IngestItem {
    collection: String,
    prefix: String,
    op: WalOp,
    sender_client_id: Option<String>,
}

#[cfg(feature = "cloud-sync")]
struct PeerInfo {
    tx: mpsc::Sender<Message>,
    prefix: String,
}

/// Admin view of one connected peer. `peer_key` is `{room_id}:{client_id}`;
/// split on the first ':' to recover both parts.
#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct PeerView {
    pub peer_key: String,
    pub prefix: String,
}

#[cfg(feature = "cloud-sync")]
impl CloudSync {
    /// Creates a Cloud Sync controller. In **server** mode the room parameters
    /// are ignored: the server is room-agnostic and hosts any room its clients
    /// ask for. Prefer the dedicated [`CloudSync::server`] / [`CloudSync::client`]
    /// constructors for clarity.
    pub fn new(
        db: Arc<FireLite>,
        mode: CloudSyncMode,
        client_id: &str,
        room_name: &str,
        room_key: &str,
        auth_token: &str,
    ) -> Self {
        match mode {
            CloudSyncMode::Server => Self::server(db, client_id, auth_token),
            CloudSyncMode::Client => Self::client(db, client_id, room_name, room_key, auth_token),
        }
    }

    /// Creates a room-agnostic cloud server ("big cloud server storage"). It is
    /// not bound to any room: clients choose the room (and the server) and the
    /// server accepts and persists any (room_name, room_key) pair, storing each
    /// room's collections under its own storage prefix and relaying sync only to
    /// the members of that room.
    pub fn server(db: Arc<FireLite>, server_id: &str, auth_token: &str) -> Self {
        let (tx, rx) = mpsc::channel(100_000);
        let registry = Arc::new(RoomRegistry::new(db.clone()));

        Self {
            db,
            mode: CloudSyncMode::Server,
            room_name: String::new(),
            room_key: String::new(),
            client_id: server_id.to_string(),
            auth_token: auth_token.to_string(),
            api_key: Arc::new(StdMutex::new(None)),
            running: Arc::new(AtomicBool::new(false)),
            echo_cache: Arc::new(StdMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(Vec::with_capacity(1000))),
            ingest_tx: tx,
            ingest_rx: Arc::new(AsyncMutex::new(Some(rx))),
            active_peers: Arc::new(AsyncRwLock::new(HashMap::new())),
            outbound_tx: Arc::new(AsyncMutex::new(None)),
            room_registry: Some(registry),
            caps: Arc::new(crate::sync_guard::CapsMap::default()),
        }
    }

    /// Creates an offline-first cloud client bound to a single room. The client
    /// decides which room to join (room_name + room_key) and which server to
    /// sync with via [`CloudSync::start`]; every other client/peer using the
    /// same (room_name, room_key) on the same server forms the sync group.
    pub fn client(
        db: Arc<FireLite>,
        client_id: &str,
        room_name: &str,
        room_key: &str,
        auth_token: &str,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100_000);

        Self {
            db,
            mode: CloudSyncMode::Client,
            room_name: room_name.to_string(),
            room_key: room_key.to_string(),
            client_id: client_id.to_string(),
            auth_token: auth_token.to_string(),
            api_key: Arc::new(StdMutex::new(None)),
            running: Arc::new(AtomicBool::new(false)),
            echo_cache: Arc::new(StdMutex::new(HashMap::new())),
            seen_messages: Arc::new(AsyncMutex::new(Vec::with_capacity(1000))),
            ingest_tx: tx,
            ingest_rx: Arc::new(AsyncMutex::new(Some(rx))),
            active_peers: Arc::new(AsyncRwLock::new(HashMap::new())),
            outbound_tx: Arc::new(AsyncMutex::new(None)),
            room_registry: None,
            caps: Arc::new(crate::sync_guard::CapsMap::default()),
        }
    }

    /// Set the group API key this client presents at handshake. Takes effect
    /// at the next (re)connect. `None` clears it (anonymous: admitted only
    /// to open groups).
    pub fn set_api_key(&self, key: Option<String>) {
        if let Ok(mut slot) = self.api_key.lock() {
            *slot = key.filter(|k| !k.is_empty());
        }
    }

    /// Spawns the Cloud Sync system.
    pub async fn start(&self, bind_or_server_url: &str) -> FLResult<()> {
        if self.running.swap(true, Ordering::SeqCst) {
            return Ok(());
        }

        // Initialize Rustls CryptoProvider (Prevents Rustls 0.23 process-level panic)
        init_crypto_provider();

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

    pub fn status(&self) -> CloudStatus {
        let active_clients = if let Ok(guard) = self.active_peers.try_read() {
            guard.len()
        } else {
            0
        };

        let (room_name, room_key, hosted_rooms) = match &self.room_registry {
            Some(reg) => {
                let hosted = reg.prefixes().len();
                ("(multi-room)".to_string(), String::new(), hosted)
            }
            None => (
                self.room_name.clone(),
                self.room_key.clone(),
                0,
            ),
        };

        CloudStatus {
            mode: self.mode,
            connected: self.running.load(Ordering::Relaxed),
            room_name,
            room_key,
            active_clients,
            queued_writes: 0,
            hosted_rooms,
        }
    }

    /// Snapshot of currently connected peers (server: all room members;
    /// client: normally just the uplink, if tracked). Best-effort read.
    pub fn peer_list(&self) -> Vec<PeerView> {
        match self.active_peers.try_read() {
            Ok(guard) => guard
                .iter()
                .map(|(k, info)| PeerView {
                    peer_key: k.clone(),
                    prefix: info.prefix.clone(),
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Per-collection versions for one storage prefix (room catch-up clock).
    /// Backs the admin dashboard's room view; pure function of the DB.
    pub fn room_versions(&self, prefix: &str) -> HashMap<String, i64> {
        Self::server_room_version_map(&self.db, prefix)
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
        let caps_flush = self.caps.clone();

        tokio::spawn(async move {
            let mut rx = match rx_option.lock().await.take() {
                Some(r) => r,
                None => return,
            };

            let mut batch_buffer: HashMap<String, Vec<IngestItem>> = HashMap::new();
            let mut interval = tokio::time::interval(Duration::from_millis(5));

            while running.load(Ordering::Relaxed) {
                tokio::select! {
                    _ = interval.tick() => {
                        Self::flush_ingest_buffer(&db, &mut batch_buffer, &echo_cache, &active_peers, &caps_flush).await;
                    }
                    item = rx.recv() => {
                        match item {
                            Some(item) => {
                                let storage_col = storage_col_name(&item.prefix, &item.collection);
                                batch_buffer.entry(storage_col)
                                    .or_default()
                                    .push(item);

                                let total_pending: usize = batch_buffer.values().map(|v| v.len()).sum();
                                if total_pending >= 512 {
                                    Self::flush_ingest_buffer(&db, &mut batch_buffer, &echo_cache, &active_peers, &caps_flush).await;
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
        buffer: &mut HashMap<String, Vec<IngestItem>>,
        echo_cache: &Arc<StdMutex<HashMap<String, i64>>>,
        peers: &Arc<AsyncRwLock<HashMap<String, PeerInfo>>>,
        caps: &Arc<crate::sync_guard::CapsMap>,
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

        for (storage_col, items) in buffer.drain() {
            if items.is_empty() {
                continue;
            }

            // Every item in the same batch key shares (prefix, plain collection).
            let prefix = items[0].prefix.clone();
            let plain_col = items[0].collection.clone();

            // Local-only signal (inbound): a locally-scoped collection
            // refuses everything the room offers. Per-key marks do NOT
            // filter inbound — a genuinely newer remote put resurrects.
            if db.is_collection_local(&plain_col) {
                continue;
            }

            // Encryption fail-closed (inbound): when THIS node encrypts the
            // collection at rest, ops are accepted only from senders that
            // proved the same key. Namespaces: server shards are storage
            // names (`prefix_col`), clients use plain names — a match on
            // either counts (documented; fail-closed direction).
            // A keyless node never matches: its own encrypted set is empty.
            let enc_local =
                db.is_collection_encrypted(&storage_col) || db.is_collection_encrypted(&plain_col);
            let local_fp =
                crate::sync_guard::local_fingerprint(db.config.encryption_key.as_deref());

            let mut apply_ops: Vec<WalOp> = Vec::with_capacity(items.len());
            let mut index_puts: Vec<(String, Arc<FireLiteDoc>)> = Vec::new();
            let mut sender_relays: HashMap<Option<String>, Vec<WalOp>> = HashMap::new();

            for item in &items {
                // Receiver rule (server side only): drop ops for
                // locally-encrypted collections unless the sender proved the
                // same key. Client-side receipts carry no sender
                // (`sender_client_id: None` — the hub is trusted transport;
                // the server already filtered what it forwards) and are
                // always accepted here.
                if enc_local {
                    if let Some(sender_key) = item.sender_client_id.as_deref() {
                        let sender = caps.get(sender_key);
                        if !crate::sync_guard::caps_allow(true, local_fp, sender.as_ref()) {
                            caps.warn(
                                sender_key,
                                &plain_col,
                                &format!(
                                    "dropping inbound ops for locally-encrypted '{plain_col}' from unverified sender '{sender_key}' (local key {}, peer needs the same encryption key; upgrade old peers)",
                                    crate::sync_guard::fp_short(&local_fp),
                                ),
                            );
                            continue;
                        }
                    }
                }
                match &item.op {
                    WalOp::PutInlined { key, value } => {
                        let ts = FireLiteDoc::decode(value)
                            .map(|d| d.get_logical_time())
                            .unwrap_or(0);
                        if ts == 0 {
                            continue;
                        }

                        // LWW Check
                        if let Ok(Some(existing_doc)) = db.get(&storage_col, key) {
                            if existing_doc.get_logical_time() >= ts {
                                continue;
                            }
                        }

                        {
                            let mut cache = echo_cache.lock().unwrap();
                            cache.insert(echo_key(&prefix, &plain_col, key), ts);
                        }
                        apply_ops.push(WalOp::PutInlined {
                            key: key.clone(),
                            value: value.clone(),
                        });
                        if let Some(doc) = FireLiteDoc::decode(value) {
                            index_puts.push((key.clone(), Arc::new(doc)));
                        }
                        sender_relays
                            .entry(item.sender_client_id.clone())
                            .or_default()
                            .push(item.op.clone());
                    }
                    WalOp::Delete { key, timestamp } => {
                        // LWW check (mirrors the put arm): a stale tombstone —
                        // e.g. replayed by handshake catch-up — must not beat
                        // a newer local put.
                        if Self::is_stale_remote_delete(&db, &storage_col, key, *timestamp) {
                            continue;
                        }
                        {
                            let mut cache = echo_cache.lock().unwrap();
                            cache.insert(echo_key(&prefix, &plain_col, key), *timestamp);
                        }
                        apply_ops.push(WalOp::Delete {
                            key: key.clone(),
                            timestamp: *timestamp,
                        });
                        sender_relays
                            .entry(item.sender_client_id.clone())
                            .or_default()
                            .push(item.op.clone());
                    }
                    _ => {}
                }
            }

            if !apply_ops.is_empty() {
                let db_clone = db.clone();
                let col = storage_col.clone();
                let keys: Vec<Arc<str>> = apply_ops
                    .iter()
                    .map(|op| Arc::from(op.get_key().to_string()))
                    .collect();
                let _ = tokio::task::spawn_blocking(move || {
                    Self::apply_timestamped(&db_clone, &col, apply_ops, index_puts, keys);
                })
                .await;
            }

            // Relay packet to connected members of the SAME room (except origin).
            // The collection name is translated back to the client-facing name.
            // Encryption fail-closed: a batch is withheld from a recipient
            // when EITHER the origin marked this collection encrypted and the
            // recipient's fingerprint mismatches, OR this server encrypts it
            // locally and the recipient mismatches. A keyless hub (no local
            // key) relays as before — receivers enforce locally. Either way
            // the plaintext never leaves silently toward an unverified peer.
            for (origin_sender, ops) in sender_relays {
                if ops.is_empty() {
                    continue;
                }

                let packet = CloudPacket::Replication {
                    msg_id: SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .unwrap()
                        .as_micros(),
                    collection: plain_col.clone(),
                    ops,
                };

                if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                    let msg = Message::Binary(bytes.into());
                    let peers_guard = peers.read().await;

                    for (peer_id, info) in peers_guard.iter() {
                        if info.prefix != prefix || Some(peer_id) == origin_sender.as_ref() {
                            continue;
                        }
                        let recipient = caps.get(peer_id);
                        let origin = origin_sender
                            .as_deref()
                            .and_then(|o| caps.get(o));
                        let origin_encrypted = origin
                            .as_ref()
                            .map(|o| o.encrypted_cols.iter().any(|c| c == &plain_col))
                            .unwrap_or(false);
                        let origin_fp = origin.map(|o| o.key_fp).unwrap_or([0u8; 32]);
                        let origin_ok = !origin_encrypted
                            || crate::sync_guard::caps_allow(
                                true,
                                origin_fp,
                                recipient.as_ref(),
                            );
                        let local_ok = !enc_local
                            || crate::sync_guard::caps_allow(true, local_fp, recipient.as_ref());
                        if !(origin_ok && local_ok) {
                            caps.warn(
                                peer_id,
                                &plain_col,
                                &format!(
                                    "withholding relay of '{plain_col}' from peer '{peer_id}' (unverified key for an encrypted room)"
                                ),
                            );
                            continue;
                        }
                        let _ = info.tx.try_send(msg.clone());
                    }
                }
            }
        }
    }

    /// Applies replicated ops to a local shard while PRESERVING each document's
    /// original logical timestamp. This is the counterpart to net_sync's remote
    /// apply: `write_batch` would re-stamp `_time` to the wall clock, which both
    /// defeats LWW conflict resolution and breaks the echo cache used by the
    /// outbound tailers (causing replication amplification loops).
    fn apply_timestamped(
        db: &Arc<FireLite>,
        collection: &str,
        ops: Vec<WalOp>,
        index_puts: Vec<(String, Arc<FireLiteDoc>)>,
        keys: Vec<Arc<str>>,
    ) {
        let shard_arc = match db.get_shard(collection) {
            Ok(s) => s,
            Err(_) => return,
        };

        {
            let mut shard = shard_arc.write().unwrap();
            let tx_id = shard.next_tx_id;
            shard.next_tx_id += 1;
            if shard.wal.append_batch_fast(tx_id, &ops, true).is_err() {
                return;
            }
            for op in &ops {
                match op {
                    WalOp::PutInlined { key, value } => {
                        shard.update_index_entry(key.clone(), Some(Pointer::Inlined(Arc::new(value.clone()))));
                    }
                    WalOp::Delete { key, timestamp } => {
                        shard.update_index_entry(key.clone(), Some(Pointer::Deleted { timestamp: *timestamp }));
                    }
                    _ => {}
                }
            }
        }

        db.bump_versions_by_keys(keys);

        if !index_puts.is_empty() {
            let _ = db.index_tx.send(IndexOp::Update {
                collection: collection.to_string(),
                puts: Arc::new(index_puts),
                deletes: vec![],
            });
        }

        for op in &ops {
            let kind = match op {
                WalOp::PutInlined { .. } => ChangeKind::Put,
                WalOp::Delete { .. } => ChangeKind::Delete,
                _ => continue,
            };
            db.notify_watchers(
                collection,
ChangeEvent {
path: Arc::from(op.get_key()),
kind,
},
            );
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
        let active_peers = self.active_peers.clone();
        let running = self.running.clone();
        let seen_messages = self.seen_messages.clone();
        let echo_cache = self.echo_cache.clone();
        let caps_all = self.caps.clone();
        // The room registry is created once (server constructor) and reused so
        // `status()` can report the number of hosted rooms.
        let rooms = self
            .room_registry
            .clone()
            .expect("server mode always has a room registry");

        // 0. Keep the in-memory room registry realtime by re-reading the
        //    durable internal collection periodically.
        {
            let rooms_refresh = rooms.clone();
            let running_refresh = running.clone();
            tokio::spawn(async move {
                let mut ticker = tokio::time::interval(Duration::from_secs(20));
                while running_refresh.load(Ordering::Relaxed) {
                    ticker.tick().await;
                    let rooms = rooms_refresh.clone();
                    tokio::task::spawn_blocking(move || rooms.refresh())
                        .await
                        .ok();
                }
            });
        }

        // 1. Spawn Server Local WAL Tailer (Broadcasts server-side writes to all
        //    connected clients of the owning room)
        self.spawn_server_outbound_tailer(echo_cache, rooms.clone());

        // 2. Accept Incoming WebSocket Clients
        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok((stream, _)) = listener.accept().await {
                    let ingest_tx = ingest_tx.clone();
                    let db = db.clone();
                    let active_peers = active_peers.clone();
                    let seen_messages = seen_messages.clone();
                    let rooms = rooms.clone();
                    let caps_one = caps_all.clone();

                    tokio::spawn(async move {
                        if let Ok(ws_stream) = tokio_tungstenite::accept_async(stream).await {
                            Self::handle_server_client(
                                ws_stream,
                                ingest_tx,
                                db,
                                rooms,
                                active_peers,
                                seen_messages,
                                caps_one,
                            )
                            .await;
                        }
                    });
                }
            }
        });

        Ok(())
    }

    /// Server-side version map restricted to one room, keyed by the
    /// client-facing (unprefixed) collection names.
    fn server_room_version_map(db: &Arc<FireLite>, prefix: &str) -> HashMap<String, i64> {
        let mut map = HashMap::new();
        if let Ok(cols) = db.list_collections() {
            let marker = format!("{}_", prefix);
            for col in cols {
                if let Some(plain) = col.strip_prefix(&marker) {
                    if !plain.is_empty() {
                        if let Ok(version) = db.get_collection_version(&col) {
                            map.insert(plain.to_string(), version);
                        }
                    }
                }
            }
        }
        map
    }

    async fn handle_server_client<S>(
        ws_stream: tokio_tungstenite::WebSocketStream<S>,
        ingest_tx: mpsc::Sender<IngestItem>,
        db: Arc<FireLite>,
        rooms: Arc<RoomRegistry>,
        peers: Arc<AsyncRwLock<HashMap<String, PeerInfo>>>,
        seen_messages: Arc<AsyncMutex<Vec<u128>>>,
        caps: Arc<crate::sync_guard::CapsMap>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_tx, mut ws_rx) = ws_stream.split();
        let (client_tx, mut client_rx) = mpsc::channel::<Message>(2000);

        let mut peer_key = String::new();
        let mut room_prefix = String::new();
        let mut authenticated = false;

        if let Some(Ok(Message::Binary(bytes))) = ws_rx.next().await {
            if let Ok(CloudPacket::Authenticate {
                token: _,
                client_id: cid,
                room_name,
                room_key,
                api_key,
                enc_fp,
                enc_cols,
                ..
            }) = rmp_serde::from_slice(&bytes)
            {
                // Group policy first: rejected peers must not create rooms.
                if let Err(e) = check_group_access(&db, &room_name, api_key.as_deref(), &cid) {
                    let nack = CloudPacket::AuthResult {
                        success: false,
                        error: Some(e),
                    };
                    if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                        let _ = ws_tx.send(Message::Binary(bytes.into())).await;
                    }
                    return;
                }
                // Resolve the room -> storage prefix (register it if needed).
                let resolve = {
                    let rooms = rooms.clone();
                    let rn = room_name.clone();
                    let rk = room_key.clone();
                    tokio::task::spawn_blocking(move || rooms.resolve(&rn, &rk)).await
                };

                match resolve {
                    Ok(Ok((room_id, prefix))) => {
                        authenticated = true;
                        room_prefix = prefix.clone();
                        // Peers are keyed by room+client so the same client_id
                        // used in different rooms can never clobber each other
                        // on a multi-room server.
                        peer_key = format!("{}:{}", room_id, cid);

                        peers.write().await.insert(
                            peer_key.clone(),
                            PeerInfo {
                                tx: client_tx,
                                prefix,
                            },
                        );
                        // Record encryption capabilities for the fail-closed
                        // rules (relay + apply consult this, not the wire).
                        // Absent fields (old peers) store as unverified.
                        caps.set(
                            &peer_key,
                            crate::sync_guard::PeerCaps {
                                key_fp: enc_fp.unwrap_or([0u8; 32]),
                                encrypted_cols: enc_cols.clone(),
                            },
                        );

                        let ack = CloudPacket::AuthResult {
                            success: true,
                            error: None,
                        };
                        let ack_bytes = rmp_serde::to_vec_named(&ack).unwrap();
                        let _ = ws_tx.send(Message::Binary(ack_bytes.into())).await;
                    }
                    Ok(Err(e)) => {
                        let nack = CloudPacket::AuthResult {
                            success: false,
                            error: Some(format!("Room registration failed: {}", e)),
                        };
                        if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                            let _ = ws_tx.send(Message::Binary(bytes.into())).await;
                        }
                        return;
                    }
                    Err(e) => {
                        let nack = CloudPacket::AuthResult {
                            success: false,
                            error: Some(format!("Room registration failed: {}", e)),
                        };
                        if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                            let _ = ws_tx.send(Message::Binary(bytes.into())).await;
                        }
                        return;
                    }
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

                                // Translate client collection -> room-scoped storage.
                                let prefix = room_prefix.clone();
                                for op in ops {
                                    let _ = ingest_tx
                                        .send(IngestItem {
                                            collection: collection.clone(),
                                            prefix: prefix.clone(),
                                            op,
                                            sender_client_id: Some(peer_key.clone()),
                                        })
                                        .await;
                                }
                            }
                            CloudPacket::VersionPing { versions: client_versions, .. } => {
                                let prefix = room_prefix.clone();

                                // 1. SYMMETRICAL REPLY: Send the Server's room-scoped
                                //    VersionMap (plain collection names) back to Client
                                let server_versions = Self::server_room_version_map(&db, &prefix);
                                let reply = CloudPacket::VersionPing {
                                    versions: server_versions.clone(),
                                };
                                if let Ok(reply_bytes) = rmp_serde::to_vec_named(&reply) {
                                    let peers_guard = peers.read().await;
                                    if let Some(tx) = peers_guard.get(&peer_key) {
                                        let _ = tx.tx.try_send(Message::Binary(reply_bytes.into()));
                                    }
                                }

                                // 2. Pull catch-up: send deltas for every room collection
                                //    where the Server is ahead of the Client (including
                                //    collections the client has never seen).
                                for (plain_col, server_ts) in server_versions {
                                    let client_ts = client_versions
                                        .get(&plain_col)
                                        .copied()
                                        .unwrap_or(0);
                                    if server_ts > client_ts {
                                        let storage_col = format!("{}_{}", prefix, plain_col);
                                        Self::send_catchup_deltas(
                                            &db,
                                            &storage_col,
                                            &plain_col,
                                            client_ts,
                                            &peer_key,
                                            &peers,
                                            &caps,
                                        )
                                        .await;
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
        peers.write().await.remove(&peer_key);
        caps.remove(&peer_key);
    }

    /// LWW guard for inbound deletes: a tombstone older than (or equal
    /// to) the local doc loses. `db.get` treats local tombstones as
    /// non-existent, so re-applying the same delete is a harmless no-op.
    #[cfg(feature = "cloud-sync")]
    fn is_stale_remote_delete(
        db: &Arc<FireLite>,
        storage_col: &str,
        key: &str,
        timestamp: i64,
    ) -> bool {
        match db.get(storage_col, key) {
            Ok(Some(existing)) => existing.get_logical_time() >= timestamp,
            _ => false,
        }
    }

    /// Shared handshake catch-up builder (server→client and client→server).
    /// Every index entry newer than `since_ts` — puts AND tombstones.
    /// Tombstones replay as `WalOp::Delete`, which is safe because deletes
    /// carry timestamps and both ingest paths enforce LWW (a stale
    /// tombstone loses to a newer local put). Previously catch-up was
    /// put-only, so a peer offline during a delete never learned of it.
    /// `filter_col` is the app-level collection name for the local-only
    /// check — the handshake must not leak what the tailers withhold.
    #[cfg(feature = "cloud-sync")]
    fn collect_catchup_ops(
        db: &Arc<FireLite>,
        storage_col: &str,
        filter_col: &str,
        since_ts: i64,
    ) -> Vec<WalOp> {
        let Ok(shard_arc) = db.get_shard(storage_col) else {
            return Vec::new();
        };
        let changed: Vec<(String, crate::storage::engine::Pointer)> = {
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
                        // ponytail: pending docs carry their time on the doc;
                        // the old scan dropped them (ts=0) — same staleness
                        // class as the tombstone bug.
                        crate::storage::engine::Pointer::BlobPending(doc) => {
                            doc.get_logical_time()
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
            for (key, ptr) in changed {
                if db.is_local_only(filter_col, &key) {
                    continue;
                }
                if let crate::storage::engine::Pointer::Deleted { timestamp } = ptr {
                    ops.push(WalOp::Delete { key, timestamp });
                    continue;
                }
                if let crate::storage::engine::Pointer::BlobPending(doc) = ptr {
                    ops.push(WalOp::PutInlined { key, value: doc.encode_buffered() });
                    continue;
                }
                if let Ok(Some(bytes)) = guard.read_pointer_internal(&ptr, false) {
                    ops.push(WalOp::PutInlined { key, value: bytes });
                }
            }
        }
        ops
    }

    async fn send_catchup_deltas(
        db: &Arc<FireLite>,
        storage_col: &str,
        plain_col: &str,
        since_ts: i64,
        peer_key: &str,
        peers: &Arc<AsyncRwLock<HashMap<String, PeerInfo>>>,
        caps: &Arc<crate::sync_guard::CapsMap>,
    ) {
        // Sender rule: a locally-encrypted room (storage namespace on the
        // server) only replays to a fingerprint-verified peer. Old/silent
        // peers pause here loudly instead of leaking on catch-up.
        if db.is_collection_encrypted(storage_col) {
            let local_fp =
                crate::sync_guard::local_fingerprint(db.config.encryption_key.as_deref());
            let peer = caps.get(peer_key);
            if !crate::sync_guard::caps_allow(true, local_fp, peer.as_ref()) {
                caps.warn(
                    peer_key,
                    plain_col,
                    &format!(
                        "withholding catch-up replay of encrypted '{plain_col}' from peer '{peer_key}' (unverified key)"
                    ),
                );
                return;
            }
        }
        let ops = Self::collect_catchup_ops(db, storage_col, plain_col, since_ts);

        if !ops.is_empty() {
            let packet = CloudPacket::Replication {
                msg_id: 0,
                collection: plain_col.to_string(),
                ops,
            };
            if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                let peers_guard = peers.read().await;
                if let Some(tx) = peers_guard.get(peer_key) {
                    let _ = tx.tx.try_send(Message::Binary(bytes.into()));
                }
            }
        }
    }

    /// Tails local WAL changes on the SERVER and broadcasts server-side writes to
    /// the connected clients of the room that owns each collection.
    fn spawn_server_outbound_tailer(
        &self,
        echo_cache: Arc<StdMutex<HashMap<String, i64>>>,
        rooms: Arc<RoomRegistry>,
    ) {
        let db = self.db.clone();
        let active_peers = self.active_peers.clone();
        let running = self.running.clone();
        let caps_srv = self.caps.clone();

        tokio::task::spawn_blocking(move || {
            let mut offsets: HashMap<String, u64> = HashMap::new();

            while running.load(Ordering::Relaxed) {
                let cols = db.list_collections().unwrap_or_default();

                for col in cols {
                    // Skip collections that don't belong to any registered room.
                    let Some((prefix, plain_col)) = rooms.prefix_of(&col) else {
                        continue;
                    };

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
                                    if !matches!(op, WalOp::PutInlined { .. } | WalOp::Delete { .. }) {
                                        continue;
                                    }
                                    let key = op.get_key();
                                    let wal_ts = match &op {
                                        WalOp::PutInlined { value, .. } => {
                                            i64::from_le_bytes(value[2..10].try_into().unwrap_or([0; 8]))
                                        }
                                        WalOp::Delete { timestamp, .. } => *timestamp,
                                        _ => 0,
                                    };

                                    // Skip writes originating from client WebSockets (already handled)
                                    let ek = echo_key(&prefix, &plain_col, &key);
                                    let is_echo = {
                                        let mut cache = echo_cache.lock().unwrap();
                                        if let Some(&cached_ts) = cache.get(&ek) {
                                            if cached_ts == wal_ts {
                                                cache.remove(&ek);
                                                true
                                            } else {
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    };

                                    if !is_echo {
                                        // Local-only signal: server never fans marked ops out.
                                        if db.is_local_only(&plain_col, key) {
                                            continue;
                                        }
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
                                    // Sender rule: when THIS server encrypts
                                    // the collection, fan out only to
                                    // fingerprint-matched peers of the room.
                                    // (Storage or plain name match counts;
                                    // see the ingest rule.)
                                    let enc_local = db.is_collection_encrypted(&col)
                                        || db.is_collection_encrypted(&plain_col);
                                    let local_fp = crate::sync_guard::local_fingerprint(
                                        db.config.encryption_key.as_deref(),
                                    );
                                    let packet = CloudPacket::Replication {
                                        msg_id: SystemTime::now()
                                            .duration_since(UNIX_EPOCH)
                                            .unwrap()
                                            .as_micros(),
                                        collection: plain_col.clone(),
                                        ops: to_send,
                                    };

                                    if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                                        let msg = Message::Binary(bytes.into());
                                        let rt = tokio::runtime::Handle::current();
                                        let peers_ptr = active_peers.clone();
                                        let caps_ptr = caps_srv.clone();
                                        let target_prefix = prefix.clone();
                                        let plain_c = plain_col.clone();
                                        rt.block_on(async move {
                                            let peers_guard = peers_ptr.read().await;
                                            for (peer_id, info) in peers_guard.iter() {
                                                if info.prefix != target_prefix {
                                                    continue;
                                                }
                                                if enc_local {
                                                    let peer = caps_ptr.get(peer_id);
                                                    if !crate::sync_guard::caps_allow(
                                                        true,
                                                        local_fp,
                                                        peer.as_ref(),
                                                    ) {
                                                        caps_ptr.warn(
                                                            peer_id,
                                                            &plain_c,
                                                            &format!(
                                                                "withholding server fan-out of '{plain_c}' from peer '{peer_id}' (unverified key for an encrypted room)"
                                                            ),
                                                        );
                                                        continue;
                                                    }
                                                }
                                                let _ = info.tx.try_send(msg.clone());
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
        let room_name = self.room_name.clone();
        let room_key_str = self.room_key.clone();
        let api_key = self.api_key.lock().ok().and_then(|g| g.clone());
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
                        room_name: room_name.clone(),
                        room_key: room_key_str.clone(),
                        api_key: api_key.clone(),
                        client_version: Some(env!("CARGO_PKG_VERSION").to_string()),
                        enc_fp: Some(crate::sync_guard::local_fingerprint(
                            db.config.encryption_key.as_deref(),
                        )),
                        enc_cols: db
                            .list_collections()
                            .unwrap_or_default()
                            .into_iter()
                            .filter(|c| db.is_collection_encrypted(c))
                            .collect(),
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
                                                                    prefix: String::new(),
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
        // NOTE: no encryption gate here by design (see client tailer above):
        // the hub enforces on receipt and relay.
        let ops = Self::collect_catchup_ops(db, collection, collection, server_ts);

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

    fn spawn_client_outbound_tailer(&self, echo_cache: Arc<StdMutex<HashMap<String, i64>>>) {
        let db = self.db.clone();
        let outbound_tx_option = self.outbound_tx.clone();
        let running = self.running.clone();

        tokio::task::spawn_blocking(move || {
            let mut offsets: HashMap<String, u64> = HashMap::new();

            while running.load(Ordering::Relaxed) {
                let outbound_guard = outbound_tx_option.blocking_lock();
                let Some(ref tx) = *outbound_guard else {
                    drop(outbound_guard);
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                };

                let cols = db.list_collections().unwrap_or_default();

                for col in cols {
                    // Sync-state, room registry, and admin credential stores
                    // never leave the device. (`__firelite_security` keeps
                    // flowing — policies replicate.)
                    if crate::engine::engine::is_sync_excluded(&col) {
                        continue;
                    }
                    // NOTE: no encryption gate here by design. This client
                    // pushes everything upstream; the hub (which sees the
                    // capabilities we announced at auth) enforces admission,
                    // relay, and storage rules. Gating here would brick
                    // encrypted rooms against the standard keyless hub.
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
                                    if !matches!(op, WalOp::PutInlined { .. } | WalOp::Delete { .. }) {
                                        continue;
                                    }
                                    let key = op.get_key();
                                    let wal_ts = match &op {
                                        WalOp::PutInlined { value, .. } => {
                                            i64::from_le_bytes(value[2..10].try_into().unwrap_or([0; 8]))
                                        }
                                        WalOp::Delete { timestamp, .. } => *timestamp,
                                        _ => 0,
                                    };

                                    let echo_key = echo_key("", &col, &key);
                                    let is_echo = {
                                        let mut cache = echo_cache.lock().unwrap();
                                        if let Some(&cached_ts) = cache.get(&echo_key) {
                                            if cached_ts == wal_ts {
                                                cache.remove(&echo_key);
                                                true
                                            } else {
                                                false
                                            }
                                        } else {
                                            false
                                        }
                                    };

                                    if !is_echo {
                                        // Local-only signal: client never pushes marked ops upstream.
                                        if db.is_local_only(&col, key) {
                                            continue;
                                        }
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

#[cfg(all(test, feature = "cloud-sync"))]
mod tests {
    use super::*;
    use crate::config::{DurabilityMode, FireLiteConfig};

    fn temp_db(tag: &str) -> (Arc<FireLite>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "firelite-rooms-{}-{}",
            std::process::id(),
            tag
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = FireLiteConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Arc::new(FireLite::open(&dir, cfg).unwrap());
        (db, dir)
    }

    #[test]
    fn same_room_key_pair_reuses_prefix() {
        let (db, _dir) = temp_db("samepair");
        let reg = RoomRegistry::new(db.clone());
        let (id1, p1) = reg.resolve("alpha", "k1").unwrap();
        let (id2, p2) = reg.resolve("alpha", "k1").unwrap();
        assert_eq!(id1, id2);
        assert_eq!(p1, p2);
        assert_eq!(p1, "alpha");
    }

    #[test]
    fn same_name_different_key_gets_suffix() {
        let (db, _dir) = temp_db("suffix");
        let reg = RoomRegistry::new(db.clone());
        let (_, p1) = reg.resolve("alpha", "k1").unwrap();
        let (_, p2) = reg.resolve("alpha", "k2").unwrap();
        let (_, p3) = reg.resolve("alpha", "k3").unwrap();
        assert_eq!(p1, "alpha");
        assert_eq!(p2, "alpha_1");
        assert_eq!(p3, "alpha_2");
    }

    #[test]
    fn different_names_are_independent() {
        let (db, _dir) = temp_db("names");
        let reg = RoomRegistry::new(db.clone());
        let (_, p1) = reg.resolve("alpha", "k1").unwrap();
        let (_, p2) = reg.resolve("beta", "k1").unwrap();
        assert_eq!(p1, "alpha");
        assert_eq!(p2, "beta");
    }

    #[test]
    fn prefix_persists_across_registry_restart() {
        let (db, dir) = temp_db("persist");
        {
            let reg = RoomRegistry::new(db.clone());
            let (_, p1) = reg.resolve("alpha", "k1").unwrap();
            let (_, p2) = reg.resolve("alpha", "k2").unwrap();
            assert_eq!(p1, "alpha");
            assert_eq!(p2, "alpha_1");
        }
        // A fresh registry on the same db reloads prefixes from the internal collection.
        let reg = RoomRegistry::new(db.clone());
        let (_, p1) = reg.resolve("alpha", "k1").unwrap();
        let (_, p2) = reg.resolve("alpha", "k2").unwrap();
        assert_eq!(p1, "alpha");
        assert_eq!(p2, "alpha_1");
        // A new distinct key after restart gets the next free suffix.
        let (_, p3) = reg.resolve("alpha", "k3").unwrap();
        assert_eq!(p3, "alpha_2");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn prefix_of_maps_storage_back_to_plain() {
        let (db, _dir) = temp_db("prefixof");
        let reg = RoomRegistry::new(db.clone());
        reg.resolve("alpha", "k1").unwrap();
        reg.resolve("alpha", "k2").unwrap();
        assert_eq!(
            reg.prefix_of("alpha_users").unwrap(),
            ("alpha".to_string(), "users".to_string())
        );
        assert_eq!(
            reg.prefix_of("alpha_1_users").unwrap(),
            ("alpha_1".to_string(), "users".to_string())
        );
        assert!(reg.prefix_of("orphan_users").is_none());
    }

    #[test]
    fn sanitize_handles_weird_names() {
        assert_eq!(sanitize_room_name("my room!"), "my_room");
        assert_eq!(sanitize_room_name("a/b\\c"), "a_b_c");
        assert_eq!(sanitize_room_name(""), "room");
        assert_eq!(sanitize_room_name("___"), "room");
    }

    fn put_simple(db: &Arc<FireLite>, col: &str, id: &str) {
        let mut doc = FireLiteDoc::default();
        doc.insert("v", Value::Int(1));
        db.put(col, id, &doc).unwrap();
    }

    #[test]
    fn catchup_emits_puts_and_tombstones() {
        let (db, _dir) = temp_db("catchup");
        put_simple(&db, "c", "keep");
        put_simple(&db, "c", "gone");
        db.delete("c", "gone").unwrap();

        let ops = CloudSync::collect_catchup_ops(&db, "c", "c", 0);
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
        assert_eq!(dels, vec!["gone".to_string()]);
    }

    #[test]
    fn catchup_withholds_local_only_marks() {
        let (db, _dir) = temp_db("catchup-local");
        put_simple(&db, "c", "keep");
        put_simple(&db, "c", "gone");
        db.delete_local("c", "gone").unwrap();

        // Key-level mark: the tombstone never enters catch-up...
        let ops = CloudSync::collect_catchup_ops(&db, "c", "c", 0);
        assert_eq!(ops.len(), 1, "local-only tombstone leaked: {ops:?}");

        // ...and the clock still advanced past it (deleter looks ahead).
        let v = db.get_collection_version("c").unwrap();
        assert!(v > 0);

        // Collection-level mark: nothing leaves at all.
        db.set_collection_local("c", true);
        let ops = CloudSync::collect_catchup_ops(&db, "c", "c", 0);
        assert!(ops.is_empty(), "local-only collection leaked: {ops:?}");
    }

    #[test]
    fn stale_remote_delete_loses_to_newer_put() {
        let (db, _dir) = temp_db("stale-del");
        put_simple(&db, "c", "a");
        let ts = db.get("c", "a").unwrap().unwrap().get_logical_time();

        assert!(CloudSync::is_stale_remote_delete(&db, "c", "a", ts));
        assert!(CloudSync::is_stale_remote_delete(&db, "c", "a", ts - 1));
        assert!(!CloudSync::is_stale_remote_delete(&db, "c", "a", ts + 1_000_000));
        assert!(!CloudSync::is_stale_remote_delete(&db, "c", "missing", ts));
    }

    fn put_group(
        db: &Arc<FireLite>,
        room: &str,
        mode: &str,
        key_hash: Option<String>,
        members: Vec<String>,
    ) {
        let mut doc = FireLiteDoc::default();
        doc.insert("mode", Value::String(mode.to_string()));
        if let Some(h) = key_hash {
            doc.insert("api_key_hash", Value::String(h));
        }
        doc.insert(
            "members",
            Value::Array(members.into_iter().map(Value::String).collect()),
        );
        db.put(GROUPS_COLLECTION, room, &doc).unwrap();
    }

    #[test]
    fn group_access_matrix() {
        let (db, _dir) = temp_db("groups");
        // No row: open group, historic behavior (old anonymous peers pass).
        assert!(check_group_access(&db, "noroom", None, "c1").is_ok());

        put_group(&db, "openroom", "open", None, vec![]);
        assert!(check_group_access(&db, "openroom", None, "c1").is_ok());

        put_group(
            &db,
            "priv",
            "registered",
            Some(hash_api_key("sekret")),
            vec![],
        );
        assert!(check_group_access(&db, "priv", Some("sekret"), "c1").is_ok());
        assert!(check_group_access(&db, "priv", Some("wrong"), "c1").is_err());
        assert!(check_group_access(&db, "priv", None, "c1").is_err());

        // Member gating: listed passes, unlisted rejected, empty list = any.
        put_group(
            &db,
            "memb",
            "registered",
            Some(hash_api_key("k")),
            vec!["alice".to_string()],
        );
        assert!(check_group_access(&db, "memb", Some("k"), "alice").is_ok());
        assert!(check_group_access(&db, "memb", Some("k"), "mallory").is_err());

        // Unknown mode fails closed.
        put_group(&db, "weird", "fortress", None, vec![]);
        assert!(check_group_access(&db, "weird", None, "c1").is_err());
    }

    #[test]
    fn api_key_hash_is_stable_and_wrong_key_misses() {
        assert_eq!(hash_api_key("abc"), hash_api_key("abc"));
        assert_ne!(hash_api_key("abc"), hash_api_key("abd"));
        assert_eq!(hash_api_key("abc").len(), 64);
    }

    #[test]
    fn old_authenticate_shape_still_parses() {
        // Pre-api_key peers send maps without the new keys; serde defaults
        // must admit them (anonymous against open groups).
        let old: CloudPacket = serde_json::from_str(
            r#"{"authenticate":{"token":"t","client_id":"c","room_name":"r","room_key":"k"}}"#,
        )
        .expect("old shape parses");
        match old {
            CloudPacket::Authenticate {
                api_key,
                client_version,
                enc_fp,
                enc_cols,
                ..
            } => {
                assert_eq!(api_key, None);
                assert_eq!(client_version, None);
                // Encryption caps likewise default: old peers authenticate
                // as unverified (fail-closed for encrypted rooms).
                assert_eq!(enc_fp, None);
                assert!(enc_cols.is_empty());
            }
            _ => panic!("wrong variant"),
        }
    }
}
