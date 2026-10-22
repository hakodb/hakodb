#[cfg(feature = "cloud-sync")]
use std::collections::HashMap;
#[cfg(feature = "cloud-sync")]
use std::sync::atomic::{AtomicBool, AtomicI64, AtomicUsize, Ordering};
#[cfg(feature = "cloud-sync")]
use std::sync::{Arc, Mutex as StdMutex};
#[cfg(feature = "cloud-sync")]
use std::time::{Duration, SystemTime, UNIX_EPOCH};

#[cfg(feature = "cloud-sync")]
use crate::engine::engine::IndexList;
#[cfg(feature = "cloud-sync")]
use crate::document::hako_doc::HakoDoc;
#[cfg(feature = "cloud-sync")]
use crate::document::value::Value;
#[cfg(feature = "cloud-sync")]
use crate::engine::{ChangeEvent, ChangeKind, Hako};
#[cfg(feature = "cloud-sync")]
use crate::error::{HakoError, Result as FLResult};
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


/// Shared transport config for BOTH ws faces (cloudserver room accept and
/// the client-side connect). Both faces can hit the transport caps:
/// a legacy hub pushes one big catch-up frame at a joining client, and a
/// legacy client pushes one big batch at the hub (issue #69 measured a
/// 103 MB single frame hub-side).
///
/// Why the caps are NOT simply tiny: tungstenite's inbound caps are checks,
/// not preallocations — a raised limit costs nothing until a big frame
/// actually arrives, then it is paid once for the message that carries it.
/// The outbound side is different: the write accumulator previously sat at
/// usize::MAX, which is how thousands of slow-stalled peers balloon memory
/// — that hard limit stays bounded here.
///
/// Public so the load-probe peers (hakocloudserver `peer-bench`) exercise
/// the exact same transport settings as the real client instead of a
/// courteous lookalike.
pub fn ws_transport_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    use tokio_tungstenite::tungstenite::protocol::WebSocketConfig;
    // WebSocketConfig is non-exhaustive upstream: mutate the default.
    let mut cfg = WebSocketConfig::default();
    // Inbound: accept legacy single frames up to 64 MiB (~120k small docs
    // on one frame, per the measured ~543 B/op). Anything beyond fails
    // loudly (logged at both faces) and reconnects — the same ceiling the
    // pager keeps new peers far away from.
    cfg.max_frame_size = Some(64 << 20);
    cfg.max_message_size = Some(64 << 20);
    // Outbound backpressure: the write accumulator only builds past
    // write_buffer_size when TCP writes stall; bound it instead of letting
    // it grow forever (16 MiB > any pager page we emit).
    cfg.max_write_buffer_size = 16 << 20;
    cfg
}
// ============================================================================
// DATA STRUCTURES & PROTOCOL PACKETS
// ============================================================================

// TLS uses vendored rustls with the single ring provider (tungstenite 0.30
// selects no provider itself; install ring as process default to prevent
// the 0.23 no-provider panic).
#[cfg(feature = "cloud-sync")]
fn init_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

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
    /// Version handshake exchanged symmetrically to catch up on missed deltas.
    /// `indexes` is additive (issue #51, serde default): old peers omit it
    /// and converge doc bytes only. Names are plain on the wire and in
    /// storage — one database, one room, no translation anywhere.
    VersionPing {
        versions: HashMap<String, i64>,
        #[serde(default)]
        indexes: Option<IndexList>,
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
    /// Secret. Kept for trusted in-process callers, but never serialized:
    /// use [`CloudStatusRedacted`] / [`CloudSync::status_redacted`] at
    /// logging, admin, or FFI boundaries.
    #[serde(skip_serializing)]
    pub room_key: String,
    pub active_clients: usize,
    pub queued_writes: usize,
}

/// Admin/log-safe cloud status: same shape as [`CloudStatus`] minus the
/// room key. This is what `hk_cloud_sync_status` returns.
#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone, PartialEq, Serialize)]
pub struct CloudStatusRedacted {
    pub mode: CloudSyncMode,
    pub connected: bool,
    pub room_name: String,
    pub active_clients: usize,
    pub queued_writes: usize,
}

// ============================================================================
// ROOM IDENTITY (single-database)
// ============================================================================
//
// A "room" is the pair (room_name, room_key): a connection-scoped channel
// credential, exactly like the mesh `Identify{room_hash}` (see net_sync).
// The server is bound to ONE room at construction; clients presenting a
// different pair are rejected at handshake — fail-closed, before any data
// moves. There is deliberately no storage namespace: the server stores
// collections under their plain names, so one server process serves one
// room. Multi-room hosting is N server processes (or hakocluster
// multidb), never name-mangled collections in one database.

/// Group policy store: a `__groups` doc per room name (admin-managed).
/// Absent row or `mode: "open"` admits everyone (historic behavior);
/// `mode: "registered"` admits only valid API keys (+ listed members when
/// the members list is non-empty). This collection never syncs.
#[cfg(feature = "cloud-sync")]
pub const GROUPS_COLLECTION: &str = "__groups";

/// #72 lag meter: ms-epoch of the last successful cloud exchange on ANY
/// client face in this process (auth round-trip completed, or any server
/// packet decoded). 0 = never reached the cloud. Announced over the mesh
/// via `NetPacket::CloudLease`; read via `cloud_last_exchange_millis`.
#[cfg(feature = "cloud-sync")]
static LAST_CLOUD_EXCHANGE_MS: AtomicI64 = AtomicI64::new(0);

#[cfg(feature = "cloud-sync")]
fn note_cloud_exchange() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    LAST_CLOUD_EXCHANGE_MS.store(now, Ordering::Relaxed);
}

/// ms-epoch of the last successful cloud round-trip on any client face,
/// or None when this process never reached the cloud. The LAN relay lag
/// meter and the phase-2 GRACE eligibility rule both read this.
#[cfg(feature = "cloud-sync")]
pub fn cloud_last_exchange_millis() -> Option<i64> {
    match LAST_CLOUD_EXCHANGE_MS.load(Ordering::Relaxed) {
        0 => None,
        ts => Some(ts),
    }
}

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
    db: &Hako,
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
fn value_to_string(v: &Value) -> Option<String> {
    match v {
        Value::String(s) => Some(s.clone()),
        _ => None,
    }
}

/// Client-facing collections are stored server-side as "<prefix>_<collection>".

/// Uniform key used by the outbound tailers and the ingest flusher to suppress
/// echoes (writes that originated locally and were only re-applied on arrival).
/// Connection-scoped like the room itself: collection + key are enough,
/// because one server process serves exactly one room.
#[cfg(feature = "cloud-sync")]
fn echo_key(collection: &str, key: &str) -> String {
    format!("{collection}:{key}")
}

// ============================================================================
// CLOUD SYNC CONTROLLER & AGGREGATOR
// ============================================================================

#[cfg(feature = "cloud-sync")]
pub struct CloudSync {
    db: Arc<Hako>,
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
    /// Peer capability announcements for the encryption fail-closed rules.
    /// Server: keyed by peer_key, set at auth, cleared on disconnect.
    /// Client: the server's caps live under the fixed key `"server"`.
    caps: Arc<crate::sync_guard::CapsMap>,
    /// #72 admission control: hard cap on distinct connected clients
    /// (0 = unlimited, the historic default). Atomic so FFI/SDK handles
    /// can tune it at runtime.
    max_clients: Arc<AtomicUsize>,
    /// Shared catch-up page cache: one materialization + encode serves ALL
    /// concurrent fresh joiners of a collection (identical byte streams —
    /// same room, same server_ts). Keyed by collection, holding the newest
    /// server_ts it was built for; a version bump simply misses and
    /// re-collects. Bounded by the collection count (each entry ~= the
    /// collection's wire size). Fixes the thundering-join HOL storm where
    /// N joiners each re-materialized the whole collection.
    catchup_cache: Arc<StdMutex<HashMap<String, CatchupCacheEntry>>>,
    /// Test/ops tripwire: how many catch-up sends were served from the
    /// shared cache instead of a fresh materialization.
    catchup_cache_hits: Arc<AtomicUsize>,
}

#[cfg(feature = "cloud-sync")]
struct CatchupCacheEntry {
    server_ts: i64,
    /// Fully encoded ws frames (msgpack page packets), ready to send.
    packets: Arc<Vec<Message>>,
}

#[cfg(feature = "cloud-sync")]
struct IngestItem {
    collection: String,
    op: WalOp,
    sender_client_id: Option<String>,
}

#[cfg(feature = "cloud-sync")]
struct PeerInfo {
    tx: mpsc::Sender<Message>,
}

/// Admin view of one connected peer. `peer_key` is the client id.
#[cfg(feature = "cloud-sync")]
#[derive(Debug, Clone, serde::Serialize)]
pub struct PeerView {
    pub peer_key: String,
}

/// Whole-database version map: the room catch-up clock. Collections are
/// stored and compared under plain names — one database, one room.
#[cfg(feature = "cloud-sync")]
fn db_version_map(db: &Arc<Hako>) -> HashMap<String, i64> {
    let mut map = HashMap::new();
    if let Ok(cols) = db.sync_collections() {
        for col in cols {
            if let Ok(version) = db.get_collection_version(&col) {
                map.insert(col, version);
            }
        }
    }
    map
}

#[cfg(feature = "cloud-sync")]
impl CloudSync {
    /// Creates a Cloud Sync controller bound to one room (both modes).
    /// Prefer the dedicated [`CloudSync::server`] / [`CloudSync::client`]
    /// constructors for clarity.
    pub fn new(
        db: Arc<Hako>,
        mode: CloudSyncMode,
        client_id: &str,
        room_name: &str,
        room_key: &str,
        auth_token: &str,
    ) -> Self {
        match mode {
            CloudSyncMode::Server => Self::server(db, client_id, auth_token, room_name, room_key),
            CloudSyncMode::Client => Self::client(db, client_id, room_name, room_key, auth_token),
        }
    }

    /// Creates a room-bound cloud server: serves exactly the configured
    /// room on this database (single-database, like the mesh). Clients
    /// presenting a different (name, key) pair are rejected at handshake.
    /// Prefer this (or [`CloudSync::client`]) over [`CloudSync::new`].
    pub fn server(
        db: Arc<Hako>,
        server_id: &str,
        auth_token: &str,
        room_name: &str,
        room_key: &str,
    ) -> Self {
        let (tx, rx) = mpsc::channel(100_000);

        Self {
            db,
            mode: CloudSyncMode::Server,
            room_name: room_name.to_string(),
            room_key: room_key.to_string(),
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
            caps: Arc::new(crate::sync_guard::CapsMap::default()),
            max_clients: Arc::new(AtomicUsize::new(0)),
            catchup_cache: Arc::new(StdMutex::new(HashMap::new())),
            catchup_cache_hits: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Creates an offline-first cloud client bound to a single room. The client
    /// decides which room to join (room_name + room_key) and which server to
    /// sync with via [`CloudSync::start`]; every other client/peer using the
    /// same (room_name, room_key) on the same server forms the sync group.
    pub fn client(
        db: Arc<Hako>,
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
            caps: Arc::new(crate::sync_guard::CapsMap::default()),
            max_clients: Arc::new(AtomicUsize::new(0)),
            catchup_cache: Arc::new(StdMutex::new(HashMap::new())),
            catchup_cache_hits: Arc::new(AtomicUsize::new(0)),
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

    /// #72 admission control (server face): hard cap on distinct connected
    /// clients per room. 0 = unlimited (historic default). A client
    /// reconnecting with its own id always re-admits (replaces its stale
    /// entry); a new client beyond the cap gets a typed loud
    /// `AuthResult { success: false, error: "room at capacity" }` and the
    /// caller retries with backoff. Works on Arc'd handles (FFI/SDK).
    pub fn set_max_clients_per_room(&self, max: usize) {
        self.max_clients.store(max, Ordering::Relaxed);
    }

    /// Current per-room client cap (0 = unlimited).
    pub fn max_clients_per_room(&self) -> usize {
        self.max_clients.load(Ordering::Relaxed)
    }

    /// How many catch-up sends were served from the shared page cache
    /// (ops visibility + test tripwire for the thundering-join fix).
    pub fn catchup_cache_hits(&self) -> usize {
        self.catchup_cache_hits.load(Ordering::Relaxed)
    }

    pub fn status(&self) -> CloudStatus {
        let redacted = self.status_redacted();
        CloudStatus {
            mode: redacted.mode,
            connected: redacted.connected,
            room_name: redacted.room_name,
            room_key: self.room_key.clone(),
            active_clients: redacted.active_clients,
            queued_writes: redacted.queued_writes,
        }
    }

    /// Status safe for logs, admin endpoints, and FFI: no room key.
    pub fn status_redacted(&self) -> CloudStatusRedacted {
        let active_clients = if let Ok(guard) = self.active_peers.try_read() {
            guard.len()
        } else {
            0
        };

        CloudStatusRedacted {
            mode: self.mode,
            connected: self.running.load(Ordering::Relaxed),
            room_name: self.room_name.clone(),
            active_clients,
            queued_writes: 0,
        }
    }

    /// Snapshot of currently connected peers (server: room members;
    /// client: normally just the uplink, if tracked). Best-effort read.
    pub fn peer_list(&self) -> Vec<PeerView> {
        match self.active_peers.try_read() {
            Ok(guard) => guard
                .iter()
                .map(|(k, _)| PeerView {
                    peer_key: k.clone(),
                })
                .collect(),
            Err(_) => Vec::new(),
        }
    }

    /// Per-collection versions of this database (the room catch-up clock).
    /// Backs the admin dashboard's room view; pure function of the DB.
    pub fn room_versions(&self) -> HashMap<String, i64> {
        db_version_map(&self.db)
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
                                batch_buffer.entry(item.collection.clone())
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
        db: &Arc<Hako>,
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

        for (collection, items) in buffer.drain() {
            if items.is_empty() {
                continue;
            }

            // Local-only signal (inbound): a locally-scoped collection
            // refuses everything the room offers. Per-key marks do NOT
            // filter inbound — a genuinely newer remote put resurrects.
            if db.is_collection_local(&collection) {
                continue;
            }

            // Sync-excluded plane (inbound): credential stores and sync
            // state never arrive over the wire. Without this, a hostile
            // Replication{collection:"__groups"} would plant a same-named
            // shard AND get relayed back out, poisoning every room
            // member's real store. The outbound tailers already skip
            // these; this is the receipt-side choke point both
            // directions route through.
            if db.is_sync_withheld(&collection)
            {
                continue;
            }

            // Encryption fail-closed (inbound): when THIS node encrypts the
            // collection at rest, ops are accepted only from senders that
            // proved the same key. A keyless node never matches: its own
            // encrypted set is empty.
            let enc_local = db.is_collection_encrypted(&collection);
            let local_fp =
                crate::sync_guard::local_fingerprint(db.config.encryption_key.as_deref());

            let mut apply_ops: Vec<WalOp> = Vec::with_capacity(items.len());
            let mut index_puts: Vec<(String, Arc<HakoDoc>)> = Vec::new();
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
                                &collection,
                                &format!(
                                    "dropping inbound ops for locally-encrypted '{collection}' from unverified sender '{sender_key}' (local key {}, peer needs the same encryption key; upgrade old peers)",
                                    crate::sync_guard::fp_short(&local_fp),
                                ),
                            );
                            continue;
                        }
                    }
                }
                match &item.op {
                    WalOp::PutInlined { key, value } => {
                        let ts = HakoDoc::decode(value)
                            .map(|d| d.get_logical_time())
                            .unwrap_or(0);
                        if ts == 0 {
                            continue;
                        }

                        // LWW Check
                        if let Ok(Some(existing_doc)) = db.get(&collection, key) {
                            if existing_doc.get_logical_time() >= ts {
                                continue;
                            }
                        }

                        {
                            let mut cache = echo_cache.lock().unwrap();
                            cache.insert(echo_key(&collection, key), ts);
                        }
                        apply_ops.push(WalOp::PutInlined {
                            key: key.clone(),
                            value: value.clone(),
                        });
                        if let Some(doc) = HakoDoc::decode(value) {
                            index_puts.push((key.clone(), Arc::new(doc)));
                        }
                        // Hybrid-clock watermark (issue #80): accepted
                        // remote ts raises the floor for local stamps.
                        db.observe_remote_ts(ts);
                        sender_relays
                            .entry(item.sender_client_id.clone())
                            .or_default()
                            .push(item.op.clone());
                    }
                    WalOp::Delete { key, timestamp } => {
                        // LWW check (mirrors the put arm): a stale tombstone —
                        // e.g. replayed by handshake catch-up — must not beat
                        // a newer local put.
                        if Self::is_stale_remote_delete(&db, &collection, key, *timestamp) {
                            continue;
                        }
                        {
                            let mut cache = echo_cache.lock().unwrap();
                            cache.insert(echo_key(&collection, key), *timestamp);
                        }
                        apply_ops.push(WalOp::Delete {
                            key: key.clone(),
                            timestamp: *timestamp,
                        });
                        // Hybrid-clock watermark (issue #80): see the put arm.
                        db.observe_remote_ts(*timestamp);
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
                let col = collection.clone();
                let keys: Vec<Arc<str>> = apply_ops
                    .iter()
                    .map(|op| Arc::from(op.get_key().to_string()))
                    .collect();
                let _ = tokio::task::spawn_blocking(move || {
                    Self::apply_timestamped(&db_clone, &col, apply_ops, index_puts, keys);
                })
                .await;
            }

            // Relay packet to connected members (except origin). One room
            // per server, so every connected peer is a member by
            // construction — no room filter needed.
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
                    collection: collection.clone(),
                    ops,
                };

                if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                    // Sized before the loop — the error log below wants the
                    // wire footprint without another clone-chase.
                    let bytes_len = bytes.len();
                    let msg = Message::Binary(bytes.into());
                    let peers_guard = peers.read().await;

                    for (peer_id, info) in peers_guard.iter() {
                        if Some(peer_id) == origin_sender.as_ref() {
                            continue;
                        }
                        let recipient = caps.get(peer_id);
                        let origin = origin_sender
                            .as_deref()
                            .and_then(|o| caps.get(o));
                        let origin_encrypted = origin
                            .as_ref()
                            .map(|o| o.encrypted_cols.iter().any(|c| c == &collection))
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
                                &collection,
                                &format!(
                                    "withholding relay of '{collection}' from peer '{peer_id}' (unverified key for an encrypted room)"
                                ),
                            );
                            continue;
                        }
                        let sent_result = info.tx.try_send(msg.clone());
                        // Issue #69 #4: every silent drop here becomes a
                        // logged, peer-keyed event.
                        if let Err(ref se) = sent_result {
                            eprintln!(
                                "[sync] relay '{collection}' to '{peer_id}': queue send failed ({} B): {}",
                                bytes_len,
                                se
                            );
                        }
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
        db: &Arc<Hako>,
        collection: &str,
        ops: Vec<WalOp>,
        index_puts: Vec<(String, Arc<HakoDoc>)>,
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
            db.enqueue_index_update(
                collection.to_string(),
                Arc::new(index_puts),
                vec![],
            );
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
            .map_err(|e| HakoError::Io(e))?;

        let ingest_tx = self.ingest_tx.clone();
        let db = self.db.clone();
        let active_peers = self.active_peers.clone();
        let running = self.running.clone();
        let seen_messages = self.seen_messages.clone();
        let echo_cache = self.echo_cache.clone();
        let caps_all = self.caps.clone();
        let max_clients = self.max_clients.clone();
        let catchup_cache = self.catchup_cache.clone();
        let catchup_cache_hits = self.catchup_cache_hits.clone();
        // Single room, configured at construction (no registry, no
        // allocation, no multi-room fallback — see module docs).
        let room_name = self.room_name.clone();
        let room_key = self.room_key.clone();

        // 1. Spawn Server Local WAL Tailer (broadcasts server-side writes
        //    to the connected clients of the room).
        self.spawn_server_outbound_tailer(echo_cache);

        // 2. Accept Incoming WebSocket Clients
        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                if let Ok((stream, _)) = listener.accept().await {
                    let ingest_tx = ingest_tx.clone();
                    let db = db.clone();
                    let active_peers = active_peers.clone();
                    let seen_messages = seen_messages.clone();
                    let room_name = room_name.clone();
                    let room_key = room_key.clone();
                    let caps_one = caps_all.clone();
                    let max_clients_clone = max_clients.clone();
                    let cache_clone = catchup_cache.clone();
                    let cache_hits_clone = catchup_cache_hits.clone();

                // TCP_NODELAY on the raw accept socket: the hub's small
                // control frames (AuthResult/VersionPing) and fan-out
                // pages must not sit behind Nagle batching.
                let _ = stream.set_nodelay(true);
                tokio::spawn(async move {
                        if let Ok(ws_stream) = tokio_tungstenite::accept_async_with_config(
                            stream,
                            Some(ws_transport_config()),
                        )
                        .await {
                            Self::handle_server_client(
                                ws_stream,
                                ingest_tx,
                                db,
                                room_name,
                                room_key,
                                active_peers,
                                seen_messages,
                                caps_one,
                                max_clients_clone,
                                cache_clone,
                                cache_hits_clone,
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
        db: Arc<Hako>,
        room_name: String,
        room_key: String,
        peers: Arc<AsyncRwLock<HashMap<String, PeerInfo>>>,
        seen_messages: Arc<AsyncMutex<Vec<u128>>>,
        caps: Arc<crate::sync_guard::CapsMap>,
        max_clients: Arc<AtomicUsize>,
        cache: Arc<StdMutex<HashMap<String, CatchupCacheEntry>>>,
        cache_hits: Arc<AtomicUsize>,
    ) where
        S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static,
    {
        let (mut ws_tx, mut ws_rx) = ws_stream.split();
        // Depth 16 (~14 MB of 0.9 MB catch-up pages): deep enough that the
        // blocking tailer's try_send never trips for a healthy client, but
        // shallow enough that N thundering joiners cannot buffer their whole
        // catch-up in hub RAM before the slowest reader drains it. The old
        // 2000 cap queued ~31 MB/client x N — the thundering-join swap-storm.
        let (client_tx, mut client_rx) = mpsc::channel::<Message>(16);

        let mut peer_key = String::new();
        let mut authenticated = false;

        if let Some(Ok(Message::Binary(bytes))) = ws_rx.next().await {
            if let Ok(CloudPacket::Authenticate {
                token: _,
                client_id: cid,
                room_name: presented_name,
                room_key: presented_key,
                api_key,
                enc_fp,
                enc_cols,
                ..
            }) = rmp_serde::from_slice(&bytes)
            {
                // Single-room enforcement, BEFORE group policy and before
                // any data moves: the presented pair must equal the
                // configured room. No registry, no allocation, no
                // multi-room fallback — a wrong room is a wrong server.
                if presented_name != room_name || presented_key != room_key {
                    let nack = CloudPacket::AuthResult {
                        success: false,
                        error: Some("room mismatch".into()),
                    };
                    if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                        let _ = ws_tx.send(Message::Binary(bytes.into())).await;
                    }
                    return;
                }
                // Group policy: rejected peers are dropped here.
                if let Err(e) = check_group_access(&db, &presented_name, api_key.as_deref(), &cid) {
                    let nack = CloudPacket::AuthResult {
                        success: false,
                        error: Some(e),
                    };
                    if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                        let _ = ws_tx.send(Message::Binary(bytes.into())).await;
                    }
                    return;
                }
                // #72 admission control: per-room client cap. A peer
                // RECONNECTING with its own id always replaces its stale
                // entry (cap counts distinct clients, not retries); a NEW
                // client beyond the cap gets a typed loud reject and the
                // LAN peers behind it stay mesh-only (they never dial).
                let max = max_clients.load(Ordering::Relaxed);
                if max > 0 {
                    let p = peers.read().await;
                    if !p.contains_key(&cid) && p.len() >= max {
                        drop(p);
                        let nack = CloudPacket::AuthResult {
                            success: false,
                            error: Some("room at capacity".into()),
                        };
                        if let Ok(bytes) = rmp_serde::to_vec_named(&nack) {
                            let _ = ws_tx.send(Message::Binary(bytes.into())).await;
                        }
                        return;
                    }
                }
                authenticated = true;
                // One room per server: the client id alone keys the peer
                // (a reconnect replaces the stale entry).
                peer_key = cid;

                peers.write().await.insert(
                    peer_key.clone(),
                    PeerInfo { tx: client_tx },
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

                                // Ingest directly: one database, one room —
                                // client collections land under plain names.
                                for op in ops {
                                    let _ = ingest_tx
                                        .send(IngestItem {
                                            collection: collection.clone(),
                                            op,
                                            sender_client_id: Some(peer_key.clone()),
                                        })
                                        .await;
                                }
                            }
                            CloudPacket::VersionPing { versions: client_versions, indexes } => {
                                // 0. Index defs converge (issue #51): names
                                // are plain on the wire both directions —
                                // apply directly, no namespace translation.
                                if let Some(idx) = indexes {
                                    crate::sync_core::apply_peer_index_defs(&db, &idx);
                                }

                                // 1. SYMMETRICAL REPLY: send this database's
                                //    version map back to the client.
                                let server_versions = db_version_map(&db);
                                let reply = CloudPacket::VersionPing {
                                    versions: server_versions.clone(),
                                    indexes: Some(db.list_indexes(None)),
                                };
                                if let Ok(reply_bytes) = rmp_serde::to_vec_named(&reply) {
                                    let peers_guard = peers.read().await;
                                    if let Some(tx) = peers_guard.get(&peer_key) {
                                        // Small control frame, but a failed
                                        // handshake reply used to vanish too.
                                        if let Err(se) = tx.tx.send(Message::Binary(reply_bytes.into())).await {
                                            eprintln!(
                                                "[sync] VersionPing reply to '{peer_key}' failed: {se}"
                                            );
                                        }
                                    }
                                }

                                // 2. Pull catch-up: send deltas for every
                                //    collection where the server is ahead
                                //    (including ones the client never saw).
                                for (collection, server_ts) in server_versions {
                                    let client_ts = client_versions
                                        .get(&collection)
                                        .copied()
                                        .unwrap_or(0);
                                    if server_ts > client_ts {
                                        Self::send_catchup_deltas(
                                            &db,
                                            &collection,
                                            client_ts,
                                            server_ts,
                                            &peer_key,
                                            &peers,
                                            &caps,
                                            &cache,
                                            &cache_hits,
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
        db: &Arc<Hako>,
        collection: &str,
        key: &str,
        timestamp: i64,
    ) -> bool {
        match db.get(collection, key) {
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
    /// `collection` is both the shard read and the local-only check —
    /// the handshake must not leak what the tailers withhold.
    #[cfg(feature = "cloud-sync")]
    fn collect_catchup_ops(
        db: &Arc<Hako>,
        collection: &str,
        since_ts: i64,
    ) -> Vec<WalOp> {
        let Ok(shard_arc) = db.get_shard(collection) else {
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
                if db.is_local_only(collection, &key) {
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

    /// Catch-up/chunk wire budget: a page must never approach the default
    /// tungstenite frame limit (16 MiB) — issue #69's single-frame history
    /// bug. 1000 ops of ~500 B docs ≈ 0.6 MB; the byte cap splits big-doc
    /// pages long before any transport limit can be hit.
    const CATCHUP_PAGE_MAX_OPS: usize = 1000;
    const CATCHUP_PAGE_MAX_BYTES: usize = 6 * 1024 * 1024;

    /// Conservative serialized-size estimate for wire budgeting
    /// (rmp_named + docs routing overhead per op; never an under-count).
    fn op_wire_len(op: &WalOp) -> usize {
        match op {
            WalOp::PutInlined { key, value } => key.len() + value.len() + 128,
            WalOp::PutBlob { key, .. } | WalOp::Put { key, .. } => key.len() + 96,
            WalOp::Delete { key, .. } => key.len() + 64,
            WalOp::BeginTx { .. } | WalOp::CommitTx { .. } => 48,
        }
    }

    /// Split a delta batch into wire-safe pages. Empty input → no pages.
    fn page_out_ops(ops: Vec<WalOp>) -> Vec<Vec<WalOp>> {
        let mut pages: Vec<Vec<WalOp>> = Vec::new();
        let mut cur: Vec<WalOp> = Vec::new();
        let mut cur_bytes = 0usize;
        for op in ops {
            let len = Self::op_wire_len(&op);
            let full = !cur.is_empty()
                && (cur.len() >= Self::CATCHUP_PAGE_MAX_OPS
                    || cur_bytes + len > Self::CATCHUP_PAGE_MAX_BYTES);
            if full {
                pages.push(std::mem::take(&mut cur));
                cur_bytes = 0;
            }
            cur_bytes += len;
            cur.push(op);
        }
        if !cur.is_empty() {
            pages.push(cur);
        }
        pages
    }

    async fn send_catchup_deltas(
        db: &Arc<Hako>,
        collection: &str,
        client_ts: i64,
        server_ts: i64,
        peer_key: &str,
        peers: &Arc<AsyncRwLock<HashMap<String, PeerInfo>>>,
        caps: &Arc<crate::sync_guard::CapsMap>,
        cache: &Arc<StdMutex<HashMap<String, CatchupCacheEntry>>>,
        cache_hits: &Arc<AtomicUsize>,
    ) {
        // Sender rule: a locally-encrypted collection only replays to a
        // fingerprint-verified peer. Old/silent peers pause here loudly
        // instead of leaking on catch-up. Sync-excluded plane is never
        // served: a collection unfortunately named like an excluded
        // store must not reach members.
        if db.is_sync_withheld(collection)
        {
            return;
        }
        if db.is_collection_encrypted(collection) {
            let local_fp =
                crate::sync_guard::local_fingerprint(db.config.encryption_key.as_deref());
            let peer = caps.get(peer_key);
            if !crate::sync_guard::caps_allow(true, local_fp, peer.as_ref()) {
                caps.warn(
                    peer_key,
                    collection,
                    &format!(
                        "withholding catch-up replay of encrypted '{collection}' from peer '{peer_key}' (unverified key)"
                    ),
                );
                return;
            }
        }
        // Shared page cache: fresh joiners of the same collection at the
        // same server_ts need IDENTICAL bytes — materialize + encode once,
        // send N times (Bytes/Message clones are refcount bumps). A version
        // bump replaces the entry on the next miss.
        let cached: Option<Arc<Vec<Message>>> = {
            let guard = cache.lock().unwrap();
            match guard.get(collection) {
                Some(entry) if entry.server_ts == server_ts && client_ts == 0 => {
                    cache_hits.fetch_add(1, Ordering::Relaxed);
                    Some(entry.packets.clone())
                }
                _ => None,
            }
        };

        let packets = match cached {
            Some(p) => p,
            None => {
                let ops = Self::collect_catchup_ops(db, collection, client_ts);
                if ops.is_empty() {
                    return;
                }
                let total_ops = ops.len();
                let pages = Self::page_out_ops(ops);
                let mut encoded: Vec<Message> = Vec::with_capacity(pages.len());
                for page_ops in pages {
                    let packet = CloudPacket::Replication {
                        msg_id: 0,
                        collection: collection.to_string(),
                        ops: page_ops,
                    };
                    match rmp_serde::to_vec_named(&packet) {
                        Ok(b) => encoded.push(Message::Binary(b.into())),
                        Err(e) => {
                            eprintln!(
                                "[sync] catchup '{collection}': encode failed: {e}"
                            );
                            return;
                        }
                    }
                }
                let packets = Arc::new(encoded);
            let mut guard = cache.lock().unwrap();
                guard.insert(
                    collection.to_string(),
                    CatchupCacheEntry { server_ts, packets: packets.clone() },
                );
                let _ = total_ops;
                packets
            }
        };

        let peers_guard = peers.read().await;
        let peer_tx = peers_guard.get(peer_key).map(|p| p.tx.clone());
        drop(peers_guard);
        let Some(client_tx) = peer_tx else { return };

        let total = packets.len();
        for (i, msg) in packets.iter().enumerate() {
            // Awaited send: backpressure against the peer queue instead of
            // a silent try_send overflow (issue #69: the old single-frame
            // path dropped everything over the frame cap with no error at
            // either end). On send failure the peer's handshake ts has not
            // advanced — next reconnect resumes naturally.
            if let Err(e) = client_tx.send(msg.clone()).await {
                eprintln!(
                    "[sync] catchup '{collection}' to '{peer_key}': page {i}/{total} failed: {e}; peer resends on next handshake"
                );
                return;
            }
        }
        eprintln!(
            "[sync] catchup '{collection}' to '{peer_key}': {total} page(s) delivered"
        );
    }

    /// Tails local WAL changes on the SERVER and broadcasts server-side
    /// writes to the connected clients. One database, one room — every
    /// synced collection belongs to the room by construction.
    fn spawn_server_outbound_tailer(
        &self,
        echo_cache: Arc<StdMutex<HashMap<String, i64>>>,
    ) {
        let db = self.db.clone();
        let active_peers = self.active_peers.clone();
        let running = self.running.clone();
        let caps_srv = self.caps.clone();

        tokio::task::spawn_blocking(move || {
            let mut offsets: HashMap<String, u64> = HashMap::new();

            while running.load(Ordering::Relaxed) {
                // Sync enumeration: hidden room collections take part; the
                // excluded plane is dropped by the enumerator itself.
                let cols = db.sync_collections().unwrap_or_default();

                for collection in cols {
                    // Never broadcast a sync-excluded collection.
                    if db.is_sync_withheld(&collection) {
                        continue;
                    }

                    if let Ok(shard_arc) = db.get_shard(&collection) {
                        let last_pos = *offsets.get(&collection).unwrap_or(&0);
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
                                    // (sync_core: shared decode + echo take, all tailers).
                                    let wal_ts = crate::sync_core::op_logical_time(&op);

                                    // Skip writes originating from client WebSockets (already handled)
                                    let ek = echo_key(&collection, &key);
                                    let is_echo = crate::sync_core::echo_take(&echo_cache, &ek, wal_ts);

                                    if !is_echo {
                                        // Local-only signal: server never fans marked ops out.
                                        if db.is_local_only(&collection, key) {
                                            continue;
                                        }
                                        let final_op = match op {
                                            WalOp::PutInlined { ref key, ref value } => {
                                                if let Some(mut doc) = HakoDoc::decode(value) {
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
                                    let enc_local = db.is_collection_encrypted(&collection);
                                    let local_fp = crate::sync_guard::local_fingerprint(
                                        db.config.encryption_key.as_deref(),
                                    );
                                    for page_ops in Self::page_out_ops(to_send).into_iter() {
                                        let packet = CloudPacket::Replication {
                                            msg_id: SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .unwrap()
                                                .as_micros(),
                                            collection: collection.clone(),
                                            ops: page_ops,
                                        };

                                        if let Ok(bytes) = rmp_serde::to_vec_named(&packet) {
                                            let bytes_len = bytes.len();
                                            let msg = Message::Binary(bytes.into());
                                            let rt = tokio::runtime::Handle::current();
                                            let peers_ptr = active_peers.clone();
                                            let caps_ptr = caps_srv.clone();
                                            let plain_c = collection.clone();
                                            rt.block_on(async move {
                                                let peers_guard = peers_ptr.read().await;
                                                for (peer_id, info) in peers_guard.iter() {
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
                                                    if let Err(se) = info.tx.try_send(msg.clone()) {
                                                        eprintln!(
                                                            "[sync] fan-out '{plain_c}' to '{peer_id}': queue send failed ({} B): {}",
                                                            bytes_len,
                                                            se
                                                        );
                                                    }
                                                }
                                            });
                                        }
                                    }
                                }

                                offsets.insert(collection, new_pos);
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

        // Outbound queue is DEEP memory (CloudPacket owns its ops), unlike
        // the hub's Binary(Bytes) queues. A stalled WAN link must not let
        // the staged queue grow unbounded: 256 pages × 6 MB cap bounds the
        // worst case, and the tailer blocks (backpressure onto the WAL
        // tail) instead of ballooning RAM.
        let (outbound_tx, mut outbound_rx) = mpsc::channel::<CloudPacket>(256);
        *self.outbound_tx.lock().await = Some(outbound_tx.clone());

        self.spawn_client_outbound_tailer(echo_cache);

        tokio::spawn(async move {
            while running.load(Ordering::Relaxed) {
                let ws_result = tokio_tungstenite::connect_async_with_config(
                    &ws_url,
                    Some(ws_transport_config()),
                    true,
                )
                .await;
                if matches!(ws_result, Ok(_)) {
                    let (mut ws_tx, mut ws_rx) = ws_result.map(|(s, _)| s.split()).unwrap();

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
                            .sync_collections()
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
                        // Auth round-trip completed: the relay lag meter
                        // and the lease freshness both live off this.
                        note_cloud_exchange();
                        let ping = CloudPacket::VersionPing {
                            versions: db.get_version_map(),
                            indexes: Some(db.list_indexes(None)),
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
                                    ws_next = ws_rx.next() => {
                                        match ws_next {
                                    Some(Ok(msg)) => {
                                        if let Message::Binary(b) = msg {
                                            if let Ok(packet) = rmp_serde::from_slice::<CloudPacket>(&b) {
                                                // Any decoded server packet
                                                // proves the cloud path is
                                                // alive right now.
                                                note_cloud_exchange();
                                                match packet {
                                                    CloudPacket::VersionPing { versions: server_versions, indexes } => {
                                                        // Server names are room-plain = client
                                                        // namespace: apply directly (issue #51).
                                                        if let Some(idx) = indexes {
                                                            crate::sync_core::apply_peer_index_defs(&db, &idx);
                                                        }
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
                                    Some(Err(e)) => {
                                        // Issue #69: read failures were
                                        // invisible — including the old
                                        // 16 MiB frame-cap rejections that
                                        // silently starved catchups.
                                        eprintln!("[cloud-sync] ws error on {ws_url}: {e}");
                                        break;
                                    }
                                    None => {
                                        eprintln!("[cloud-sync] ws closed by peer");
                                        break;
                                    }
                                        }
                                    }
                                    else => break,
                                }
                            }
                        }
                    }
                } else {
                    // Issue #69: reconnect cycles were fully silent — a client
                    // that cannot reach the hub used to sit there forever
                    // with no trace, its last_ts frozen.
                    eprintln!(
                        "[cloud-sync] connect {} failed: {}",
                        ws_url,
                        ws_result.err().map(|e| e.to_string()).unwrap_or_default()
                    );
                }

                tokio::time::sleep(Duration::from_secs(3)).await;
            }
        });

        Ok(())
    }

    async fn push_client_deltas_upstream(
        db: &Arc<Hako>,
        collection: &str,
        server_ts: i64,
        outbound_tx: &mpsc::Sender<CloudPacket>,
    ) {
        // NOTE: no encryption gate here by design (see client tailer above):
        // the hub enforces on receipt and relay.
        // Sync-excluded plane never pushes upstream (defense in depth:
        // enumeration already drops it, but an explicit check keeps it
        // true if collection routing ever bypasses enumeration).
        if db.is_sync_withheld(collection) {
            return;
        }
        let ops = Self::collect_catchup_ops(db, collection, server_ts);
        if ops.is_empty() {
            return;
        }
        let total_ops = ops.len();
        let mut op_written = 0usize;
        for (i, page_ops) in Self::page_out_ops(ops).into_iter().enumerate() {
            op_written += page_ops.len();
            // Distinct msg_id per page so the hub's dedupe window never
            // swallows a sibling page.
            let msg_id = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_micros()
                + (i as u128);
            let packet = CloudPacket::Replication {
                msg_id,
                collection: collection.to_string(),
                ops: page_ops,
            };
            // Pages stay under the transport frame cap (issue #69); a
            // failed push (hub down / queue closed) is a real condition —
            // announce instead of dropping half silently.
            if outbound_tx.send(packet).await.is_err() {
                eprintln!(
                    "[sync] upstream '{collection}': push failed at page {}/{} ({} ops sent of {})",
                    i + 1,
                    (op_written / Self::CATCHUP_PAGE_MAX_OPS) + 1,
                    op_written,
                    total_ops
                );
                return;
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
                    drop(outbound_guard);
                    std::thread::sleep(Duration::from_millis(500));
                    continue;
                };

                // Sync enumeration (hidden included, excluded dropped) —
                // the explicit skip below stays as defense in depth.
                let cols = db.sync_collections().unwrap_or_default();

                for col in cols {
                    // Sync-state, room registry, and admin credential stores
                    // never leave the device. (`__hako_security` keeps
                    // flowing — policies replicate.)
                    if db.is_sync_withheld(&col) {
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
                                    // (sync_core: shared decode + echo take, all tailers).
                                    let wal_ts = crate::sync_core::op_logical_time(&op);

                                    let is_echo = crate::sync_core::echo_take(&echo_cache, &echo_key(&col, &key), wal_ts);

                                    if !is_echo {
                                        // Local-only signal: client never pushes marked ops upstream.
                                        if db.is_local_only(&col, key) {
                                            continue;
                                        }
                                        let final_op = match op {
                                            WalOp::PutInlined { ref key, ref value } => {
                                                if let Some(mut doc) = HakoDoc::decode(value) {
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
                                    for (i, page_ops) in
                                        Self::page_out_ops(to_send).into_iter().enumerate()
                                    {
                                        let packet = CloudPacket::Replication {
                                            msg_id: SystemTime::now()
                                                .duration_since(UNIX_EPOCH)
                                                .unwrap()
                                                .as_micros(),
                                            collection: col.clone(),
                                            ops: page_ops,
                                        };
                                        if tx.blocking_send(packet).is_err() {
                                            // Hub-side outbound channel is closed
                                            // (client shutting down / reconnect
                                            // abandons the old one) — logged,
                                            // not swallowed.
                                            eprintln!(
                                                "[cloud] tailer '{col}': outbound closed at page {}",
                                                i + 1
                                            );
                                            break;
                                        }
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
}

#[cfg(all(test, feature = "cloud-sync"))]
mod tests {
    use super::*;
    use crate::config::{DurabilityMode, HakoConfig};

    fn temp_db(tag: &str) -> (Arc<Hako>, std::path::PathBuf) {
        let dir = std::env::temp_dir().join(format!(
            "hakodb-rooms-{}-{}",
            std::process::id(),
            tag
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        let db = Arc::new(Hako::open(&dir, cfg).unwrap());
        (db, dir)
    }

    #[test]
    fn server_constructor_binds_the_room() {
        let (db, _dir) = temp_db("serverbind");
        let s = CloudSync::server(db, "hub-1", "tok", "game", "s3cret");
        assert_eq!(s.room_name, "game");
        assert_eq!(s.room_key, "s3cret");
        assert_eq!(s.mode, CloudSyncMode::Server);
        let st = s.status();
        assert_eq!(st.room_name, "game");
    }

    #[test]
    fn status_serialization_never_leaks_room_key() {
        let (db, _dir) = temp_db("statusredact");
        let s = CloudSync::server(db, "hub-1", "tok", "game", "s3cret");
        let full = s.status();
        assert_eq!(full.room_key, "s3cret");

        // Direct serialization of the full status is already safe.
        let full_json = serde_json::to_string(&full).expect("full status json");
        assert!(!full_json.contains("room_key"));
        assert!(!full_json.contains("s3cret"));

        let redacted = s.status_redacted();
        let redacted_json = serde_json::to_string(&redacted).expect("redacted status json");
        assert!(!redacted_json.contains("room_key"));
        assert!(!redacted_json.contains("s3cret"));
        assert_eq!(redacted.room_name, "game");
        assert_eq!(redacted.mode, CloudSyncMode::Server);
    }

    #[test]
    fn client_constructor_binds_the_room() {
        let (db, _dir) = temp_db("clientbind");
        let c = CloudSync::client(db, "node-7", "game", "s3cret", "tok");
        assert_eq!(c.room_name, "game");
        assert_eq!(c.room_key, "s3cret");
        assert_eq!(c.mode, CloudSyncMode::Client);
    }

    fn put_simple(db: &Arc<Hako>, col: &str, id: &str) {
        let mut doc = HakoDoc::default();
        doc.insert("v", Value::Int(1));
        db.put(col, id, &doc).unwrap();
    }

    #[test]
    fn catchup_emits_puts_and_tombstones() {
        let (db, _dir) = temp_db("catchup");
        put_simple(&db, "c", "keep");
        put_simple(&db, "c", "gone");
        db.delete("c", "gone").unwrap();

        let ops = CloudSync::collect_catchup_ops(&db, "c", 0);
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
        let ops = CloudSync::collect_catchup_ops(&db, "c", 0);
        assert_eq!(ops.len(), 1, "local-only tombstone leaked: {ops:?}");

        // ...and the clock still advanced past it (deleter looks ahead).
        let v = db.get_collection_version("c").unwrap();
        assert!(v > 0);

        // Collection-level mark: nothing leaves at all.
        db.set_collection_local("c", true);
        let ops = CloudSync::collect_catchup_ops(&db, "c", 0);
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

    #[tokio::test]
    async fn ingest_drops_sync_excluded_plane() {        // A hostile Replication{collection:"__groups"} must neither plant a
        // same-named shard nor poison the real credential store — while
        // legit room traffic still applies.
        let (db, _dir) = temp_db("excluded-ingest");
        put_simple(&db, "seed", "k");
        let hostile_value = db
            .get("seed", "k")
            .unwrap()
            .unwrap()
            .encode_buffered();
        let hostile = |collection: &str| IngestItem {
            collection: collection.to_string(),
            op: WalOp::PutInlined {
                key: "evil".to_string(),
                value: hostile_value.clone(),
            },
            sender_client_id: Some("mallory".to_string()),
        };
        let legit = IngestItem {
            collection: "users".to_string(),
            op: WalOp::PutInlined {
                key: "ok".to_string(),
                value: hostile_value.clone(),
            },
            sender_client_id: Some("mallory".to_string()),
        };
        let mut buffer: std::collections::HashMap<String, Vec<IngestItem>> =
            std::collections::HashMap::new();
        buffer.insert("__groups".to_string(), vec![hostile("__groups")]);
        buffer.insert("__users".to_string(), vec![hostile("__users")]);
        buffer.insert("users".to_string(), vec![legit]);
        let echo_cache = Arc::new(StdMutex::new(
            std::collections::HashMap::<String, i64>::new(),
        ));
        let peers = Arc::new(AsyncRwLock::new(
            std::collections::HashMap::<String, PeerInfo>::new(),
        ));
        let caps = Arc::new(crate::sync_guard::CapsMap::default());
        CloudSync::flush_ingest_buffer(&db, &mut buffer, &echo_cache, &peers, &caps)
            .await;

        assert!(
            db.get("__groups", "evil").unwrap().is_none(),
            "hostile packet poisoned the real credential store"
        );
        assert!(
            db.get("__users", "evil").unwrap().is_none(),
            "hostile packet poisoned the user store"
        );
        assert!(
            db.get("users", "ok").unwrap().is_some(),
            "legit room traffic must still apply"
        );
    }

    #[test]
    fn sync_enumeration_is_opt_out_not_underscore() {
        // Sync enumeration includes `_`-hidden collections and drops only
        // the explicit excluded plane (builtin + per-deployment extras).
        let (db, _dir) = temp_db("sync-enum");
        put_simple(&db, "users", "a");
        put_simple(&db, "_hidden", "b");
        put_simple(&db, "__hako_security", "c");
        put_simple(&db, "__groups", "d");
        put_simple(&db, "__hako_system", "e");

        let cols = db.sync_collections().unwrap();
        assert!(cols.contains(&"users".to_string()), "plain missing: {cols:?}");
        assert!(
            cols.contains(&"_hidden".to_string()),
            "hidden non-excluded must sync: {cols:?}"
        );
        assert!(
            cols.contains(&"__hako_security".to_string()),
            "security replicates by design: {cols:?}"
        );
        for excluded in [
            "__groups",
            "__users",
            "__hako_rooms",
            "__hako_system",
        ] {
            assert!(
                !cols.contains(&excluded.to_string()),
                "excluded plane leaked into enumeration: {cols:?}"
            );
        }
        // User listing keeps `_` hiding (unchanged behavior).
        let listed = db.list_collections().unwrap();
        assert!(!listed.contains(&"_hidden".to_string()));

        // Per-deployment extras ride along.
        let dir2 = std::env::temp_dir().join(format!(
            "hakodb-rooms-{}-sync-enum-extra",
            std::process::id()
        ));
        let _ = std::fs::remove_dir_all(&dir2);
        std::fs::create_dir_all(&dir2).unwrap();
        let mut cfg = HakoConfig::default();
        cfg.durability_mode = DurabilityMode::Manual;
        cfg.sync_excluded = vec!["_hidden".to_string()];
        let db2 = Arc::new(Hako::open(&dir2, cfg).unwrap());
        put_simple(&db2, "users", "a");
        put_simple(&db2, "_hidden", "b");
        let cols2 = db2.sync_collections().unwrap();
        assert!(cols2.contains(&"users".to_string()));
        assert!(
            !cols2.contains(&"_hidden".to_string()),
            "config extra must drop: {cols2:?}"
        );
        assert!(db2.is_sync_excluded_effective("_hidden"));
        assert!(db2.is_sync_excluded_effective("__groups"));
        assert!(!db2.is_sync_excluded_effective("users"));
        let _ = std::fs::remove_dir_all(&dir2);
    }

    fn put_group(
        db: &Arc<Hako>,
        room: &str,
        mode: &str,
        key_hash: Option<String>,
        members: Vec<String>,
    ) {
        let mut doc = HakoDoc::default();
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

    // ---- issue #69: catch-up paging (Stage A) ----------------------------

    fn small_ops(n: usize) -> Vec<WalOp> {
        (0..n)
            .map(|i| WalOp::PutInlined { key: format!("k{i:04}"), value: vec![7u8; 256] })
            .collect()
    }

    #[test]
    fn pages_split_at_doc_cap() {
        let pages = CloudSync::page_out_ops(small_ops(2500));
        assert_eq!(pages.len(), 3);
        assert_eq!(pages[0].len(), CloudSync::CATCHUP_PAGE_MAX_OPS);
        assert_eq!(pages[1].len(), CloudSync::CATCHUP_PAGE_MAX_OPS);
        assert_eq!(pages[2].len(), 500);
    }

    #[test]
    fn pages_split_at_byte_cap_before_doc_cap() {
        // 8 KB docs: doc cap allows 1000 (≈8.6 MB over budget) — the byte
        // budget must trip first.
        let ops: Vec<WalOp> = (0..3000)
            .map(|i| WalOp::PutInlined { key: format!("k{i:04}"), value: vec![1u8; 8192] })
            .collect();
        let pages = CloudSync::page_out_ops(ops);
        // ~8.3 KB wire each: the byte budget trips before the 1000-doc cap.
        assert_eq!(pages.len(), 4);
        assert!(
            pages.iter().all(|p| p.len() < CloudSync::CATCHUP_PAGE_MAX_OPS),
            "byte budget must bound page size first"
        );
    }

    #[test]
    fn single_op_over_budget_is_one_page_not_a_split() {
        // One op alone over the byte budget cannot be split further; it
        // must still flow as a page (same exposure as the pre-pager single
        // frame), and the transport frame cap is the honest ceiling.
        let big = WalOp::PutInlined { key: "big".into(), value: vec![9u8; CloudSync::CATCHUP_PAGE_MAX_BYTES + 4096] };
        let pages = CloudSync::page_out_ops(vec![big]);
        assert_eq!(pages.len(), 1);
        assert_eq!(pages[0].len(), 1);
    }

    #[test]
    fn empty_batch_pages_to_nothing() {
        assert!(CloudSync::page_out_ops(Vec::new()).is_empty());
    }

    // ---- issue #69: transport bounds shared by both faces ----------------

    #[test]
    fn ws_transport_is_bounded_and_legacy_friendly() {
        // Non-regression guard: the shared transport config must (a) be able
        // to receive a big legacy single frame instead of silently failing
        // the peer, and (b) never let the outbound write accumulator balloon
        // memory under thousands of stalled peers (was usize::MAX).
        let cfg = ws_transport_config();
        assert_eq!(cfg.max_frame_size, Some(64 << 20));
        assert_eq!(cfg.max_message_size, Some(64 << 20));
        assert_eq!(cfg.max_write_buffer_size, 16 << 20);
    }

    #[test]
    fn no_page_ever_approaches_the_transport_cap() {
        // Every page with ≥2 ops must sit under the byte budget + one op;
        // the classic stress shape (40k × ~543 B wire ≈ 21 MB single frame)
        // must end up ~21 small pages.
        let pages = CloudSync::page_out_ops(small_ops(40_000));
        assert!(pages.len() >= 40);
        for (i, p) in pages.iter().enumerate() {
            let bytes: usize = p.iter().map(CloudSync::op_wire_len).sum();
            assert!(
                bytes <= CloudSync::CATCHUP_PAGE_MAX_BYTES
                    || (i == pages.len() - 1 && p.len() == 1),
                "page {} over budget: {} B in {} ops",
                i,
                bytes,
                p.len()
            );
        }
    }

    /// #72 lag meter: a successful exchange stamps the process-wide clock;
    /// the getter exposes it (None = never reached the cloud).
    #[test]
    fn cloud_last_exchange_tracks_round_trips() {
        // Any pre-existing value (earlier tests in this binary may have
        // exchanged) only ever moves FORWARD from a fresh stamp.
        let before = cloud_last_exchange_millis();
        note_cloud_exchange();
        let after = cloud_last_exchange_millis().expect("stamp must be visible");
        if let Some(b) = before {
            assert!(after >= b, "clock must be monotonic: {b} -> {after}");
        }
    }

    /// #72 admission control: cap 1 admits the first client, rejects a
    /// NEW client with the typed loud error, and still admits a
    /// reconnect of the SAME client id (stale-entry replacement).
    #[tokio::test]
    async fn room_at_capacity_rejects_new_clients() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let (db, _dir) = temp_db("cap-room");
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            p
        };
        let server = CloudSync::server(db, "hub", "tok-cap", "caproom", "k-cap");
        server.set_max_clients_per_room(1);
        server.start(&format!("127.0.0.1:{port}")).await.unwrap();

        let url = format!("ws://127.0.0.1:{port}");
        let auth = |cid: &str| {
            rmp_serde::to_vec_named(&CloudPacket::Authenticate {
                token: "tok-cap".into(),
                client_id: cid.into(),
                room_name: "caproom".into(),
                room_key: "k-cap".into(),
                api_key: None,
                client_version: Some("t".into()),
                enc_fp: None,
                enc_cols: Vec::new(),
            })
            .unwrap()
        };

        // Client 1: admitted.
        let (mut ws1, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws1.send(Message::Binary(auth("c1").into())).await.unwrap();
        let Some(Ok(Message::Binary(b))) = ws1.next().await else {
            panic!("no auth reply for c1");
        };
        match rmp_serde::from_slice::<CloudPacket>(&b).unwrap() {
            CloudPacket::AuthResult { success: true, .. } => {}
            other => panic!("c1 must be admitted, got {other:?}"),
        }

        // Client 2 (new id): beyond the cap — typed loud reject.
        let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws2.send(Message::Binary(auth("c2").into())).await.unwrap();
        let Some(Ok(Message::Binary(b))) = ws2.next().await else {
            panic!("no auth reply for c2");
        };
        match rmp_serde::from_slice::<CloudPacket>(&b).unwrap() {
            CloudPacket::AuthResult { success: false, error } => {
                assert_eq!(
                    error.as_deref(),
                    Some("room at capacity"),
                    "the reject must be typed and loud"
                );
            }
            other => panic!("c2 must be rejected, got {other:?}"),
        }

        // Client 1 reconnects (same id): stale-entry replacement admits.
        drop(ws1);
        let (mut ws1b, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        ws1b.send(Message::Binary(auth("c1").into())).await.unwrap();
        let Some(Ok(Message::Binary(b))) = ws1b.next().await else {
            panic!("no auth reply for c1 reconnect");
        };
        match rmp_serde::from_slice::<CloudPacket>(&b).unwrap() {
            CloudPacket::AuthResult { success: true, .. } => {}
            other => panic!("c1 reconnect must be admitted, got {other:?}"),
        }
        server.stop();
    }

    /// Shared catch-up page cache: the second fresh joiner of the same
    /// collection at the same server_ts is served from the cache (one
    /// materialization + encode, N sends) — the thundering-join HOL fix.
    #[tokio::test]
    async fn catchup_pages_shared_across_fresh_joiners() {
        use futures_util::{SinkExt, StreamExt};
        use tokio_tungstenite::tungstenite::Message;

        let (db, _dir) = temp_db("cache-room");
        // 2500 docs x ~200B: three 1000-op pages per fresh join.
        let mut doc = HakoDoc::default();
        doc.insert("v", Value::String("x".repeat(180)));
        for i in 0..2500 {
            db.put("docs", &format!("k{i:05}"), &doc).unwrap();
        }
        let port = {
            let l = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
            let p = l.local_addr().unwrap().port();
            drop(l);
            p
        };
        let server = CloudSync::server(db, "hub", "tok-cc", "ccroom", "k-cc");
        server.start(&format!("127.0.0.1:{port}")).await.unwrap();
        let url = format!("ws://127.0.0.1:{port}");

        let join_and_count = |cid: &'static str| {
            let url = url.clone();
            async move {
                let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
                let auth = rmp_serde::to_vec_named(&CloudPacket::Authenticate {
                    token: "tok-cc".into(),
                    client_id: cid.into(),
                    room_name: "ccroom".into(),
                    room_key: "k-cc".into(),
                    api_key: None,
                    client_version: Some("t".into()),
                    enc_fp: None,
                    enc_cols: Vec::new(),
                })
                .unwrap();
                ws.send(Message::Binary(auth.into())).await.unwrap();
                let Some(Ok(Message::Binary(b))) = ws.next().await else {
                    panic!("{cid}: no auth reply");
                };
                assert!(matches!(
                    rmp_serde::from_slice::<CloudPacket>(&b).unwrap(),
                    CloudPacket::AuthResult { success: true, .. }
                ));
                let ping = rmp_serde::to_vec_named(&CloudPacket::VersionPing {
                    versions: HashMap::new(),
                    indexes: None,
                })
                .unwrap();
                ws.send(Message::Binary(ping.into())).await.unwrap();

                let mut ops = 0usize;
                let deadline = tokio::time::Instant::now() + Duration::from_secs(30);
                while ops < 2500 {
                    assert!(tokio::time::Instant::now() < deadline, "{cid}: timeout at {ops}");
                    let Some(Ok(Message::Binary(b))) = tokio::time::timeout(
                        Duration::from_secs(10),
                        ws.next(),
                    )
                    .await
                    .ok()
                    .flatten()
                    else {
                        break;
                    };
                    if let Ok(CloudPacket::Replication { ops: page, .. }) =
                        rmp_serde::from_slice::<CloudPacket>(&b)
                    {
                        ops += page.len();
                    }
                }
                ws.close(None).await.ok();
                ops
            }
        };

        let got1 = join_and_count("cc1").await;
        assert_eq!(got1, 2500, "first joiner materializes and receives all");
        let got2 = join_and_count("cc2").await;
        assert_eq!(got2, 2500, "second joiner must receive the identical stream");
        assert!(
            server.catchup_cache_hits() >= 1,
            "second join must be served from the shared cache"
        );
        server.stop();
    }
}
