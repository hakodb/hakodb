//! socket_sync: co-located instance sync over unix sockets (0.9.0).
//!
//! Same sync_core machinery as mesh/cloud — only the transport is new:
//! length-prefix framing over `tokio::net::UnixStream`, no discovery
//! (explicit paths), no relay, no rooms. Built for the balancer use-case:
//! N instances on one box, byte-identical data, single writer.
//!
//! Protocol (bincode, like mesh framing, unlike cloud msgpack):
//! ```text
//!   dialer/accepted -> Hello { versions }   (both directions)
//!   either side     -> Snapshot { collection, ops }  (full live + tombs)
//!   either side     -> Data { collection, ops }      (live tail, 500ms)
//! ```
//! Full snapshot per connect (not delta): correct via LWW dedup, simple
//! to reason about. Delta optimization later if snapshots get heavy.
//! Unix-only (AF_UNIX in tokio is cfg(unix)); the feature is a silent
//! no-op on Windows so default builds stay green there.

use std::collections::{HashMap, HashSet};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{UnixListener, UnixStream};

use crate::document::hako_doc::HakoDoc;
use crate::engine::Hako;
use crate::storage::engine::Pointer;
use crate::storage::wal::WalOp;
use crate::sync_core::{apply_replicated_batch, echo_take, op_logical_time, resolve_send_bytes};

/// Fail-closed frame cap: a corrupt length prefix must not allocate the
/// box. Snapshots bigger than this fail the connection, not the process.
const MAX_FRAME: usize = 256 * 1024 * 1024;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
enum SockPacket {
    Hello { versions: HashMap<String, i64> },
    Snapshot { collection: String, ops: Vec<WalOp> },
    Data { collection: String, ops: Vec<WalOp> },
}

/// Frame a packet (length-prefix + bincode), pure bytes for the channel.
fn frame_packet(pkt: &SockPacket) -> std::io::Result<Vec<u8>> {
    let bytes = bincode::serialize(pkt)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    if bytes.len() > MAX_FRAME {
        return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "frame too large"));
    }
    let mut frame = Vec::with_capacity(4 + bytes.len());
    frame.extend_from_slice(&(bytes.len() as u32).to_le_bytes());
    frame.extend_from_slice(&bytes);
    Ok(frame)
}

/// One side of a socket peering: shared tailer + ingest state. Both the
/// accepted and the dialed socket run the same handler after Hello.
pub struct SocketSync {
    db: Arc<Hako>,
    echo_cache: Arc<Mutex<HashMap<String, i64>>>,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
    peers: Arc<Mutex<HashSet<String>>>,
    excluded: HashSet<String>,
    running: AtomicBool,
}

impl SocketSync {
    pub fn new(db: Arc<Hako>, mut excluded: Vec<String>) -> Self {
        excluded.extend(
            crate::engine::engine::SYNC_EXCLUDED_COLLECTIONS
                .iter()
                .map(|s| s.to_string()),
        );
        excluded.extend(db.config.sync_excluded.iter().cloned());
        Self {
            db,
            echo_cache: Arc::new(Mutex::new(HashMap::new())),
            tasks: Mutex::new(Vec::new()),
            peers: Arc::new(Mutex::new(HashSet::new())),
            excluded: excluded.into_iter().collect(),
            running: AtomicBool::new(false),
        }
    }

    /// Listen on a socket path (stale file from a crash is cleared first).
    pub fn serve(&self, path: &str) -> std::io::Result<()> {
        let _ = std::fs::remove_file(path);
        let listener = std::os::unix::net::UnixListener::bind(path)?;
        listener.set_nonblocking(true)?;
        let listener = UnixListener::from_std(listener)?;
        self.running.store(true, Ordering::Release);
        let this = self.shared_state();
        let h = tokio::spawn(async move {
            loop {
                if !this.running.load(Ordering::Acquire) {
                    break;
                }
                match listener.accept().await {
                    Ok((stream, _)) => {
                        let n = this.conn_next();
                        let st = this.clone();
                        tokio::spawn(async move {
                            handle_conn(st, stream, format!("in-{n}")).await;
                        });
                    }
                    Err(_) => {
                        tokio::time::sleep(std::time::Duration::from_millis(100)).await;
                    }
                }
            }
        });
        self.tasks.lock().unwrap().push(h);
        Ok(())
    }

    /// Connect to a listening peer. Returns after the handshake; tailing
    /// and ingest continue in the background until stop().
    pub async fn dial(&self, path: &str) -> std::io::Result<()> {
        let stream = UnixStream::connect(path).await?;
        self.running.store(true, Ordering::Release);
        let st = self.shared_state();
        let n = st.conn_next();
        let h = tokio::spawn(async move {
            handle_conn(st, stream, format!("out-{n}")).await;
        });
        self.tasks.lock().unwrap().push(h);
        Ok(())
    }

    pub fn stop(&self) {
        self.running.store(false, Ordering::Release);
        let mut guard = self.tasks.lock().unwrap();
        for h in guard.drain(..) {
            h.abort();
        }
        self.peers.lock().unwrap().clear();
    }

    pub fn peer_count(&self) -> usize {
        self.peers.lock().unwrap().len()
    }

    fn shared_state(&self) -> Shared {
        Shared {
            db: self.db.clone(),
            echo_cache: self.echo_cache.clone(),
            offsets: Arc::new(Mutex::new(HashMap::new())),
            peers: self.peers.clone(),
            excluded: self.excluded.clone(),
            running: Arc::new(AtomicBool::new(true)),
            conn_seq: Arc::new(std::sync::atomic::AtomicU64::new(0)),
        }
    }
}

/// Per-connection shared handles (the listener-level SocketSync keeps
/// only task bookkeeping; each peering owns its offsets/peer set).
#[derive(Clone)]
struct Shared {
    db: Arc<Hako>,
    echo_cache: Arc<Mutex<HashMap<String, i64>>>,
    offsets: Arc<Mutex<HashMap<String, u64>>>,
    peers: Arc<Mutex<HashSet<String>>>,
    excluded: HashSet<String>,
    running: Arc<AtomicBool>,
    conn_seq: Arc<std::sync::atomic::AtomicU64>,
}

impl Shared {
    fn conn_next(&self) -> u64 {
        self.conn_seq.fetch_add(1, Ordering::Relaxed)
    }
}

/// Full live + tombstone snapshot of one collection. LWW on ingest
/// dedups anything the peer already has — correctness without deltas.
fn snapshot_collection(db: &Arc<Hako>, col: &str) -> Vec<WalOp> {
    let shard_arc = match db.get_shard(col) {
        Ok(s) => s,
        Err(_) => return Vec::new(),
    };
    let guard = match shard_arc.read() {
        Ok(g) => g,
        Err(_) => return Vec::new(),
    };
    let mut ops = Vec::new();
    for (key, ptr) in guard.index.iter() {
        match ptr {
            Pointer::Deleted { timestamp } => {
                ops.push(WalOp::Delete { key: key.clone(), timestamp: *timestamp });
            }
            _ => {
                if let Ok(Some(bytes)) = guard.read_pointer_internal(ptr, false) {
                    if HakoDoc::decode(&bytes).is_some() {
                        ops.push(WalOp::PutInlined { key: key.clone(), value: bytes });
                    }
                }
            }
        }
    }
    ops
}

async fn handle_conn(st: Shared, stream: UnixStream, label: String) {
    let (rd, wr) = stream.into_split();
    // Single writer task per connection (tokio MutexGuard doesn't forward
    // AsyncWrite): hello/snapshot/tailer all send framed bytes through
    // this channel; a dead peer ends the writer, which ends the tailer.
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Vec<u8>>();
    let writer = tokio::spawn(async move {
        let mut wr = wr;
        while let Some(frame) = rx.recv().await {
            if wr.write_all(&frame).await.is_err() {
                break;
            }
        }
    });
    let send = |pkt: &SockPacket| {
        frame_packet(pkt)
            .ok()
            .and_then(|f| tx.send(f).ok())
            .is_some()
    };
    // Tailer gets its own sender: writer death (peer gone) fails sends
    // and ends the tailer too.
    let tx_tail = tx.clone();

    // 1. Hello exchange (both directions).
    let versions = st.db.get_version_map();
    if !send(&SockPacket::Hello { versions }) {
        return;
    }
    let mut rd_buf = ReadHalf { inner: rd };
    let _peer_versions = match rd_buf.recv().await {
        Ok(SockPacket::Hello { versions }) => versions,
        _ => return,
    };
    st.peers.lock().unwrap().insert(label.clone());

    // 2. Full snapshot, both directions handled by symmetric send here
    // (the peer sends theirs; LWW dedups on ingest).
    for col in st.db.sync_collections().unwrap_or_default() {
        if st.excluded.contains(&col) {
            continue;
        }
        let ops = snapshot_collection(&st.db, &col);
        if ops.is_empty() {
            continue;
        }
        if !send(&SockPacket::Snapshot { collection: col, ops }) {
            break;
        }
    }

    // 3. Live tail (500ms, mesh cadence) + read loop, until stop/error.
    // The tailer sends through the channel; if the writer died (peer
    // gone), sends fail and the tailer exits too.
    let st_tail = st.clone();
    let tailer = tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(500));
        loop {
            interval.tick().await;
            if !st_tail.running.load(Ordering::Acquire) {
                break;
            }
            for col in st_tail.db.sync_collections().unwrap_or_default() {
                if st_tail.excluded.contains(&col) {
                    continue;
                }
                let shard_arc = match st_tail.db.get_shard(&col) {
                    Ok(s) => s,
                    Err(_) => continue,
                };
                let last_pos = *st_tail.offsets.lock().unwrap().get(&col).unwrap_or(&0);
                let (ops, new_pos) = {
                    let guard = match shard_arc.read() {
                        Ok(g) => g,
                        Err(_) => continue,
                    };
                    match guard.wal.tail(last_pos) {
                        Ok(t) => t,
                        Err(_) => continue,
                    }
                };
                if ops.is_empty() {
                    continue;
                }
                let mut logical = Vec::new();
                for op in ops {
                    let key = op.get_key().to_string();
                    let ts = op_logical_time(&op);
                    if echo_take(&st_tail.echo_cache, &key, ts) {
                        continue;
                    }
                    if st_tail.db.is_local_only(&col, &key) {
                        continue;
                    }
                    if let Some(bytes) = resolve_send_bytes(&shard_arc, &st_tail.db, &op) {
                        logical.push(WalOp::PutInlined { key, value: bytes });
                    } else if matches!(op, WalOp::Delete { .. }) {
                        logical.push(op);
                    }
                }
                st_tail.offsets.lock().unwrap().insert(col.clone(), new_pos);
                if logical.is_empty() {
                    continue;
                }
                if frame_packet(&SockPacket::Data { collection: col, ops: logical })
                    .ok()
                    .and_then(|f| tx_tail.send(f).ok())
                    .is_none()
                {
                    return;
                }
            }
        }
    });

    // 4. Read loop: ingest snapshots + data through the shared path.
    // Ending here drops our senders; the writer then drains and ends.
    loop {
        if !st.running.load(Ordering::Acquire) {
            break;
        }
        match rd_buf.recv().await {
            Ok(SockPacket::Snapshot { collection, ops })
            | Ok(SockPacket::Data { collection, ops }) => {
                apply_replicated_batch(st.db.clone(), collection, ops, st.echo_cache.clone()).await;
            }
            _ => break,
        }
    }
    tailer.abort();
    writer.abort();
    st.peers.lock().unwrap().remove(&label);
}

/// Owned read half with framing (UnixStream split halves borrow the
/// socket; this re-owns the read side for the read loop).
struct ReadHalf {
    inner: tokio::net::unix::OwnedReadHalf,
}

impl ReadHalf {
    async fn recv(&mut self) -> std::io::Result<SockPacket> {
        let mut len_buf = [0u8; 4];
        self.inner.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        if len == 0 || len > MAX_FRAME {
            return Err(std::io::Error::new(std::io::ErrorKind::InvalidData, "bad frame length"));
        }
        let mut buf = vec![0u8; len];
        self.inner.read_exact(&mut buf).await?;
        bincode::deserialize(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
    }
}
