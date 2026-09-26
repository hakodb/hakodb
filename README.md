# HakoDB

**HakoDB is an embedded, Firestore-style document database written in Rust.**

It stores typed JSON-like documents in binary form, runs **fully in-process** like SQLite (no server process, no daemon, no network config), and exposes a **flat C ABI** (`include/hakodb.h`) so any language can embed it. Ready-made SDKs live in their own repos — Go, JavaScript/TypeScript, Pascal/Lazarus, Tauri, and more (see [Repositories](#repositories)).

HakoDB speaks "documents", not tables: collections of flexible, schemaless objects with a query API that feels like Google Firestore (`collection().doc().set()`, `.where().orderBy().limit()`), while keeping the zero-deploy footprint of an embedded engine.

> **Current status: v0.8.29 (production-candidate).** The core engine supports physical data sharding, zero-copy field projection, near-instant recovery, composite + full-text + secondary indexing, encryption at rest, deferred blob fetching, bulk JSON result export, TopN heap for unindexed order+limit, and high-throughput local or cloud synchronization capable of **50,000+ OPS** under heavy concurrent workloads.

---

## What's new (0.7.2 → 0.8.29)

### v0.8.29 — cheaper batch path (apply −23%, seed −10%)
- Apply zips WAL ops with index puts positionally (lockstep push order)
  instead of building a per-batch key HashMap; hot-cache invalidation
  skips entirely when the cache is empty; shard-map entry no longer
  clones the collection key on hit. Server seed: 0.122→0.111s with
  apply 30→23ms and cache 5.3→0ms (single-fsync variance aside).

### v0.8.28 — zero-clone query fan-out (−20% unindexed scans)
- Task sharding moves id Strings (`drain`, no `to_vec` re-clone),
  matched ids move (no `to_string` per row), single-worker queries skip
  the HashMap order-restoration round-trip (2 hashes/row, only needed
  for parallel completion order). Server bench-lab: filter-eq
  0.020→0.015s ×3 runs, all other lanes flat, gate PASS.

### v0.8.27 — FxHash interning (+7–14% decode-heavy queries)
- Field-name interning pool moves from std `HashMap` (SipHash) to
  `FxHashMap`: every decoded field paid a SipHash before. hakobench gate
  A/B on quiet hardware: Qry +7%, Cmp +14%, Off/Cur +10%, Batch +14%,
  point-get flat (cache-hit, no decode). Zero behavior change.

### v0.8.26 — get fast path (lock-free gates, contention-free populate)
- `allowed()`: atomic no-rules gate skips the RwLock on every op when no
  security rules are set (get/query/batch all benefit).
- `get()`: hot-cache key alloc deferred until a version exists to compare;
  cache populate uses `try_write` so a contended cache no longer serializes
  concurrent readers (read hits 8-thread scaling).

### v0.8.25 — TopN heap for unindexed order+limit
- Unsatisfied `ORDER BY` + `LIMIT` no longer decodes every doc + full sort:
  key-only scan via views, bounded heap (limit+offset), exact stable-sort
  parity (scan-seq tiebreak), corrupt-row backfill. ~3-4x on 2000-doc
  ordered pages; over-fetch and cursor shapes keep the legacy path.
- Linux release matrix per distro family (glibc floors: EL8/Ubuntu22/
  Ubuntu24/Arch) + static musl-core (rlib-only) + existing Windows/Android.

### v0.8.24 — ordered limit-pushdown correctness + public Linux matrix
- Planner no longer pushes scan limits under unsatisfied `ORDER BY`
  (was wrong TOP-N for direct callers); per-distro Linux release assets.

### v0.8.23 — header rename + query-decode micro-opts
- C header renamed `hako.h` → `hakodb.h` (guard `HAKODB_H`); release
  bundles and satellite sync scripts follow the new filename.
- Query decode paths shed per-row allocs (projection borrow-compare,
  header-sized output Vec, thread-local match scratch) plus lazy filter
  key matching and dotted-path pulls (`DocView::get_path`, ~18× vs
  whole-subtree decode on nested fixtures).
- `cbindgen.toml` actually loads now (absolute path, valid keys); the
  generated header is real C with `extern "C"` guards.

### v0.8.22 — pre-rebrand aliases removed
- `SYNC_EXCLUDED` no longer recognizes the `__firelite_*` spellings;
  the open-time migration (`__firelite_*` → `__hako_*`) stays as the
  upgrade path — databases last opened by ≤0.8.20 migrate on first open
  with 0.8.21+. Leftover orphans from downgrade cycles are ordinary
  collection names now: remove them manually.
- Satellite repos renamed dash-less (`hakocli`, `hakocloudserver`,
  `hakotauri`, `hakobench`, `hakogo`, `hakojs`, `hakopascal`,
  `hakotaurits`); `hakodb 0.8.21` published to crates.io,
  `@hakodb/client` + `@hakodb/tauri` to npm.

### v0.8.21 — rebrand to HakoDB
- Crate `hakodb`, main type `Hako` (`HakoConfig`, `HakoDoc`,
  `HakoError`), FFI prefix `HK_*`/`hk_*`, header `include/hakodb.h`,
  binaries `hakodb.dll` / `libhakodb.so`.
- Data plane migrates on open: `__firelite_*` directories become their
  `__hako_*` canonical names with data intact (both-present keeps
  canonical; old spellings stay sync-excluded as aliases).
- Fixed along the way: `cbindgen.toml` never loaded (relative path +
  unknown fields → silent C++ defaults for years); the header is real
  C now, with `extern "C"` guards for C++ consumers.

## Table of Contents

- [What is HakoDB?](#what-is-hakodb)
- [What's new (0.7.2 → 0.8.23)](#whats-new-072--0823)
- [Changelog (older releases)](CHANGELOG.md)
- [When to use HakoDB (sync vs non-sync)](#when-to-use-hakodb-sync-vs-non-sync)
- [Key features](#key-features)
- [Quick Start (Rust)](#quick-start-rust)
- [Rust usage](#rust-usage)
- [Multi-language platform support (C ABI)](#multi-language-platform-support-c-abi)
- [Net Sync (LAN replication)](#net-sync-lan-replication)
- [Cloud Sync (centralized replication)](#cloud-sync-centralized-replication)
- [Sync encryption posture (read this before encrypting)](#sync-encryption-posture-read-this-before-encrypting)
- [Guide: choosing a read path](docs/reads.md)
- [Repositories](#repositories)
- [Architecture](#architecture)
- [Implementation status](#implementation-status)
- [Contribution notes](#contribution-notes)
- [License](#license)

---

## What is HakoDB?

HakoDB is a **document-oriented embedded database** for applications that want:

- **Firestore-like ergonomics** — collections, documents, `set/get/delete`, fluent queries, real-time change streams.
- **SQLite-style embedding** — link a library into your process and open a database file; there is nothing to install or operate.
- **Durability without sacrifice** — a WAL + tiered-segment storage engine with configurable durability (from full `fsync` per write to group-commit `on-commit` batches) and crash recovery.
- **Encryption at rest** — ChaCha20-Poly1305 encryption of WAL and segment payloads via a master key.
- **Real-time locally** — `watch_collection` streams document changes (`put` / `delete`) to subscribers in-process.
- **Synchronization when you need it** — two optional replication layers:
  - **Net Sync** (`net-sync` feature): peer-to-peer mesh replication over LAN with mDNS discovery.
  - **Cloud Sync** (`cloud-sync` feature): centralized client-server replication over WebSockets + MessagePack.

Because it is a library, HakoDB has no "database server" to manage. Your app *is* the database host. This makes it ideal for local-first and offline-first products, desktop and CLI tooling, edge devices, games, and apps that occasionally need to sync with the cloud or with each other.

---

## When to use HakoDB (sync vs non-sync)

| Scenario | Recommended mode | Why |
|---|---|---|
| Desktop / CLI / local tool needs a real DB with zero setup | **Embedded (no sync)** | In-process, single file, no services. |
| Offline-first mobile/edge/desktop app that syncs to a central backend | **Cloud Sync** (client) | Local reads/writes keep working offline; deltas sync over `ws://`/`wss://`. |
| Central hub collecting writes from many devices | **Cloud Sync** (server) | Thousands of concurrent WebSocket clients, 5ms micro-batched writes, WAL tailer broadcast. |
| Real-time multiplayer / collaborative session on one LAN | **Net Sync** (mesh) | mDNS discovery, room-key isolation, delta replication across peers. |
| Local app that must *also* be reachable by other processes/languages | **Embedded + FFI** | C ABI plus per-language SDK repos; watch streams for reactive UIs. |
| Analytics / ad-hoc queries over large datasets | **Embedded** | Composite indexes, FTS, aggregates, zero-copy projection, parallel scans. |

**In short:** use HakoDB **without sync** when your data is local to one process. Turn on **Net Sync** when you need peer-to-peer replication across devices on a network you control. Turn on **Cloud Sync** when you need offline-first clients to converge through a central server (or to build a real-time multi-client hub).

---

## Key features

- **Core engine API** — `open`, `put`, `get`, `delete`, `query`, `compact`, `flush`, `write_batch`.
- **Durable storage stack** — shard-oriented segment storage (`segment-l{level}-{id}.dat`) with tiered compaction; WAL with transactional markers and group-commit batching; committed-op recovery replay; index snapshot + WAL rewrite after compaction/recovery.
- **Durability tuning** — `DurabilityMode`: `Always`, `Interval`, `Manual`, `OnCommit`; group-commit control via `group_commit_max_ops`.
- **Encryption at rest** — optional key-based encryption (ChaCha20-Poly1305) for WAL and segment payloads; selectable encrypted collections.
- **Transactions** — `begin_transaction` + staged mutations + `commit`; `begin_serializable_transaction` with conflict-aware commit validation (read/write version checks).
- **Queries** — filters (`eq`, `ne`, `gt`, `gte`, `lt`, `lte`, `in`, `notIn`, `arrayContains`, `arrayContainsAny`, `match`, `contains`, `startsWith`); ordering, limit, offset, cursor bounds (`startAt/startAfter/endAt/endBefore`); cost-aware parallel planner; predicate + projection pushdown; `count/sum/avg` aggregates.
- **Indexing** — single-field secondary B-Tree indexes, composite indexes, inverted (full-text) indexes; async background index building.
- **Zero-copy projection** — queries cherry-pick fields directly from memory-mapped binary slices without inflating full documents.
- **Real-time watch streams** — `watch_collection` with change events; FFI bridge for cross-language reactive UIs.
- **Subcollections** — `put/get/delete/query` on hierarchical nested collections.
- **Security & operations** — collection-prefix allow/deny policy rules; in-memory and file-backed audit logging (`audit.log`).
- **Multi-language FFI** — opaque handle C ABI with document builder, CRUD, query, batch, transaction, watch, result-set and cloud-sync functions; per-language SDKs live in their own repos (see [Repositories](#repositories)).

---

## Quick Start (Rust)

```rust
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::{BatchMutation, Hako};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = HakoConfig::default();
    cfg.encryption_key = Some("change-me-secret".to_string());

    let db = Hako::open(".hakodb-example", cfg)?;

    let mut doc = HakoDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));

    db.put("users", "1", &doc)?;
    db.flush()?;
    Ok(())
}
```

---

## Rust usage

```rust
use hakodb::config::HakoConfig;
use hakodb::engine::{BatchMutation, Hako};

let db = Hako::open("./data", HakoConfig::default())?;

// atomic batch write
db.write_batch(vec![
    BatchMutation::Put {
        collection: "users".into(),
        doc_id: "1".into(),
        doc: /* HakoDoc */ doc1,
    },
    BatchMutation::Delete { collection: "users".into(), doc_id: "2".into() },
])?;

// serializable transaction
let mut tx = db.begin_serializable_transaction();
tx.get(&db, "accounts", "alice")?;
tx.put("accounts", "alice", updated_balance_doc);
let ids = tx.commit(&db)?;

// reactive watch
let rx = db.watch_collection("users");
while let Ok(event) = rx.recv() {
    println!("{:?} {}", event.kind, event.path);
}
```

See [`example/rust/basic`](example/rust/basic) for a complete, working example of the engine API.

> More Rust examples: [`example/rust/basic`](example/rust/basic) — a runnable
> Cargo project (`cargo run`) covering open, CRUD, query, aggregation, batch,
> transaction and the real-time watch channel.

---

## Multi-language platform support (C ABI)

HakoDB exposes a flat C ABI for embedding in other languages and integration layers. Opaque handle types are defined in `include/hakodb.h`. Language SDKs wrapping this ABI live in their own repos — see [Repositories](#repositories).

### Build artifacts

- Cargo crate types: `cdylib` (dynamic library consumers) and `rlib` (Rust consumers).
- Auto-generated C header via `build.rs` + `cbindgen.toml`: `include/hakodb.h`.

```bash
cargo build --release
```

Platform outputs:

- Linux: `target/release/libhakodb.so`
- macOS: `target/release/libhakodb.dylib`
- Windows: `target\release\hakodb.dll`

### Opaque handle types

- `HK_Engine` — main database instance
- `HK_Doc` — document builder / result handle
- `HK_Batch` — atomic write-batch container
- `HK_Query` — query definition builder
- `HK_Config` — advanced configuration builder
- `HK_Watch` — real-time subscription handle
- `HK_Transaction` — serializable transaction handle
- `HK_ResultSet` — query result handle set
- `HK_NetSyncer` — LAN net-sync handle
- `HK_CloudSync` — cloud-sync handle

### C API highlights

- **Engine / memory:** `hk_engine_open`, `hk_engine_open_with_config`, `hk_engine_is_indexes_ready`, `hk_engine_free`, `hk_engine_backup`, `hk_engine_compact`, `hk_engine_list_collections`, `hk_engine_list_indexes`, `hk_engine_get_stats`, `hk_engine_get_audit_log`, `hk_engine_snapshot_indices`, `hk_last_error`, `hk_string_free`.
- **Configuration:** `hk_config_new/free`, `hk_config_set_durability`, `hk_config_set_encryption_key`, `hk_config_set_encrypted_collections`, `hk_config_set_audit_log`, `hk_config_set_query_workers`, `hk_config_set_memory_limits`, `hk_config_set_storage_tuning`, `hk_config_set_blob_threshold`, `hk_config_set_compression`, `hk_config_set_wal_reserve_bytes`.
- **Real-time:** `hk_engine_watch`, `hk_watch_free`.
- **Documents:** `hk_doc_new/free`, `hk_doc_insert_str/int/float/bool/null/bin/timestamp/server_timestamp/doc/array/reference`, `hk_doc_to_json`, `hk_doc_resolve_blobs` (materialize deferred `__blob__` placeholders).
- **CRUD:** `hk_engine_insert`, `hk_engine_insert_take` (owned doc, no clone), `hk_engine_get`, `hk_engine_delete`, `hk_engine_patch`, `hk_engine_get_by_ref`, `hk_engine_insert_subdoc`.
- **Batches:** `hk_batch_new/free`, `hk_batch_set`, `hk_batch_delete`, `hk_batch_commit`.
- **Transactions:** `hk_transaction_begin/get/set/commit/free`.
- **Queries:** `hk_query_new/free`, all `hk_query_where_*` filters, `hk_query_order_by`, `hk_query_limit/offset`, `hk_query_select_field`, `hk_query_defer_blobs`, cursor functions (`start_at/start_after/end_at/end_before`), `hk_query_execute`, `hk_query_execute_to_handles`, `hk_query_delete`, `hk_query_patch`, aggregates (`hk_query_aggregate_count/sum/avg`, `hk_query_execute_aggregation`).
- **Result sets:** `hk_result_set_count/get_doc/free`, `hk_result_set_to_json` (bulk single-call export).
- **Diagnostics:** `hk_debug_write_stats` (write-phase timing breakdown; see `--wstats`).
- **Indexing:** `hk_engine_create_index` (composite JSON), `hk_engine_create_simple_index`, `hk_engine_create_fts_index`.
- **Net Sync:** `hk_net_syncer_new/start/status/free`.
- **Cloud Sync:** `hk_cloud_sync_new/start/status/stop/free`, plus the room-agnostic `hk_cloud_sync_server_new` and the room-bound `hk_cloud_sync_client_new`.

The per-language SDKs (see [Repositories](#repositories)) wrap these APIs.

---

## Net Sync (LAN replication)

The `net-sync` feature provides peer-to-peer replication over the local network:

- mDNS-based peer discovery for local mesh clusters.
- Room-key isolation using SHA-256 room hashing (`Identify` handshake validation).
- Delta replication via WAL operation payloads (`Replication` packets).
- Live node status telemetry (`idle` / `connected` / `syncing`, peer count, known peers).
- Relay/mesh fan-out controls for multi-hop LAN topologies.

Core FFI functions: `hk_net_syncer_new`, `hk_net_syncer_start`, `hk_net_syncer_status`, `hk_net_syncer_free`. All FFI-based gateways include wrappers for these APIs.

### Enable the feature

```toml
[dependencies]
hakodb = { version = "0.8", features = ["net-sync"] }
tokio = { version = "1", features = ["full"] }
```

### Android notes

On Android, discovery defaults to UDP subnet broadcast (no `MulticastLock`
needed — broadcast bypasses the WiFi multicast filter). Override with
`with_discovery()` if the app holds a lock and prefers mDNS as well.
Requirements live on the app side: `INTERNET` permission, same WiFi as the
group, and realistic expectations about Doze (the mesh stalls with the screen
off; use a foreground service for always-on sync). Mixed groups: the desktop
side must opt into `Both` — a default-configured desktop never hears
broadcast-only mobile peers. The core library needs no Java glue.

---

## Cloud Sync (centralized replication)

The `cloud-sync` feature provides cloud-level, **bi-directional synchronization** over WebSockets and MessagePack. HakoDB instances can act as a **central cloud server** or as an **offline-first cloud client**.

### Enable the feature

```toml
[dependencies]
hakodb = { version = "0.8", features = ["cloud-sync"] }
tokio = { version = "1", features = ["full"] }
```

### Architecture overview

- **Server mode (`CloudSyncMode::Server`)** — central hub handling thousands of concurrent WebSocket connections. An ingress batch flusher coalesces incoming stream mutations into 5ms micro-batches (preventing write-lock bottlenecks); a server outbound WAL tailer captures local server-side writes (CLI, FFI, or `db.put()`) and broadcasts them to all room subscribers.
- **Client mode (`CloudSyncMode::Client`)** — connects to the cloud server over `ws://` or `wss://` (with automatic `https://` → `wss://` conversion). An outbound WAL tailer streams local embedded changes upstream; incoming remote changes are applied locally using LWW (last-write-wins) timestamp filtering.
- **Symmetrical 2-way handshake (`VersionPing`)** — exchanged automatically on connection so that either side (server or client) catches up any deltas missed while offline or restarting.
- **TLS-friendly URLs** — `https://`/`wss://` connect seamlessly through cloud proxies (GitHub Codespaces, Cloudflare Tunnels, AWS ALB, Heroku).
- **High-throughput flusher** — drains and coalesces incoming client mutations every 5ms or 512 ops, reducing HakoDB write-lock acquisitions by ~500x.
- **Anti-echo & deduplication** — self-pruning echo cache plus `msg_id` deduplication prevents infinite loopbacks and stale re-transmissions.

### Rooms and storage layout

A **room** is uniquely identified by the pair `(room_name, room_key)`. Clients
that share a room name but use a different security key are treated as being in
different rooms, so their data is fully isolated on the server.

- `CloudSync::new(db, mode, client_id, room_name, room_key, auth_token)` — the
  `room_name` argument was added in **v0.7.0** (inserted before `room_key`).
  Prefer the dedicated constructors:
  - `CloudSync::server(db, server_id, auth_token)` — a **room-agnostic cloud
    server** ("big cloud server storage"). It is *not* bound to any room: it
    accepts and persists any `(room_name, room_key)` pair its clients ask for.
  - `CloudSync::client(db, client_id, room_name, room_key, auth_token)` — an
    offline-first client that decides which room (and, via `start(server_url)`,
    which server) to sync with. Every client using the same `(room_name,
    room_key)` on the same server forms one sync group.
- The server is **multi-room and room-agnostic**: it accepts any `(room_name,
  room_key)` pair and hosts all of them in one database.
- On the server, every room owns a **storage prefix**:
  - first distinct `(name, key)` for a name → `roomname`
  - each additional distinct key → `roomname_1`, `roomname_2`, …
- Client collections are stored server-side as `<prefix>_<collection>` (e.g. a
  client's `users` collection lands in `game_users`) and are presented back to
  clients as plain `<collection>`, so data never mixes across rooms even when
  clients use identical collection names.
- The room→prefix mapping is kept in the internal, hidden
  `__hako_rooms` collection and is re-read periodically by the server, so
  new rooms are picked up without a restart.
- Peer routing on the server is keyed by `(room, client_id)`, so two clients in
  different rooms may safely reuse the same `client_id` without clobbering each
  other's connections.

### Cloud Sync server example (Rust)

```rust
use std::sync::Arc;
use hakodb::config::HakoConfig;
use hakodb::engine::Hako;
use hakodb::cloud_sync::CloudSync;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Hako::open("./data/cloud_server_db", HakoConfig::default())?);

    // Room-agnostic server: not bound to any room, hosts any (room, key).
    let cloud_server = CloudSync::server(db.clone(), "server_node_01", "master_jwt_secret");

    cloud_server.start("0.0.0.0:8080").await?;
    println!("HakoDB Cloud Sync Server listening on ws://0.0.0.0:8080");

    tokio::signal::ctrl_c().await?;
    cloud_server.stop();
    Ok(())
}
```

### Cloud Sync client example (Rust)

```rust
use std::sync::Arc;
use hakodb::config::HakoConfig;
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::cloud_sync::CloudSync;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(Hako::open("./data/client_db", HakoConfig::default())?);

    // The client picks its room (room_name + room_key) and the server URL.
    let cloud_client = CloudSync::client(
        db.clone(),
        "user_client_42",          // Client ID
        "game",                    // Room name (must match other clients of the room)
        "secret_game_room_key",    // Must match the room key
        "user_jwt_token_123",      // Authentication Token
    );

    cloud_client.start("wss://my-cloud-server.app.dev").await?;
    println!("Connected to Cloud Sync Server");

    // Local writes are synced to the server in the background.
    let mut doc = HakoDoc::default();
    doc.insert("username", Value::String("player_one".to_string()));
    doc.insert("score", Value::Int(9500));
    db.put("players", "user_42", &doc)?;

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    cloud_client.stop();
    Ok(())
}
```

---

## Sync encryption posture (read this before encrypting)

Encryption at rest (WAL/segments, via `encryption_key` + `encrypted_cols`)
and sync-time plaintext are **independent properties**. The sync tailers
read through the storage decryption layer and emit decoded documents, so
an encrypted collection replicates as **plaintext on the wire** unless the
rules below refuse the transfer. There is deliberately no silent path.

### What is (and isn't) protected

| Threat | Status |
|---|---|
| Disk / backup theft on any node | Protected when the node holds the key (at-rest encryption). |
| Keyless or wrong-key peer receiving your encrypted docs | **Refused, loudly.** Both directions enforce: senders skip the room per peer, receivers drop the batch. |
| Old (pre-capability) peer in the room | Treated as unverified: encrypted rooms pause for it (it keeps syncing plaintext rooms). Upgrade the peer to resume. |
| Cloud server operator / DB thief | **Not protected.** The hub sees whatever clients send (and needs `room_key` in the clear to route). If the operator must not read a room, that room needs end-to-end encryption (future work), not at-rest keys. |
| Passive LAN observer (mesh) | **Not protected.** Packets between verified key-holders are still plaintext. Use mTLS/`wss://` segments or E2E rooms for observer resistance. |
| Active impersonator replaying a fingerprint | **Not protected.** Fingerprints are assertions, not proofs (no challenge-response yet). Mitigated by group admission + room keys, not eliminated. |

Admission control (groups, API keys, room keys) is **not** confidentiality:
it decides *who may join*, these rules decide *what may leave*.

### How it works

Every sync handshake now carries encryption capabilities alongside auth:

- **Mesh**: a `SyncCaps` packet (`key_fp` = SHA-256 of the at-rest secret,
  plus the node's encrypted collection list) right after `Identify`.
  Appended as the last enum variant, so old peers fail the decode and
  skip it silently — mixed-version meshes stay connected.
- **Cloud**: `enc_fp` / `enc_cols` fields on `Authenticate` (serde
  defaults; old clients omit them and parse fine both directions).

The decision (`caps_allow`, in `src/sync_guard.rs`) is one rule everywhere:
a locally-encrypted collection flows to/from a peer **iff** that peer
presented the same non-zero fingerprint. Everything else — old peers,
keyless peers, wrong-key peers — is refused per collection with a
throttled loud warning naming peer, collection, and key prefix
(`[sync-guard] ...`), plus an info line when caps arrive. Plaintext
collections behave byte-identically to before (zero behavior delta when
nothing is encrypted).

Enforcement points: mesh tailer (per-peer fan-out), bootstrap, delta
sender, inbound apply, and mesh relay (origin-aware: an encrypted room's
bytes are forwarded only to fingerprint-matched peers, original bytes
untouched); cloud relay fan-out, server apply, server tailer, and both
catch-up senders. The cloud **client trusts its configured hub** (it
sends everything upstream; the server enforces on receipt and relay) —
otherwise encrypted rooms could never use the standard keyless hub.
Catch-up and version-driven deltas self-heal anything skipped while a
peer was unknown: skipped ops stay behind the receiver's version vector
and are re-sent on the next exchange (including the re-`Ping` triggered
by every caps announcement).

### Operating it

1. Put the **same** `encryption_key` on every node that must share the
   room; list the room in `encrypted_cols` (or leave the list empty for
   global encryption).
2. On the cloud server, configure `encrypted_cols` with **storage** names
   (`<prefix>_<collection>`); clients use plain names. Either side
   matching counts.
3. Expect `[sync-guard]` warnings while any peer is keyless, wrong-keyed,
   or old — fix by distributing the key / upgrading, not by relaxing.
4. There is intentionally **no override flag**: a downgrade switch would
   reintroduce the exact silent leak this removes. Mixed-version rooms
   keep working for plaintext collections throughout the upgrade.

---

## Repositories

HakoDB lives under the [`hakodb`](https://github.com/hakodb) organization
— one repo per deliverable, each versioned independently:

| Repo | Delivers | Version |
|---|---|---|
| [`hakodb/hakodb`](https://github.com/hakodb/hakodb) | Core library: engine, storage, query, FFI (`hakodb.h`), net/cloud sync | 0.8.23 |
| [`hakodb/hakocli`](https://github.com/hakodb/hakocli) | Command-line manager + serve REPL | 0.2.1 |
| [`hakodb/hakocloudserver`](https://github.com/hakodb/hakocloudserver) | Managed sync hub + admin console | 0.1.1 |
| [`hakodb/hakotauri`](https://github.com/hakodb/hakotauri) | Tauri gateway crate (Rust) | 0.2.0 |
| [`hakodb/hakotaurits`](https://github.com/hakodb/hakotaurits) | Tauri client (`@hakodb/tauri`) | 0.2.0 |
| [`hakodb/hakobench`](https://github.com/hakodb/hakobench) | C++ benchmark harnesses + SQLite duel | 0.1.1 |
| [`hakodb/hakogo`](https://github.com/hakodb/hakogo) | Go SDK (cgo) | 0.1.2 |
| [`hakodb/hakojs`](https://github.com/hakodb/hakojs) | JS/TS SDK (`@hakodb/client`, Node + Bun) | 0.5.13 |
| [`hakodb/hakopascal`](https://github.com/hakodb/hakopascal) | Lazarus/FPC wrapper + components | 0.1.1 |
| [`hakodb/hakobackend`](https://github.com/hakodb/hakobackend) | Universal HTTP backend gateway + plug-and-play DBs | 0.1.0 |
| [`hakodb/hakobackend-ts`](https://github.com/hakodb/hakobackend-ts) | Backend TS client (`@hakodb/backend`) | 0.1.0 |

Branches: **`main`** (stable — merged releases only) and **`cloud_sync`**
(active development). Before v0.8.20 the project was developed privately
as FireLite, so there is no public repo or history to browse — the
[CHANGELOG](CHANGELOG.md) in this repo is the authoritative record of
what changed and when. Migrating a private FireLite (≤0.8.19) database?
Open it once with v0.8.21+: internal collections migrate automatically
(`__firelite_*` → `__hako_*`).

---

## Architecture

```text
API (HakoDB + FFI + SDKs)
  -> Query (Parallel sharded executor + projection pushdown)
    -> Index (Async background manager + lock-free metadata)
      -> Storage (WAL Inlining + RAM-to-Disk Checkpointing + Tiered Segments)
        -> Hardware (Positional I/O + Atomic Sync Coordination)
```

---

## Implementation status

### Performance & durability

| Area | Status | Technical detail |
|---|---|---|
| **Write performance** | Elite | WAL inlining (small docs bypass segments); single hardware flush per write. |
| **Read performance** | Elite | Zero-lock positional reads allow many parallel readers without contention. |
| **Indexing** | Async | B-Tree updates offloaded to a background worker thread to keep main-thread latency low. |
| **Memory management** | Adaptive | Inlined RAM data spills to tiered segments when thresholds are met. |
| **Concurrency** | Atomic | Atomic versioning and async auditing remove global mutex bottlenecks. |
| **Durability** | ACID | `OnCommit` mode mirrors Firestore `batch.commit()` semantics. |
| **Encryption** | Optimized | ChaCha20-Poly1305 with thread-local RNG for high-frequency encrypted I/O. |
| **Real-time** | Threaded | FFI-compatible background listener bridge with `user_data` context passing. |

### Feature status vs Firestore-style target

| Area | HakoDB status | Notes |
|---|---|---|
| Embedded engine | Implemented | High-concurrency Rust runtime with FFI bridge |
| Durable WAL + recovery | Implemented | Fully encrypted WAL with committed-op filter and crash recovery |
| Encryption at rest | Implemented | ChaCha20-Poly1305 on both WAL and segment layers |
| Atomic batches | Implemented | Single-I/O memory buffer writes for maximum throughput |
| Transactions | Implemented | Serializable snapshot isolation with conflict detection |
| Composite indexes | Implemented | Background-updated B-Trees with equality scan support |
| Query engine | Implemented | Cost-aware planner + predicate pushdown doc-view filter |
| Zero-copy pipeline | Implemented | Direct field projection from binary views across all FFI layers |
| Real-time listeners | Implemented | Non-blocking cross-language callback architecture |
| Subcollections | Implemented | Prefix-based hierarchical document nesting |
| Multi-platform FFI | Implemented | Windows (`.dll`), Linux (`.so`), macOS (`.dylib`) |
| Compaction | Implemented | Tiered LSM-style background merging + memory checkpointing |
| Parity (Cloud) | Implemented | Central hub + offline-first clients, room isolation, auth tokens + group API keys |

### Module map

| Module | Path | Status |
|---|---|---|
| Engine API | `src/engine/*` | Implemented |
| Storage + crypto | `src/storage/*` | Implemented |
| Indexing | `src/index/*` | Implemented |
| Query planner/executor | `src/query/*` | Implemented |
| Document model | `src/document/*` | Implemented |
| C-FFI | `src/ffi.rs`, `include/hakodb.h` | Implemented |

---

## Contribution notes

- Keep module boundaries aligned with the Module map below.
- Add recovery tests when touching storage/WAL/indexing.
- Document binary format or compatibility-impacting changes.
- Keep C ABI additions reflected in cbindgen config + the generated header.

---

## License

MIT — see [LICENSE-MIT](LICENSE-MIT).
