# FireLite

**FireLite is an embedded, Firestore-style document database written in Rust.**

It stores typed JSON-like documents in binary form, runs **fully in-process** like SQLite (no server process, no daemon, no network config), and exposes a **flat C ABI** so it can be embedded in applications written in Rust, C/C++, Go, JavaScript/TypeScript (Node.js + Bun), Pascal/Lazarus, and more.

FireLite speaks "documents", not tables: collections of flexible, schemaless objects with a query API that feels like Google Firestore (`collection().doc().set()`, `.where().orderBy().limit()`), while keeping the zero-deploy footprint of an embedded engine.

> **Current status: v0.6.65 (production-candidate).** The core engine supports physical data sharding, zero-copy field projection, near-instant recovery, composite + full-text + secondary indexing, encryption at rest, and high-throughput local or cloud synchronization capable of **50,000+ OPS** under heavy concurrent workloads.

---

## Table of Contents

- [What is FireLite?](#what-is-firelite)
- [When to use FireLite (sync vs non-sync)](#when-to-use-firelite-sync-vs-non-sync)
- [Key features](#key-features)
- [Quick Start (Rust)](#quick-start-rust)
- [Command-line tool (firelite-cli)](#command-line-tool-firelite-cli)
- [Rust usage](#rust-usage)
- [Go SDK](#go-sdk)
- [JavaScript / TypeScript SDK](#javascript--typescript-sdk)
- [Lazarus / Free Pascal wrapper](#lazarus--free-pascal-wrapper)
- [Multi-language platform support (C ABI)](#multi-language-platform-support-c-abi)
- [Net Sync (LAN replication)](#net-sync-lan-replication)
- [Cloud Sync (centralized replication)](#cloud-sync-centralized-replication)
- [Benchmark (official tool)](#benchmark-official-tool)
- [Architecture](#architecture)
- [Implementation status](#implementation-status)
- [Contribution notes](#contribution-notes)
- [License](#license)

---

## What is FireLite?

FireLite is a **document-oriented embedded database** for applications that want:

- **Firestore-like ergonomics** — collections, documents, `set/get/delete`, fluent queries, real-time change streams.
- **SQLite-style embedding** — link a library into your process and open a database file; there is nothing to install or operate.
- **Durability without sacrifice** — a WAL + tiered-segment storage engine with configurable durability (from full `fsync` per write to group-commit `on-commit` batches) and crash recovery.
- **Encryption at rest** — ChaCha20-Poly1305 encryption of WAL and segment payloads via a master key.
- **Real-time locally** — `watch_collection` streams document changes (`put` / `delete`) to subscribers in-process.
- **Synchronization when you need it** — two optional replication layers:
  - **Net Sync** (`net-sync` feature): peer-to-peer mesh replication over LAN with mDNS discovery.
  - **Cloud Sync** (`cloud-sync` feature): centralized client-server replication over WebSockets + MessagePack.

Because it is a library, FireLite has no "database server" to manage. Your app *is* the database host. This makes it ideal for local-first and offline-first products, desktop and CLI tooling, edge devices, games, and apps that occasionally need to sync with the cloud or with each other.

---

## When to use FireLite (sync vs non-sync)

| Scenario | Recommended mode | Why |
|---|---|---|
| Desktop / CLI / local tool needs a real DB with zero setup | **Embedded (no sync)** | In-process, single file, no services. |
| Offline-first mobile/edge/desktop app that syncs to a central backend | **Cloud Sync** (client) | Local reads/writes keep working offline; deltas sync over `ws://`/`wss://`. |
| Central hub collecting writes from many devices | **Cloud Sync** (server) | Thousands of concurrent WebSocket clients, 5ms micro-batched writes, WAL tailer broadcast. |
| Real-time multiplayer / collaborative session on one LAN | **Net Sync** (mesh) | mDNS discovery, room-key isolation, delta replication across peers. |
| Local app that must *also* be reachable by other processes/languages | **Embedded + FFI** | C ABI with Go/JS/Pascal gateways; watch streams for reactive UIs. |
| Analytics / ad-hoc queries over large datasets | **Embedded** | Composite indexes, FTS, aggregates, zero-copy projection, parallel scans. |

**In short:** use FireLite **without sync** when your data is local to one process. Turn on **Net Sync** when you need peer-to-peer replication across devices on a network you control. Turn on **Cloud Sync** when you need offline-first clients to converge through a central server (or to build a real-time multi-client hub).

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
- **Multi-language FFI** — opaque handle C ABI with document builder, CRUD, query, batch, transaction, watch, result-set and cloud-sync functions; gateways for Go, JavaScript/TypeScript and Pascal.

---

## Quick Start (Rust)

```rust
use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::{BatchMutation, FireLite};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let mut cfg = FireLiteConfig::default();
    cfg.encryption_key = Some("change-me-secret".to_string());

    let db = FireLite::open(".firelite-example", cfg)?;

    let mut doc = FireLiteDoc::default();
    doc.insert("name", Value::String("alice".to_string()));
    doc.insert("age", Value::Int(30));

    db.put("users", "1", &doc)?;
    db.flush()?;
    Ok(())
}
```

---

## Command-line tool (firelite-cli)

FireLite ships with a full-featured CLI under `cli/`. It can be used in two ways:

1. **Non-serve mode (one-shot commands)** — run a single command against a database and exit. Ideal for scripting, debugging, and shell pipelines.
2. **Serve mode (interactive REPL + networking)** — starts a server loop with an interactive prompt, optionally enabling **Net Sync** (LAN mesh) and/or **Cloud Sync** (server or client).

### Build

```bash
cargo build --release -p firelite-cli
# binary: target/release/firelite-cli
```

The CLI is compiled with `net-sync` and `cloud-sync` enabled by default.

### Global options

| Flag | Description |
|---|---|
| `--db <path>` | Database path (default `./firelite.db`) |
| `--durability <mode>` | `always` \| `interval` \| `manual` \| `on-commit` (default `on-commit`) |
| `--encryption-key <key>` | Enables storage encryption with the given master key |
| `--encrypted-cols <list>` | Comma-separated collections to encrypt (empty = all) |
| `--time` | Print execution time for the operation |
| `--count` | Print the number of results (queries and collections) |

### Non-serve mode: one-shot commands

Seed a collection with sample data, then run typical operations:

```bash
# seed 100 documents with a few sample indexes (simple + FTS + composite)
firelite-cli --db ./demo.db seed users 100

# write / read / update / delete a document
firelite-cli --db ./demo.db set users/alice --data '{"name":"Alice","age":30,"active":true}'
firelite-cli --db ./demo.db get users/alice
firelite-cli --db ./demo.db update users/alice --data '{"age":31}'
firelite-cli --db ./demo.db delete users/alice

# batch write from a JSON array (path is the collection name)
firelite-cli --db ./demo.db set users --batch --data '[
  {"id":"a","data":{"name":"Alice","age":30}},
  {"id":"b","data":{"name":"Bob","age":25}}
]'

# query with filters, ordering, and pagination
firelite-cli --db ./demo.db query users --where age:gte:21 --order name:asc --limit 10 --offset 5
firelite-cli --db ./demo.db query users --or status:eq:active --or status:eq:pending --count

# full-text search (match) and projections
firelite-cli --db ./demo.db query users --fts description:seeded
firelite-cli --db ./demo.db query users --select name,age

# aggregates
firelite-cli --db ./demo.db aggregate users count
firelite-cli --db ./demo.db aggregate users sum --field age --where active:eq:true
firelite-cli --db ./demo.db query users --aggregate count --aggregate avg:age

# mass update / mass delete from a query
firelite-cli --db ./demo.db query users --where status:eq:trial --set --data '{"tier":"pro"}'
firelite-cli --db ./demo.db query users --where active:eq:false --delete

# index management
firelite-cli --db ./demo.db index create users age
firelite-cli --db ./demo.db index create-composite users --fields age:asc,name:asc
firelite-cli --db ./demo.db index create-fts users description
firelite-cli --db ./demo.db index list users

# real-time watch (blocks and prints change events until Ctrl+C)
firelite-cli --db ./demo.db watch users

# database maintenance and inspection
firelite-cli --db ./demo.db collections
firelite-cli --db ./demo.db stats
firelite-cli --db ./demo.db compact

# REST-like one-shot surface: METHOD PATH [--data JSON]
firelite-cli --db ./demo.db rest GET users/alice
firelite-cli --db ./demo.db rest PATCH users/alice --data '{"age":32}'
```

Query filter syntax is `field:op:value` where `op` is one of `eq, ne, gt, gte, lt, lte, in, notIn, match, contains, startsWith, arrayContains, arrayContainsAny`. Array values use JSON, e.g. `tags:in:["a","b"]`. Ordering uses `field:asc` / `field:desc`.

> **Windows PowerShell note:** when passing inline JSON to `--data`, use `--fromfile payload.json` (or `cmd.exe`) instead of `'{"key":"value"}'` — PowerShell 5.1 strips the inner double quotes when invoking native executables.

### Serve mode: interactive REPL + networking

`serve` opens the database and drops you into an interactive prompt where every CLI command keeps working (type `collections`, `query users --where ...`, `peers`, `exit`):

```bash
# 1) Plain standalone server (interactive shell only)
firelite-cli --db ./demo.db serve

# 2) LAN Net Sync mesh node (mDNS discovery, room-key isolated)
firelite-cli --db ./demo.db serve --port 7070 --node-id node-1 --key my-room-key

# 3) Cloud Sync SERVER (central WebSocket hub on 0.0.0.0:8080)
firelite-cli --db ./cloud.db serve \
  --node-id cloud-1 --key room-key \
  --bind 0.0.0.0:8080 --token s3cret-token

# 4) Cloud Sync CLIENT (connects to the central server, offline-first)
firelite-cli --db ./local.db serve \
  --node-id device-1 --key room-key \
  --server ws://cloud-host:8080 --token s3cret-token
```

The prompt shows the live sync status, e.g.:

```text
firelite(node-1 | LAN:Online (Peers:2)) > query users --where active:eq:true --limit 10
firelite(node-1 | LAN:Online (Peers:2)) > peers
firelite(node-1 | LAN:Online (Peers:2)) > exit
```

#### Serve flags

| Flag | Purpose |
|---|---|
| `--port <u16>` | Enable **Net Sync**; start LAN mesh listener on this port |
| `--node-id <id>` | Unique node identifier |
| `--key <key>` | Room key (SHA-256 hashed for room isolation) |
| `--bind <addr>` | Enable **Cloud Sync server** on this bind address (e.g. `0.0.0.0:8080`) |
| `--server <url>` | Enable **Cloud Sync client**; connect to this server (`ws://`, `wss://`, or `https://`) |
| `--token <token>` | Auth token shared with the cloud server |

---

## Rust usage

```rust
use firelite::config::FireLiteConfig;
use firelite::engine::{BatchMutation, FireLite};

let db = FireLite::open("./data", FireLiteConfig::default())?;

// atomic batch write
db.write_batch(vec![
    BatchMutation::Put {
        collection: "users".into(),
        doc_id: "1".into(),
        doc: /* FireLiteDoc */ doc1,
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

See `cli/src/main.rs` for a complete, working example of the engine API.

> More Rust examples: [`example/rust/basic`](example/rust/basic) — a runnable
> Cargo project (`cargo run`) covering open, CRUD, query, aggregation, batch,
> transaction and the real-time watch channel.

---

## Go SDK

A complete cgo gateway lives in `go/firelite/` with typed wrappers for every C-ABI function, plus a Firestore-style facade.

```go
package main

import (
    "fmt"

    "github.com/firelite-db/firelite-go/firelite"
)

func main() {
    db, err := firelite.Open("./data.firelite")
    if err != nil {
        panic(err)
    }
    defer db.Close()

    doc := db.NewDoc().
        SetString("name", "alice").
        SetInt("age", 30)
    if err := db.Put("users", "alice", doc); err != nil {
        panic(err)
    }

    got, err := db.Get("users", "alice")
    if err != nil {
        panic(err)
    }
    fmt.Println(got.JSON())

    // Firestore-style client facade
    c := db.Client()
    _ = c.Collection("users").Doc("bob").Set(map[string]any{"name": "Bob"})
}
```

Coverage includes `Engine`, `Config`, `Doc`, `Array`, `Query`, `Batch`, `Transaction`, `Watch` (native cgo callback bridge), `ResultSet`, `NetSyncer` and `CloudSync`.

> Run it: [`example/go`](example/go) is a complete Go program (`go run ./example/go`)
> that also demonstrates NetSync and CloudSync setup.

---

## JavaScript / TypeScript SDK

A high-level SDK is available under `js/`, built over the C-FFI layer with dual loaders for **Node.js (koffi)** and **Bun (dlopen)**.

```bash
cd js
npm install
```

```ts
import { FireLiteClient } from "@firelite/client";

const db = await FireLiteClient.open("./data.firelite", {
  libraryPath: "./target/release/libfirelite.so", // optional override
});

await db.collection("users").doc("alice").set({
  name: "Alice",
  age: 30,
  active: true,
});

const snap = await db.collection("users").doc("alice").get();
if (snap.exists) {
  console.log(snap.data());
}

const rows = await db
  .collection("users")
  .where("age", "==", 30)
  .orderBy("name", "asc")
  .limit(10)
  .select("name", "age")
  .get();

const batch = db.batch();
batch
  .set(db.collection("users").doc("bob"), { name: "Bob", age: 31 })
  .delete(db.collection("users").doc("alice"));
await batch.commit();

// cloud sync + real-time snapshots
const cs = await db.createCloudSync("client", "device-1", "room-key", "token");
await cs.start("ws://host:8080");

const stop = await db.collection("users").onSnapshot((rows) => {
  console.log("live rows", rows);
});
// later: await stop();

await db.close();
```

Value mapping to the FFI builder: `string` → `fl_doc_insert_str`, integer/float → `fl_doc_insert_int`/`fl_doc_insert_float`, `boolean` → `fl_doc_insert_bool`, `null` → `fl_doc_insert_null`, `Uint8Array` → `fl_doc_insert_bin`, document references → `fl_doc_insert_reference`. The SDK also includes a `TauriFireLite` gateway client (see `js/src/tauri.ts`).

> Run it: [`example/js/node`](example/js/node) (koffi backend, run with tsx) and
> [`example/js/bun`](example/js/bun) (`bun example/js/bun/index.ts`, uses
> `bun:ffi`, no install step) are complete runnable TypeScript examples.

---

## Lazarus / Free Pascal wrapper

A production-focused Pascal wrapper is available under `pascal/`:

- `pascal/FireLiteRaw.pas` — C-ABI translation with opaque handles (`PFL_Engine`, `PFL_Doc`, `PFL_Batch`, `PFL_Query`, `PFL_ResultSet`, `PFL_CloudSync`, …) and `cdecl` imports for Windows/Linux/macOS.
- `pascal/FireLite.pas` — object-oriented API: `TFireLite`, `TFLCollection`, `TFLDocument`, `TFLQuery`, `TFLBatch`, `TFLTransaction`, `TFLCloudSync`.
  - fluent Firestore-like flow (`Collection(...).Doc(...).SetDoc/Get/Delete`, query chaining)
  - projection pushdown (`Select([...])`) wired to `fl_query_select_field`
  - advanced filters (`WhereNotIn`, `ArrayContains`, `ArrayContainsAny`, `WhereOr*`) mapped to FFI
  - callback-based `OnSnapshot` via a polling thread with optional main-thread queue dispatch.

```pascal
var
  DB: TFireLite;
  Col: TFLCollection;
  Doc: TFLDocument;
begin
  DB := TFireLite.Create('./data.firelite');
  try
    Col := DB.Collection('users');
    Doc := TFLDocument.Create.InsertStr('name', 'alice').InsertInt('age', 30);
    try
      Col.Doc('u1').SetDoc(Doc);
    finally
      Doc.Free;
    end;
  finally
    DB.Free;
  end;
end;
```

The wrapper also ships as a ready-to-use **Lazarus package**:

- `pascal/firelite.lpk` — install it via `Package > Open Package File (.lpk)` in
  the Lazarus IDE (`Compile`, then `Install`). It provides `FireLiteRaw`,
  `FireLite`, and `FireLiteComponent` (a `TComponent` wrapper you can drop on a
  form) and puts a **FireLite** tab on the component palette.
- `pascal/FireLiteComponent.pas` — the drop-on-form component. NetSync and
  CloudSync are fully exposed as Object Inspector properties:
  `NetSyncName`, `NetSyncRoomKey`, `NetSyncPort`,
  `CloudSyncMode`, `CloudSyncClientID`, `CloudSyncRoomKey`, `CloudSyncAuthToken`,
  `CloudSyncAddress`, with one-call `StartNetSync` / `StartCloudSync` methods.
- `pascal/FireLitePkgReg.pas` — design-time registration unit.

> Run it: [`example/pascal/console`](example/pascal/console) is a plain FPC
> program (`fpc -Fu..\..\..\pascal console_demo.lpr`), and
> [`example/pascal/lazarus`](example/pascal/lazarus) is a minimal Lazarus GUI
> demo (`example.lpi`) using a `TFireLiteComponent` dropped on a form.

---

## Multi-language platform support (C ABI)

FireLite exposes a flat C ABI for Node.js/Python/C++/C# and other integration layers. Opaque handle types are defined in `include/firelite.h`.

### Build artifacts

- Cargo crate types: `cdylib` (dynamic library consumers) and `rlib` (Rust consumers).
- Auto-generated C header via `build.rs` + `cbindgen.toml`: `include/firelite.h`.

```bash
cargo build --release
```

Platform outputs:

- Linux: `target/release/libfirelite.so`
- macOS: `target/release/libfirelite.dylib`
- Windows: `target\release\firelite.dll`

### Opaque handle types

- `FL_Engine` — main database instance
- `FL_Doc` — document builder / result handle
- `FL_Batch` — atomic write-batch container
- `FL_Query` — query definition builder
- `FL_Config` — advanced configuration builder
- `FL_Watch` — real-time subscription handle
- `FL_Transaction` — serializable transaction handle
- `FL_ResultSet` — query result handle set
- `FL_NetSyncer` — LAN net-sync handle
- `FL_CloudSync` — cloud-sync handle

### C API highlights

- **Engine / memory:** `fl_engine_open`, `fl_engine_open_with_config`, `fl_engine_is_indexes_ready`, `fl_engine_free`, `fl_engine_backup`, `fl_engine_compact`, `fl_engine_list_collections`, `fl_engine_list_indexes`, `fl_engine_get_stats`, `fl_engine_get_audit_log`, `fl_engine_snapshot_indices`, `fl_last_error`, `fl_string_free`.
- **Configuration:** `fl_config_new/free`, `fl_config_set_durability`, `fl_config_set_encryption_key`, `fl_config_set_encrypted_collections`, `fl_config_set_audit_log`, `fl_config_set_query_workers`, `fl_config_set_memory_limits`, `fl_config_set_storage_tuning`, `fl_config_set_blob_threshold`, `fl_config_set_compression`.
- **Real-time:** `fl_engine_watch`, `fl_watch_free`.
- **Documents:** `fl_doc_new/free`, `fl_doc_insert_str/int/float/bool/null/bin/timestamp/server_timestamp/doc/array/reference`, `fl_doc_to_json`.
- **CRUD:** `fl_engine_insert`, `fl_engine_get`, `fl_engine_delete`, `fl_engine_patch`, `fl_engine_get_by_ref`, `fl_engine_insert_subdoc`.
- **Batches:** `fl_batch_new/free`, `fl_batch_set`, `fl_batch_delete`, `fl_batch_commit`.
- **Transactions:** `fl_transaction_begin/get/set/commit/free`.
- **Queries:** `fl_query_new/free`, all `fl_query_where_*` filters, `fl_query_order_by`, `fl_query_limit/offset`, `fl_query_select_field`, cursor functions (`start_at/start_after/end_at/end_before`), `fl_query_execute`, `fl_query_execute_to_handles`, `fl_query_delete`, `fl_query_patch`, aggregates (`fl_query_aggregate_count/sum/avg`, `fl_query_execute_aggregation`).
- **Result sets:** `fl_result_set_count/get_doc/free`.
- **Indexing:** `fl_engine_create_index` (composite JSON), `fl_engine_create_simple_index`, `fl_engine_create_fts_index`.
- **Net Sync:** `fl_net_syncer_new/start/status/free`.
- **Cloud Sync:** `fl_cloud_sync_new/start/status/stop/free`.

All FFI gateways (Go / JS-TS / Pascal) wrap these APIs.

---

## Net Sync (LAN replication)

The `net-sync` feature provides peer-to-peer replication over the local network:

- mDNS-based peer discovery for local mesh clusters.
- Room-key isolation using SHA-256 room hashing (`Identify` handshake validation).
- Delta replication via WAL operation payloads (`Replication` packets).
- Live node status telemetry (`idle` / `connected` / `syncing`, peer count, known peers).
- Relay/mesh fan-out controls for multi-hop LAN topologies.

Core FFI functions: `fl_net_syncer_new`, `fl_net_syncer_start`, `fl_net_syncer_status`, `fl_net_syncer_free`. All FFI-based gateways include wrappers for these APIs.

### Enable the feature

```toml
[dependencies]
firelite = { version = "0.6.65", features = ["net-sync"] }
tokio = { version = "1", features = ["full"] }
```

See [Serve mode](#serve-mode-interactive-repl--networking) for the CLI workflow, or `cli/src/main.rs` for the Rust `NetSyncer` usage.

---

## Cloud Sync (centralized replication)

The `cloud-sync` feature provides cloud-level, **bi-directional synchronization** over WebSockets and MessagePack. FireLite instances can act as a **central cloud server** or as an **offline-first cloud client**.

### Enable the feature

```toml
[dependencies]
firelite = { version = "0.6.65", features = ["cloud-sync"] }
tokio = { version = "1", features = ["full"] }
```

### Architecture overview

- **Server mode (`CloudSyncMode::Server`)** — central hub handling thousands of concurrent WebSocket connections. An ingress batch flusher coalesces incoming stream mutations into 5ms micro-batches (preventing write-lock bottlenecks); a server outbound WAL tailer captures local server-side writes (CLI, FFI, or `db.put()`) and broadcasts them to all room subscribers.
- **Client mode (`CloudSyncMode::Client`)** — connects to the cloud server over `ws://` or `wss://` (with automatic `https://` → `wss://` conversion). An outbound WAL tailer streams local embedded changes upstream; incoming remote changes are applied locally using LWW (last-write-wins) timestamp filtering.
- **Symmetrical 2-way handshake (`VersionPing`)** — exchanged automatically on connection so that either side (server or client) catches up any deltas missed while offline or restarting.
- **TLS-friendly URLs** — `https://`/`wss://` connect seamlessly through cloud proxies (GitHub Codespaces, Cloudflare Tunnels, AWS ALB, Heroku).
- **High-throughput flusher** — drains and coalesces incoming client mutations every 5ms or 512 ops, reducing FireLite write-lock acquisitions by ~500x.
- **Anti-echo & deduplication** — self-pruning echo cache plus `msg_id` deduplication prevents infinite loopbacks and stale re-transmissions.

### Cloud Sync server example (Rust)

```rust
use std::sync::Arc;
use firelite::config::FireLiteConfig;
use firelite::engine::FireLite;
use firelite::cloud_sync::{CloudSync, CloudSyncMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(FireLite::open("./data/cloud_server_db", FireLiteConfig::default())?);

    let cloud_server = CloudSync::new(
        db.clone(),
        CloudSyncMode::Server,
        "server_node_01",        // Server Node ID
        "secret_game_room_key",  // Room Key (SHA-256 hashed for room isolation)
        "master_jwt_secret",     // Authentication Secret
    );

    cloud_server.start("0.0.0.0:8080").await?;
    println!("FireLite Cloud Sync Server listening on ws://0.0.0.0:8080");

    tokio::signal::ctrl_c().await?;
    cloud_server.stop();
    Ok(())
}
```

### Cloud Sync client example (Rust)

```rust
use std::sync::Arc;
use firelite::config::FireLiteConfig;
use firelite::document::firelite_doc::FireLiteDoc;
use firelite::document::value::Value;
use firelite::engine::FireLite;
use firelite::cloud_sync::{CloudSync, CloudSyncMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let db = Arc::new(FireLite::open("./data/client_db", FireLiteConfig::default())?);

    let cloud_client = CloudSync::new(
        db.clone(),
        CloudSyncMode::Client,
        "user_client_42",          // Client ID
        "secret_game_room_key",    // Must match the server room key
        "user_jwt_token_123",      // Authentication Token
    );

    cloud_client.start("wss://my-cloud-server.app.dev").await?;
    println!("Connected to Cloud Sync Server");

    // Local writes are synced to the server in the background.
    let mut doc = FireLiteDoc::default();
    doc.insert("username", Value::String("player_one".to_string()));
    doc.insert("score", Value::Int(9500));
    db.put("players", "user_42", &doc)?;

    tokio::time::sleep(tokio::time::Duration::from_secs(10)).await;
    cloud_client.stop();
    Ok(())
}
```

---

## Benchmark (official tool)

The official benchmark harness is **`benchmark.cpp`** — a C++ program that drives the engine exclusively through the public C ABI (`include/firelite.h`). It is the reference tool for measuring and reporting FireLite performance.

### What it measures

For each profile it reports throughput (operations per second) and system metrics:

| Metric | Description |
|---|---|
| `WPS (Sgl/Btc)` | Single writes/sec and batch writes/sec |
| `RPS (Seq/Par)` | Sequential and multi-threaded point-reads/sec |
| `STRESS (Get/Qry/Cmp)` | Mixed point-get, indexed-query, and composite-query throughput |
| `QPS (Off/Cur)` | Offset-pagination and cursor-pagination queries/sec |
| `Agg QPS` | Aggregate queries/sec (`sum`) |
| `Tx WPS` | Serializable transactions/sec |
| `Bulk Upd/Del` | Bulk update and bulk delete ops/sec |
| `Startup/Flush` | Engine open (ms) and clean shutdown (ms) |
| `Size` | On-disk database size |

It runs six profiles across durability and workload mixes: `Always`, `Interval`, `Manual`, `OnCommit`, `Enc_Comp` (encrypted + compressed), and `Gaming` (large documents, parallel workers).

### Build the cdylib first

```bash
cargo build --release
```

### Compile the benchmark

**Linux / macOS:**

```bash
g++ -O2 -std=c++17 -Iinclude benchmark.cpp -Ltarget/release -lfirelite -o benchmark
```

**Windows (MinGW / MSYS2):**

```bash
g++ -O2 -std=c++17 -Iinclude benchmark.cpp -Ltarget/release -lfirelite -o benchmark.exe
```

On Windows, ensure `target\release\firelite.dll` is on `PATH` when running.

### Run

```bash
# default dataset (1,000 docs per profile)
./benchmark --docs=1000

# larger dataset
./benchmark --docs=10000

# if the shared library is not on the default loader path (Linux/macOS)
LD_LIBRARY_PATH=target/release ./benchmark --docs=1000
```

> `--docs` controls how many documents each profile inserts (batch-written documents are `--docs - 100`). Use `--docs >= 1000` for meaningful numbers; very small values (e.g. `100`) leave too little data for the batch/query stages.

The program prints a per-profile progress line followed by a full markdown-style matrix report. Each run creates and destroys temporary `bench_data_*` directories — no existing database is touched.

---

## Architecture

```text
API (FireLite + FFI + SDKs)
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

| Area | FireLite status | Notes |
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
| Parity (Cloud) | Non-goal | No remote authentication or globally distributed state |

### Module map

| Module | Path | Status |
|---|---|---|
| Engine API | `src/engine/*` | Implemented |
| Storage + crypto | `src/storage/*` | Implemented |
| Indexing | `src/index/*` | Implemented |
| Query planner/executor | `src/query/*` | Implemented |
| Document model | `src/document/*` | Implemented |
| C-FFI | `src/ffi.rs`, `include/firelite.h` | Implemented |
| CLI | `cli/src/main.rs` | Implemented |
| JS/TS SDK | `js/src/*` | Implemented |
| Lazarus/FPC wrapper | `pascal/FireLiteRaw.pas`, `pascal/FireLite.pas` | Implemented |
| Tauri gateway | `src/tauri_gateway.rs`, `js/src/tauri.ts` | Implemented |
| Benchmark harness | `benchmark.cpp` | Implemented |

---

## Contribution notes

- Keep module boundaries aligned with `STRUCTURE.md`.
- Add recovery tests when touching storage/WAL/indexing.
- Document binary format or compatibility-impacting changes.
- Keep C ABI additions reflected in cbindgen config + the generated header.
- Keep SDK API changes reflected in this README and examples.
- When changing the benchmark, update `benchmark.cpp` and the CI workflow (`.github/workflows/perf.yml`).

---

## License

TBD
