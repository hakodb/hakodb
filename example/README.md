# FireLite Examples

Ready-to-run examples for every supported language. Each example opens a
FireLite database file, writes/reads documents, runs a query, uses a batch
and a transaction, and (where noted) starts NetSync / CloudSync.

The FireLite shared library must exist before running any example. Build it
once from the repository root:

```bash
# everything the examples use, including net/cloud sync:
cargo build --release --features net-sync,cloud-sync

# or, if you only care about the core CRUD examples (skip sync):
cargo build --release
```

This produces `target/release/firelite.dll` (Windows),
`libfirelite.so` (Linux) or `libfirelite.dylib` (macOS). Every example
resolves that file for you automatically.

> **About NetSync/CloudSync from other languages:** the sync features are
> built on Tokio. When called through the C FFI from Go / Node / Bun / Pascal
> there is no Tokio runtime on the calling thread, so `start()` reports
> "No tokio runtime found" and the examples degrade gracefully (they never
> crash). For a working LAN/cloud mesh from these languages, run the provided
> `firelite serve` modes (Rust, Tokio host) and connect to them, or embed
> FireLite in a Rust app with `--features net-sync,cloud-sync`.

---

## Rust — `example/rust/basic`

Minimal `cargo` project that depends on the FireLite crate by path.

```bash
cd example/rust/basic
cargo run            # or: cargo run --release
```

Shows open, insert, get, filtered/ordered query, aggregation, atomic batch,
serializable transaction and the real-time watch channel.

## Go — `example/go`

cgo wrapper around the C FFI.

```bash
go run ./example/go
```

The Go SDK links against `target/debug`, so also build the debug library:

```bash
cargo build --features net-sync,cloud-sync
cd example/go && go run .          # add target/debug to PATH / LD_LIBRARY_PATH if needed
```

Shows CRUD, query, batch, transaction, plus NetSync and CloudSync setup.

## JavaScript / TypeScript — `example/js`

Two runtimes, one TypeScript SDK (auto-detects the runtime):

### Node.js (`example/js/node`) — koffi FFI backend

```bash
npm install ./js                       # koffi + msgpack used by the SDK
npm install --prefix example/js/node   # tsx, to run the TypeScript
node --import tsx example/js/node/index.ts
```

### Bun (`example/js/bun`) — `bun:ffi` backend, no install step

```bash
bun example/js/bun/index.ts
```

Both show open-with-config, write, read, query, aggregation, batch, real-time
snapshots and Net/Cloud sync. The sync section requires the
`--features net-sync,cloud-sync` build and is skipped with a warning if the
symbols are missing.

## Pascal — `example/pascal`

### Console (`example/pascal/console`) — plain FPC

```bash
fpc -Fu"..\..\..\pascal" console_demo.lpr
# make the shared library findable:
copy ..\..\..\target\release\firelite.dll .      # Windows
# LD_LIBRARY_PATH=../../target/release ./console_demo   # Linux
./console_demo
```

> **Bitness matters**: the Pascal compiler must match the bitness of the
> FireLite shared library. The Rust release build is 64-bit, so use a 64-bit
> FPC/Lazarus on x64 machines.

Shows CRUD, query, aggregation, batch, transaction and NetSync.

### Lazarus GUI (`example/pascal/lazarus`) — minimal form app

Open `example.lpi` in Lazarus, press F9, then click **Run demo**. The form
has a `TFireLiteComponent` dropped on it (from the FireLite package's
component palette tab) and demonstrates document writes, reads and stats.
NetSync/CloudSync settings are exposed as Object Inspector properties on the
component (`NetSyncName`, `NetSyncRoomKey`, `NetSyncPort`, `CloudSyncMode`,
`CloudSyncClientID`, `CloudSyncRoomName`, `CloudSyncRoomKey`,
`CloudSyncAuthToken`, `CloudSyncAddress`) and started with `StartNetSync` /
`StartCloudSync`. For the room-agnostic big-cloud-server shape, use
`CreateCloudServerSyncer(ServerID, AuthToken)`; for a client that picks its own
room, use `CreateCloudClientSyncer(ClientID, RoomName, RoomKey, AuthToken)`.

### Installing the FireLite package into Lazarus (optional)

1. `Package > Open Package File (.lpk)` → open `pascal/firelite.lpk`.
2. Click **Compile**, then **Install** (restart the IDE when asked).
3. `TFireLiteComponent` now appears on the **FireLite** palette tab.
4. In any project: `Project > Project Inspector > Add > New Requirement` and
   add the installed FireLite package, then put `FireLite` in your `uses`.

The `.lpi` in this folder already adds `../../pascal` to the unit search
path, so the demo builds even before the package is installed.
