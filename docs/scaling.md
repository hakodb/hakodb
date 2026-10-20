# Scaling hakocloudserver: writer pods + mesh replicas

Issue [#74](https://github.com/hakodb/hakodb/issues/74). Companion:
`docs/lan-relay.md` (the LAN/relay side of the same problem).

## Why horizontal, not vertical

Measured on a co-hosted 6-core box (#72): fresh-cold catch-up plateaus at
~105k docs/s aggregate (~95 MB/s wire) from 10 through 50 concurrent
clients — flat scaling, meaning the send+apply pipeline is saturated, not
the collect path. Meanwhile every fresh client materializes its own local
database, so 100+ concurrent FULL pulls are client-RAM physics a single
server cannot tune away. The knob that moves aggregate throughput is the
number of independent serve points, not one bigger process.

## The shape

```
            ┌── clients (multi) ── LB/router ──┐
            ▼                                  ▼
 pod A (writer)           pod B (replica)        pod C (replica)
 hakocloudserver          hakocloudserver        hakocloudserver
  └ hakodb engine          └ engine (read-only)   └ engine (read-only)
      sync :8411               sync :8412             sync :8413
      mesh :9101 ──────────────▲ dialer B─────────────▲ dialer C
               net_sync WAL tail (mesh, TCP)
```

- Every pod declares the SAME room (`name`, `key`, `token`) — one
  whitelist per pod is allowed: only the WRITER owns a writable engine.
- Writes land on the writer pod; its engine's WAL tail broadcasts over
  the engine mesh (net_sync) to every replica.
- Reads and cold catch-up are served by ANY pod from its own engine plus
  its own shared catch-up page cache (#72, commit `306f217`).
- Replicas accept the mesh's bootstrap handshake — the same paged
  bootstrap hardened in #69 (1000-doc / 8MB pages, loud failures).

The room-level state discipline is hakocluster's: N instances kept
identical by the sync layer, ONE designated writer per dataset, manual
failover. There is no automatic failover by design; promotion is
operator-driven.

## Config

Writer pod:

```toml
server_id = "hako-writer-0"
rooms_dir = "./wrooms"
admin_bind = "0.0.0.0:8301"

[[rooms]]
name = "g35"
key = "k-mesh"
token = "t-mesh"
sync_bind = "0.0.0.0:8311"
mesh_bind = "0.0.0.0:9101"   # replicas dial this
```

Replica pod:

```toml
server_id = "hako-replica-0"
rooms_dir = "./rrooms"
admin_bind = "0.0.0.0:8302"

[[rooms]]
name = "g35"
key = "k-mesh"
token = "t-mesh"
sync_bind = "0.0.0.0:8312"
mesh_peers = ["tcp://hako-writer-0.hako-svc:9101"]
read_only = true
```

Validation is fail-closed:

- Every pod needs a distinct `server_id`. The mesh node id is
  `<server_id>-<room>-<role>`; duplicate ids collide in the writer's peer
  map, so two replicas evict each other's connection and reconnect-loop.
- `read_only = true` without `mesh_peers` refuses to boot (a replica with
  no writer would stay silently empty).
- A read-only room may not declare `mesh_bind` (replicas dial; they are
  never dial targets while fenced).
- Every peer address is parsed at boot — a typo stays loud instead of
  turning into an invalid empty room.

Rooms that declare neither knob run exactly as before (single hub pods,
zero behavior change).

## Kubernetes mapping

- **Deployment**: one pod per serve point. Replicas are (nearly)
  stateless-ish — a fresh pod pulls the whole writer engine over mesh
  bootstrap, so a wiped `rooms/` self-heals; keep it on an ephemeral
  volume if you want faster starts.
- **Services**: `hako-writer` (all writer pods) and `hako-replica`
  (all replica pods). Client LB: sync-plane sessions go to either;
  write sessions go to the writer Service.
- **Boot wiring**: the config above is the wiring — peer addresses use
  the writer's stable Service DNS (`hako-writer-0.hako-svc:9101`).
- **Probes**:
  `/api/health` lists rooms; readiness may additionally check
  `NetSyncer::status()` (peer_count / status) via a future admin exposing
  mesh state — deliverable listed in #74.
- **Failover (manual)**: stop the old writer, flip one replica into
  writer form (`read_only = false`, `mesh_bind = ...`), restart it, then
  restart the old writer as a read-only replica pointed at the new
  writer's mesh address. The demoted writer keeps its WAL; on connect it
  bootstraps from the new writer, applies newer writes and tombstones
  (a doc deleted on the new writer stays deleted), and converges. Give
  every pod a distinct `server_id` (see Config). Clients reconnect to
  whatever Service route they have; catch-ups just re-sync from the new
  state — the engine's epoch/last-write-timestamp semantics (handshake +
  VersionPing) keep ordering consistent.

## Ops notes

- Dial retries run on a 15s cadence per peer address; connection outcomes
  log loudly (`[mesh] static dial ...: connected / refused / timeout`).
  A dead writer pod is visible in BOTH's logs within one cycle.
- Replica staleness is real (asynchronous mesh): a fresh catch-up served
  by a lagging replica gives slightly older data. Eventual-gather clients
  converge on the next VersionPing cycle; this is the tradeoff for
  `write once → read everywhere`.
- Read-only replicas only ingest. They never serve a bootstrap snapshot or
  fan out their own WAL; a demoted writer still holds a full snapshot, and
  two simultaneous full sends fill both socket buffers and deadlock the
  handshake. `sync_checkpoint` is a local write and is therefore blocked
  on a read-only replica, so promotion starts at offset 0 and re-reads the
  WAL; the #80 tail clamp covers a stale checkpoint past the current WAL.
- `/api/compact` is a no-op below `auto_compaction_threshold_bytes`
  (8 MiB default). A 5k-doc / ~1.6 MB WAL stays put, so compare doc counts
  and query results, not WAL byte sizes.
- `docker stop` on 0.16.5 still exits 137 after the timeout (no graceful
  SIGTERM path was observed). The WAL replayed cleanly after the restart,
  but treat shutdown as a crash: promote only after the new writer is
  healthy, and keep the old writer stopped until you start it as a
  read-only replica.
- Multicast (`mDNS`/broadcast discovery) is OFF for mesh-wired rooms:
  container networks don't carry it and turning it off removes an entire
  failure class. Desktop LAN groups keep normal discovery — this mode is
  server-only.
- `net-sync` compiles out on non-desktop targets per feature table; the
  hakocloudserver binary always has it on Linux containers.

## What this does NOT solve

- Same-room active-active writes: replication is strictly one-writer.
- Automatic failover / leader election: operator-driven only, matching
  hakocluster's design intent.
- 100+ concurrent cold pulls on small boxes: still client-RAM physics
  (#72 comment); more serve points spread that cost, they don't remove
  it. The relay tier in docs/lan-relay.md remains the tool for serving
  many LAN peers through ONE cloud pull.
