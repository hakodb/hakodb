# FireLite Examples

Ready-to-run Rust example for the core library.

> **About NetSync/CloudSync from other languages:** the sync features are
> built on Tokio. When called through the C FFI there is no Tokio runtime
> on the calling thread, so `start()` reports "No tokio runtime found" and
> callers degrade gracefully (never crash). For a working LAN/cloud mesh,
> run a Rust Tokio host and connect to it, or embed FireLite in a Rust app
> with `--features net-sync,cloud-sync`.

---

## Rust — `example/rust/basic`

Minimal `cargo` project that depends on the FireLite crate by path.

```bash
cd example/rust/basic
cargo run            # or: cargo run --release
```

Shows open, insert, get, filtered/ordered query, aggregation, atomic batch,
serializable transaction and the real-time watch channel.
