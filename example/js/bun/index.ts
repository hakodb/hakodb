/**
 * FireLite Bun example (TypeScript SDK, bun:ffi backend - no koffi needed).
 *
 * Setup & run (from the repository root):
 *
 *   cargo build --release --features net-sync,cloud-sync   # -> target/release/firelite.dll
 *   bun example/js/bun/index.ts
 *
 * Bun runs TypeScript natively, so there is no install step. The SDK detects
 * the Bun runtime automatically and uses bun:ffi instead of koffi.
 */
import path from "node:path";
import {
  DurabilityMode,
  FireLiteClient,
} from "../../../js/src/index.ts";

const repoRoot = path.resolve(import.meta.dir, "..", "..", "..");
const libName =
  process.platform === "win32"
    ? "firelite.dll"
    : process.platform === "darwin"
      ? "libfirelite.dylib"
      : "libfirelite.so";
const libraryPath = path.join(repoRoot, "target", "release", libName);

async function main(): Promise<void> {
  // ---- Open with a configuration ----
  const config = await FireLiteClient.createConfig(libraryPath);
  config.setDurability(DurabilityMode.Always);
  const db = await FireLiteClient.open("demo.db", { config, libraryPath });

  // ---- Write documents ----
  await db.set("users", "u1", { name: "Alice", age: 32, active: true });
  await db.set("users", "u2", { name: "Bob", age: 27, active: true });

  // ---- Read ----
  const snap = await db.get("users", "u1");
  console.log("u1 ->", snap.exists ? snap.data() : "not found");

  // ---- Query ----
  const rows = await db.collection("users").where("age", ">=", 27).get();
  console.log("query(age>=27) ->", rows);

  // ---- Batch ----
  const batch = db.batch();
  batch.set(db.collection("users").doc("u3"), { name: "Carol", age: 41 });
  await batch.commit();

  // ---- Watch ----
  const unsubscribe = db
    .collection("users")
    .onSnapshot((docs) => console.log("snapshot:", docs));
  await db.set("users", "u4", { name: "Dave", age: 50 });
  await Bun.sleep(250);
  await unsubscribe();

  // ---- Net + Cloud sync (requires --features net-sync,cloud-sync build;
  //      NetSync also needs a Tokio host runtime, so it degrades gracefully) ----
  const syncer = db.createNetSyncer("demo-room", "secret-key");
  try {
    await syncer.start(4456);
    console.log("net sync status:", await syncer.status());
  } catch (err) {
    console.warn("net sync skipped:", err);
  } finally {
    await syncer.close();
  }

  // Room-agnostic cloud SERVER: not bound to any room, hosts any (room, key).
  const server = db.createCloudSyncServer("bun-server", "server-token");
  server.close();

  // Offline-first cloud CLIENT: picks its own room + server.
  const cloud = db.createCloudSyncClient("c1", "room", "secret-key", "token");
  cloud.close();

  await db.close();
  console.log("done");
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
