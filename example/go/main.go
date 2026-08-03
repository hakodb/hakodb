// FireLite embedded document database - Go example.
//
// The FireLite Go SDK is a cgo wrapper around the C FFI, so it links against
// the shared library at build time. First build the DLL/so with the sync
// features enabled (the SDK references the net/cloud sync symbols):
//
//   cargo build --features net-sync,cloud-sync          # -> target/debug
//
// Then run the example from the example/go directory:
//
//   cd example/go
//   go run .
//
// On Windows add target/debug to PATH so firelite.dll is found at runtime;
// on Linux/macOS use LD_LIBRARY_PATH / DYLD_LIBRARY_PATH.
package main

import (
	"fmt"
	"log"

	"github.com/firelite-db/firelite-go/firelite"
)

func main() {
	engine, err := firelite.Open("demo.db")
	if err != nil {
		log.Fatal(err)
	}
	defer engine.Close()

	// ---- Insert a document ----
	alice := firelite.NewDoc()
	alice.InsertString("name", "Alice")
	alice.InsertInt("age", 32)
	alice.InsertBool("active", true)
	if err := engine.Set("users", "u1", alice); err != nil {
		log.Fatal(err)
	}
	alice.Free()
	fmt.Println("inserted u1")

	// ---- Read a document back ----
	doc, err := engine.GetDoc("users", "u1")
	if err != nil {
		log.Fatal(err)
	}
	jsonStr, _ := doc.ToJSON()
	fmt.Println("u1 ->", jsonStr)
	doc.Free()

	// ---- Query ----
	q := firelite.NewQuery("users")
	if err := q.WhereEqInt("age", 32); err != nil {
		log.Fatal(err)
	}
	if err := q.Limit(10); err != nil {
		log.Fatal(err)
	}
	rowsJSON, err := engine.ExecuteQuery(q)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println("query(age=32):", rowsJSON)
	q.Free()

	// ---- Atomic batch ----
	batch := firelite.NewBatch()
	bob := firelite.NewDoc()
	bob.InsertString("name", "Bob")
	bob.InsertInt("age", 27)
	batch.Set("users", "u2", bob)
	batch.Delete("users", "u1")
	if err := engine.CommitBatch(batch); err != nil {
		log.Fatal(err)
	}
	batch.Free()
	fmt.Println("batch committed (u2 added, u1 deleted)")

	// ---- Serializable transaction ----
	tx, err := engine.BeginTransaction()
	if err != nil {
		log.Fatal(err)
	}
	txDoc, err := engine.TxGet(tx, "users", "u2")
	if err != nil {
		log.Fatal(err)
	}
	if txDoc != nil {
		txJSON, _ := txDoc.ToJSON()
		fmt.Println("tx read u2 ->", txJSON)
		txDoc.Free()
	}
	if err := engine.CommitTransaction(tx); err != nil {
		log.Fatal(err)
	}
	tx.Free()

	// ---- NetSync (LAN replication) ----
	// Requires the DLL/so built with --features net-sync AND a Tokio host
	// runtime on the calling thread. From plain cgo there is none, so this
	// degrades gracefully; use firelite-cli serve --net-sync (or an embedded
	// Rust tokio app) for a fully working LAN mesh.
	syncer, err := engine.NewNetSyncer("demo-room", "secret-key")
	if err != nil {
		log.Fatal(err)
	}
	if err := syncer.Start(4456); err != nil {
		fmt.Println("note: NetSync did not start:", err)
		fmt.Println("      NetSync via FFI needs a Tokio host runtime; see example/README.md")
	} else if status, err := syncer.StatusJSON(); err == nil {
		fmt.Println("net sync status:", status)
	}
	syncer.Free()

	// ---- CloudSync ----
	// Requires the DLL/so built with --features cloud-sync.
	// A server/room must exist; this example only shows construction.
	cloud, err := engine.NewCloudSync(firelite.CloudSyncServer, "", "demo-room", "")
	if err != nil {
		log.Fatal(err)
	}
	cloud.Free()

	fmt.Println("done")
}
