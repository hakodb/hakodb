# FireLite Go Gateway SDK (v0.5.12)

A complete Go SDK over the FireLite C-FFI surface, with a Firestore-style ergonomic layer.

## Features

- Full FFI handle coverage: engine, config, doc, array, query, batch, transaction, watch.
- Complete query/filter/operator coverage available in `firelite.Query`.
- Firestore-style fluent API:
  - `client.Collection("users").Doc("alice").Set(...)`
  - `Where(...).OrderBy(...).Limit(...).Get()`
  - `Batch().Set(...).Delete(...).Commit()`
  - `RunTransaction(func(tx *firelite.Tx) error { ... })`
- Watch callback bridge backed by `fl_engine_watch`.

## Install

```bash
go mod init example
# then copy/replace module path as desired
```

## Minimal usage

```go
package main

import (
  "fmt"
  firelite "github.com/firelite-db/firelite-go/firelite"
)

func main() {
  db, err := firelite.OpenClient("./data.firelite")
  if err != nil { panic(err) }
  defer db.Close()

  users := db.Collection("users")
  err = users.Doc("alice").Set(map[string]any{
    "name": "Alice",
    "age":  30,
    "tags": []any{"admin", "beta"},
  })
  if err != nil { panic(err) }

  snap, err := users.Doc("alice").Get()
  if err != nil { panic(err) }
  fmt.Println(snap.Exists, snap.Data)
}
```
