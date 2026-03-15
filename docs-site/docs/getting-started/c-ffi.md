---
title: C-FFI (Flat C API)
---

## Build shared library

```bash
cargo build --release
```

Use generated header `include/firelite.h` and load the produced dynamic library:

- Linux: `libfirelite.so`
- macOS: `libfirelite.dylib`
- Windows: `firelite.dll`

## Hello World

```c
#include "firelite.h"
#include <stdio.h>

int main(void) {
  FL_Engine* db = fl_engine_open("./data.firelite");
  FL_Doc* doc = fl_doc_new();
  fl_doc_insert_str(doc, "name", "alice");
  fl_doc_insert_int(doc, "age", 30);

  fl_engine_insert(db, "users", "u1", doc);

  FL_Doc* loaded = fl_engine_get(db, "users", "u1");
  char* json = fl_doc_to_json(loaded);
  printf("%s\n", json);

  fl_string_free(json);
  fl_doc_free(loaded);
  fl_doc_free(doc);
  fl_engine_free(db);
  return 0;
}
```
