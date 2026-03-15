---
title: C-FFI API Signatures (include/firelite.h)
---

## Engine and errors

```c
FL_Engine *fl_engine_open(const char *path);
void fl_engine_free(FL_Engine *engine);
const char *fl_last_error(void);
void fl_string_free(char *value);
```

## Document builder

```c
FL_Doc *fl_doc_new(void);
void fl_doc_free(FL_Doc *doc);
int32_t fl_doc_insert_str(FL_Doc *doc, const char *key, const char *value);
int32_t fl_doc_insert_int(FL_Doc *doc, const char *key, int64_t value);
int32_t fl_doc_insert_float(FL_Doc *doc, const char *key, double value);
int32_t fl_doc_insert_bool(FL_Doc *doc, const char *key, bool value);
int32_t fl_doc_insert_null(FL_Doc *doc, const char *key);
int32_t fl_doc_insert_bin(FL_Doc *doc, const char *key, const uint8_t *data, uintptr_t len);
char *fl_doc_to_json(const FL_Doc *doc);
```

## CRUD / query / batch

```c
int32_t fl_engine_insert(FL_Engine*, const char *collection, const char *doc_id, const FL_Doc *doc);
FL_Doc *fl_engine_get(FL_Engine*, const char *collection, const char *doc_id);
int32_t fl_engine_delete(FL_Engine*, const char *collection, const char *doc_id);

FL_Batch *fl_batch_new(void);
void fl_batch_free(FL_Batch *batch);
int32_t fl_batch_set(FL_Batch*, const char *collection, const char *doc_id, const FL_Doc *doc);
int32_t fl_batch_delete(FL_Batch*, const char *collection, const char *doc_id);
int32_t fl_batch_commit(FL_Engine*, FL_Batch *batch);

FL_Query *fl_query_new(const char *collection);
void fl_query_free(FL_Query *query);
int32_t fl_query_where_eq_str(FL_Query*, const char *field, const char *value);
int32_t fl_query_where_eq_int(FL_Query*, const char *field, int64_t value);
int32_t fl_query_order_by(FL_Query*, const char *field, bool ascending);
int32_t fl_query_limit(FL_Query*, uintptr_t limit);
int32_t fl_query_select_field(FL_Query*, const char *field);
char *fl_query_execute(FL_Engine*, const FL_Query *query);
```
