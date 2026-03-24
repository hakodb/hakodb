#ifndef FIRELITE_C_H
#define FIRELITE_C_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

typedef struct FL_Array FL_Array;
typedef struct FL_Batch FL_Batch;
typedef struct FL_Config FL_Config;
typedef struct FL_Doc FL_Doc;
typedef struct FL_Engine FL_Engine;
typedef struct FL_Query FL_Query;
typedef struct FL_Transaction FL_Transaction;
typedef struct FL_Watch FL_Watch;

typedef void (*FL_OnSnapshotCallback)(const char *collection, const char *path, int32_t kind, void *user_data);

FL_Engine *fl_engine_open(const char *path);
FL_Engine *fl_engine_open_with_config(const char *path, FL_Config *config);
void fl_engine_free(FL_Engine *engine);

FL_Config *fl_config_new(void);
void fl_config_free(FL_Config *config);
void fl_config_set_durability(FL_Config *config, int32_t mode);
void fl_config_set_encryption_key(FL_Config *config, const char *key);
void fl_config_set_audit_log(FL_Config *config, bool enabled, const char *path);
void fl_config_set_query_workers(FL_Config *config, uintptr_t count);
void fl_config_set_memory_limits(FL_Config *config, uintptr_t mmap_size, uintptr_t max_inlined_bytes);
void fl_config_set_storage_tuning(FL_Config *config, uintptr_t page_size, uintptr_t compaction_threshold, uintptr_t group_commit_max_ops);
void fl_config_set_compression(FL_Config *config, bool enabled, int32_t level);

FL_Watch *fl_engine_watch(FL_Engine *engine, const char *collection, FL_OnSnapshotCallback callback, void *user_data_ptr);
void fl_watch_free(FL_Watch *watch);

FL_Doc *fl_doc_new(void);
void fl_doc_free(FL_Doc *doc);
char *fl_doc_to_json(const FL_Doc *doc);
int32_t fl_doc_insert_str(FL_Doc *doc, const char *key, const char *value);
int32_t fl_doc_insert_int(FL_Doc *doc, const char *key, int64_t value);
int32_t fl_doc_insert_float(FL_Doc *doc, const char *key, double value);
int32_t fl_doc_insert_bool(FL_Doc *doc, const char *key, bool value);
int32_t fl_doc_insert_null(FL_Doc *doc, const char *key);
int32_t fl_doc_insert_bin(FL_Doc *doc, const char *key, const uint8_t *data, uintptr_t len);
int32_t fl_doc_insert_timestamp(FL_Doc *doc, const char *key, int64_t micros);
int32_t fl_doc_insert_server_timestamp(FL_Doc *doc, const char *key);
int32_t fl_doc_insert_doc(FL_Doc *parent, const char *key, const FL_Doc *child);
int32_t fl_doc_insert_array(FL_Doc *doc, const char *key, FL_Array *array);
int32_t fl_doc_insert_reference(FL_Doc *doc, const char *key, const char *target_collection, const char *target_id);

FL_Array *fl_array_new(void);
void fl_array_free(FL_Array *array);
int32_t fl_array_append_str(FL_Array *array, const char *value);
int32_t fl_array_append_int(FL_Array *array, int64_t value);
int32_t fl_array_append_doc(FL_Array *array, const FL_Doc *doc);

int32_t fl_engine_insert(FL_Engine *engine, const char *collection, const char *doc_id, const FL_Doc *doc);
FL_Doc *fl_engine_get(FL_Engine *engine, const char *collection, const char *doc_id);
int32_t fl_engine_delete(FL_Engine *engine, const char *collection, const char *doc_id);
int32_t fl_engine_patch(FL_Engine *engine, const char *collection, const char *doc_id, const FL_Doc *updates);
FL_Doc *fl_engine_get_by_ref(FL_Engine *engine, const FL_Doc *doc, const char *field_key);

FL_Batch *fl_batch_new(void);
void fl_batch_free(FL_Batch *batch);
int32_t fl_batch_set(FL_Batch *batch, const char *collection, const char *doc_id, const FL_Doc *doc);
int32_t fl_batch_delete(FL_Batch *batch, const char *collection, const char *doc_id);
int32_t fl_batch_commit(FL_Engine *engine, FL_Batch *batch);

FL_Query *fl_query_new(const char *collection);
void fl_query_free(FL_Query *query);
int32_t fl_query_where_eq_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_eq_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_or_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_or_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_in(FL_Query *query, const char *field, FL_Array *array);
int32_t fl_query_where_not_in(FL_Query *query, const char *field, FL_Array *array);
int32_t fl_query_where_array_contains_any(FL_Query *query, const char *field, FL_Array *array);
int32_t fl_query_where_array_contains(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_match(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_contains(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_starts_with(FL_Query *query, const char *field, const char *value);
int32_t fl_query_order_by(FL_Query *query, const char *field, bool ascending);
int32_t fl_query_limit(FL_Query *query, uintptr_t limit);
int32_t fl_query_offset(FL_Query *query, uintptr_t offset);
int32_t fl_query_select_field(FL_Query *query, const char *field);
int32_t fl_query_start_after(FL_Query *query, const FL_Doc *anchor_doc);
int32_t fl_query_start_at(FL_Query *query, const FL_Doc *anchor_doc);
int32_t fl_query_end_at(FL_Query *query, const FL_Doc *anchor_doc);
int32_t fl_query_end_before(FL_Query *query, const FL_Doc *anchor_doc);
char *fl_query_execute(FL_Engine *engine, const FL_Query *query);
int32_t fl_query_aggregate_count(FL_Query *query);
int32_t fl_query_aggregate_sum(FL_Query *query, const char *field);
int32_t fl_query_aggregate_avg(FL_Query *query, const char *field);
char *fl_query_execute_aggregation(FL_Engine *engine, const FL_Query *query);

uint32_t fl_engine_create_index(FL_Engine *engine, const char *collection, const char *fields_json);
int32_t fl_engine_create_simple_index(FL_Engine *engine, const char *collection, const char *field);
int32_t fl_engine_create_fts_index(FL_Engine *engine, const char *collection, const char *field);
int32_t fl_engine_snapshot_indices(FL_Engine *engine);

FL_Transaction *fl_transaction_begin(FL_Engine *engine);
FL_Doc *fl_transaction_get(FL_Engine *engine, FL_Transaction *tx, const char *collection, const char *doc_id);
int32_t fl_transaction_set(FL_Transaction *tx, const char *collection, const char *doc_id, const FL_Doc *doc);
int32_t fl_transaction_commit(FL_Engine *engine, FL_Transaction *tx);
void fl_transaction_free(FL_Transaction *tx);

int32_t fl_engine_backup(FL_Engine *engine, const char *path);
int32_t fl_engine_compact(FL_Engine *engine);
char *fl_engine_list_collections(FL_Engine *engine);
char *fl_engine_get_stats(FL_Engine *engine);
char *fl_engine_get_audit_log(FL_Engine *engine);
int32_t fl_engine_insert_subdoc(FL_Engine *engine, const char *col, const char *id, const char *sub_col, const char *sub_id, const FL_Doc *doc);

const char *fl_last_error(void);
void fl_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif
