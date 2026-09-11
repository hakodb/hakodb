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
typedef struct FL_CloudSync FL_CloudSync;
typedef struct FL_Config FL_Config;
typedef struct FL_Doc FL_Doc;
typedef struct FL_Engine FL_Engine;
typedef struct FL_NetSyncer FL_NetSyncer;
typedef struct FL_Query FL_Query;
typedef struct FL_RawDoc FL_RawDoc;
typedef struct FL_RawResultSet FL_RawResultSet;
typedef struct FL_ViewDoc FL_ViewDoc;
typedef struct FL_ResultSet FL_ResultSet;
typedef struct FL_Transaction FL_Transaction;
typedef struct FL_Watch FL_Watch;

typedef void (*FL_OnSnapshotCallback)(const char *collection, const char *path, int32_t kind, void *user_data);

FL_Engine *fl_engine_open(const char *path);
bool fl_engine_is_indexes_ready(FL_Engine *engine);
FL_Engine *fl_engine_open_with_config(const char *path, FL_Config *config);
void fl_engine_free(FL_Engine *engine);

FL_Config *fl_config_new(void);
void fl_config_free(FL_Config *config);
void fl_config_set_durability(FL_Config *config, int32_t mode);
void fl_config_set_encryption_key(FL_Config *config, const char *key);
int32_t fl_config_set_encrypted_collections(FL_Config *config, const char *collections_json);
void fl_config_set_audit_log(FL_Config *config, bool enabled, const char *path);
void fl_config_set_query_workers(FL_Config *config, uintptr_t count);
void fl_config_set_memory_limits(FL_Config *config, uintptr_t mmap_size, uintptr_t max_inlined_bytes);
void fl_config_set_storage_tuning(FL_Config *config, uintptr_t page_size, uintptr_t compaction_threshold, uintptr_t group_commit_max_ops);
void fl_config_set_blob_threshold(FL_Config *config, uintptr_t threshold_bytes);
void fl_config_set_wal_reserve_bytes(FL_Config *config, uint64_t bytes);
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
/* Owned-doc insert: consumes the FL_Doc handle (no deep clone). */
int32_t fl_engine_insert_take(FL_Engine *engine, const char *collection, const char *doc_id, FL_Doc *doc);
/* Resolve deferred blob fields of a query-returned doc in place. */
int32_t fl_doc_resolve_blobs(FL_Engine *engine, const char *collection, FL_Doc *doc);
FL_Doc *fl_engine_get(FL_Engine *engine, const char *collection, const char *doc_id);
int32_t fl_engine_delete(FL_Engine *engine, const char *collection, const char *doc_id);
int32_t fl_engine_delete_local(FL_Engine *engine, const char *collection, const char *doc_id);
int32_t fl_engine_set_collection_local(FL_Engine *engine, const char *collection, int32_t local);
int32_t fl_engine_replicate_key(FL_Engine *engine, const char *collection, const char *doc_id);
int32_t fl_engine_replicate_collection(FL_Engine *engine, const char *collection);
int32_t fl_engine_vacuum_collection(FL_Engine *engine, const char *collection);
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
int32_t fl_query_where_eq_bool(FL_Query *query, const char *field, bool value);
int32_t fl_query_where_eq_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_ne_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_ne_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_gt_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_gt_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_gte_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_gte_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_lt_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_lt_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_lte_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_lte_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_or_str(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_or_int(FL_Query *query, const char *field, int64_t value);
int32_t fl_query_where_in(FL_Query *query, const char *field, FL_Array *array);
int32_t fl_query_where_not_in(FL_Query *query, const char *field, FL_Array *array);
int32_t fl_query_where_array_contains_any(FL_Query *query, const char *field, FL_Array *array);
int32_t fl_query_where_array_contains(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_match(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_match_prefix(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_contains(FL_Query *query, const char *field, const char *value);
int32_t fl_query_where_starts_with(FL_Query *query, const char *field, const char *value);
int32_t fl_query_order_by(FL_Query *query, const char *field, bool ascending);
int32_t fl_query_limit(FL_Query *query, uintptr_t limit);
int32_t fl_query_offset(FL_Query *query, uintptr_t offset);
int32_t fl_query_select_field(FL_Query *query, const char *field);
int32_t fl_query_defer_blobs(FL_Query *query, int defer);
int32_t fl_query_start_after(FL_Query *query, const FL_Doc *anchor_doc);
int32_t fl_query_start_at(FL_Query *query, const FL_Doc *anchor_doc);
int32_t fl_query_end_at(FL_Query *query, const FL_Doc *anchor_doc);
int32_t fl_query_end_before(FL_Query *query, const FL_Doc *anchor_doc);
char *fl_query_execute(FL_Engine *engine, const FL_Query *query);
int32_t fl_query_delete(FL_Engine *engine, FL_Query *query);
int32_t fl_query_delete_local(FL_Engine *engine, FL_Query *query);
int32_t fl_query_patch(FL_Engine *engine, FL_Query *query, const FL_Doc *patch_doc);
FL_ResultSet *fl_query_execute_to_handles(FL_Engine *engine, const FL_Query *query);
uintptr_t fl_result_set_count(FL_ResultSet *results);
FL_Doc *fl_result_set_get_doc(FL_ResultSet *results, uintptr_t index);
void fl_result_set_free(FL_ResultSet *results);
/* Bulk result-set to JSON: one call, one JSON array string. Free with fl_string_free. */
char *fl_result_set_to_json(FL_ResultSet *results);
/* Raw result sets (v0.8.3): pinned storage bytes instead of decoded docs.
   Borrowed-handle contract mirrors FL_ResultSet: row pointers die with
   fl_rawresult_free. Bytes are opaque storage encoding. */
FL_RawResultSet *fl_query_execute_raw(FL_Engine *engine, const FL_Query *query);
uintptr_t fl_rawresult_count(FL_RawResultSet *results);
FL_RawDoc *fl_rawresult_get(FL_RawResultSet *results, uintptr_t index);
void fl_rawresult_free(FL_RawResultSet *results);
const uint8_t *fl_rawdoc_bytes(const FL_RawDoc *doc, uintptr_t *len_out);
const char *fl_rawdoc_id(const FL_RawDoc *doc, uintptr_t *len_out);
int32_t fl_query_start_after_raw(FL_Query *query, const FL_RawDoc *anchor_doc);
FL_Doc *fl_rawdoc_to_doc(FL_Engine *engine, const FL_RawDoc *raw_doc, const char *collection);
/* Zero-alloc walk (v0.8.6): one call per scan; rows lent to the callback
   (true = continue). Returns rows visited, -1 on error. */
typedef bool (*FlWalkCallback)(const char *id, uintptr_t id_len, const uint8_t *bytes, uintptr_t bytes_len, void *userdata);
int64_t fl_cursor_walk(FL_Engine *engine, const FL_Query *query, FlWalkCallback callback, void *userdata);
/* Borrowed views (v0.8.11): pinned bytes + lazy typed pulls, no owned
   construction. Strict scalar matches; views never inflate blobs. */
FL_ViewDoc *fl_view_get(FL_Engine *engine, const char *collection, const char *doc_id);
void fl_view_free(FL_ViewDoc *view);
uintptr_t fl_view_field_count(const FL_ViewDoc *view);
bool fl_view_has_field(const FL_ViewDoc *view, const char *key);
bool fl_view_get_int(const FL_ViewDoc *view, const char *key, int64_t *out);
bool fl_view_get_float(const FL_ViewDoc *view, const char *key, double *out);
int32_t fl_view_get_bool(const FL_ViewDoc *view, const char *key);
const char *fl_view_get_str(const FL_ViewDoc *view, const char *key, uintptr_t *len_out);
const uint8_t *fl_view_get_bytes(const FL_ViewDoc *view, const char *key, uintptr_t *len_out);
FL_Doc *fl_view_to_doc(const FL_ViewDoc *view, const char *doc_id);
typedef bool (*FlViewWalkCallback)(const char *id, uintptr_t id_len, const FL_ViewDoc *view, void *userdata);
int64_t fl_cursor_walk_view(FL_Engine *engine, const FL_Query *query, FlViewWalkCallback callback, void *userdata);
int32_t fl_query_aggregate_count(FL_Query *query);
int32_t fl_query_aggregate_sum(FL_Query *query, const char *field);
int32_t fl_query_aggregate_avg(FL_Query *query, const char *field);
char *fl_query_execute_aggregation(FL_Engine *engine, const FL_Query *query);

uint32_t fl_engine_create_index(FL_Engine *engine, const char *collection, const char *fields_json);
int32_t fl_engine_create_simple_index(FL_Engine *engine, const char *collection, const char *field);
int32_t fl_engine_create_fts_index(FL_Engine *engine, const char *collection, const char *field);
char *fl_engine_list_indexes(FL_Engine *engine, const char *collection);
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

FL_NetSyncer *fl_net_syncer_new(FL_Engine *engine, const char *name, const char *room_key);
int32_t fl_net_syncer_start(FL_NetSyncer *syncer, uint16_t port);
int32_t fl_net_syncer_set_discovery(FL_NetSyncer *syncer, int32_t mode);
char *fl_net_syncer_status(FL_NetSyncer *syncer);
void fl_net_syncer_free(FL_NetSyncer *syncer);

FL_CloudSync *fl_cloud_sync_new(FL_Engine *engine, int32_t mode, const char *client_id, const char *room_name, const char *room_key, const char *auth_token);
FL_CloudSync *fl_cloud_sync_server_new(FL_Engine *engine, const char *server_id, const char *auth_token);
FL_CloudSync *fl_cloud_sync_client_new(FL_Engine *engine, const char *client_id, const char *room_name, const char *room_key, const char *auth_token);
int32_t fl_cloud_sync_start(FL_CloudSync *cloud_sync, const char *address);
char *fl_cloud_sync_status(FL_CloudSync *cloud_sync);
void fl_cloud_sync_stop(FL_CloudSync *cloud_sync);
void fl_cloud_sync_free(FL_CloudSync *cloud_sync);

const char *fl_last_error(void);
void fl_string_free(char *value);

#ifdef __cplusplus
}
#endif

#endif
