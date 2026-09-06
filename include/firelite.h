#include <cstdarg>
#include <cstdint>
#include <cstdlib>
#include <ostream>
#include <new>

constexpr static const uint8_t INSERT = 1;

constexpr static const uint8_t DELETE = 2;

struct FL_Array;

struct FL_Batch;

struct FL_CloudSync;

struct FL_Config;

struct FL_Doc;

struct FL_Engine;

struct FL_NetSyncer;

struct FL_Query;

struct FL_ResultSet;

struct FL_Transaction;

struct FL_Watch;

using FL_OnSnapshotCallback = void(*)(const char *collection,
                                      const char *path,
                                      int32_t kind,
                                      void *user_data);

extern "C" {

FL_Engine *fl_engine_open(const char *path);

bool fl_engine_is_indexes_ready(FL_Engine *engine);

FL_Config *fl_config_new();

void fl_config_free(FL_Config *config);

void fl_config_set_durability(FL_Config *config, int32_t mode);

void fl_config_set_encryption_key(FL_Config *config, const char *key);

/// Set which collections should be encrypted.
/// collections_json: A JSON array of strings, e.g., '["secrets", "private_messages"]'
int32_t fl_config_set_encrypted_collections(FL_Config *config, const char *collections_json);

void fl_config_set_audit_log(FL_Config *config, bool enabled, const char *path);

void fl_config_set_query_workers(FL_Config *config, uintptr_t count);

void fl_config_set_memory_limits(FL_Config *config,
                                 uintptr_t mmap_size,
                                 uintptr_t max_inlined_bytes);

void fl_config_set_storage_tuning(FL_Config *config,
                                  uintptr_t page_size,
                                  uintptr_t compaction_threshold,
                                  uintptr_t group_commit_max_ops);

void fl_config_set_blob_threshold(FL_Config *config, uintptr_t threshold_bytes);

/// WAL headroom reservation in bytes (0 = off, default 4MB). Preallocated
/// ahead of the write position so steady-state appends never extend the
/// file. Sparse: consumes no disk until written. Ignored for Manual.
void fl_config_set_wal_reserve_bytes(FL_Config *config, uint64_t bytes);

/// Write-path phase breakdown (see engine::write_stats_report). Returns a
/// fresh C string the caller frees with fl_string_free. Counters reset.
char *fl_debug_write_stats();

/// Opens the engine using a custom config.
/// Note: This function takes ownership of the config and will free it automatically.
FL_Engine *fl_engine_open_with_config(const char *path, FL_Config *config);

FL_Watch *fl_engine_watch(FL_Engine *engine,
                          const char *collection,
                          FL_OnSnapshotCallback callback,
                          void *user_data_ptr);

void fl_watch_free(FL_Watch *watch);

void fl_engine_free(FL_Engine *engine);

FL_Doc *fl_doc_new();

void fl_doc_free(FL_Doc *doc);

int32_t fl_doc_insert_str(FL_Doc *doc, const char *key, const char *value);

int32_t fl_doc_insert_int(FL_Doc *doc, const char *key, int64_t value);

int32_t fl_doc_insert_float(FL_Doc *doc, const char *key, double value);

int32_t fl_doc_insert_bool(FL_Doc *doc, const char *key, bool value);

int32_t fl_doc_insert_null(FL_Doc *doc, const char *key);

int32_t fl_doc_insert_bin(FL_Doc *doc, const char *key, const uint8_t *data, uintptr_t len);

int32_t fl_engine_insert(FL_Engine *engine,
                         const char *collection,
                         const char *doc_id,
                         const FL_Doc *doc);

/// Owned-doc insert: takes over the FL_Doc handle (no deep clone).
/// The handle is ALWAYS consumed — success or failure — do not use or free
/// `doc` after the call.
int32_t fl_engine_insert_take(FL_Engine *engine,
                              const char *collection,
                              const char *doc_id,
                              FL_Doc *doc);

FL_Doc *fl_engine_get(FL_Engine *engine, const char *collection, const char *doc_id);

int32_t fl_engine_delete(FL_Engine *engine, const char *collection, const char *doc_id);

/// Local-only delete: marks the key so no sync tailer or handshake
/// catch-up ever transmits it, then deletes normally (fresh tombstone
/// timestamp keeps the version clock advanced — handshake-stable).
int32_t fl_engine_delete_local(FL_Engine *engine, const char *collection, const char *doc_id);

/// Marks a collection local-only (`local != 0`) or rejoins it to sync.
/// A local-only collection never emits nor is caught up from the network.
int32_t fl_engine_set_collection_local(FL_Engine *engine, const char *collection, int32_t local);

/// Opts a key back into replication (future ops only).
int32_t fl_engine_replicate_key(FL_Engine *engine, const char *collection, const char *doc_id);

/// Opts a whole collection back into replication (clears flag + key marks).
int32_t fl_engine_replicate_collection(FL_Engine *engine, const char *collection);

/// Vacuum: purge a collection's tombstones. Emits no WAL op (never
/// replicates); drops the version so the next handshake pulls peer state.
/// Returns tombstones purged, or -1 on error.
int32_t fl_engine_vacuum_collection(FL_Engine *engine, const char *collection);

FL_Batch *fl_batch_new();

void fl_batch_free(FL_Batch *batch);

int32_t fl_batch_set(FL_Batch *batch, const char *collection, const char *doc_id, FL_Doc *doc);

int32_t fl_batch_delete(FL_Batch *batch, const char *collection, const char *doc_id);

int32_t fl_batch_commit(FL_Engine *engine, FL_Batch *batch);

FL_Query *fl_query_new(const char *collection);

void fl_query_free(FL_Query *query);

int32_t fl_query_where_eq_str(FL_Query *query, const char *field, const char *value);

int32_t fl_query_where_eq_bool(FL_Query *query, const char *field, bool value);

/// Executes the query and deletes all matching documents.
/// Returns the number of deleted documents, or -1 on error.
int32_t fl_query_delete(FL_Engine *engine, FL_Query *query);

/// Local-only mass delete: marks every match so the wipe never leaves
/// this device, then deletes. See `fl_engine_delete_local`.
int32_t fl_query_delete_local(FL_Engine *engine, FL_Query *query);

/// Executes the query and applies the updates from 'patch_doc' to all matches.
/// Returns the number of updated documents, or -1 on error.
int32_t fl_query_patch(FL_Engine *engine, FL_Query *query, const FL_Doc *patch_doc);

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

int32_t fl_query_where_array_contains(FL_Query *query, const char *field, const char *value);

int32_t fl_query_where_array_contains_any(FL_Query *query, const char *field, FL_Array *array);

int32_t fl_query_where_not_in(FL_Query *query, const char *field, FL_Array *array);

int32_t fl_query_order_by(FL_Query *query, const char *field, bool ascending);

int32_t fl_query_limit(FL_Query *query, uintptr_t limit);

/// Opt in to deferred blobs: matching docs come back with blob-backed
/// fields as `Value::BlobLink` placeholders (no blob-file reads).
/// Resolve later with `fl_doc_resolve_blobs`. Default off (eager).
int32_t fl_query_defer_blobs(FL_Query *query, int defer);

/// Resolve deferred blob fields of a query-returned doc in place.
/// No-op for docs without BlobLinks. Needs the owning collection (blob
/// addresses are per-shard).
int32_t fl_doc_resolve_blobs(FL_Engine *engine, const char *collection, FL_Doc *doc);

int32_t fl_query_offset(FL_Query *query, uintptr_t offset);

int32_t fl_query_select_field(FL_Query *query, const char *field);

char *fl_query_execute(FL_Engine *engine, const FL_Query *query);

FL_ResultSet *fl_query_execute_to_handles(FL_Engine *engine, const FL_Query *query);

uintptr_t fl_result_set_count(FL_ResultSet *results);

FL_Doc *fl_result_set_get_doc(FL_ResultSet *results, uintptr_t index);

void fl_result_set_free(FL_ResultSet *results);

/// Bulk result-set to JSON: one call, one JSON array string, no per-doc
/// DOM and no per-doc FFI round trips. Byte-identical to joining
/// `fl_doc_to_json` per row. Caller frees with `fl_string_free`.
char *fl_result_set_to_json(FL_ResultSet *results);

char *fl_doc_to_json(const FL_Doc *doc);

const char *fl_last_error();

/// Enable library diagnostic logging to stderr. Default OFF. Idempotent.
void fl_log_enable_stderr();

/// Disable library diagnostic logging to stderr. Default OFF. Idempotent.
void fl_log_disable_stderr();

void fl_string_free(char *value);

int32_t fl_query_aggregate_count(FL_Query *query);

int32_t fl_query_aggregate_sum(FL_Query *query, const char *field);

int32_t fl_query_aggregate_avg(FL_Query *query, const char *field);

char *fl_query_execute_aggregation(FL_Engine *engine, const FL_Query *query);

int32_t fl_doc_insert_timestamp(FL_Doc *doc, const char *key, int64_t micros);

int32_t fl_doc_insert_server_timestamp(FL_Doc *doc, const char *key);

int32_t fl_engine_backup(FL_Engine *engine, const char *path);

int32_t fl_query_where_match(FL_Query *query, const char *field, const char *value);

int32_t fl_query_where_match_prefix(FL_Query *query, const char *field, const char *value);

int32_t fl_query_where_contains(FL_Query *query, const char *field, const char *value);

int32_t fl_query_where_starts_with(FL_Query *query, const char *field, const char *value);

char *fl_engine_list_collections(FL_Engine *engine);

FL_Array *fl_array_new();

void fl_array_free(FL_Array *array);

int32_t fl_array_append_str(FL_Array *array, const char *value);

int32_t fl_array_append_int(FL_Array *array, int64_t value);

int32_t fl_array_append_doc(FL_Array *array, const FL_Doc *doc);

/// Takes the contents of 'child' and inserts it as a Map into 'parent'
int32_t fl_doc_insert_doc(FL_Doc *parent, const char *key, const FL_Doc *child);

/// Takes the contents of 'array' and inserts it into the document
int32_t fl_doc_insert_array(FL_Doc *doc, const char *key, FL_Array *array);

int32_t fl_engine_patch(FL_Engine *engine,
                        const char *collection,
                        const char *doc_id,
                        const FL_Doc *updates);

/// Creates a composite index from C++.
/// fields_json should be like: [{"field": "age", "desc": false}]
uint32_t fl_engine_create_index(FL_Engine *engine, const char *collection, const char *fields_json);

/// Simplified indexer: Create an index for a single field.
int32_t fl_engine_create_simple_index(FL_Engine *engine, const char *collection, const char *field);

int32_t fl_engine_create_fts_index(FL_Engine *engine, const char *collection, const char *field);

char *fl_engine_list_indexes(FL_Engine *engine, const char *collection);

FL_Transaction *fl_transaction_begin(FL_Engine *engine);

FL_Doc *fl_transaction_get(FL_Engine *engine,
                           FL_Transaction *tx,
                           const char *collection,
                           const char *doc_id);

int32_t fl_transaction_set(FL_Transaction *tx,
                           const char *collection,
                           const char *doc_id,
                           const FL_Doc *doc);

int32_t fl_transaction_commit(FL_Engine *engine, FL_Transaction *tx);

void fl_transaction_free(FL_Transaction *tx);

int32_t fl_engine_insert_subdoc(FL_Engine *engine,
                                const char *col,
                                const char *id,
                                const char *sub_col,
                                const char *sub_id,
                                const FL_Doc *doc);

int32_t fl_engine_compact(FL_Engine *engine);

char *fl_engine_get_stats(FL_Engine *engine);

int32_t fl_doc_insert_reference(FL_Doc *doc,
                                const char *key,
                                const char *target_collection,
                                const char *target_id);

/// Given a document and a field name containing a Reference, fetch the target document.
/// Returns a new FL_Doc handle, or null if the field is not a reference or target not found.
FL_Doc *fl_engine_get_by_ref(FL_Engine *engine, const FL_Doc *doc, const char *field_key);

int32_t fl_query_start_after(FL_Query *query, const FL_Doc *anchor_doc);

int32_t fl_query_start_at(FL_Query *query, const FL_Doc *anchor_doc);

int32_t fl_query_end_at(FL_Query *query, const FL_Doc *anchor_doc);

int32_t fl_query_end_before(FL_Query *query, const FL_Doc *anchor_doc);

char *fl_engine_get_audit_log(FL_Engine *engine);

int32_t fl_query_where_or_str(FL_Query *query, const char *field, const char *value);

int32_t fl_query_where_or_int(FL_Query *query, const char *field, int64_t value);

/// Adds an IN filter: field IN [array_items]
/// This takes ownership of the FL_Array and frees it.
int32_t fl_query_where_in(FL_Query *query, const char *field, FL_Array *array);

int32_t fl_engine_snapshot_indices(FL_Engine *engine);

void fl_config_set_compression(FL_Config *config, bool enabled, int32_t level);

FL_NetSyncer *fl_net_syncer_new(FL_Engine *engine, const char *name, const char *room_key);

int32_t fl_net_syncer_start(FL_NetSyncer *syncer, uint16_t port);

char *fl_net_syncer_status(FL_NetSyncer *syncer);

void fl_net_syncer_free(FL_NetSyncer *syncer);

FL_CloudSync *fl_cloud_sync_new(FL_Engine *engine,
                                int32_t mode,
                                const char *client_id,
                                const char *room_name,
                                const char *room_key,
                                const char *auth_token);

/// Creates a room-agnostic cloud SERVER. Not bound to any room: the server
/// accepts and persists any (room_name, room_key) pair its clients ask for and
/// routes sync to the matching room group.
FL_CloudSync *fl_cloud_sync_server_new(FL_Engine *engine,
                                       const char *server_id,
                                       const char *auth_token);

/// Creates an offline-first cloud CLIENT bound to a room of the caller's
/// choosing. The client picks the room (room_name + room_key) and later picks
/// the server via `fl_cloud_sync_start`.
FL_CloudSync *fl_cloud_sync_client_new(FL_Engine *engine,
                                       const char *client_id,
                                       const char *room_name,
                                       const char *room_key,
                                       const char *auth_token);

int32_t fl_cloud_sync_start(FL_CloudSync *cloud_sync, const char *address);

char *fl_cloud_sync_status(FL_CloudSync *cloud_sync);

void fl_cloud_sync_stop(FL_CloudSync *cloud_sync);

void fl_cloud_sync_free(FL_CloudSync *cloud_sync);

}  // extern "C"
