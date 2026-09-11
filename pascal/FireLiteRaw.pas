unit FireLiteRaw;

{$mode objfpc}{$H+}

interface

uses
  ctypes;

type
  PFL_Engine = Pointer;
  PFL_Doc = Pointer;
  PFL_Batch = Pointer;
  PFL_Query = Pointer;
  PFL_Config = Pointer;
  PFL_Watch = Pointer;
  PFL_NetSyncer = Pointer;
  PFL_Array = Pointer;       // Added in v0.5.9
  PFL_Transaction = Pointer; // Added in v0.5.9
  PFL_ResultSet = Pointer;   // Added in v0.6.x
  PFL_CloudSync = Pointer;   // Added in v0.6.65
  PFL_RawDoc = Pointer;      // Added in v0.8.3
  PFL_RawResultSet = Pointer;// Added in v0.8.3
  PFL_ViewDoc = Pointer;     // Added in v0.8.11

  { Callback for real-time snapshots }
  TFL_OnSnapshotCallback = procedure(collection: PChar; path: PChar; kind: cint32; user_data: Pointer); cdecl;
  { Raw walk callback (v0.8.6): return True to continue. Borrowed pointers,
    valid for the call only. Must not re-enter the engine. }
  TFL_WalkCallback = function(id: PChar; id_len: SizeUInt; bytes: PByte; bytes_len: SizeUInt; user_data: Pointer): cbool; cdecl;
  { View-walk callback (v0.8.11): borrowed id + view handle, valid for the call only. }
  TFL_ViewWalkCallback = function(id: PChar; id_len: SizeUInt; view: PFL_ViewDoc; user_data: Pointer): cbool; cdecl;

const
  FL_CHANGE_PUT = 1;
  FL_CHANGE_DELETE = 2;

{$if defined(Windows)}
const FIRELITE_LIB = 'firelite.dll';
{$elseif defined(Darwin)}
const FIRELITE_LIB = 'libfirelite.dylib';
{$else}
const FIRELITE_LIB = 'libfirelite.so';
{$endif}

{ Engine Management }
function fl_engine_open(path: PChar): PFL_Engine; cdecl; external FIRELITE_LIB;
function fl_engine_is_indexes_ready(engine: PFL_Engine): cbool; cdecl; external FIRELITE_LIB;
function fl_engine_open_with_config(path: PChar; config: PFL_Config): PFL_Engine; cdecl; external FIRELITE_LIB;
procedure fl_engine_free(engine: PFL_Engine); cdecl; external FIRELITE_LIB;
function fl_engine_backup(engine: PFL_Engine; path: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_compact(engine: PFL_Engine): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_list_collections(engine: PFL_Engine): PChar; cdecl; external FIRELITE_LIB;
function fl_engine_list_indexes(engine: PFL_Engine; collection: PChar): PChar; cdecl; external FIRELITE_LIB;
function fl_engine_get_stats(engine: PFL_Engine): PChar; cdecl; external FIRELITE_LIB;
function fl_engine_get_audit_log(engine: PFL_Engine): PChar; cdecl; external FIRELITE_LIB;
function fl_engine_snapshot_indices(engine: PFL_Engine): cint32; cdecl; external FIRELITE_LIB;

{ Configuration Builder }
function fl_config_new: PFL_Config; cdecl; external FIRELITE_LIB;
procedure fl_config_free(config: PFL_Config); cdecl; external FIRELITE_LIB;
procedure fl_config_set_durability(config: PFL_Config; mode: cint32); cdecl; external FIRELITE_LIB;
procedure fl_config_set_encryption_key(config: PFL_Config; key: PChar); cdecl; external FIRELITE_LIB;
function fl_config_set_encrypted_collections(config: PFL_Config; collections_json: PChar): cint32; cdecl; external FIRELITE_LIB;
procedure fl_config_set_audit_log(config: PFL_Config; enabled: cbool; path: PChar); cdecl; external FIRELITE_LIB;
procedure fl_config_set_query_workers(config: PFL_Config; count: SizeUInt); cdecl; external FIRELITE_LIB;
procedure fl_config_set_memory_limits(config: PFL_Config; mmap_size, max_inlined_bytes: SizeUInt); cdecl; external FIRELITE_LIB;
procedure fl_config_set_storage_tuning(config: PFL_Config; page_size, compaction_threshold, group_commit_max_ops: SizeUInt); cdecl; external FIRELITE_LIB;
procedure fl_config_set_blob_threshold(config: PFL_Config; threshold_bytes: SizeUInt); cdecl; external FIRELITE_LIB;
procedure fl_config_set_wal_reserve_bytes(config: PFL_Config; bytes: QWord); cdecl; external FIRELITE_LIB;
procedure fl_config_set_compression(config: PFL_Config; enabled: cbool; level: cint32); cdecl; external FIRELITE_LIB;

{ Real-time Snapshots }
function fl_engine_watch(engine: PFL_Engine; collection: PChar; callback: TFL_OnSnapshotCallback; user_data_ptr: Pointer): PFL_Watch; cdecl; external FIRELITE_LIB;
procedure fl_watch_free(watch: PFL_Watch); cdecl; external FIRELITE_LIB;

{ Document Builder }
function fl_doc_new: PFL_Doc; cdecl; external FIRELITE_LIB;
procedure fl_doc_free(doc: PFL_Doc); cdecl; external FIRELITE_LIB;
function fl_doc_insert_str(doc: PFL_Doc; key, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_int(doc: PFL_Doc; key: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_float(doc: PFL_Doc; key: PChar; value: cdouble): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_bool(doc: PFL_Doc; key: PChar; value: cbool): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_null(doc: PFL_Doc; key: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_bin(doc: PFL_Doc; key: PChar; data: PByte; len: SizeUInt): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_timestamp(doc: PFL_Doc; key: PChar; micros: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_server_timestamp(doc: PFL_Doc; key: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_doc(parent: PFL_Doc; key: PChar; child: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_array(doc: PFL_Doc; key: PChar; arr: PFL_Array): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_insert_reference(doc: PFL_Doc; key, target_col, target_id: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_doc_to_json(doc: PFL_Doc): PChar; cdecl; external FIRELITE_LIB;

{ Array Builder }
function fl_array_new: PFL_Array; cdecl; external FIRELITE_LIB;
procedure fl_array_free(arr: PFL_Array); cdecl; external FIRELITE_LIB;
function fl_array_append_str(arr: PFL_Array; value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_array_append_int(arr: PFL_Array; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_array_append_doc(arr: PFL_Array; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;

{ Sharded Operations }
function fl_engine_insert(engine: PFL_Engine; col, doc_id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
{ Owned-doc insert: consumes the FL_Doc handle (no deep clone). Do not use or free doc afterwards. }
function fl_engine_insert_take(engine: PFL_Engine; col, doc_id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
{ Resolve deferred blob fields of a query-returned doc in place. }
function fl_doc_resolve_blobs(engine: PFL_Engine; col: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_get(engine: PFL_Engine; col, doc_id: PChar): PFL_Doc; cdecl; external FIRELITE_LIB;
function fl_engine_delete(engine: PFL_Engine; col, doc_id: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_delete_local(engine: PFL_Engine; col, doc_id: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_set_collection_local(engine: PFL_Engine; col: PChar; local: cint32): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_replicate_key(engine: PFL_Engine; col, doc_id: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_replicate_collection(engine: PFL_Engine; col: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_vacuum_collection(engine: PFL_Engine; col: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_patch(engine: PFL_Engine; col, doc_id: PChar; updates: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_get_by_ref(engine: PFL_Engine; doc: PFL_Doc; field_key: PChar): PFL_Doc; cdecl; external FIRELITE_LIB;
function fl_engine_insert_subdoc(engine: PFL_Engine; col, id, sub_col, sub_id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;

{ Atomic Batches }
function fl_batch_new: PFL_Batch; cdecl; external FIRELITE_LIB;
procedure fl_batch_free(batch: PFL_Batch); cdecl; external FIRELITE_LIB;
function fl_batch_set(batch: PFL_Batch; col, doc_id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_batch_delete(batch: PFL_Batch; col, doc_id: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_batch_commit(engine: PFL_Engine; batch: PFL_Batch): cint32; cdecl; external FIRELITE_LIB;

{ Serializable Transactions }
function fl_transaction_begin(engine: PFL_Engine): PFL_Transaction; cdecl; external FIRELITE_LIB;
function fl_transaction_get(engine: PFL_Engine; tx: PFL_Transaction; col, id: PChar): PFL_Doc; cdecl; external FIRELITE_LIB;
function fl_transaction_set(tx: PFL_Transaction; col, id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_transaction_commit(engine: PFL_Engine; tx: PFL_Transaction): cint32; cdecl; external FIRELITE_LIB;
procedure fl_transaction_free(tx: PFL_Transaction); cdecl; external FIRELITE_LIB;

{ Queries }
function fl_query_new(collection: PChar): PFL_Query; cdecl; external FIRELITE_LIB;
procedure fl_query_free(query: PFL_Query); cdecl; external FIRELITE_LIB;
function fl_query_where_eq_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_eq_bool(query: PFL_Query; field: PChar; value: cbool): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_eq_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_ne_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_ne_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_gt_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_gt_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_gte_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_gte_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_lt_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_lt_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_lte_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_lte_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_or_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_or_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_in(query: PFL_Query; field: PChar; arr: PFL_Array): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_not_in(query: PFL_Query; field: PChar; arr: PFL_Array): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_array_contains_any(query: PFL_Query; field: PChar; arr: PFL_Array): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_array_contains(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_order_by(query: PFL_Query; field: PChar; ascending: cbool): cint32; cdecl; external FIRELITE_LIB;
function fl_query_limit(query: PFL_Query; limit: SizeUInt): cint32; cdecl; external FIRELITE_LIB;
function fl_query_offset(query: PFL_Query; offset: SizeUInt): cint32; cdecl; external FIRELITE_LIB;
function fl_query_select_field(query: PFL_Query; field: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_defer_blobs(query: PFL_Query; defer: cint32): cint32; cdecl; external FIRELITE_LIB;
function fl_query_execute(engine: PFL_Engine; query: PFL_Query): PChar; cdecl; external FIRELITE_LIB;
function fl_query_delete(engine: PFL_Engine; query: PFL_Query): cint32; cdecl; external FIRELITE_LIB;
function fl_query_delete_local(engine: PFL_Engine; query: PFL_Query): cint32; cdecl; external FIRELITE_LIB;
function fl_query_patch(engine: PFL_Engine; query: PFL_Query; patch_doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_query_execute_to_handles(engine: PFL_Engine; query: PFL_Query): PFL_ResultSet; cdecl; external FIRELITE_LIB;
function fl_result_set_count(results: PFL_ResultSet): SizeUInt; cdecl; external FIRELITE_LIB;
function fl_result_set_get_doc(results: PFL_ResultSet; index: SizeUInt): PFL_Doc; cdecl; external FIRELITE_LIB;
procedure fl_result_set_free(results: PFL_ResultSet); cdecl; external FIRELITE_LIB;
{ Bulk result-set to JSON: one call, one JSON array string. Free with fl_string_free. }
function fl_result_set_to_json(results: PFL_ResultSet): PChar; cdecl; external FIRELITE_LIB;
{ Borrowed views (v0.8.11): pinned bytes + lazy typed pulls, no owned
  construction. Strict scalar matches; views never inflate blobs. }
function fl_view_get(engine: PFL_Engine; col, doc_id: PChar): PFL_ViewDoc; cdecl; external FIRELITE_LIB;
procedure fl_view_free(view: PFL_ViewDoc); cdecl; external FIRELITE_LIB;
function fl_view_field_count(view: PFL_ViewDoc): SizeUInt; cdecl; external FIRELITE_LIB;
function fl_view_has_field(view: PFL_ViewDoc; key: PChar): cbool; cdecl; external FIRELITE_LIB;
function fl_view_get_int(view: PFL_ViewDoc; key: PChar; out_value: PInt64): cbool; cdecl; external FIRELITE_LIB;
function fl_view_get_float(view: PFL_ViewDoc; key: PChar; out_value: PDouble): cbool; cdecl; external FIRELITE_LIB;
function fl_view_get_bool(view: PFL_ViewDoc; key: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_view_get_str(view: PFL_ViewDoc; key: PChar; len_out: PSizeUInt): PChar; cdecl; external FIRELITE_LIB;
function fl_view_get_bytes(view: PFL_ViewDoc; key: PChar; len_out: PSizeUInt): PByte; cdecl; external FIRELITE_LIB;
function fl_view_to_doc(view: PFL_ViewDoc; doc_id: PChar): PFL_Doc; cdecl; external FIRELITE_LIB;
{ View-walk callback: borrowed id + view handle, valid for the call only. }
{ Lazy view walk (v0.8.11+): one call per scan. Returns rows visited, -1 on error. }
function fl_cursor_walk_view(engine: PFL_Engine; query: PFL_Query; callback: TFL_ViewWalkCallback; user_data: Pointer): Int64; cdecl; external FIRELITE_LIB;
{ Raw result sets (v0.8.3): pinned storage bytes, not decoded docs.
  Borrowed-handle contract mirrors FL_ResultSet. Bytes are opaque. }
function fl_query_execute_raw(engine: PFL_Engine; query: PFL_Query): PFL_RawResultSet; cdecl; external FIRELITE_LIB;
function fl_rawresult_count(results: PFL_RawResultSet): SizeUInt; cdecl; external FIRELITE_LIB;
function fl_rawresult_get(results: PFL_RawResultSet; index: SizeUInt): PFL_RawDoc; cdecl; external FIRELITE_LIB;
procedure fl_rawresult_free(results: PFL_RawResultSet); cdecl; external FIRELITE_LIB;
function fl_rawdoc_bytes(doc: PFL_RawDoc; len_out: PSizeUInt): PByte; cdecl; external FIRELITE_LIB;
function fl_rawdoc_id(doc: PFL_RawDoc; len_out: PSizeUInt): PChar; cdecl; external FIRELITE_LIB;
function fl_query_start_after_raw(query: PFL_Query; anchor: PFL_RawDoc): cint32; cdecl; external FIRELITE_LIB;
function fl_rawdoc_to_doc(engine: PFL_Engine; raw_doc: PFL_RawDoc; collection: PChar): PFL_Doc; cdecl; external FIRELITE_LIB;
{ Zero-alloc walk (v0.8.6): one call per scan. Returns rows visited, -1 on error. }
function fl_cursor_walk(engine: PFL_Engine; query: PFL_Query; callback: TFL_WalkCallback; user_data: Pointer): Int64; cdecl; external FIRELITE_LIB;
function fl_query_where_match(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_match_prefix(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_contains(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_starts_with(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_start_at(query: PFL_Query; anchor: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_query_start_after(query: PFL_Query; anchor: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_query_end_at(query: PFL_Query; anchor: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_query_end_before(query: PFL_Query; anchor: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;

{ Aggregates }
function fl_query_aggregate_count(query: PFL_Query): cint32; cdecl; external FIRELITE_LIB;
function fl_query_aggregate_sum(query: PFL_Query; field: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_aggregate_avg(query: PFL_Query; field: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_execute_aggregation(engine: PFL_Engine; query: PFL_Query): PChar; cdecl; external FIRELITE_LIB;

{ Manual Indexing }
function fl_engine_create_index(engine: PFL_Engine; col, json_def: PChar): cuint32; cdecl; external FIRELITE_LIB;
function fl_engine_create_simple_index(engine: PFL_Engine; col, field: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_create_fts_index(engine: PFL_Engine; col, field: PChar): cint32; cdecl; external FIRELITE_LIB;

{ Net Sync }
function fl_net_syncer_new(engine: PFL_Engine; name, room_key: PChar): PFL_NetSyncer; cdecl; external FIRELITE_LIB;
function fl_net_syncer_start(syncer: PFL_NetSyncer; port: Word): cint32; cdecl; external FIRELITE_LIB;
function fl_net_syncer_set_discovery(syncer: PFL_NetSyncer; mode: cint32): cint32; cdecl; external FIRELITE_LIB;
function fl_net_syncer_status(syncer: PFL_NetSyncer): PChar; cdecl; external FIRELITE_LIB;
procedure fl_net_syncer_free(syncer: PFL_NetSyncer); cdecl; external FIRELITE_LIB;

{ Cloud Sync }
function fl_cloud_sync_new(engine: PFL_Engine; mode: cint32; client_id, room_name, room_key, auth_token: PChar): PFL_CloudSync; cdecl; external FIRELITE_LIB;
function fl_cloud_sync_server_new(engine: PFL_Engine; server_id, auth_token: PChar): PFL_CloudSync; cdecl; external FIRELITE_LIB;
function fl_cloud_sync_client_new(engine: PFL_Engine; client_id, room_name, room_key, auth_token: PChar): PFL_CloudSync; cdecl; external FIRELITE_LIB;
function fl_cloud_sync_start(cloud_sync: PFL_CloudSync; address: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_cloud_sync_status(cloud_sync: PFL_CloudSync): PChar; cdecl; external FIRELITE_LIB;
procedure fl_cloud_sync_stop(cloud_sync: PFL_CloudSync); cdecl; external FIRELITE_LIB;
procedure fl_cloud_sync_free(cloud_sync: PFL_CloudSync); cdecl; external FIRELITE_LIB;

{ Errors and Helpers }
function fl_last_error: PChar; cdecl; external FIRELITE_LIB;
procedure fl_string_free(value: PChar); cdecl; external FIRELITE_LIB;

implementation
end.
