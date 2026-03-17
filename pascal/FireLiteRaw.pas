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
  PFL_Config = Pointer; // New
  PFL_Watch = Pointer;  // New

  { Callback for real-time snapshots }
  TFL_OnSnapshotCallback = procedure(collection: PChar; path: PChar; kind: cint32; user_data: Pointer); cdecl;

const
  { Snapshot change types }
  FL_CHANGE_PUT = 1;
  FL_CHANGE_DELETE = 2;

{$ifdef Windows}
const
  FIRELITE_LIB = 'firelite.dll';
{$elseif Darwin}
const
  FIRELITE_LIB = 'libfirelite.dylib';
{$else}
const
  FIRELITE_LIB = 'libfirelite.so';
{$endif}

{ Engine Management }
function fl_engine_open(path: PChar): PFL_Engine; cdecl; external FIRELITE_LIB;
function fl_engine_open_with_config(path: PChar; config: PFL_Config): PFL_Engine; cdecl; external FIRELITE_LIB;
procedure fl_engine_free(engine: PFL_Engine); cdecl; external FIRELITE_LIB;

{ Configuration Builder }
function fl_config_new: PFL_Config; cdecl; external FIRELITE_LIB;
procedure fl_config_free(config: PFL_Config); cdecl; external FIRELITE_LIB;
procedure fl_config_set_durability(config: PFL_Config; mode: cint32); cdecl; external FIRELITE_LIB;
procedure fl_config_set_encryption_key(config: PFL_Config; key: PChar); cdecl; external FIRELITE_LIB;
procedure fl_config_set_audit_log(config: PFL_Config; enabled: cbool; path: PChar); cdecl; external FIRELITE_LIB;
procedure fl_config_set_query_workers(config: PFL_Config; count: SizeUInt); cdecl; external FIRELITE_LIB;
procedure fl_config_set_memory_limits(config: PFL_Config; mmap_size, max_inlined_bytes: SizeUInt); cdecl; external FIRELITE_LIB;
procedure fl_config_set_storage_tuning(config: PFL_Config; page_size, page_cache_capacity, compaction_threshold, group_commit_max_ops: SizeUInt); cdecl; external FIRELITE_LIB;

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
function fl_doc_to_json(doc: PFL_Doc): PChar; cdecl; external FIRELITE_LIB;

{ Single Operations }
function fl_engine_insert(engine: PFL_Engine; collection, doc_id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_engine_get(engine: PFL_Engine; collection, doc_id: PChar): PFL_Doc; cdecl; external FIRELITE_LIB;
function fl_engine_delete(engine: PFL_Engine; collection, doc_id: PChar): cint32; cdecl; external FIRELITE_LIB;

{ Atomic Batches }
function fl_batch_new: PFL_Batch; cdecl; external FIRELITE_LIB;
procedure fl_batch_free(batch: PFL_Batch); cdecl; external FIRELITE_LIB;
function fl_batch_set(batch: PFL_Batch; collection, doc_id: PChar; doc: PFL_Doc): cint32; cdecl; external FIRELITE_LIB;
function fl_batch_delete(batch: PFL_Batch; collection, doc_id: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_batch_commit(engine: PFL_Engine; batch: PFL_Batch): cint32; cdecl; external FIRELITE_LIB;

{ Queries }
function fl_query_new(collection: PChar): PFL_Query; cdecl; external FIRELITE_LIB;
procedure fl_query_free(query: PFL_Query); cdecl; external FIRELITE_LIB;
function fl_query_where_eq_str(query: PFL_Query; field, value: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_where_eq_int(query: PFL_Query; field: PChar; value: cint64): cint32; cdecl; external FIRELITE_LIB;
function fl_query_order_by(query: PFL_Query; field: PChar; ascending: cbool): cint32; cdecl; external FIRELITE_LIB;
function fl_query_limit(query: PFL_Query; limit: SizeUInt): cint32; cdecl; external FIRELITE_LIB;
function fl_query_select_field(query: PFL_Query; field: PChar): cint32; cdecl; external FIRELITE_LIB;
function fl_query_execute(engine: PFL_Engine; query: PFL_Query): PChar; cdecl; external FIRELITE_LIB;

{ Errors and Helpers }
function fl_last_error: PChar; cdecl; external FIRELITE_LIB;
procedure fl_string_free(value: PChar); cdecl; external FIRELITE_LIB;

implementation

end.