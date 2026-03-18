/* eslint-disable @typescript-eslint/no-explicit-any */

type Handle = any;

/**
 * matches FL_OnSnapshotCallback in firelite.h
 */
export type WatchCallback = (collection: string, path: string, kind: number) => void;

export interface NativeBindings {
  engineOpen(path: string): Handle;
  engineOpenWithConfig(path: string, config: Handle): Handle;
  engineFree(engine: Handle): void;
  engineBackup(engine: Handle, path: string): number; // Added

  // Configuration Builder
  configNew(): Handle;
  configFree(config: Handle): void;
  configSetDurability(config: Handle, mode: number): void;
  configSetEncryptionKey(config: Handle, key: string | null): void;
  configSetAuditLog(config: Handle, enabled: boolean, path: string | null): void;
  configSetQueryWorkers(config: Handle, count: number): void;
  configSetMemoryLimits(config: Handle, mmap: number, maxInlined: number): void;
  configSetStorageTuning(
    config: Handle, 
    pageSize: number, 
    pageCacheCapacity: number, 
    threshold: number, 
    groupCommit: number
  ): void;

  // Real-time Watch
  engineWatch(engine: Handle, collection: string, callback: WatchCallback): Handle;
  watchFree(watch: Handle): void;

  // Document Builder
  docNew(): Handle;
  docFree(doc: Handle): void;
  docInsertStr(doc: Handle, key: string, value: string): number;
  docInsertInt(doc: Handle, key: string, value: number | bigint): number;
  docInsertFloat(doc: Handle, key: string, value: number): number;
  docInsertBool(doc: Handle, key: string, value: boolean): number;
  docInsertNull(doc: Handle, key: string): number;
  docInsertBin(doc: Handle, key: string, bytes: Uint8Array): number;
  docInsertTimestamp(doc: Handle, key: string, micros: bigint): number; // Added
  docInsertServerTimestamp(doc: Handle, key: string): number; // Added
  docToJson(doc: Handle): string | null;

  // Engine CRUD
  engineInsert(engine: Handle, collection: string, docId: string, doc: Handle): number;
  engineGet(engine: Handle, collection: string, docId: string): Handle;
  engineDelete(engine: Handle, collection: string, docId: string): number;

  // Atomic Batch
  batchNew(): Handle;
  batchFree(batch: Handle): void;
  batchSet(batch: Handle, collection: string, docId: string, doc: Handle): number;
  batchDelete(batch: Handle, collection: string, docId: string): number;
  batchCommit(engine: Handle, batch: Handle): number;

  // Query API
  queryNew(collection: string): Handle;
  queryFree(query: Handle): void;
  queryWhereEqStr(query: Handle, field: string, value: string): number;
  queryWhereEqInt(query: Handle, field: string, value: number | bigint): number;
  queryOrderBy(query: Handle, field: string, ascending: boolean): number;
  queryLimit(query: Handle, limit: number): number;
  querySelectField(query: Handle, field: string): number;
  queryExecute(engine: Handle, query: Handle): string | null;

  // Full-Text Search Queries (Added)
  queryWhereMatch(query: Handle, field: string, value: string): number;
  queryWhereContains(query: Handle, field: string, value: string): number;
  queryWhereStartsWith(query: Handle, field: string, value: string): number;

  // Aggregation API
  queryAggregateCount(query: Handle): number;
  queryAggregateSum(query: Handle, field: string): number;
  queryAggregateAvg(query: Handle, field: string): number;
  queryExecuteAggregation(engine: Handle, query: Handle): string | null;

  engineListCollections(engine: Handle): string | null;

  lastError(): string;
}

function isBunRuntime(): boolean {
  return typeof (globalThis as any).Bun !== 'undefined';
}

function defaultLibraryPath(): string {
  const libName = process.platform === 'win32' ? 'firelite.dll' : 
                  process.platform === 'darwin' ? 'libfirelite.dylib' : 'libfirelite.so';
  return `./target/release/${libName}`;
}

function resolveLibraryPath(explicitPath?: string): string {
  return explicitPath ?? defaultLibraryPath();
}

async function createBunBindings(libPath: string): Promise<NativeBindings> {
  const ffi = await import('bun:ffi');
  const { dlopen, FFIType, CString, JSCallback } = ffi as any;

  const symbols = dlopen(libPath, {
    fl_engine_open: { args: [FFIType.cstring], returns: FFIType.ptr },
    fl_engine_open_with_config: { args: [FFIType.cstring, FFIType.ptr], returns: FFIType.ptr },
    fl_engine_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_engine_backup: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },

    fl_config_new: { args: [], returns: FFIType.ptr },
    fl_config_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_config_set_durability: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.void },
    fl_config_set_encryption_key: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.void },
    fl_config_set_audit_log: { args: [FFIType.ptr, FFIType.bool, FFIType.cstring], returns: FFIType.void },
    fl_config_set_query_workers: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.void },
    fl_config_set_memory_limits: { args: [FFIType.ptr, FFIType.usize, FFIType.usize], returns: FFIType.void },
    fl_config_set_storage_tuning: { args: [FFIType.ptr, FFIType.usize, FFIType.usize, FFIType.usize, FFIType.usize], returns: FFIType.void },

    fl_engine_watch: { args: [FFIType.ptr, FFIType.cstring, FFIType.function, FFIType.ptr], returns: FFIType.ptr },
    fl_watch_free: { args: [FFIType.ptr], returns: FFIType.void },

    fl_doc_new: { args: [], returns: FFIType.ptr },
    fl_doc_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_doc_insert_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_doc_insert_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_doc_insert_float: { args: [FFIType.ptr, FFIType.cstring, FFIType.f64], returns: FFIType.i32 },
    fl_doc_insert_bool: { args: [FFIType.ptr, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
    fl_doc_insert_null: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_doc_insert_bin: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr, FFIType.usize], returns: FFIType.i32 },
    fl_doc_insert_timestamp: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_doc_insert_server_timestamp: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_doc_to_json: { args: [FFIType.ptr], returns: FFIType.ptr },

    fl_engine_insert: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_engine_get: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.ptr },
    fl_engine_delete: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },

    fl_batch_new: { args: [], returns: FFIType.ptr },
    fl_batch_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_batch_set: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_batch_delete: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_batch_commit: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },

    fl_query_new: { args: [FFIType.cstring], returns: FFIType.ptr },
    fl_query_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_query_where_eq_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_eq_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_match: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_contains: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_starts_with: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_order_by: { args: [FFIType.ptr, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
    fl_query_limit: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.i32 },
    fl_query_select_field: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_execute: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },

    fl_query_aggregate_count: { args: [FFIType.ptr], returns: FFIType.i32 },
    fl_query_aggregate_sum: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_aggregate_avg: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_execute_aggregation: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },

    fl_engine_list_collections: { args: [FFIType.ptr], returns: FFIType.ptr },

    fl_last_error: { args: [], returns: FFIType.ptr },
    fl_string_free: { args: [FFIType.ptr], returns: FFIType.void }
  }).symbols;

  const toC = (s: string | null) => s ? Buffer.from(s + '\0') : null;
  const ptrToStringAndFree = (ptr: any): string | null => {
    if (!ptr) return null;
    const text = new CString(ptr).toString();
    symbols.fl_string_free(ptr);
    return text;
  };

  return {
    engineOpen: (path) => symbols.fl_engine_open(toC(path)),
    engineOpenWithConfig: (path, config) => symbols.fl_engine_open_with_config(toC(path), config),
    engineFree: (engine) => symbols.fl_engine_free(engine),
    engineBackup: (e, p) => symbols.fl_engine_backup(e, toC(p)),

    configNew: () => symbols.fl_config_new(),
    configFree: (c) => symbols.fl_config_free(c),
    configSetDurability: (c, m) => symbols.fl_config_set_durability(c, m),
    configSetEncryptionKey: (c, k) => symbols.fl_config_set_encryption_key(c, toC(k)),
    configSetAuditLog: (c, e, p) => symbols.fl_config_set_audit_log(c, e, toC(p)),
    configSetQueryWorkers: (c, count) => symbols.fl_config_set_query_workers(c, count),
    configSetMemoryLimits: (c, m, mi) => symbols.fl_config_set_memory_limits(c, m, mi),
    configSetStorageTuning: (c, ps, pcc, th, gc) => symbols.fl_config_set_storage_tuning(c, ps, pcc, th, gc),

    engineWatch: (engine, collection, callback) => {
      const cb = new JSCallback((c: any, p: any, kind: number) => {
        callback(new CString(c).toString(), new CString(p).toString(), kind);
      }, { args: [FFIType.ptr, FFIType.ptr, FFIType.i32], returns: FFIType.void });
      return symbols.fl_engine_watch(engine, toC(collection), cb, null);
    },
    watchFree: (watch) => symbols.fl_watch_free(watch),

    docNew: () => symbols.fl_doc_new(),
    docFree: (doc) => symbols.fl_doc_free(doc),
    docInsertStr: (doc, key, value) => symbols.fl_doc_insert_str(doc, toC(key), toC(value)),
    docInsertInt: (doc, key, value) => symbols.fl_doc_insert_int(doc, toC(key), BigInt(value)),
    docInsertFloat: (doc, key, value) => symbols.fl_doc_insert_float(doc, toC(key), value),
    docInsertBool: (doc, key, value) => symbols.fl_doc_insert_bool(doc, toC(key), value),
    docInsertNull: (doc, key) => symbols.fl_doc_insert_null(doc, toC(key)),
    docInsertBin: (doc, key, bytes) => symbols.fl_doc_insert_bin(doc, toC(key), bytes, bytes.byteLength),
    docInsertTimestamp: (doc, key, micros) => symbols.fl_doc_insert_timestamp(doc, toC(key), micros),
    docInsertServerTimestamp: (doc, key) => symbols.fl_doc_insert_server_timestamp(doc, toC(key)),
    docToJson: (doc) => ptrToStringAndFree(symbols.fl_doc_to_json(doc)),

    engineInsert: (engine, collection, docId, doc) => symbols.fl_engine_insert(engine, toC(collection), toC(docId), doc),
    engineGet: (engine, collection, docId) => symbols.fl_engine_get(engine, toC(collection), toC(docId)),
    engineDelete: (engine, collection, docId) => symbols.fl_engine_delete(engine, toC(collection), toC(docId)),

    batchNew: () => symbols.fl_batch_new(),
    batchFree: (batch) => symbols.fl_batch_free(batch),
    batchSet: (batch, collection, docId, doc) => symbols.fl_batch_set(batch, toC(collection), toC(docId), doc),
    batchDelete: (batch, collection, docId) => symbols.fl_batch_delete(batch, toC(collection), toC(docId)),
    batchCommit: (engine, batch) => symbols.fl_batch_commit(engine, batch),

    queryNew: (collection) => symbols.fl_query_new(toC(collection)),
    queryFree: (query) => symbols.fl_query_free(query),
    queryWhereEqStr: (query, field, value) => symbols.fl_query_where_eq_str(query, toC(field), toC(value)),
    queryWhereEqInt: (query, field, value) => symbols.fl_query_where_eq_int(query, toC(field), BigInt(value)),
    queryWhereMatch: (query, field, value) => symbols.fl_query_where_match(query, toC(field), toC(value)),
    queryWhereContains: (query, field, value) => symbols.fl_query_where_contains(query, toC(field), toC(value)),
    queryWhereStartsWith: (query, field, value) => symbols.fl_query_where_starts_with(query, toC(field), toC(value)),
    queryOrderBy: (query, field, asc) => symbols.fl_query_order_by(query, toC(field), asc),
    queryLimit: (query, limit) => symbols.fl_query_limit(query, limit),
    querySelectField: (query, field) => symbols.fl_query_select_field(query, toC(field)),
    queryExecute: (engine, query) => ptrToStringAndFree(symbols.fl_query_execute(engine, query)),

    queryAggregateCount: (q) => symbols.fl_query_aggregate_count(q),
    queryAggregateSum: (q, f) => symbols.fl_query_aggregate_sum(q, toC(f)),
    queryAggregateAvg: (q, f) => symbols.fl_query_aggregate_avg(q, toC(f)),
    queryExecuteAggregation: (e, q) => ptrToStringAndFree(symbols.fl_query_execute_aggregation(e, q)),

    lastError: () => {
      const ptr = symbols.fl_last_error();
      return ptr ? new CString(ptr).toString() : 'unknown ffi error';
    }
  };
}

async function createNodeBindings(libPath: string): Promise<NativeBindings> {
  const koffiModule = await import('koffi');
  const koffi: any = (koffiModule as any).default ?? koffiModule;
  const lib = koffi.load(libPath);

  const OnSnapshotCB = koffi.proto('void FL_OnSnapshotCallback(const char *collection, const char *path, int32_t kind, void *user_data)');

  const fn = {
    fl_engine_open: lib.func('FL_Engine* fl_engine_open(const char* path)'),
    fl_engine_open_with_config: lib.func('FL_Engine* fl_engine_open_with_config(const char* path, FL_Config* config)'),
    fl_engine_free: lib.func('void fl_engine_free(FL_Engine* engine)'),
    fl_engine_backup: lib.func('int fl_engine_backup(FL_Engine* engine, const char* path)'),

    fl_config_new: lib.func('FL_Config* fl_config_new()'),
    fl_config_free: lib.func('void fl_config_free(FL_Config* config)'),
    fl_config_set_durability: lib.func('void fl_config_set_durability(FL_Config* config, int32_t mode)'),
    fl_config_set_encryption_key: lib.func('void fl_config_set_encryption_key(FL_Config* config, const char* key)'),
    fl_config_set_audit_log: lib.func('void fl_config_set_audit_log(FL_Config* config, bool enabled, const char* path)'),
    fl_config_set_query_workers: lib.func('void fl_config_set_query_workers(FL_Config* config, size_t count)'),
    fl_config_set_memory_limits: lib.func('void fl_config_set_memory_limits(FL_Config* config, size_t mmap_size, size_t max_inlined_bytes)'),
    fl_config_set_storage_tuning: lib.func('void fl_config_set_storage_tuning(FL_Config* config, size_t page_size, size_t page_cache_capacity, size_t compaction_threshold, size_t group_commit_max_ops)'),

    fl_engine_watch: lib.func('FL_Watch* fl_engine_watch(FL_Engine* engine, const char* collection, OnSnapshotCB* callback, void* user_data)'),
    fl_watch_free: lib.func('void fl_watch_free(FL_Watch* watch)'),

    fl_doc_new: lib.func('FL_Doc* fl_doc_new()'),
    fl_doc_free: lib.func('void fl_doc_free(FL_Doc* doc)'),
    fl_doc_insert_str: lib.func('int fl_doc_insert_str(FL_Doc* doc, const char* key, const char* value)'),
    fl_doc_insert_int: lib.func('int fl_doc_insert_int(FL_Doc* doc, const char* key, int64_t value)'),
    fl_doc_insert_float: lib.func('int fl_doc_insert_float(FL_Doc* doc, const char* key, double value)'),
    fl_doc_insert_bool: lib.func('int fl_doc_insert_bool(FL_Doc* doc, const char* key, bool value)'),
    fl_doc_insert_null: lib.func('int fl_doc_insert_null(FL_Doc* doc, const char* key)'),
    fl_doc_insert_bin: lib.func('int fl_doc_insert_bin(FL_Doc* doc, const char* key, const uint8_t* data, size_t len)'),
    fl_doc_insert_timestamp: lib.func('int fl_doc_insert_timestamp(FL_Doc* doc, const char* key, int64_t micros)'),
    fl_doc_insert_server_timestamp: lib.func('int fl_doc_insert_server_timestamp(FL_Doc* doc, const char* key)'),
    fl_doc_to_json: lib.func('char* fl_doc_to_json(const FL_Doc* doc)'),

    fl_engine_insert: lib.func('int fl_engine_insert(FL_Engine* engine, const char* collection, const char* doc_id, const FL_Doc* doc)'),
    fl_engine_get: lib.func('FL_Doc* fl_engine_get(FL_Engine* engine, const char* collection, const char* doc_id)'),
    fl_engine_delete: lib.func('int fl_engine_delete(FL_Engine* engine, const char* collection, const char* doc_id)'),

    fl_batch_new: lib.func('FL_Batch* fl_batch_new()'),
    fl_batch_free: lib.func('void fl_batch_free(FL_Batch* batch)'),
    fl_batch_set: lib.func('int fl_batch_set(FL_Batch* batch, const char* collection, const char* doc_id, const FL_Doc* doc)'),
    fl_batch_delete: lib.func('int fl_batch_delete(FL_Batch* batch, const char* collection, const char* doc_id)'),
    fl_batch_commit: lib.func('int fl_batch_commit(FL_Engine* engine, FL_Batch* batch)'),

    fl_query_new: lib.func('FL_Query* fl_query_new(const char* collection)'),
    fl_query_free: lib.func('void fl_query_free(FL_Query* query)'),
    fl_query_where_eq_str: lib.func('int fl_query_where_eq_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_eq_int: lib.func('int fl_query_where_eq_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_match: lib.func('int fl_query_where_match(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_contains: lib.func('int fl_query_where_contains(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_starts_with: lib.func('int fl_query_where_starts_with(FL_Query* query, const char* field, const char* value)'),
    fl_query_order_by: lib.func('int fl_query_order_by(FL_Query* query, const char* field, bool ascending)'),
    fl_query_limit: lib.func('int fl_query_limit(FL_Query* query, size_t limit)'),
    fl_query_select_field: lib.func('int fl_query_select_field(FL_Query* query, const char* field)'),
    fl_query_execute: lib.func('char* fl_query_execute(FL_Engine* engine, const FL_Query* query)'),

    fl_query_aggregate_count: lib.func('int fl_query_aggregate_count(FL_Query* query)'),
    fl_query_aggregate_sum: lib.func('int fl_query_aggregate_sum(FL_Query* query, const char* field)'),
    fl_query_aggregate_avg: lib.func('int fl_query_aggregate_avg(FL_Query* query, const char* field)'),
    fl_query_execute_aggregation: lib.func('char* fl_query_execute_aggregation(FL_Engine* engine, const FL_Query* query)'),

    fl_engine_list_collections: lib.func('char* fl_engine_list_collections(FL_Engine* engine)'),

    fl_last_error: lib.func('const char* fl_last_error()'),
    fl_string_free: lib.func('void fl_string_free(char* value)')
  };

  const ptrToStringAndFree = (ptr: any): string | null => {
    if (!ptr) return null;
    const text = koffi.decode(ptr, 'char*') as string;
    fn.fl_string_free(ptr);
    return text;
  };

  return {
    engineOpen: (path) => fn.fl_engine_open(path),
    engineOpenWithConfig: (path, config) => fn.fl_engine_open_with_config(path, config),
    engineFree: (engine) => fn.fl_engine_free(engine),
    engineBackup: (e, p) => fn.fl_engine_backup(e, p),

    configNew: () => fn.fl_config_new(),
    configFree: (c) => fn.fl_config_free(c),
    configSetDurability: (c, m) => fn.fl_config_set_durability(c, m),
    configSetEncryptionKey: (c, k) => fn.fl_config_set_encryption_key(c, k),
    configSetAuditLog: (c, e, p) => fn.fl_config_set_audit_log(c, e, p),
    configSetQueryWorkers: (c, count) => fn.fl_config_set_query_workers(c, count),
    configSetMemoryLimits: (c, m, mi) => fn.fl_config_set_memory_limits(c, m, mi),
    configSetStorageTuning: (c, ps, pcc, th, gc) => fn.fl_config_set_storage_tuning(c, ps, pcc, th, gc),

    engineWatch: (engine, collection, callback) => {
      const wrapper = (c: string, p: string, kind: number, _user: any) => callback(c, p, kind);
      return fn.fl_engine_watch(engine, collection, koffi.register(wrapper, OnSnapshotCB), null);
    },
    watchFree: (watch) => fn.fl_watch_free(watch),

    docNew: () => fn.fl_doc_new(),
    docFree: (doc) => fn.fl_doc_free(doc),
    docInsertStr: (doc, key, value) => fn.fl_doc_insert_str(doc, key, value),
    docInsertInt: (doc, key, value) => fn.fl_doc_insert_int(doc, key, value),
    docInsertFloat: (doc, key, value) => fn.fl_doc_insert_float(doc, key, value),
    docInsertBool: (doc, key, value) => fn.fl_doc_insert_bool(doc, key, value),
    docInsertNull: (doc, key) => fn.fl_doc_insert_null(doc, key),
    docInsertBin: (doc, key, bytes) => fn.fl_doc_insert_bin(doc, key, Buffer.from(bytes), bytes.byteLength),
    docInsertTimestamp: (doc, key, micros) => fn.fl_doc_insert_timestamp(doc, key, micros),
    docInsertServerTimestamp: (doc, key) => fn.fl_doc_insert_server_timestamp(doc, key),
    docToJson: (doc) => ptrToStringAndFree(fn.fl_doc_to_json(doc)),

    engineInsert: (engine, collection, docId, doc) => fn.fl_engine_insert(engine, collection, docId, doc),
    engineGet: (engine, collection, docId) => fn.fl_engine_get(engine, collection, docId),
    engineDelete: (engine, collection, docId) => fn.fl_engine_delete(engine, collection, docId),

    batchNew: () => fn.fl_batch_new(),
    batchFree: (batch) => fn.fl_batch_free(batch),
    batchSet: (batch, collection, docId, doc) => fn.fl_batch_set(batch, collection, docId, doc),
    batchDelete: (batch, collection, docId) => fn.fl_batch_delete(batch, collection, docId),
    batchCommit: (engine, batch) => fn.fl_batch_commit(engine, batch),

    queryNew: (collection) => fn.fl_query_new(collection),
    queryFree: (query) => fn.fl_query_free(query),
    queryWhereEqStr: (query, field, value) => fn.fl_query_where_eq_str(query, field, value),
    queryWhereEqInt: (query, field, value) => fn.fl_query_where_eq_int(query, field, value),
    queryWhereMatch: (query, field, value) => fn.fl_query_where_match(query, field, value),
    queryWhereContains: (query, field, value) => fn.fl_query_where_contains(query, field, value),
    queryWhereStartsWith: (query, field, value) => fn.fl_query_where_starts_with(query, field, value),
    queryOrderBy: (query, field, asc) => fn.fl_query_order_by(query, field, asc),
    queryLimit: (query, limit) => fn.fl_query_limit(query, limit),
    querySelectField: (query, field) => fn.fl_query_select_field(query, field),
    queryExecute: (engine, query) => ptrToStringAndFree(fn.fl_query_execute(engine, query)),

    queryAggregateCount: (q) => fn.fl_query_aggregate_count(q),
    queryAggregateSum: (q, f) => fn.fl_query_aggregate_sum(q, f),
    queryAggregateAvg: (q, f) => fn.fl_query_aggregate_avg(q, f),
    queryExecuteAggregation: (e, q) => ptrToStringAndFree(fn.fl_query_execute_aggregation(e, q)),

    lastError: () => (fn.fl_last_error() as string) || 'unknown ffi error'
  };
}

export async function loadNativeBindings(explicitPath?: string): Promise<NativeBindings> {
  const libPath = resolveLibraryPath(explicitPath);
  return isBunRuntime() ? createBunBindings(libPath) : createNodeBindings(libPath);
}