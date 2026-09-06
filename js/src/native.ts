/* eslint-disable @typescript-eslint/no-explicit-any */

type Handle = any;

/**
 * matches FL_OnSnapshotCallback in firelite.h
 */
export type WatchCallback = (collection: string, path: string, kind: number) => void;

export interface NativeBindings {
  engineOpen(path: string): Handle;
  engineIsIndexesReady(engine: Handle): boolean;
  engineOpenWithConfig(path: string, config: Handle): Handle;
  engineFree(engine: Handle): void;
  engineBackup(engine: Handle, path: string): number; // Added
  engineCompact(engine: Handle): number;
  engineGetStats(engine: Handle): string | null;
  engineGetAuditLog(engine: Handle): string | null;
  engineSnapshotIndices(engine: Handle): number;
  engineListIndexes(engine: Handle, collection: string | null): string | null;

  // Configuration Builder
  configNew(): Handle;
  configFree(config: Handle): void;
  configSetDurability(config: Handle, mode: number): void;
  configSetEncryptionKey(config: Handle, key: string | null): void;
  configSetEncryptedCollections(config: Handle, collectionsJson: string | null): number;
  configSetAuditLog(config: Handle, enabled: boolean, path: string | null): void;
  configSetQueryWorkers(config: Handle, count: number): void;
  configSetMemoryLimits(config: Handle, mmap: number, maxInlined: number): void;
  configSetStorageTuning(
    config: Handle, 
    pageSize: number, 
    threshold: number, 
    groupCommit: number
  ): void;
  configSetBlobThreshold(config: Handle, thresholdBytes: number): void;
  configSetCompression(config: Handle, enabled: boolean, level: number): void;

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
  docInsertReference(doc: Handle, key: string, targetCollection: string, targetId: string): number;
  docToJson(doc: Handle): string | null;

  // Engine CRUD
  engineInsert(engine: Handle, collection: string, docId: string, doc: Handle): number;
  engineGet(engine: Handle, collection: string, docId: string): Handle;
  engineDelete(engine: Handle, collection: string, docId: string): number;
  engineDeleteLocal(engine: Handle, collection: string, docId: string): number;
  engineSetCollectionLocal(engine: Handle, collection: string, local: number): number;
  engineReplicateKey(engine: Handle, collection: string, docId: string): number;
  engineReplicateCollection(engine: Handle, collection: string): number;
  engineVacuumCollection(engine: Handle, collection: string): number;
  enginePatch(engine: Handle, collection: string, docId: string, updates: Handle): number;
  engineInsertSubDoc(engine: Handle, col: string, id: string, subCol: string, subId: string, doc: Handle): number;
  engineGetByRef(engine: Handle, doc: Handle, fieldKey: string): Handle;
  engineCreateIndex(engine: Handle, collection: string, fieldsJson: string): number;

  // Atomic Batch
  batchNew(): Handle;
  batchFree(batch: Handle): void;
  batchSet(batch: Handle, collection: string, docId: string, doc: Handle): number;
  batchDelete(batch: Handle, collection: string, docId: string): number;
  batchCommit(engine: Handle, batch: Handle): number;

  // Serializable Transactions
  transactionBegin(engine: Handle): Handle;
  transactionGet(engine: Handle, tx: Handle, collection: string, docId: string): Handle;
  transactionSet(tx: Handle, collection: string, docId: string, doc: Handle): number;
  transactionCommit(engine: Handle, tx: Handle): number;
  transactionFree(tx: Handle): void;

  // Query API
  queryNew(collection: string): Handle;
  queryFree(query: Handle): void;
  queryWhereEqStr(query: Handle, field: string, value: string): number;
  queryWhereEqBool(query: Handle, field: string, value: boolean): number;
  queryWhereEqInt(query: Handle, field: string, value: number | bigint): number;
  queryWhereNeStr(query: Handle, field: string, value: string): number;
  queryWhereNeInt(query: Handle, field: string, value: number | bigint): number;
  queryWhereGtStr(query: Handle, field: string, value: string): number;
  queryWhereGtInt(query: Handle, field: string, value: number | bigint): number;
  queryWhereGteStr(query: Handle, field: string, value: string): number;
  queryWhereGteInt(query: Handle, field: string, value: number | bigint): number;
  queryWhereLtStr(query: Handle, field: string, value: string): number;
  queryWhereLtInt(query: Handle, field: string, value: number | bigint): number;
  queryWhereLteStr(query: Handle, field: string, value: string): number;
  queryWhereLteInt(query: Handle, field: string, value: number | bigint): number;
  queryOrderBy(query: Handle, field: string, ascending: boolean): number;
  queryLimit(query: Handle, limit: number): number;
  queryOffset(query: Handle, offset: number): number;
  querySelectField(query: Handle, field: string): number;
  queryExecute(engine: Handle, query: Handle): string | null;
  queryDelete(engine: Handle, query: Handle): number;
  queryDeleteLocal(engine: Handle, query: Handle): number;
  queryPatch(engine: Handle, query: Handle, patchDoc: Handle): number;
  queryExecuteToHandles(engine: Handle, query: Handle): Handle;
  resultSetCount(results: Handle): number;
  resultSetGetDoc(results: Handle, index: number): Handle;
  resultSetFree(results: Handle): void;
  resultSetToJson(results: Handle): string | null;
  queryDeferBlobs(query: Handle, defer: boolean): number;
  docResolveBlobs(engine: Handle, collection: string, doc: Handle): number;
  engineInsertTake(engine: Handle, collection: string, docId: string, doc: Handle): number;
  configSetWalReserveBytes(c: Handle, bytes: number | bigint): void;

  // Full-Text Search Queries (Added)
  queryWhereMatch(query: Handle, field: string, value: string): number;
  queryWhereMatchPrefix(query: Handle, field: string, value: string): number;
  queryWhereContains(query: Handle, field: string, value: string): number;
  queryWhereStartsWith(query: Handle, field: string, value: string): number;
  queryWhereOrStr(query: Handle, field: string, value: string): number;
  queryWhereOrInt(query: Handle, field: string, value: number | bigint): number;

  // Aggregation API
  queryAggregateCount(query: Handle): number;
  queryAggregateSum(query: Handle, field: string): number;
  queryAggregateAvg(query: Handle, field: string): number;
  queryExecuteAggregation(engine: Handle, query: Handle): string | null;

  engineListCollections(engine: Handle): string | null;

  // Net Sync
  netSyncerNew(engine: Handle, name: string, roomKey: string): Handle;
  netSyncerStart(syncer: Handle, port: number): number;
  netSyncerSetDiscovery(syncer: Handle, mode: number): number;
  netSyncerStatus(syncer: Handle): string | null;
  netSyncerFree(syncer: Handle): void;

  // Cloud Sync (bi-directional WebSocket replication)
  cloudSyncNew(engine: Handle, mode: number, clientId: string | null, roomName: string | null, roomKey: string | null, authToken: string | null): Handle;
  cloudSyncServerNew(engine: Handle, serverId: string | null, authToken: string | null): Handle;
  cloudSyncClientNew(engine: Handle, clientId: string | null, roomName: string | null, roomKey: string | null, authToken: string | null): Handle;
  cloudSyncStart(cloudSync: Handle, address: string): number;
  cloudSyncStatus(cloudSync: Handle): string | null;
  cloudSyncStop(cloudSync: Handle): void;
  cloudSyncFree(cloudSync: Handle): void;

  // v0.5.9
  createFtsIndex(engine: Handle, collection: string, field: string): number;
  createSimpleIndex(engine: Handle, collection: string, field: string): number;
  
  // Array API
  arrayNew(): Handle;
  arrayFree(array: Handle): void;
  arrayAppendStr(array: Handle, value: string): number;
  arrayAppendInt(array: Handle, value: number | bigint): number;
  arrayAppendDoc(array: Handle, doc: Handle): number;

  // Nested structures
  docInsertDoc(parent: Handle, key: string, child: Handle): number;
  docInsertArray(parent: Handle, key: string, array: Handle): number;
  
  // Query extensions
  queryWhereIn(query: Handle, field: string, array: Handle): number;
  queryWhereNotIn(query: Handle, field: string, array: Handle): number;
  queryWhereArrayContainsAny(query: Handle, field: string, array: Handle): number;
  queryWhereArrayContainsStr(query: Handle, field: string, value: string): number;
  queryWhereArrayContainsInt(query: Handle, field: string, value: number | bigint): number;
  queryStartAfter(query: Handle, anchorDoc: Handle): number;
  queryStartAt(query: Handle, anchorDoc: Handle): number;
  queryEndAt(query: Handle, anchorDoc: Handle): number;
  queryEndBefore(query: Handle, anchorDoc: Handle): number;

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
    fl_engine_is_indexes_ready: { args: [FFIType.ptr], returns: FFIType.bool },
    fl_engine_open_with_config: { args: [FFIType.cstring, FFIType.ptr], returns: FFIType.ptr },
    fl_engine_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_engine_backup: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_engine_compact: { args: [FFIType.ptr], returns: FFIType.i32 },
    fl_engine_get_stats: { args: [FFIType.ptr], returns: FFIType.ptr },
    fl_engine_get_audit_log: { args: [FFIType.ptr], returns: FFIType.ptr },
    fl_engine_snapshot_indices: { args: [FFIType.ptr], returns: FFIType.i32 },
    fl_engine_list_indexes: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.ptr },

    fl_config_new: { args: [], returns: FFIType.ptr },
    fl_config_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_config_set_durability: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.void },
    fl_config_set_encryption_key: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.void },
    fl_config_set_encrypted_collections: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_config_set_audit_log: { args: [FFIType.ptr, FFIType.bool, FFIType.cstring], returns: FFIType.void },
    fl_config_set_query_workers: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.void },
    fl_config_set_memory_limits: { args: [FFIType.ptr, FFIType.usize, FFIType.usize], returns: FFIType.void },
    fl_config_set_storage_tuning: { args: [FFIType.ptr, FFIType.usize, FFIType.usize, FFIType.usize], returns: FFIType.void },
    fl_config_set_blob_threshold: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.void },
    fl_config_set_compression: { args: [FFIType.ptr, FFIType.bool, FFIType.i32], returns: FFIType.void },

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
    fl_engine_delete_local: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_engine_set_collection_local: { args: [FFIType.ptr, FFIType.cstring, FFIType.i32], returns: FFIType.i32 },
    fl_engine_replicate_key: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_engine_replicate_collection: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_engine_vacuum_collection: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_engine_patch: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_engine_insert_subdoc: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_engine_get_by_ref: { args: [FFIType.ptr, FFIType.ptr, FFIType.cstring], returns: FFIType.ptr },
    fl_engine_create_index: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.u32 },

    fl_batch_new: { args: [], returns: FFIType.ptr },
    fl_batch_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_batch_set: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_batch_delete: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_batch_commit: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },

    fl_transaction_begin: { args: [FFIType.ptr], returns: FFIType.ptr },
    fl_transaction_get: { args: [FFIType.ptr, FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.ptr },
    fl_transaction_set: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_transaction_commit: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_transaction_free: { args: [FFIType.ptr], returns: FFIType.void },

    fl_query_new: { args: [FFIType.cstring], returns: FFIType.ptr },
    fl_query_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_query_where_eq_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_eq_bool: { args: [FFIType.ptr, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
    fl_query_where_eq_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_ne_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_ne_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_gt_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_gt_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_gte_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_gte_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_lt_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_lt_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_lte_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_lte_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_query_where_match: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_match_prefix: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_contains: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_starts_with: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_order_by: { args: [FFIType.ptr, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
    fl_query_limit: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.i32 },
    fl_query_offset: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.i32 },
    fl_query_select_field: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_execute: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },
    fl_query_delete: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_query_delete_local: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_query_patch: { args: [FFIType.ptr, FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_query_execute_to_handles: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },
    fl_result_set_count: { args: [FFIType.ptr], returns: FFIType.usize },
    fl_result_set_get_doc: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.ptr },
    fl_result_set_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_result_set_to_json: { args: [FFIType.ptr], returns: FFIType.ptr },
    fl_query_defer_blobs: { args: [FFIType.ptr, FFIType.i32], returns: FFIType.i32 },
    fl_doc_resolve_blobs: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_engine_insert_take: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_config_set_wal_reserve_bytes: { args: [FFIType.ptr, FFIType.u64], returns: FFIType.void },

    fl_query_aggregate_count: { args: [FFIType.ptr], returns: FFIType.i32 },
    fl_query_aggregate_sum: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_aggregate_avg: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_execute_aggregation: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },
    fl_query_where_or_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_where_or_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },

    // v0.5.9
    // Indexing
    fl_engine_create_fts_index: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_engine_create_simple_index: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },

    // Array API
    fl_array_new: { args: [], returns: FFIType.ptr },
    fl_array_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_array_append_str: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_array_append_int: { args: [FFIType.ptr, FFIType.i64], returns: FFIType.i32 },
    fl_array_append_doc: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },

    // Nested structures
    fl_doc_insert_doc: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_doc_insert_array: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },

    // Query extensions
    fl_query_where_in: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_query_where_not_in: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_query_where_array_contains_any: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr], returns: FFIType.i32 },
    fl_query_where_array_contains: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_query_start_after: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_query_start_at: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_query_end_at: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },
    fl_query_end_before: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.i32 },

    fl_doc_insert_reference: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },

    fl_cloud_sync_new: { args: [FFIType.ptr, FFIType.i32, FFIType.cstring, FFIType.cstring, FFIType.cstring], returns: FFIType.ptr },
    fl_cloud_sync_server_new: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.ptr },
    fl_cloud_sync_client_new: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring, FFIType.cstring, FFIType.cstring], returns: FFIType.ptr },
    fl_cloud_sync_start: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_cloud_sync_status: { args: [FFIType.ptr], returns: FFIType.ptr },
    fl_cloud_sync_stop: { args: [FFIType.ptr], returns: FFIType.void },
    fl_cloud_sync_free: { args: [FFIType.ptr], returns: FFIType.void },

    // ================= ERRORS =================
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
    engineIsIndexesReady: (engine) => symbols.fl_engine_is_indexes_ready(engine),
    engineOpenWithConfig: (path, config) => symbols.fl_engine_open_with_config(toC(path), config),
    engineFree: (engine) => symbols.fl_engine_free(engine),
    engineBackup: (e, p) => symbols.fl_engine_backup(e, toC(p)),
    engineCompact: (e) => symbols.fl_engine_compact(e),
    engineGetStats: (e) => ptrToStringAndFree(symbols.fl_engine_get_stats(e)),
    engineGetAuditLog: (e) => ptrToStringAndFree(symbols.fl_engine_get_audit_log(e)),
    engineSnapshotIndices: (e) => symbols.fl_engine_snapshot_indices(e),
    engineListIndexes: (e, c) => ptrToStringAndFree(symbols.fl_engine_list_indexes(e, toC(c))),

    configNew: () => symbols.fl_config_new(),
    configFree: (c) => symbols.fl_config_free(c),
    configSetDurability: (c, m) => symbols.fl_config_set_durability(c, m),
    configSetEncryptionKey: (c, k) => symbols.fl_config_set_encryption_key(c, toC(k)),
    configSetEncryptedCollections: (c, json) => symbols.fl_config_set_encrypted_collections(c, toC(json)),
    configSetAuditLog: (c, e, p) => symbols.fl_config_set_audit_log(c, e, toC(p)),
    configSetQueryWorkers: (c, count) => symbols.fl_config_set_query_workers(c, count),
    configSetMemoryLimits: (c, m, mi) => symbols.fl_config_set_memory_limits(c, m, mi),
    configSetStorageTuning: (c, ps, th, gc) => symbols.fl_config_set_storage_tuning(c, ps, th, gc),
    configSetBlobThreshold: (c, t) => symbols.fl_config_set_blob_threshold(c, t),
    configSetCompression: (c, e, l) => symbols.fl_config_set_compression(c, e, l),

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
    docInsertReference: (doc, key, tc, tid) => symbols.fl_doc_insert_reference(doc, toC(key), toC(tc), toC(tid)),
    docToJson: (doc) => ptrToStringAndFree(symbols.fl_doc_to_json(doc)),

    engineInsert: (engine, collection, docId, doc) => symbols.fl_engine_insert(engine, toC(collection), toC(docId), doc),
    engineGet: (engine, collection, docId) => symbols.fl_engine_get(engine, toC(collection), toC(docId)),
    engineDelete: (engine, collection, docId) => symbols.fl_engine_delete(engine, toC(collection), toC(docId)),
    engineDeleteLocal: (engine, collection, docId) => symbols.fl_engine_delete_local(engine, toC(collection), toC(docId)),
    engineSetCollectionLocal: (engine, collection, local) => symbols.fl_engine_set_collection_local(engine, toC(collection), local),
    engineReplicateKey: (engine, collection, docId) => symbols.fl_engine_replicate_key(engine, toC(collection), toC(docId)),
    engineReplicateCollection: (engine, collection) => symbols.fl_engine_replicate_collection(engine, toC(collection)),
    engineVacuumCollection: (engine, collection) => symbols.fl_engine_vacuum_collection(engine, toC(collection)),
    enginePatch: (engine, collection, docId, updates) => symbols.fl_engine_patch(engine, toC(collection), toC(docId), updates),
    engineInsertSubDoc: (engine, col, id, subCol, subId, doc) => symbols.fl_engine_insert_subdoc(engine, toC(col), toC(id), toC(subCol), toC(subId), doc),
    engineGetByRef: (engine, doc, fieldKey) => symbols.fl_engine_get_by_ref(engine, doc, toC(fieldKey)),
    engineCreateIndex: (engine, collection, fieldsJson) => symbols.fl_engine_create_index(engine, toC(collection), toC(fieldsJson)),

    batchNew: () => symbols.fl_batch_new(),
    batchFree: (batch) => symbols.fl_batch_free(batch),
    batchSet: (batch, collection, docId, doc) => symbols.fl_batch_set(batch, toC(collection), toC(docId), doc),
    batchDelete: (batch, collection, docId) => symbols.fl_batch_delete(batch, toC(collection), toC(docId)),
    batchCommit: (engine, batch) => symbols.fl_batch_commit(engine, batch),

    transactionBegin: (engine) => symbols.fl_transaction_begin(engine),
    transactionGet: (engine, tx, collection, docId) => symbols.fl_transaction_get(engine, tx, toC(collection), toC(docId)),
    transactionSet: (tx, collection, docId, doc) => symbols.fl_transaction_set(tx, toC(collection), toC(docId), doc),
    transactionCommit: (engine, tx) => symbols.fl_transaction_commit(engine, tx),
    transactionFree: (tx) => symbols.fl_transaction_free(tx),

    queryNew: (collection) => symbols.fl_query_new(toC(collection)),
    queryFree: (query) => symbols.fl_query_free(query),
    queryWhereEqStr: (query, field, value) => symbols.fl_query_where_eq_str(query, toC(field), toC(value)),
    queryWhereEqBool: (query, field, value) => symbols.fl_query_where_eq_bool(query, toC(field), value),
    queryWhereEqInt: (query, field, value) => symbols.fl_query_where_eq_int(query, toC(field), BigInt(value)),
    queryWhereNeStr: (query, field, value) => symbols.fl_query_where_ne_str(query, toC(field), toC(value)),
    queryWhereNeInt: (query, field, value) => symbols.fl_query_where_ne_int(query, toC(field), BigInt(value)),
    queryWhereGtStr: (query, field, value) => symbols.fl_query_where_gt_str(query, toC(field), toC(value)),
    queryWhereGtInt: (query, field, value) => symbols.fl_query_where_gt_int(query, toC(field), BigInt(value)),
    queryWhereGteStr: (query, field, value) => symbols.fl_query_where_gte_str(query, toC(field), toC(value)),
    queryWhereGteInt: (query, field, value) => symbols.fl_query_where_gte_int(query, toC(field), BigInt(value)),
    queryWhereLtStr: (query, field, value) => symbols.fl_query_where_lt_str(query, toC(field), toC(value)),
    queryWhereLtInt: (query, field, value) => symbols.fl_query_where_lt_int(query, toC(field), BigInt(value)),
    queryWhereLteStr: (query, field, value) => symbols.fl_query_where_lte_str(query, toC(field), toC(value)),
    queryWhereLteInt: (query, field, value) => symbols.fl_query_where_lte_int(query, toC(field), BigInt(value)),
    queryWhereMatch: (query, field, value) => symbols.fl_query_where_match(query, toC(field), toC(value)),
    queryWhereMatchPrefix: (query, field, value) => symbols.fl_query_where_match_prefix(query, toC(field), toC(value)),
    queryWhereContains: (query, field, value) => symbols.fl_query_where_contains(query, toC(field), toC(value)),
    queryWhereStartsWith: (query, field, value) => symbols.fl_query_where_starts_with(query, toC(field), toC(value)),
    queryWhereOrStr: (query, field, value) => symbols.fl_query_where_or_str(query, toC(field), toC(value)),
    queryWhereOrInt: (query, field, value) => symbols.fl_query_where_or_int(query, toC(field), BigInt(value)),
    queryOrderBy: (query, field, asc) => symbols.fl_query_order_by(query, toC(field), asc),
    queryLimit: (query, limit) => symbols.fl_query_limit(query, limit),
    queryOffset: (query, offset) => symbols.fl_query_offset(query, offset),
    querySelectField: (query, field) => symbols.fl_query_select_field(query, toC(field)),
    queryExecute: (engine, query) => ptrToStringAndFree(symbols.fl_query_execute(engine, query)),
    queryDelete: (engine, query) => symbols.fl_query_delete(engine, query),
    queryDeleteLocal: (engine, query) => symbols.fl_query_delete_local(engine, query),
    queryPatch: (engine, query, patchDoc) => symbols.fl_query_patch(engine, query, patchDoc),
    queryExecuteToHandles: (engine, query) => symbols.fl_query_execute_to_handles(engine, query),
    resultSetCount: (results) => symbols.fl_result_set_count(results),
    resultSetGetDoc: (results, index) => symbols.fl_result_set_get_doc(results, index),
    resultSetFree: (results) => symbols.fl_result_set_free(results),
    resultSetToJson: (results) => ptrToStringAndFree(symbols.fl_result_set_to_json(results)),
    queryDeferBlobs: (query, defer) => symbols.fl_query_defer_blobs(query, defer ? 1 : 0),
    docResolveBlobs: (engine, collection, doc) => symbols.fl_doc_resolve_blobs(engine, toC(collection), doc),
    engineInsertTake: (engine, collection, docId, doc) => symbols.fl_engine_insert_take(engine, toC(collection), toC(docId), doc),
    configSetWalReserveBytes: (c, bytes) => symbols.fl_config_set_wal_reserve_bytes(c, bytes),

    queryAggregateCount: (q) => symbols.fl_query_aggregate_count(q),
    queryAggregateSum: (q, f) => symbols.fl_query_aggregate_sum(q, toC(f)),
    queryAggregateAvg: (q, f) => symbols.fl_query_aggregate_avg(q, toC(f)),
    queryExecuteAggregation: (e, q) => ptrToStringAndFree(symbols.fl_query_execute_aggregation(e, q)),
    engineListCollections: (engine) => ptrToStringAndFree(symbols.fl_engine_list_collections(engine)),
    netSyncerNew: (engine, name, roomKey) => symbols.fl_net_syncer_new(engine, toC(name), toC(roomKey)),
    netSyncerStart: (syncer, port) => symbols.fl_net_syncer_start(syncer, port),
    netSyncerSetDiscovery: (syncer, mode) => symbols.fl_net_syncer_set_discovery(syncer, mode),
    netSyncerStatus: (syncer) => ptrToStringAndFree(symbols.fl_net_syncer_status(syncer)),
    netSyncerFree: (syncer) => symbols.fl_net_syncer_free(syncer),

    cloudSyncNew: (engine, mode, clientId, roomName, roomKey, authToken) => symbols.fl_cloud_sync_new(engine, mode, toC(clientId), toC(roomName), toC(roomKey), toC(authToken)),
    cloudSyncServerNew: (engine, serverId, authToken) => symbols.fl_cloud_sync_server_new(engine, toC(serverId), toC(authToken)),
    cloudSyncClientNew: (engine, clientId, roomName, roomKey, authToken) => symbols.fl_cloud_sync_client_new(engine, toC(clientId), toC(roomName), toC(roomKey), toC(authToken)),
    cloudSyncStart: (cs, address) => symbols.fl_cloud_sync_start(cs, toC(address)),
    cloudSyncStatus: (cs) => ptrToStringAndFree(symbols.fl_cloud_sync_status(cs)),
    cloudSyncStop: (cs) => symbols.fl_cloud_sync_stop(cs),
    cloudSyncFree: (cs) => symbols.fl_cloud_sync_free(cs),

    // ================= v0.5.9 =================

    // Indexing
    createFtsIndex: (engine, collection, field) => symbols.fl_engine_create_fts_index(engine, toC(collection), toC(field)),
    createSimpleIndex: (engine, collection, field) => symbols.fl_engine_create_simple_index(engine, toC(collection), toC(field)),

    // Array API
    arrayNew: () => symbols.fl_array_new(),
    arrayFree: (arr) => symbols.fl_array_free(arr),
    arrayAppendStr: (arr, value) => symbols.fl_array_append_str(arr, toC(value)),
    arrayAppendInt: (arr, value) => symbols.fl_array_append_int(arr, BigInt(value)),
    arrayAppendDoc: (arr, doc) => symbols.fl_array_append_doc(arr, doc),

    // Nested
    docInsertDoc: (parent, key, child) => symbols.fl_doc_insert_doc(parent, toC(key), child),
    docInsertArray: (parent, key, arr) => symbols.fl_doc_insert_array(parent, toC(key), arr),

    // Query extensions
    queryWhereIn: (query, field, arr) => symbols.fl_query_where_in(query, toC(field), arr),
    queryWhereNotIn: (query, field, arr) => symbols.fl_query_where_not_in(query, toC(field), arr),
    queryWhereArrayContainsAny: (query, field, arr) => symbols.fl_query_where_array_contains_any(query, toC(field), arr),
    queryWhereArrayContainsStr: (query, field, value) => symbols.fl_query_where_array_contains(query, toC(field), toC(String(value))),
    queryWhereArrayContainsInt: (query, field, value) => symbols.fl_query_where_array_contains(query, toC(field), toC(String(value))),
    queryStartAfter: (query, anchorDoc) => symbols.fl_query_start_after(query, anchorDoc),
    queryStartAt: (query, anchorDoc) => symbols.fl_query_start_at(query, anchorDoc),
    queryEndAt: (query, anchorDoc) => symbols.fl_query_end_at(query, anchorDoc),
    queryEndBefore: (query, anchorDoc) => symbols.fl_query_end_before(query, anchorDoc),

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
    fl_engine_is_indexes_ready: lib.func('bool fl_engine_is_indexes_ready(FL_Engine* engine)'),
    fl_engine_open_with_config: lib.func('FL_Engine* fl_engine_open_with_config(const char* path, FL_Config* config)'),
    fl_engine_free: lib.func('void fl_engine_free(FL_Engine* engine)'),
    fl_engine_backup: lib.func('int fl_engine_backup(FL_Engine* engine, const char* path)'),
    fl_engine_compact: lib.func('int fl_engine_compact(FL_Engine* engine)'),
    fl_engine_get_stats: lib.func('char* fl_engine_get_stats(FL_Engine* engine)'),
    fl_engine_get_audit_log: lib.func('char* fl_engine_get_audit_log(FL_Engine* engine)'),
    fl_engine_snapshot_indices: lib.func('int fl_engine_snapshot_indices(FL_Engine* engine)'),
    fl_engine_list_indexes: lib.func('char* fl_engine_list_indexes(FL_Engine* engine, const char* collection)'),

    fl_config_new: lib.func('FL_Config* fl_config_new()'),
    fl_config_free: lib.func('void fl_config_free(FL_Config* config)'),
    fl_config_set_durability: lib.func('void fl_config_set_durability(FL_Config* config, int32_t mode)'),
    fl_config_set_encryption_key: lib.func('void fl_config_set_encryption_key(FL_Config* config, const char* key)'),
    fl_config_set_encrypted_collections: lib.func('int fl_config_set_encrypted_collections(FL_Config* config, const char* collections_json)'),
    fl_config_set_audit_log: lib.func('void fl_config_set_audit_log(FL_Config* config, bool enabled, const char* path)'),
    fl_config_set_query_workers: lib.func('void fl_config_set_query_workers(FL_Config* config, size_t count)'),
    fl_config_set_memory_limits: lib.func('void fl_config_set_memory_limits(FL_Config* config, size_t mmap_size, size_t max_inlined_bytes)'),
    fl_config_set_storage_tuning: lib.func('void fl_config_set_storage_tuning(FL_Config* config, size_t page_size, size_t compaction_threshold, size_t group_commit_max_ops)'),
    fl_config_set_blob_threshold: lib.func('void fl_config_set_blob_threshold(FL_Config* config, size_t threshold_bytes)'),
    fl_config_set_compression: lib.func('void fl_config_set_compression(FL_Config* config, bool enabled, int32_t level)'),

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
    fl_engine_delete_local: lib.func('int fl_engine_delete_local(FL_Engine* engine, const char* collection, const char* doc_id)'),
    fl_engine_set_collection_local: lib.func('int fl_engine_set_collection_local(FL_Engine* engine, const char* collection, int local)'),
    fl_engine_replicate_key: lib.func('int fl_engine_replicate_key(FL_Engine* engine, const char* collection, const char* doc_id)'),
    fl_engine_replicate_collection: lib.func('int fl_engine_replicate_collection(FL_Engine* engine, const char* collection)'),
    fl_engine_vacuum_collection: lib.func('int fl_engine_vacuum_collection(FL_Engine* engine, const char* collection)'),
    fl_engine_patch: lib.func('int fl_engine_patch(FL_Engine* engine, const char* collection, const char* doc_id, const FL_Doc* updates)'),
    fl_engine_insert_subdoc: lib.func('int fl_engine_insert_subdoc(FL_Engine* engine, const char* col, const char* id, const char* sub_col, const char* sub_id, const FL_Doc* doc)'),
    fl_engine_get_by_ref: lib.func('FL_Doc* fl_engine_get_by_ref(FL_Engine* engine, const FL_Doc* doc, const char* field_key)'),
    fl_engine_create_index: lib.func('uint32_t fl_engine_create_index(FL_Engine* engine, const char* collection, const char* fields_json)'),

    fl_batch_new: lib.func('FL_Batch* fl_batch_new()'),
    fl_batch_free: lib.func('void fl_batch_free(FL_Batch* batch)'),
    fl_batch_set: lib.func('int fl_batch_set(FL_Batch* batch, const char* collection, const char* doc_id, const FL_Doc* doc)'),
    fl_batch_delete: lib.func('int fl_batch_delete(FL_Batch* batch, const char* collection, const char* doc_id)'),
    fl_batch_commit: lib.func('int fl_batch_commit(FL_Engine* engine, FL_Batch* batch)'),

    fl_transaction_begin: lib.func('FL_Transaction* fl_transaction_begin(FL_Engine* engine)'),
    fl_transaction_get: lib.func('FL_Doc* fl_transaction_get(FL_Engine* engine, FL_Transaction* tx, const char* collection, const char* doc_id)'),
    fl_transaction_set: lib.func('int fl_transaction_set(FL_Transaction* tx, const char* collection, const char* doc_id, const FL_Doc* doc)'),
    fl_transaction_commit: lib.func('int fl_transaction_commit(FL_Engine* engine, FL_Transaction* tx)'),
    fl_transaction_free: lib.func('void fl_transaction_free(FL_Transaction* tx)'),

    fl_query_new: lib.func('FL_Query* fl_query_new(const char* collection)'),
    fl_query_free: lib.func('void fl_query_free(FL_Query* query)'),
    fl_query_where_eq_str: lib.func('int fl_query_where_eq_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_eq_bool: lib.func('int fl_query_where_eq_bool(FL_Query* query, const char* field, bool value)'),
    fl_query_where_eq_int: lib.func('int fl_query_where_eq_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_ne_str: lib.func('int fl_query_where_ne_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_ne_int: lib.func('int fl_query_where_ne_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_gt_str: lib.func('int fl_query_where_gt_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_gt_int: lib.func('int fl_query_where_gt_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_gte_str: lib.func('int fl_query_where_gte_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_gte_int: lib.func('int fl_query_where_gte_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_lt_str: lib.func('int fl_query_where_lt_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_lt_int: lib.func('int fl_query_where_lt_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_lte_str: lib.func('int fl_query_where_lte_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_lte_int: lib.func('int fl_query_where_lte_int(FL_Query* query, const char* field, int64_t value)'),
    fl_query_where_match: lib.func('int fl_query_where_match(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_match_prefix: lib.func('int fl_query_where_match_prefix(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_contains: lib.func('int fl_query_where_contains(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_starts_with: lib.func('int fl_query_where_starts_with(FL_Query* query, const char* field, const char* value)'),
    fl_query_order_by: lib.func('int fl_query_order_by(FL_Query* query, const char* field, bool ascending)'),
    fl_query_limit: lib.func('int fl_query_limit(FL_Query* query, size_t limit)'),
    fl_query_offset: lib.func('int fl_query_offset(FL_Query* query, size_t offset)'),
    fl_query_select_field: lib.func('int fl_query_select_field(FL_Query* query, const char* field)'),
    fl_query_execute: lib.func('char* fl_query_execute(FL_Engine* engine, const FL_Query* query)'),
    fl_query_delete: lib.func('int fl_query_delete(FL_Engine* engine, FL_Query* query)'),
    fl_query_delete_local: lib.func('int fl_query_delete_local(FL_Engine* engine, FL_Query* query)'),
    fl_query_patch: lib.func('int fl_query_patch(FL_Engine* engine, FL_Query* query, const FL_Doc* patch_doc)'),
    fl_query_execute_to_handles: lib.func('FL_ResultSet* fl_query_execute_to_handles(FL_Engine* engine, const FL_Query* query)'),
    fl_result_set_count: lib.func('size_t fl_result_set_count(FL_ResultSet* results)'),
    fl_result_set_get_doc: lib.func('FL_Doc* fl_result_set_get_doc(FL_ResultSet* results, size_t index)'),
    fl_result_set_free: lib.func('void fl_result_set_free(FL_ResultSet* results)'),
    fl_result_set_to_json: lib.func('char* fl_result_set_to_json(FL_ResultSet* results)'),
    fl_query_defer_blobs: lib.func('int fl_query_defer_blobs(FL_Query* query, int defer)'),
    fl_doc_resolve_blobs: lib.func('int fl_doc_resolve_blobs(FL_Engine* engine, const char* collection, FL_Doc* doc)'),
    fl_engine_insert_take: lib.func('int fl_engine_insert_take(FL_Engine* engine, const char* collection, const char* doc_id, FL_Doc* doc)'),
    fl_config_set_wal_reserve_bytes: lib.func('void fl_config_set_wal_reserve_bytes(FL_Config* config, uint64_t bytes)'),

    fl_query_aggregate_count: lib.func('int fl_query_aggregate_count(FL_Query* query)'),
    fl_query_aggregate_sum: lib.func('int fl_query_aggregate_sum(FL_Query* query, const char* field)'),
    fl_query_aggregate_avg: lib.func('int fl_query_aggregate_avg(FL_Query* query, const char* field)'),
    fl_query_execute_aggregation: lib.func('char* fl_query_execute_aggregation(FL_Engine* engine, const FL_Query* query)'),
    fl_query_where_or_str: lib.func('int fl_query_where_or_str(FL_Query* query, const char* field, const char* value)'),
    fl_query_where_or_int: lib.func('int fl_query_where_or_int(FL_Query* query, const char* field, int64_t value)'),

    fl_engine_list_collections: lib.func('char* fl_engine_list_collections(FL_Engine* engine)'),
    fl_net_syncer_new: lib.func('FL_NetSyncer* fl_net_syncer_new(FL_Engine* engine, const char* name, const char* room_key)'),
    fl_net_syncer_start: lib.func('int fl_net_syncer_start(FL_NetSyncer* syncer, uint16_t port)'),
    fl_net_syncer_set_discovery: lib.func('int fl_net_syncer_set_discovery(FL_NetSyncer* syncer, int mode)'),
    fl_net_syncer_status: lib.func('char* fl_net_syncer_status(FL_NetSyncer* syncer)'),
    fl_net_syncer_free: lib.func('void fl_net_syncer_free(FL_NetSyncer* syncer)'),

    // v0.5.9
    // Indexing
    fl_engine_create_fts_index: lib.func('int fl_engine_create_fts_index(FL_Engine* engine, const char* collection, const char* field)'),
    fl_engine_create_simple_index: lib.func('int fl_engine_create_simple_index(FL_Engine* engine, const char* collection, const char* field)'),

    // Array API
    fl_array_new: lib.func('FL_Array* fl_array_new()'),
    fl_array_free: lib.func('void fl_array_free(FL_Array* arr)'),
    fl_array_append_str: lib.func('int fl_array_append_str(FL_Array* arr, const char* value)'),
    fl_array_append_int: lib.func('int fl_array_append_int(FL_Array* arr, int64_t value)'),
    fl_array_append_doc: lib.func('int fl_array_append_doc(FL_Array* arr, const FL_Doc* doc)'),

    // Nested
    fl_doc_insert_doc: lib.func('int fl_doc_insert_doc(FL_Doc* parent, const char* key, FL_Doc* child)'),
    fl_doc_insert_array: lib.func('int fl_doc_insert_array(FL_Doc* parent, const char* key, FL_Array* arr)'),

    // Query extensions
    fl_query_where_in: lib.func('int fl_query_where_in(FL_Query* query, const char* field, FL_Array* arr)'),
    fl_query_where_not_in: lib.func('int fl_query_where_not_in(FL_Query* query, const char* field, FL_Array* arr)'),
    fl_query_where_array_contains_any: lib.func('int fl_query_where_array_contains_any(FL_Query* query, const char* field, FL_Array* arr)'),
    fl_query_where_array_contains: lib.func('int fl_query_where_array_contains(FL_Query* query, const char* field, const char* value)'),
    fl_query_start_after: lib.func('int fl_query_start_after(FL_Query* query, FL_Doc* anchor)'),
    fl_query_start_at: lib.func('int fl_query_start_at(FL_Query* query, const FL_Doc* anchor)'),
    fl_query_end_at: lib.func('int fl_query_end_at(FL_Query* query, const FL_Doc* anchor)'),
    fl_query_end_before: lib.func('int fl_query_end_before(FL_Query* query, const FL_Doc* anchor)'),

    fl_doc_insert_reference: lib.func('int fl_doc_insert_reference(FL_Doc* doc, const char* key, const char* target_collection, const char* target_id)'),

    fl_cloud_sync_new: lib.func('FL_CloudSync* fl_cloud_sync_new(FL_Engine* engine, int32_t mode, const char* client_id, const char* room_name, const char* room_key, const char* auth_token)'),
    fl_cloud_sync_server_new: lib.func('FL_CloudSync* fl_cloud_sync_server_new(FL_Engine* engine, const char* server_id, const char* auth_token)'),
    fl_cloud_sync_client_new: lib.func('FL_CloudSync* fl_cloud_sync_client_new(FL_Engine* engine, const char* client_id, const char* room_name, const char* room_key, const char* auth_token)'),
    fl_cloud_sync_start: lib.func('int fl_cloud_sync_start(FL_CloudSync* cloud_sync, const char* address)'),
    fl_cloud_sync_status: lib.func('char* fl_cloud_sync_status(FL_CloudSync* cloud_sync)'),
    fl_cloud_sync_stop: lib.func('void fl_cloud_sync_stop(FL_CloudSync* cloud_sync)'),
    fl_cloud_sync_free: lib.func('void fl_cloud_sync_free(FL_CloudSync* cloud_sync)'),

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
    engineIsIndexesReady: (engine) => fn.fl_engine_is_indexes_ready(engine),
    engineOpenWithConfig: (path, config) => fn.fl_engine_open_with_config(path, config),
    engineFree: (engine) => fn.fl_engine_free(engine),
    engineBackup: (e, p) => fn.fl_engine_backup(e, p),
    engineCompact: (e) => fn.fl_engine_compact(e),
    engineGetStats: (e) => ptrToStringAndFree(fn.fl_engine_get_stats(e)),
    engineGetAuditLog: (e) => ptrToStringAndFree(fn.fl_engine_get_audit_log(e)),
    engineSnapshotIndices: (e) => fn.fl_engine_snapshot_indices(e),
    engineListIndexes: (e, c) => ptrToStringAndFree(fn.fl_engine_list_indexes(e, c ?? null)),

    configNew: () => fn.fl_config_new(),
    configFree: (c) => fn.fl_config_free(c),
    configSetDurability: (c, m) => fn.fl_config_set_durability(c, m),
    configSetEncryptionKey: (c, k) => fn.fl_config_set_encryption_key(c, k),
    configSetEncryptedCollections: (c, json) => fn.fl_config_set_encrypted_collections(c, json),
    configSetAuditLog: (c, e, p) => fn.fl_config_set_audit_log(c, e, p),
    configSetQueryWorkers: (c, count) => fn.fl_config_set_query_workers(c, count),
    configSetMemoryLimits: (c, m, mi) => fn.fl_config_set_memory_limits(c, m, mi),
    configSetStorageTuning: (c, ps, th, gc) => fn.fl_config_set_storage_tuning(c, ps, th, gc),
    configSetBlobThreshold: (c, t) => fn.fl_config_set_blob_threshold(c, t),
    configSetCompression: (c, e, l) => fn.fl_config_set_compression(c, e, l),

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
    docInsertReference: (doc, key, tc, tid) => fn.fl_doc_insert_reference(doc, key, tc, tid),
    docToJson: (doc) => ptrToStringAndFree(fn.fl_doc_to_json(doc)),

    engineInsert: (engine, collection, docId, doc) => fn.fl_engine_insert(engine, collection, docId, doc),
    engineGet: (engine, collection, docId) => fn.fl_engine_get(engine, collection, docId),
    engineDelete: (engine, collection, docId) => fn.fl_engine_delete(engine, collection, docId),
    engineDeleteLocal: (engine, collection, docId) => fn.fl_engine_delete_local(engine, collection, docId),
    engineSetCollectionLocal: (engine, collection, local) => fn.fl_engine_set_collection_local(engine, collection, local),
    engineReplicateKey: (engine, collection, docId) => fn.fl_engine_replicate_key(engine, collection, docId),
    engineReplicateCollection: (engine, collection) => fn.fl_engine_replicate_collection(engine, collection),
    engineVacuumCollection: (engine, collection) => fn.fl_engine_vacuum_collection(engine, collection),
    enginePatch: (engine, collection, docId, updates) => fn.fl_engine_patch(engine, collection, docId, updates),
    engineInsertSubDoc: (engine, col, id, subCol, subId, doc) => fn.fl_engine_insert_subdoc(engine, col, id, subCol, subId, doc),
    engineGetByRef: (engine, doc, fieldKey) => fn.fl_engine_get_by_ref(engine, doc, fieldKey),
    engineCreateIndex: (engine, collection, fieldsJson) => fn.fl_engine_create_index(engine, collection, fieldsJson),

    batchNew: () => fn.fl_batch_new(),
    batchFree: (batch) => fn.fl_batch_free(batch),
    batchSet: (batch, collection, docId, doc) => fn.fl_batch_set(batch, collection, docId, doc),
    batchDelete: (batch, collection, docId) => fn.fl_batch_delete(batch, collection, docId),
    batchCommit: (engine, batch) => fn.fl_batch_commit(engine, batch),

    transactionBegin: (engine) => fn.fl_transaction_begin(engine),
    transactionGet: (engine, tx, collection, docId) => fn.fl_transaction_get(engine, tx, collection, docId),
    transactionSet: (tx, collection, docId, doc) => fn.fl_transaction_set(tx, collection, docId, doc),
    transactionCommit: (engine, tx) => fn.fl_transaction_commit(engine, tx),
    transactionFree: (tx) => fn.fl_transaction_free(tx),

    queryNew: (collection) => fn.fl_query_new(collection),
    queryFree: (query) => fn.fl_query_free(query),
    queryWhereEqStr: (query, field, value) => fn.fl_query_where_eq_str(query, field, value),
    queryWhereEqBool: (query, field, value) => fn.fl_query_where_eq_bool(query, field, value),
    queryWhereEqInt: (query, field, value) => fn.fl_query_where_eq_int(query, field, value),
    queryWhereNeStr: (query, field, value) => fn.fl_query_where_ne_str(query, field, value),
    queryWhereNeInt: (query, field, value) => fn.fl_query_where_ne_int(query, field, value),
    queryWhereGtStr: (query, field, value) => fn.fl_query_where_gt_str(query, field, value),
    queryWhereGtInt: (query, field, value) => fn.fl_query_where_gt_int(query, field, value),
    queryWhereGteStr: (query, field, value) => fn.fl_query_where_gte_str(query, field, value),
    queryWhereGteInt: (query, field, value) => fn.fl_query_where_gte_int(query, field, value),
    queryWhereLtStr: (query, field, value) => fn.fl_query_where_lt_str(query, field, value),
    queryWhereLtInt: (query, field, value) => fn.fl_query_where_lt_int(query, field, value),
    queryWhereLteStr: (query, field, value) => fn.fl_query_where_lte_str(query, field, value),
    queryWhereLteInt: (query, field, value) => fn.fl_query_where_lte_int(query, field, value),
    queryWhereMatch: (query, field, value) => fn.fl_query_where_match(query, field, value),
    queryWhereMatchPrefix: (query, field, value) => fn.fl_query_where_match_prefix(query, field, value),
    queryWhereContains: (query, field, value) => fn.fl_query_where_contains(query, field, value),
    queryWhereStartsWith: (query, field, value) => fn.fl_query_where_starts_with(query, field, value),
    queryWhereOrStr: (query, field, value) => fn.fl_query_where_or_str(query, field, value),
    queryWhereOrInt: (query, field, value) => fn.fl_query_where_or_int(query, field, value),
    queryOrderBy: (query, field, asc) => fn.fl_query_order_by(query, field, asc),
    queryLimit: (query, limit) => fn.fl_query_limit(query, limit),
    queryOffset: (query, offset) => fn.fl_query_offset(query, offset),
    querySelectField: (query, field) => fn.fl_query_select_field(query, field),
    queryExecute: (engine, query) => ptrToStringAndFree(fn.fl_query_execute(engine, query)),
    queryDelete: (engine, query) => fn.fl_query_delete(engine, query),
    queryDeleteLocal: (engine, query) => fn.fl_query_delete_local(engine, query),
    queryPatch: (engine, query, patchDoc) => fn.fl_query_patch(engine, query, patchDoc),
    queryExecuteToHandles: (engine, query) => fn.fl_query_execute_to_handles(engine, query),
    resultSetCount: (results) => fn.fl_result_set_count(results),
    resultSetGetDoc: (results, index) => fn.fl_result_set_get_doc(results, index),
    resultSetFree: (results) => fn.fl_result_set_free(results),
    resultSetToJson: (results) => ptrToStringAndFree(fn.fl_result_set_to_json(results)),
    queryDeferBlobs: (query, defer) => fn.fl_query_defer_blobs(query, defer ? 1 : 0),
    docResolveBlobs: (engine, collection, doc) => fn.fl_doc_resolve_blobs(engine, collection, doc),
    engineInsertTake: (engine, collection, docId, doc) => fn.fl_engine_insert_take(engine, collection, docId, doc),
    configSetWalReserveBytes: (c, bytes) => fn.fl_config_set_wal_reserve_bytes(c, bytes),

    queryAggregateCount: (q) => fn.fl_query_aggregate_count(q),
    queryAggregateSum: (q, f) => fn.fl_query_aggregate_sum(q, f),
    queryAggregateAvg: (q, f) => fn.fl_query_aggregate_avg(q, f),
    queryExecuteAggregation: (e, q) => ptrToStringAndFree(fn.fl_query_execute_aggregation(e, q)),
    engineListCollections: (engine) => ptrToStringAndFree(fn.fl_engine_list_collections(engine)),
    netSyncerNew: (engine, name, roomKey) => fn.fl_net_syncer_new(engine, name, roomKey),
    netSyncerStart: (syncer, port) => fn.fl_net_syncer_start(syncer, port),
    netSyncerSetDiscovery: (syncer, mode) => fn.fl_net_syncer_set_discovery(syncer, mode),
    netSyncerStatus: (syncer) => ptrToStringAndFree(fn.fl_net_syncer_status(syncer)),
    netSyncerFree: (syncer) => fn.fl_net_syncer_free(syncer),

    cloudSyncNew: (engine, mode, clientId, roomName, roomKey, authToken) => fn.fl_cloud_sync_new(engine, mode, clientId, roomName, roomKey, authToken),
    cloudSyncServerNew: (engine, serverId, authToken) => fn.fl_cloud_sync_server_new(engine, serverId, authToken),
    cloudSyncClientNew: (engine, clientId, roomName, roomKey, authToken) => fn.fl_cloud_sync_client_new(engine, clientId, roomName, roomKey, authToken),
    cloudSyncStart: (cs, address) => fn.fl_cloud_sync_start(cs, address),
    cloudSyncStatus: (cs) => ptrToStringAndFree(fn.fl_cloud_sync_status(cs)),
    cloudSyncStop: (cs) => fn.fl_cloud_sync_stop(cs),
    cloudSyncFree: (cs) => fn.fl_cloud_sync_free(cs),

    // ================= v0.5.9 =================

    // Indexing
    createFtsIndex: (engine, collection, field) => fn.fl_engine_create_fts_index(engine, collection, field),
    createSimpleIndex: (engine, collection, field) => fn.fl_engine_create_simple_index(engine, collection, field),

    // Array API
    arrayNew: () => fn.fl_array_new(),
    arrayFree: (arr) => fn.fl_array_free(arr),
    arrayAppendStr: (arr, value) => fn.fl_array_append_str(arr, value),
    arrayAppendInt: (arr, value) => fn.fl_array_append_int(arr, value),
    arrayAppendDoc: (arr, doc) => fn.fl_array_append_doc(arr, doc),

    // Nested
    docInsertDoc: (parent, key, child) => fn.fl_doc_insert_doc(parent, key, child),
    docInsertArray: (parent, key, arr) => fn.fl_doc_insert_array(parent, key, arr),

    // Query extensions
    queryWhereIn: (query, field, arr) => fn.fl_query_where_in(query, field, arr),
    queryWhereNotIn: (query, field, arr) => fn.fl_query_where_not_in(query, field, arr),
    queryWhereArrayContainsAny: (query, field, arr) => fn.fl_query_where_array_contains_any(query, field, arr),
    queryWhereArrayContainsStr: (query, field, value) => fn.fl_query_where_array_contains(query, field, String(value)),
    queryWhereArrayContainsInt: (query, field, value) => fn.fl_query_where_array_contains(query, field, String(value)),
    queryStartAfter: (query, anchorDoc) => fn.fl_query_start_after(query, anchorDoc),
    queryStartAt: (query, anchorDoc) => fn.fl_query_start_at(query, anchorDoc),
    queryEndAt: (query, anchorDoc) => fn.fl_query_end_at(query, anchorDoc),
    queryEndBefore: (query, anchorDoc) => fn.fl_query_end_before(query, anchorDoc),

    lastError: () => (fn.fl_last_error() as string) || 'unknown ffi error'
  };
}

export async function loadNativeBindings(explicitPath?: string): Promise<NativeBindings> {
  const libPath = resolveLibraryPath(explicitPath);
  // return isBunRuntime() ? createBunBindings(libPath) : createNodeBindings(libPath);

  // 1. Check for Bun
  if (typeof (globalThis as any).Bun !== 'undefined') {
    return createBunBindings(libPath);
  }

  // 2. Check for Node.js (via process)
  if (typeof process !== 'undefined' && process.versions && process.versions.node) {
    return createNodeBindings(libPath);
  }

  throw new Error("FireLite Native Bindings are only supported in Node.js or Bun environments. For browsers, use the Tauri Gateway.");
}
