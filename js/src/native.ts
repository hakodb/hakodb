/* eslint-disable @typescript-eslint/no-explicit-any */

type Handle = any;

export interface NativeBindings {
  engineOpen(path: string): Handle;
  engineFree(engine: Handle): void;

  docNew(): Handle;
  docFree(doc: Handle): void;
  docInsertStr(doc: Handle, key: string, value: string): number;
  docInsertInt(doc: Handle, key: string, value: number): number;
  docInsertFloat(doc: Handle, key: string, value: number): number;
  docInsertBool(doc: Handle, key: string, value: boolean): number;
  docInsertNull(doc: Handle, key: string): number;
  docInsertBin(doc: Handle, key: string, bytes: Uint8Array): number;
  docToJson(doc: Handle): string | null;

  engineInsert(engine: Handle, collection: string, docId: string, doc: Handle): number;
  engineGet(engine: Handle, collection: string, docId: string): Handle;
  engineDelete(engine: Handle, collection: string, docId: string): number;

  batchNew(): Handle;
  batchFree(batch: Handle): void;
  batchSet(batch: Handle, collection: string, docId: string, doc: Handle): number;
  batchDelete(batch: Handle, collection: string, docId: string): number;
  batchCommit(engine: Handle, batch: Handle): number;

  queryNew(collection: string): Handle;
  queryFree(query: Handle): void;
  queryWhereEqStr(query: Handle, field: string, value: string): number;
  queryWhereEqInt(query: Handle, field: string, value: number): number;
  queryOrderBy(query: Handle, field: string, ascending: boolean): number;
  queryLimit(query: Handle, limit: number): number;
  querySelectField(query: Handle, field: string): number;
  queryExecute(engine: Handle, query: Handle): string | null;

  lastError(): string;
}

function isBunRuntime(): boolean {
  return typeof (globalThis as any).Bun !== 'undefined';
}

function defaultLibraryPath(): string {
  if (isBunRuntime()) {
    const platform = (globalThis as any).Bun?.platform;
    if (platform === 'darwin') return './target/release/libfirelite.dylib';
    if (platform === 'win32') return './target/release/firelite.dll';
    return './target/release/libfirelite.so';
  }

  switch (process.platform) {
    case 'win32':
      return './target/release/firelite.dll';
    case 'darwin':
      return './target/release/libfirelite.dylib';
    default:
      return './target/release/libfirelite.so';
  }
}

function resolveLibraryPath(explicitPath?: string): string {
  return explicitPath ?? defaultLibraryPath();
}

async function createBunBindings(libPath: string): Promise<NativeBindings> {
  const ffi = await import('bun:ffi');
  const { dlopen, FFIType, CString } = ffi as any;

  const symbols = dlopen(libPath, {
    fl_engine_open: { args: [FFIType.cstring], returns: FFIType.ptr },
    fl_engine_free: { args: [FFIType.ptr], returns: FFIType.void },

    fl_doc_new: { args: [], returns: FFIType.ptr },
    fl_doc_free: { args: [FFIType.ptr], returns: FFIType.void },
    fl_doc_insert_str: { args: [FFIType.ptr, FFIType.cstring, FFIType.cstring], returns: FFIType.i32 },
    fl_doc_insert_int: { args: [FFIType.ptr, FFIType.cstring, FFIType.i64], returns: FFIType.i32 },
    fl_doc_insert_float: { args: [FFIType.ptr, FFIType.cstring, FFIType.f64], returns: FFIType.i32 },
    fl_doc_insert_bool: { args: [FFIType.ptr, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
    fl_doc_insert_null: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_doc_insert_bin: { args: [FFIType.ptr, FFIType.cstring, FFIType.ptr, FFIType.usize], returns: FFIType.i32 },
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
    fl_query_order_by: { args: [FFIType.ptr, FFIType.cstring, FFIType.bool], returns: FFIType.i32 },
    fl_query_limit: { args: [FFIType.ptr, FFIType.usize], returns: FFIType.i32 },
    fl_query_select_field: { args: [FFIType.ptr, FFIType.cstring], returns: FFIType.i32 },
    fl_query_execute: { args: [FFIType.ptr, FFIType.ptr], returns: FFIType.ptr },

    fl_last_error: { args: [], returns: FFIType.ptr },
    fl_string_free: { args: [FFIType.ptr], returns: FFIType.void }
  }).symbols;

  const ptrToStringAndFree = (ptr: any): string | null => {
    if (!ptr) return null;
    const text = new CString(ptr).toString();
    symbols.fl_string_free(ptr);
    return text;
  };

  return {
    engineOpen: (path) => symbols.fl_engine_open(path),
    engineFree: (engine) => symbols.fl_engine_free(engine),

    docNew: () => symbols.fl_doc_new(),
    docFree: (doc) => symbols.fl_doc_free(doc),
    docInsertStr: (doc, key, value) => symbols.fl_doc_insert_str(doc, key, value),
    docInsertInt: (doc, key, value) => symbols.fl_doc_insert_int(doc, key, BigInt(Math.trunc(value))),
    docInsertFloat: (doc, key, value) => symbols.fl_doc_insert_float(doc, key, value),
    docInsertBool: (doc, key, value) => symbols.fl_doc_insert_bool(doc, key, value),
    docInsertNull: (doc, key) => symbols.fl_doc_insert_null(doc, key),
    docInsertBin: (doc, key, bytes) => symbols.fl_doc_insert_bin(doc, key, bytes, bytes.byteLength),
    docToJson: (doc) => ptrToStringAndFree(symbols.fl_doc_to_json(doc)),

    engineInsert: (engine, collection, docId, doc) => symbols.fl_engine_insert(engine, collection, docId, doc),
    engineGet: (engine, collection, docId) => symbols.fl_engine_get(engine, collection, docId),
    engineDelete: (engine, collection, docId) => symbols.fl_engine_delete(engine, collection, docId),

    batchNew: () => symbols.fl_batch_new(),
    batchFree: (batch) => symbols.fl_batch_free(batch),
    batchSet: (batch, collection, docId, doc) => symbols.fl_batch_set(batch, collection, docId, doc),
    batchDelete: (batch, collection, docId) => symbols.fl_batch_delete(batch, collection, docId),
    batchCommit: (engine, batch) => symbols.fl_batch_commit(engine, batch),

    queryNew: (collection) => symbols.fl_query_new(collection),
    queryFree: (query) => symbols.fl_query_free(query),
    queryWhereEqStr: (query, field, value) => symbols.fl_query_where_eq_str(query, field, value),
    queryWhereEqInt: (query, field, value) => symbols.fl_query_where_eq_int(query, field, BigInt(Math.trunc(value))),
    queryOrderBy: (query, field, asc) => symbols.fl_query_order_by(query, field, asc),
    queryLimit: (query, limit) => symbols.fl_query_limit(query, limit),
    querySelectField: (query, field) => symbols.fl_query_select_field(query, field),
    queryExecute: (engine, query) => ptrToStringAndFree(symbols.fl_query_execute(engine, query)),

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

  const fn = {
    fl_engine_open: lib.func('FL_Engine* fl_engine_open(const char* path)'),
    fl_engine_free: lib.func('void fl_engine_free(FL_Engine* engine)'),

    fl_doc_new: lib.func('FL_Doc* fl_doc_new()'),
    fl_doc_free: lib.func('void fl_doc_free(FL_Doc* doc)'),
    fl_doc_insert_str: lib.func('int fl_doc_insert_str(FL_Doc* doc, const char* key, const char* value)'),
    fl_doc_insert_int: lib.func('int fl_doc_insert_int(FL_Doc* doc, const char* key, int64_t value)'),
    fl_doc_insert_float: lib.func('int fl_doc_insert_float(FL_Doc* doc, const char* key, double value)'),
    fl_doc_insert_bool: lib.func('int fl_doc_insert_bool(FL_Doc* doc, const char* key, bool value)'),
    fl_doc_insert_null: lib.func('int fl_doc_insert_null(FL_Doc* doc, const char* key)'),
    fl_doc_insert_bin: lib.func('int fl_doc_insert_bin(FL_Doc* doc, const char* key, const uint8_t* data, uintptr_t len)'),
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
    fl_query_order_by: lib.func('int fl_query_order_by(FL_Query* query, const char* field, bool ascending)'),
    fl_query_limit: lib.func('int fl_query_limit(FL_Query* query, uintptr_t limit)'),
    fl_query_select_field: lib.func('int fl_query_select_field(FL_Query* query, const char* field)'),
    fl_query_execute: lib.func('char* fl_query_execute(FL_Engine* engine, const FL_Query* query)'),

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
    engineFree: (engine) => fn.fl_engine_free(engine),

    docNew: () => fn.fl_doc_new(),
    docFree: (doc) => fn.fl_doc_free(doc),
    docInsertStr: (doc, key, value) => fn.fl_doc_insert_str(doc, key, value),
    docInsertInt: (doc, key, value) => fn.fl_doc_insert_int(doc, key, Math.trunc(value)),
    docInsertFloat: (doc, key, value) => fn.fl_doc_insert_float(doc, key, value),
    docInsertBool: (doc, key, value) => fn.fl_doc_insert_bool(doc, key, value),
    docInsertNull: (doc, key) => fn.fl_doc_insert_null(doc, key),
    docInsertBin: (doc, key, bytes) => fn.fl_doc_insert_bin(doc, key, Buffer.from(bytes), bytes.byteLength),
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
    queryWhereEqInt: (query, field, value) => fn.fl_query_where_eq_int(query, field, Math.trunc(value)),
    queryOrderBy: (query, field, asc) => fn.fl_query_order_by(query, field, asc),
    queryLimit: (query, limit) => fn.fl_query_limit(query, limit),
    querySelectField: (query, field) => fn.fl_query_select_field(query, field),
    queryExecute: (engine, query) => ptrToStringAndFree(fn.fl_query_execute(engine, query)),

    lastError: () => (fn.fl_last_error() as string) || 'unknown ffi error'
  };
}

export async function loadNativeBindings(explicitPath?: string): Promise<NativeBindings> {
  const libPath = resolveLibraryPath(explicitPath);
  return isBunRuntime() ? createBunBindings(libPath) : createNodeBindings(libPath);
}
