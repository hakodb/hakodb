import { loadNativeBindings, type NativeBindings, type WatchCallback } from './native';

// 1. UPDATED: Recursive Type Definitions to support Nested Maps and Arrays
// export type Primitive = string | number | boolean | null | Uint8Array | Date | FireLiteDocData | Array<any>;
export type Primitive =
  | string
  | number
  | boolean
  | null
  | Uint8Array
  | Date
  | { [key: string]: Primitive }
  | Primitive[];


export type FireLiteDocData = { [key: string]: Primitive };

const SERVER_TIMESTAMP_SENTINEL = "__FL_SERVER_TIMESTAMP__";

export enum DurabilityMode {
  Always = 0,
  Interval = 1,
  Manual = 2,
  OnCommit = 3,
}

export interface FireLiteClientOptions {
  libraryPath?: string;
  native?: NativeBindings;
  config?: FireLiteConfig;
}

// 2. FIXED: Added 'in' to the interface to match the Query class
export interface QueryConstraint {
  field: string;
  op: '==' | 'match' | 'contains' | 'startsWith' | 'in';
  value: any; // Use any because 'in' takes an array
}

export interface QueryOrder {
  field: string;
  ascending: boolean;
}

export type Unsubscribe = () => Promise<void>;

/**
 * Advanced Configuration Builder
 */
export class FireLiteConfig {
  private _handle: unknown;
  private _native: NativeBindings;

  constructor(native: NativeBindings) {
    this._native = native;
    this._handle = native.configNew();
  }

  setDurability(mode: DurabilityMode): this {
    this._native.configSetDurability(this._handle, mode);
    return this;
  }

  setEncryptionKey(key: string): this {
    this._native.configSetEncryptionKey(this._handle, key);
    return this;
  }

  setAuditLog(enabled: boolean, path: string | null = null): this {
    this._native.configSetAuditLog(this._handle, enabled, path);
    return this;
  }

  setQueryWorkers(count: number): this {
    this._native.configSetQueryWorkers(this._handle, count);
    return this;
  }

  setMemoryLimits(mmapBytes: number, maxInlinedBytes: number): this {
    this._native.configSetMemoryLimits(this._handle, mmapBytes, maxInlinedBytes);
    return this;
  }

  setStorageTuning(pageSize: number, pageCache: number, threshold: number, groupCommitMaxOps: number): this {
    this._native.configSetStorageTuning(this._handle, pageSize, pageCache, threshold, groupCommitMaxOps);
    return this;
  }

  getHandle(): unknown {
    return this._handle;
  }
}

function ensureOk(code: number, native: NativeBindings, ctx: string): void {
  if (code !== 0) {
    throw new Error(`${ctx}: ${native.lastError()}`);
  }
}

function parseDocJson(json: string | null): FireLiteDocData {
  if (!json) return {};
  return JSON.parse(json) as FireLiteDocData;
}

function parseQueryRows(json: string | null): FireLiteDocData[] {
  if (!json) return [];
  const parsed = JSON.parse(json);
  return Array.isArray(parsed) ? (parsed as FireLiteDocData[]) : [];
}

/**
 * Recursive field inserter (v0.5.6)
 */
function insertField(native: NativeBindings, handle: unknown, key: string, value: Primitive): void {
  if (value === SERVER_TIMESTAMP_SENTINEL) {
    ensureOk(native.docInsertServerTimestamp(handle, key), native, 'serverTimestamp');
    return;
  }
  if (value instanceof Date) {
    native.docInsertTimestamp(handle, key, BigInt(value.getTime()) * 1000n);
    return;
  }
  if (value instanceof Uint8Array) {
    ensureOk(native.docInsertBin(handle, key, value), native, 'insertBin');
    return;
  }

  if (Array.isArray(value)) {
    const arrHandle = native.arrayNew();
    for (const item of value) {
      if (typeof item === 'string') native.arrayAppendStr(arrHandle, item);
      else if (typeof item === 'number') native.arrayAppendInt(arrHandle, item);
      else if (typeof item === 'object' && item !== null) {
        const tempDoc = toNativeDoc(native, item as FireLiteDocData);
        native.arrayAppendDoc(arrHandle, tempDoc);
        native.docFree(tempDoc); // FFI copies data into the array
      }
    }
    // ownership transfers to parent doc handle
    ensureOk(native.docInsertArray(handle, key, arrHandle), native, 'insertArray');
    return;
  }

  if (typeof value === 'object' && value !== null) {
    const childDocHandle = toNativeDoc(native, value as FireLiteDocData);
    ensureOk(native.docInsertDoc(handle, key, childDocHandle), native, 'insertDoc');
    native.docFree(childDocHandle); // FFI copies data into the parent
    return;
  }

  // Primitives (string, number, bool, null)
  if (typeof value === 'string') {
    ensureOk(native.docInsertStr(handle, key, value), native, 'insertStr');
  } else if (typeof value === 'number') {
    if (Number.isInteger(value)) ensureOk(native.docInsertInt(handle, key, value), native, 'insertInt');
    else ensureOk(native.docInsertFloat(handle, key, value), native, 'insertFloat');
  } else if (typeof value === 'boolean') {
    ensureOk(native.docInsertBool(handle, key, value), native, 'insertBool');
  } else if (value === null) {
    ensureOk(native.docInsertNull(handle, key), native, 'insertNull');
  }
}

function toNativeDoc(native: NativeBindings, data: FireLiteDocData): unknown {
  const doc = native.docNew();
  if (!doc) throw new Error(`docNew failed: ${native.lastError()}`);
  try {
    for (const [key, value] of Object.entries(data)) {
      insertField(native, doc, key, value);
    }
    return doc;
  } catch (err) {
    native.docFree(doc);
    throw err;
  }
}

export class DocumentSnapshot {
  constructor(
    public readonly id: string,
    public readonly exists: boolean,
    private readonly payload?: FireLiteDocData,
    public readonly _nativeHandle?: unknown
  ) { }

  data(): FireLiteDocData | undefined {
    return this.payload;
  }
}

export class FireLiteClient {
  private readonly native: NativeBindings;
  private readonly engine: unknown;
  private isClosed = false;

  private constructor(native: NativeBindings, engine: unknown) {
    this.native = native;
    this.engine = engine;
  }

  static async open(path: string, options?: FireLiteClientOptions): Promise<FireLiteClient> {
    const native = options?.native ?? (await loadNativeBindings(options?.libraryPath));

    let engine: unknown;
    if (options?.config) {
      engine = native.engineOpenWithConfig(path, options.config.getHandle());
    } else {
      engine = native.engineOpen(path);
    }

    if (!engine) {
      throw new Error(`Failed to open FireLite: ${native.lastError()}`);
    }
    return new FireLiteClient(native, engine);
  }

  static serverTimestamp(): any {
    return SERVER_TIMESTAMP_SENTINEL;
  }

  static async createConfig(libraryPath?: string): Promise<FireLiteConfig> {
    const native = await loadNativeBindings(libraryPath);
    return new FireLiteConfig(native);
  }

  collection(name: string): CollectionReference {
    this.assertOpen();
    return new CollectionReference(this, name);
  }

  batch(): WriteBatch {
    this.assertOpen();
    return new WriteBatch(this);
  }

  async listCollections(): Promise<string[]> {
    this.assertOpen();
    const json = this.native.engineListCollections(this.engine);
    return json ? JSON.parse(json) : [];
  }

  async backup(destinationPath: string): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineBackup(this.engine, destinationPath), this.native, 'engineBackup');
  }

  async close(): Promise<void> {
    if (this.isClosed) return;
    this.native.engineFree(this.engine);
    this.isClosed = true;
  }

  async set(collection: string, docId: string, data: FireLiteDocData): Promise<void> {
    this.assertOpen();
    const doc = toNativeDoc(this.native, data);
    try {
      ensureOk(this.native.engineInsert(this.engine, collection, docId, doc), this.native, 'engineInsert');
    } finally {
      this.native.docFree(doc);
    }
  }

  async get(collection: string, docId: string): Promise<DocumentSnapshot> {
    this.assertOpen();
    const doc = this.native.engineGet(this.engine, collection, docId);
    if (!doc) return new DocumentSnapshot(docId, false);

    const json = this.native.docToJson(doc);
    // Keep 'doc' handle for startAfter. Native memory management should be handled
    // by engineFree or manual free if the user keeps thousands of snapshots.
    return new DocumentSnapshot(docId, true, parseDocJson(json), doc);
  }

  async delete(collection: string, docId: string): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineDelete(this.engine, collection, docId), this.native, 'engineDelete');
  }

  nativeBindings(): NativeBindings { return this.native; }
  engineHandle(): unknown { return this.engine; }

  private assertOpen(): void {
    if (this.isClosed) throw new Error('FireLiteClient is already closed');
  }
}

export class CollectionReference {
  constructor(private readonly client: FireLiteClient, private readonly name: string) { }

  doc(id: string): DocumentReference {
    return new DocumentReference(this.client, this.name, id);
  }

  onSnapshot(callback: (snapshot: FireLiteDocData[]) => void): Unsubscribe {
    return new Query(this.client, this.name).onSnapshot(callback);
  }

  // 3. FIXED: Updated 'op' signature to include 'in'
  where(field: string, op: '==' | 'match' | 'contains' | 'startsWith' | 'in', value: any): Query {
    return new Query(this.client, this.name).where(field, op, value);
  }

  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): Query {
    return new Query(this.client, this.name).orderBy(field, direction);
  }

  limit(max: number): Query {
    return new Query(this.client, this.name).limit(max);
  }

  select(...fields: string[]): Query {
    return new Query(this.client, this.name).select(...fields);
  }

  async get(): Promise<FireLiteDocData[]> {
    return new Query(this.client, this.name).get();
  }

  async count(): Promise<number> {
    return new Query(this.client, this.name).count();
  }

  async createIndex(field: string): Promise<void> {
    ensureOk(this.client.nativeBindings().createSimpleIndex(this.client.engineHandle(), this.name, field), this.client.nativeBindings(), 'createIndex');
  }

  async createFtsIndex(field: string): Promise<void> {
    ensureOk(this.client.nativeBindings().createFtsIndex(this.client.engineHandle(), this.name, field), this.client.nativeBindings(), 'createFtsIndex');
  }
}

export class DocumentReference {
  constructor(private readonly client: FireLiteClient, readonly _collection: string, readonly _id: string) { }

  async set(data: FireLiteDocData): Promise<void> {
    await this.client.set(this._collection, this._id, data);
  }

  async get(): Promise<DocumentSnapshot> {
    return this.client.get(this._collection, this._id);
  }

  async delete(): Promise<void> {
    await this.client.delete(this._collection, this._id);
  }
}

export class Query {
  private _startAfterSnapshot?: DocumentSnapshot;
  private readonly filters: QueryConstraint[] = [];
  private order?: QueryOrder;
  private queryLimit?: number;
  private projection: string[] = [];

  constructor(private readonly client: FireLiteClient, private readonly collection: string) { }

  where(field: string, op: '==' | 'match' | 'contains' | 'startsWith' | 'in', value: any): Query {
    this.filters.push({ field, op, value });
    return this;
  }

  startAfter(snapshot: DocumentSnapshot): Query {
    this._startAfterSnapshot = snapshot;
    return this;
  }

  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): Query {
    this.order = { field, ascending: direction === 'asc' };
    return this;
  }

  limit(max: number): Query {
    this.queryLimit = max;
    return this;
  }

  select(...fields: string[]): Query {
    this.projection = fields;
    return this;
  }

  private prepareNativeQuery(): unknown {
    const native = this.client.nativeBindings();
    const handle = native.queryNew(this.collection);
    if (!handle) throw new Error(`queryNew failed: ${native.lastError()}`);

    try {
      for (const filter of this.filters) {
        switch (filter.op) {
          case '==':
            if (typeof filter.value === 'string') {
              ensureOk(native.queryWhereEqStr(handle, filter.field, filter.value), native, 'queryWhereEqStr');
            } else {
              ensureOk(native.queryWhereEqInt(handle, filter.field, filter.value), native, 'queryWhereEqInt');
            }
            break;
          case 'match':
            ensureOk(native.queryWhereMatch(handle, filter.field, String(filter.value)), native, 'queryWhereMatch');
            break;
          case 'contains':
            ensureOk(native.queryWhereContains(handle, filter.field, String(filter.value)), native, 'queryWhereContains');
            break;
          case 'startsWith':
            ensureOk(native.queryWhereStartsWith(handle, filter.field, String(filter.value)), native, 'queryWhereStartsWith');
            break;
          case 'in':
            const arr = native.arrayNew();
            (filter.value as any[]).forEach(v => {
              if (typeof v === 'string') native.arrayAppendStr(arr, v);
              else native.arrayAppendInt(arr, v);
            });
            // queryWhereIn consumes the array handle
            ensureOk(native.queryWhereIn(handle, filter.field, arr), native, 'queryWhereIn');
            break;
        }
      }

      if (this._startAfterSnapshot?._nativeHandle) {
        ensureOk(native.queryStartAfter(handle, this._startAfterSnapshot._nativeHandle), native, 'queryStartAfter');
      }

      if (this.order) ensureOk(native.queryOrderBy(handle, this.order.field, this.order.ascending), native, 'queryOrderBy');
      if (this.queryLimit !== undefined) ensureOk(native.queryLimit(handle, this.queryLimit), native, 'queryLimit');
      for (const field of this.projection) ensureOk(native.querySelectField(handle, field), native, 'querySelectField');

      return handle;
    } catch (err) {
      native.queryFree(handle);
      throw err;
    }
  }

  async get(): Promise<FireLiteDocData[]> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    try {
      return parseQueryRows(native.queryExecute(this.client.engineHandle(), handle));
    } finally {
      native.queryFree(handle);
    }
  }

  async count(): Promise<number> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    try {
      ensureOk(native.queryAggregateCount(handle), native, 'queryAggregateCount');
      const json = native.queryExecuteAggregation(this.client.engineHandle(), handle);
      return json ? (JSON.parse(json).count || 0) : 0;
    } finally {
      native.queryFree(handle);
    }
  }

  async sum(field: string): Promise<number> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    try {
      ensureOk(native.queryAggregateSum(handle, field), native, 'queryAggregateSum');
      const json = native.queryExecuteAggregation(this.client.engineHandle(), handle);
      return json ? (JSON.parse(json)[`sum_${field}`] || 0) : 0;
    } finally {
      native.queryFree(handle);
    }
  }

  async avg(field: string): Promise<number> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    try {
      ensureOk(native.queryAggregateAvg(handle, field), native, 'queryAggregateAvg');
      const json = native.queryExecuteAggregation(this.client.engineHandle(), handle);
      return json ? (JSON.parse(json)[`avg_${field}`] || 0) : 0;
    } finally {
      native.queryFree(handle);
    }
  }

  onSnapshot(callback: (snapshot: FireLiteDocData[]) => void): Unsubscribe {
    const native = this.client.nativeBindings();
    const internalWatcher: WatchCallback = async () => {
      const data = await this.get();
      callback(data);
    };
    const watchHandle = native.engineWatch(this.client.engineHandle(), this.collection, internalWatcher);
    this.get().then(callback);
    return async () => { native.watchFree(watchHandle); };
  }
}

export class WriteBatch {
  private readonly native: NativeBindings;
  private readonly handle: unknown;
  private committed = false;

  constructor(private readonly client: FireLiteClient) {
    this.native = client.nativeBindings();
    this.handle = this.native.batchNew();
    if (!this.handle) throw new Error(`batchNew failed: ${this.native.lastError()}`);
  }

  set(docRef: DocumentReference, data: FireLiteDocData): WriteBatch {
    this.ensureActive();
    const doc = toNativeDoc(this.native, data);
    try {
      ensureOk(this.native.batchSet(this.handle, docRef._collection, docRef._docId, doc), this.native, 'batchSet');
      return this;
    } finally {
      this.native.docFree(doc);
    }
  }

  delete(docRef: DocumentReference): WriteBatch {
    this.ensureActive();
    ensureOk(this.native.batchDelete(this.handle, docRef._collection, docRef._docId), this.native, 'batchDelete');
    return this;
  }

  async commit(): Promise<void> {
    this.ensureActive();
    ensureOk(this.native.batchCommit(this.client.engineHandle(), this.handle), this.native, 'batchCommit');
    this.native.batchFree(this.handle);
    this.committed = true;
  }

  dispose(): void {
    if (this.committed) return;
    this.native.batchFree(this.handle);
    this.committed = true;
  }

  private ensureActive(): void {
    if (this.committed) throw new Error('WriteBatch is already committed/disposed');
  }
}
