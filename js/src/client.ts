import { loadNativeBindings, type NativeBindings, type WatchCallback } from './native';

export type Primitive = string | number | boolean | null | Uint8Array | Date;
export type FireLiteDocData = Record<string, Primitive>;

// Sentinel value for server-side timestamps
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

export interface QueryConstraint {
  field: string;
  op: '==' | 'match' | 'contains' | 'startsWith';
  value: string | number;
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

function insertField(native: NativeBindings, handle: unknown, key: string, value: Primitive): void {
  // Handle Server Timestamp Sentinel
  if (value === SERVER_TIMESTAMP_SENTINEL) {
    ensureOk(native.docInsertServerTimestamp(handle, key), native, `docInsertServerTimestamp(${key})`);
    return;
  }

  if (value instanceof Date) {
    const micros = BigInt(value.getTime()) * 1000n;
    ensureOk(native.docInsertTimestamp(handle, key, micros), native, `docInsertTimestamp(${key})`);
    return;
  }

  if (typeof value === 'string') {
    ensureOk(native.docInsertStr(handle, key, value), native, `docInsertStr(${key})`);
  } else if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      ensureOk(native.docInsertInt(handle, key, value), native, `docInsertInt(${key})`);
    } else {
      ensureOk(native.docInsertFloat(handle, key, value), native, `docInsertFloat(${key})`);
    }
  } else if (typeof value === 'boolean') {
    ensureOk(native.docInsertBool(handle, key, value), native, `docInsertBool(${key})`);
  } else if (value === null) {
    ensureOk(native.docInsertNull(handle, key), native, `docInsertNull(${key})`);
  } else if (value instanceof Uint8Array) {
    ensureOk(native.docInsertBin(handle, key, value), native, `docInsertBin(${key})`);
  } else {
    throw new Error(`Unsupported field type for '${key}'.`);
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
    private readonly payload?: FireLiteDocData
  ) {}

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

  /** Static sentinel for server-generated timestamps */
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
    try {
      const json = this.native.docToJson(doc);
      return new DocumentSnapshot(docId, true, parseDocJson(json));
    } finally {
      this.native.docFree(doc);
    }
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
  constructor(private readonly client: FireLiteClient, private readonly name: string) {}

  doc(id: string): DocumentReference {
    return new DocumentReference(this.client, this.name, id);
  }

  onSnapshot(callback: (snapshot: FireLiteDocData[]) => void): Unsubscribe {
    const query = new Query(this.client, this.name);
    return query.onSnapshot(callback);
  }

  where(field: string, op: '==' | 'match' | 'contains' | 'startsWith', value: string | number): Query {
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
}

export class DocumentReference {
  constructor(
    private readonly client: FireLiteClient, 
    readonly _collection: string, 
    readonly _id: string
  ) {}

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
  private readonly filters: QueryConstraint[] = [];
  private order?: QueryOrder;
  private queryLimit?: number;
  private projection: string[] = [];

  constructor(private readonly client: FireLiteClient, private readonly collection: string) {}

  where(field: string, op: '==' | 'match' | 'contains' | 'startsWith', value: string | number): Query {
    this.filters.push({ field, op, value });
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
        }
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
      ensureOk(this.native.batchSet(this.handle, docRef._collection, docRef._id, doc), this.native, 'batchSet');
      return this;
    } finally {
      this.native.docFree(doc);
    }
  }

  delete(docRef: DocumentReference): WriteBatch {
    this.ensureActive();
    ensureOk(this.native.batchDelete(this.handle, docRef._collection, docRef._id), this.native, 'batchDelete');
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