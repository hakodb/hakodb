import { loadNativeBindings, type NativeBindings, type WatchCallback } from './native';

export type Primitive = string | number | boolean | null | Uint8Array;
export type FireLiteDocData = Record<string, Primitive>;

export enum DurabilityMode {
  Always = 0,
  Interval = 1,
  Manual = 2,
  OnCommit = 3,
}

export interface FireLiteClientOptions {
  libraryPath?: string;
  native?: NativeBindings;
  config?: FireLiteConfig; // New: support passing a config object
}

export interface QueryConstraint {
  field: string;
  op: '==';
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

  setStorageTuning(pageSize: number, compactionThreshold: number, groupCommitMaxOps: number): this {
    this._native.configSetStorageTuning(this._handle, pageSize, compactionThreshold, groupCommitMaxOps);
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
      // Open with advanced config builder
      engine = native.engineOpenWithConfig(path, options.config.getHandle());
    } else {
      // Default open
      engine = native.engineOpen(path);
    }

    if (!engine) {
      throw new Error(`Failed to open FireLite: ${native.lastError()}`);
    }
    return new FireLiteClient(native, engine);
  }

  /** Create a config builder instance */
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

  async runQuery(
    collection: string,
    filters: QueryConstraint[],
    order?: QueryOrder,
    queryLimit?: number,
    projection: string[] = []
  ): Promise<FireLiteDocData[]> {
    this.assertOpen();
    const query = this.native.queryNew(collection);
    if (!query) throw new Error(`queryNew failed: ${this.native.lastError()}`);

    try {
      for (const filter of filters) {
        if (typeof filter.value === 'string') {
          ensureOk(this.native.queryWhereEqStr(query, filter.field, filter.value), this.native, 'queryWhereEqStr');
        } else {
          ensureOk(this.native.queryWhereEqInt(query, filter.field, filter.value), this.native, 'queryWhereEqInt');
        }
      }
      if (order) ensureOk(this.native.queryOrderBy(query, order.field, order.ascending), this.native, 'queryOrderBy');
      if (queryLimit !== undefined) ensureOk(this.native.queryLimit(query, queryLimit), this.native, 'queryLimit');
      for (const field of projection) ensureOk(this.native.querySelectField(query, field), this.native, 'querySelectField');

      return parseQueryRows(this.native.queryExecute(this.engine, query));
    } finally {
      this.native.queryFree(query);
    }
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

  /** Reactive real-time listener */
  onSnapshot(callback: (snapshot: FireLiteDocData[]) => void): Unsubscribe {
    const native = this.client.nativeBindings();
    
    // Internal callback that re-runs the query when the collection changes
    const internalWatcher: WatchCallback = async () => {
      const data = await this.get();
      callback(data);
    };

    const watchHandle = native.engineWatch(this.client.engineHandle(), this.name, internalWatcher);
    
    // Initial data trigger
    this.get().then(callback);

    return async () => {
      native.watchFree(watchHandle);
    };
  }

  where(field: string, op: '==', value: string | number): Query {
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
    return this.client.runQuery(this.name, []);
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

  /** Reactive real-time listener for filtered query */
  onSnapshot(callback: (snapshot: FireLiteDocData[]) => void): Unsubscribe {
    const native = this.client.nativeBindings();
    
    const internalWatcher: WatchCallback = async () => {
      const data = await this.get();
      callback(data);
    };

    const watchHandle = native.engineWatch(this.client.engineHandle(), this.collection, internalWatcher);
    
    // Initial fetch
    this.get().then(callback);

    return async () => {
      native.watchFree(watchHandle);
    };
  }

  where(field: string, op: '==', value: string | number): Query {
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

  async get(): Promise<FireLiteDocData[]> {
    return this.client.runQuery(this.collection, this.filters, this.order, this.queryLimit, this.projection);
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