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
  | FireLiteReference
  | { [key: string]: Primitive }
  | Primitive[];


export type FireLiteDocData = { [key: string]: Primitive };

const SERVER_TIMESTAMP_SENTINEL = "__FL_SERVER_TIMESTAMP__";

export class FireLiteReference {
  constructor(public readonly collection: string, public readonly id: string) {}
}

export enum DurabilityMode {
  Always = 0,
  Interval = 1,
  Manual = 2,
  OnCommit = 3,
}

export enum CloudSyncMode {
  Server = 0,
  Client = 1,
}

export interface FireLiteClientOptions {
  libraryPath?: string;
  native?: NativeBindings;
  config?: FireLiteConfig;
}

// 2. FIXED: Added 'in' to the interface to match the Query class
export interface QueryConstraint {
  field: string;
  op: '==' | '!=' | '>' | '>=' | '<' | '<=' | 'match' | 'matchPrefix' | 'contains' | 'startsWith' | 'in' | 'not-in' | 'array-contains' | 'array-contains-any';
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

  setStorageTuning(pageSize: number, threshold: number, groupCommitMaxOps: number): this {
    this._native.configSetStorageTuning(this._handle, pageSize, threshold, groupCommitMaxOps);
    return this;
  }

  setEncryptedCollections(collections: string[]): this {
    ensureOk(this._native.configSetEncryptedCollections(this._handle, JSON.stringify(collections)), this._native, 'setEncryptedCollections');
    return this;
  }

  setBlobThreshold(thresholdBytes: number): this {
    this._native.configSetBlobThreshold(this._handle, thresholdBytes);
    return this;
  }

  setCompression(enabled: boolean, level: number = 3): this {
    this._native.configSetCompression(this._handle, enabled, level);
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
 * Recursive field inserter (v0.5.9)
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
  if (value instanceof FireLiteReference) {
    ensureOk(native.docInsertReference(handle, key, value.collection, value.id), native, 'insertReference');
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

  createNetSyncer(name: string, roomKey: string): NetSyncer {
    this.assertOpen();
    const handle = this.native.netSyncerNew(this.engine, name, roomKey);
    if (!handle) {
      throw new Error(`netSyncerNew failed: ${this.native.lastError()}`);
    }
    return new NetSyncer(this.native, handle);
  }

  createCloudSync(
    mode: CloudSyncMode,
    clientId: string | null = null,
    roomName: string | null = null,
    roomKey: string | null = null,
    authToken: string | null = null
  ): CloudSync {
    this.assertOpen();
    const handle = this.native.cloudSyncNew(this.engine, mode, clientId, roomName, roomKey, authToken);
    if (!handle) {
      throw new Error(`cloudSyncNew failed: ${this.native.lastError()}`);
    }
    return new CloudSync(this.native, handle);
  }

  /**
   * Creates a room-agnostic cloud SERVER ("big cloud server storage"). It is
   * not bound to any room: clients choose their room (and this server) and the
   * server accepts and persists any (roomName, roomKey) pair, storing each
   * room's collections under its own storage prefix and relaying sync only to
   * the members of that room.
   */
  createCloudSyncServer(serverId: string | null = null, authToken: string | null = null): CloudSync {
    this.assertOpen();
    const handle = this.native.cloudSyncServerNew(this.engine, serverId, authToken);
    if (!handle) {
      throw new Error(`cloudSyncServerNew failed: ${this.native.lastError()}`);
    }
    return new CloudSync(this.native, handle);
  }

  /**
   * Creates an offline-first cloud CLIENT bound to a room of the caller's
   * choosing. The client picks the room (roomName + roomKey) and later picks
   * the server via `CloudSync.start()`.
   */
  createCloudSyncClient(
    clientId: string | null = null,
    roomName: string | null = null,
    roomKey: string | null = null,
    authToken: string | null = null
  ): CloudSync {
    this.assertOpen();
    const handle = this.native.cloudSyncClientNew(this.engine, clientId, roomName, roomKey, authToken);
    if (!handle) {
      throw new Error(`cloudSyncClientNew failed: ${this.native.lastError()}`);
    }
    return new CloudSync(this.native, handle);
  }

  isIndexesReady(): boolean {
    this.assertOpen();
    return this.native.engineIsIndexesReady(this.engine);
  }

  async compact(): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineCompact(this.engine), this.native, 'engineCompact');
  }

  async getStats(): Promise<Record<string, unknown>> {
    this.assertOpen();
    const raw = this.native.engineGetStats(this.engine);
    return raw ? JSON.parse(raw) : {};
  }

  async getAuditLog(): Promise<unknown[]> {
    this.assertOpen();
    const raw = this.native.engineGetAuditLog(this.engine);
    return raw ? JSON.parse(raw) : [];
  }

  async snapshotIndices(): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineSnapshotIndices(this.engine), this.native, 'engineSnapshotIndices');
  }

  async listIndexes(collection: string | null = null): Promise<unknown> {
    this.assertOpen();
    const raw = this.native.engineListIndexes(this.engine, collection);
    return raw ? JSON.parse(raw) : null;
  }

  async patch(collection: string, docId: string, data: FireLiteDocData): Promise<void> {
    this.assertOpen();
    const updates = toNativeDoc(this.native, data);
    try {
      ensureOk(this.native.enginePatch(this.engine, collection, docId, updates), this.native, 'enginePatch');
    } finally {
      this.native.docFree(updates);
    }
  }

  async insertSubDoc(col: string, id: string, subCol: string, subId: string, data: FireLiteDocData): Promise<void> {
    this.assertOpen();
    const doc = toNativeDoc(this.native, data);
    try {
      ensureOk(this.native.engineInsertSubDoc(this.engine, col, id, subCol, subId, doc), this.native, 'engineInsertSubDoc');
    } finally {
      this.native.docFree(doc);
    }
  }

  async getByReference(doc: DocumentSnapshot, fieldKey: string): Promise<DocumentSnapshot | null> {
    this.assertOpen();
    if (!doc._nativeHandle) return null;
    const target = this.native.engineGetByRef(this.engine, doc._nativeHandle, fieldKey);
    if (!target) return null;
    const json = this.native.docToJson(target);
    this.native.docFree(target);
    return new DocumentSnapshot(fieldKey, true, parseDocJson(json));
  }

  async createCompositeIndex(collection: string, fields: { field: string; desc?: boolean }[]): Promise<number> {
    this.assertOpen();
    const id = this.native.engineCreateIndex(this.engine, collection, JSON.stringify(fields.map(f => ({ field: f.field, desc: !!f.desc }))));
    if (id === 0) throw new Error(`engineCreateIndex failed: ${this.native.lastError()}`);
    return id;
  }

  async runTransaction<T>(fn: (tx: Transaction) => Promise<T>): Promise<T> {
    this.assertOpen();
    const tx = new Transaction(this.native, this.engine);
    try {
      const result = await fn(tx);
      ensureOk(this.native.transactionCommit(this.engine, tx.handle), this.native, 'transactionCommit');
      return result;
    } finally {
      tx.dispose();
    }
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

  /** Local-only delete: never leaves this device (no sync tailer or handshake transmits it). */
  async deleteLocal(collection: string, docId: string): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineDeleteLocal(this.engine, collection, docId), this.native, 'engineDeleteLocal');
  }

  /** Mark a collection local-only (never syncs) or rejoin it with `local=false`. */
  async setCollectionLocal(collection: string, local: boolean): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineSetCollectionLocal(this.engine, collection, local ? 1 : 0), this.native, 'engineSetCollectionLocal');
  }

  /** Opt a key back into replication (future ops only). */
  async replicateKey(collection: string, docId: string): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineReplicateKey(this.engine, collection, docId), this.native, 'engineReplicateKey');
  }

  /** Opt a whole collection back into replication. */
  async replicateCollection(collection: string): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineReplicateCollection(this.engine, collection), this.native, 'engineReplicateCollection');
  }

  /** Vacuum: purge tombstones (never replicates); next handshake pulls peer state. */
  async vacuumCollection(collection: string): Promise<number> {
    this.assertOpen();
    const n = this.native.engineVacuumCollection(this.engine, collection);
    if (n < 0) throw new Error(`engineVacuumCollection failed: ${this.native.lastError()}`);
    return n;
  }

  nativeBindings(): NativeBindings { return this.native; }
  engineHandle(): unknown { return this.engine; }

  private assertOpen(): void {
    if (this.isClosed) throw new Error('FireLiteClient is already closed');
  }
}

export class NetSyncer {
  private closed = false;

  constructor(private readonly native: NativeBindings, private readonly handle: unknown) {}

  async start(port: number): Promise<void> {
    ensureOk(this.native.netSyncerStart(this.handle, port), this.native, 'netSyncerStart');
  }

  async status<T = unknown>(): Promise<T | null> {
    const raw = this.native.netSyncerStatus(this.handle);
    return raw ? (JSON.parse(raw) as T) : null;
  }

  async close(): Promise<void> {
    if (this.closed) return;
    this.native.netSyncerFree(this.handle);
    this.closed = true;
  }
}

export class CloudSync {
  private closed = false;

  constructor(private readonly native: NativeBindings, private readonly handle: unknown) {}

  async start(address: string): Promise<void> {
    ensureOk(this.native.cloudSyncStart(this.handle, address), this.native, 'cloudSyncStart');
  }

  async status<T = unknown>(): Promise<T | null> {
    const raw = this.native.cloudSyncStatus(this.handle);
    return raw ? (JSON.parse(raw) as T) : null;
  }

  stop(): void {
    if (this.closed) return;
    this.native.cloudSyncStop(this.handle);
  }

  close(): void {
    this.stop();
    this.native.cloudSyncFree(this.handle);
    this.closed = true;
  }
}

export class Transaction {
  private disposed = false;

  constructor(
    private readonly native: NativeBindings,
    private readonly engine: unknown,
    readonly handle: unknown = native.transactionBegin(engine)
  ) {
    if (!handle) throw new Error(`transactionBegin failed: ${native.lastError()}`);
  }

  async get(collection: string, docId: string): Promise<DocumentSnapshot> {
    this.ensureActive();
    const doc = this.native.transactionGet(this.engine, this.handle, collection, docId);
    if (!doc) return new DocumentSnapshot(docId, false);
    const json = this.native.docToJson(doc);
    this.native.docFree(doc);
    return new DocumentSnapshot(docId, true, parseDocJson(json));
  }

  set(collection: string, docId: string, data: FireLiteDocData): this {
    this.ensureActive();
    const doc = toNativeDoc(this.native, data);
    try {
      ensureOk(this.native.transactionSet(this.handle, collection, docId, doc), this.native, 'transactionSet');
    } finally {
      this.native.docFree(doc);
    }
    return this;
  }

  dispose(): void {
    if (this.disposed) return;
    this.native.transactionFree(this.handle);
    this.disposed = true;
  }

  private ensureActive(): void {
    if (this.disposed) throw new Error('Transaction has already been committed/disposed');
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
  where(field: string, op: '==' | '!=' | '>' | '>=' | '<' | '<=' | 'match' | 'matchPrefix' | 'contains' | 'startsWith' | 'in' | 'not-in' | 'array-contains' | 'array-contains-any', value: any): Query {
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

  async createCompositeIndex(fields: { field: string; desc?: boolean }[]): Promise<number> {
    return this.client.createCompositeIndex(this.name, fields);
  }
}

export class DocumentReference {
  constructor(private readonly client: FireLiteClient, readonly _collection: string, readonly _id: string) { }

  async set(data: FireLiteDocData): Promise<void> {
    await this.client.set(this._collection, this._id, data);
  }

  async update(data: FireLiteDocData): Promise<void> {
    await this.client.patch(this._collection, this._id, data);
  }

  async get(): Promise<DocumentSnapshot> {
    return this.client.get(this._collection, this._id);
  }

  async delete(): Promise<void> {
    await this.client.delete(this._collection, this._id);
  }

  async getByReference(fieldKey: string): Promise<DocumentSnapshot | null> {
    const snap = await this.get();
    if (!snap.exists) return null;
    return this.client.getByReference(snap, fieldKey);
  }
}

export class Query {
  private _startAfterSnapshot?: DocumentSnapshot;
  private _startAtSnapshot?: DocumentSnapshot;
  private _endAtSnapshot?: DocumentSnapshot;
  private _endBeforeSnapshot?: DocumentSnapshot;
  private readonly filters: QueryConstraint[] = [];
  private readonly orFilters: QueryConstraint[] = [];
  private order?: QueryOrder;
private queryLimit?: number;
private queryOffset?: number;
private projection: string[] = [];
private deferBlobsDef = false;

  constructor(private readonly client: FireLiteClient, private readonly collection: string) { }

  where(field: string, op: '==' | '!=' | '>' | '>=' | '<' | '<=' | 'match' | 'matchPrefix' | 'contains' | 'startsWith' | 'in' | 'not-in' | 'array-contains' | 'array-contains-any', value: any): Query {
    this.filters.push({ field, op, value });
    return this;
  }

  orWhere(field: string, op: '==' | '!=' | '>' | '>=' | '<' | '<=' | 'match' | 'matchPrefix' | 'contains' | 'startsWith', value: any): Query {
    this.orFilters.push({ field, op, value });
    return this;
  }

  startAfter(snapshot: DocumentSnapshot): Query {
    this._startAfterSnapshot = snapshot;
    return this;
  }

  startAt(snapshot: DocumentSnapshot): Query {
    this._startAtSnapshot = snapshot;
    return this;
  }

  endAt(snapshot: DocumentSnapshot): Query {
    this._endAtSnapshot = snapshot;
    return this;
  }

  endBefore(snapshot: DocumentSnapshot): Query {
    this._endBeforeSnapshot = snapshot;
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

  offset(skip: number): Query {
    this.queryOffset = skip;
    return this;
  }

select(...fields: string[]): Query {
this.projection = fields;
return this;
}

deferBlobs(defer = true): Query {
this.deferBlobsDef = defer;
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
            } else if (typeof filter.value === 'boolean') {
              ensureOk(native.queryWhereEqBool(handle, filter.field, filter.value), native, 'queryWhereEqBool');
            } else {
              ensureOk(native.queryWhereEqInt(handle, filter.field, filter.value), native, 'queryWhereEqInt');
            }
            break;
          case '!=':
          case '>':
          case '>=':
          case '<':
          case '<=':
            if (typeof filter.value === 'string') {
              if (filter.op === '!=') ensureOk(native.queryWhereNeStr(handle, filter.field, filter.value), native, 'queryWhereNeStr');
              else if (filter.op === '>') ensureOk(native.queryWhereGtStr(handle, filter.field, filter.value), native, 'queryWhereGtStr');
              else if (filter.op === '>=') ensureOk(native.queryWhereGteStr(handle, filter.field, filter.value), native, 'queryWhereGteStr');
              else if (filter.op === '<') ensureOk(native.queryWhereLtStr(handle, filter.field, filter.value), native, 'queryWhereLtStr');
              else ensureOk(native.queryWhereLteStr(handle, filter.field, filter.value), native, 'queryWhereLteStr');
            } else {
              if (filter.op === '!=') ensureOk(native.queryWhereNeInt(handle, filter.field, filter.value), native, 'queryWhereNeInt');
              else if (filter.op === '>') ensureOk(native.queryWhereGtInt(handle, filter.field, filter.value), native, 'queryWhereGtInt');
              else if (filter.op === '>=') ensureOk(native.queryWhereGteInt(handle, filter.field, filter.value), native, 'queryWhereGteInt');
              else if (filter.op === '<') ensureOk(native.queryWhereLtInt(handle, filter.field, filter.value), native, 'queryWhereLtInt');
              else ensureOk(native.queryWhereLteInt(handle, filter.field, filter.value), native, 'queryWhereLteInt');
            }
            break;
          case 'match':
            ensureOk(native.queryWhereMatch(handle, filter.field, String(filter.value)), native, 'queryWhereMatch');
            break;
          case 'matchPrefix':
            ensureOk(native.queryWhereMatchPrefix(handle, filter.field, String(filter.value)), native, 'queryWhereMatchPrefix');
            break;
          case 'contains':
            ensureOk(native.queryWhereContains(handle, filter.field, String(filter.value)), native, 'queryWhereContains');
            break;
          case 'startsWith':
            ensureOk(native.queryWhereStartsWith(handle, filter.field, String(filter.value)), native, 'queryWhereStartsWith');
            break;
          case 'in':
          case 'not-in':
          case 'array-contains-any':
            const arr = native.arrayNew();
            (filter.value as any[]).forEach(v => {
              if (typeof v === 'string') native.arrayAppendStr(arr, v);
              else native.arrayAppendInt(arr, v);
            });
            if (filter.op === 'in') ensureOk(native.queryWhereIn(handle, filter.field, arr), native, 'queryWhereIn');
            else if (filter.op === 'not-in') ensureOk(native.queryWhereNotIn(handle, filter.field, arr), native, 'queryWhereNotIn');
            else ensureOk(native.queryWhereArrayContainsAny(handle, filter.field, arr), native, 'queryWhereArrayContainsAny');
            break;
          case 'array-contains':
            if (typeof filter.value === 'string') ensureOk(native.queryWhereArrayContainsStr(handle, filter.field, filter.value), native, 'queryWhereArrayContainsStr');
            else ensureOk(native.queryWhereArrayContainsInt(handle, filter.field, filter.value), native, 'queryWhereArrayContainsInt');
            break;
        }
      }

      for (const filter of this.orFilters) {
        if (typeof filter.value === 'string') {
          ensureOk(native.queryWhereOrStr(handle, filter.field, filter.value), native, 'queryWhereOrStr');
        } else {
          ensureOk(native.queryWhereOrInt(handle, filter.field, filter.value), native, 'queryWhereOrInt');
        }
      }

      if (this._startAtSnapshot?._nativeHandle) {
        ensureOk(native.queryStartAt(handle, this._startAtSnapshot._nativeHandle), native, 'queryStartAt');
      }
      if (this._startAfterSnapshot?._nativeHandle) {
        ensureOk(native.queryStartAfter(handle, this._startAfterSnapshot._nativeHandle), native, 'queryStartAfter');
      }
      if (this._endAtSnapshot?._nativeHandle) {
        ensureOk(native.queryEndAt(handle, this._endAtSnapshot._nativeHandle), native, 'queryEndAt');
      }
      if (this._endBeforeSnapshot?._nativeHandle) {
        ensureOk(native.queryEndBefore(handle, this._endBeforeSnapshot._nativeHandle), native, 'queryEndBefore');
      }

      if (this.order) ensureOk(native.queryOrderBy(handle, this.order.field, this.order.ascending), native, 'queryOrderBy');
      if (this.queryLimit !== undefined) ensureOk(native.queryLimit(handle, this.queryLimit), native, 'queryLimit');
      if (this.queryOffset !== undefined) ensureOk(native.queryOffset(handle, this.queryOffset), native, 'queryOffset');
      for (const field of this.projection) ensureOk(native.querySelectField(handle, field), native, 'querySelectField');
      if (this.deferBlobsDef) ensureOk(native.queryDeferBlobs(handle, true), native, 'queryDeferBlobs');

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

  async delete(): Promise<number> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    try {
      const n = native.queryDelete(this.client.engineHandle(), handle);
      if (n < 0) throw new Error(`queryDelete failed: ${native.lastError()}`);
      return n;
    } finally {
      native.queryFree(handle);
    }
  }

  /** Local-only mass delete: matched docs never leave this device. */
  async deleteLocal(): Promise<number> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    try {
      const n = native.queryDeleteLocal(this.client.engineHandle(), handle);
      if (n < 0) throw new Error(`queryDeleteLocal failed: ${native.lastError()}`);
      return n;
    } finally {
      native.queryFree(handle);
    }
  }

  async patch(data: FireLiteDocData): Promise<number> {
    const native = this.client.nativeBindings();
    const handle = this.prepareNativeQuery();
    const patchDoc = toNativeDoc(native, data);
    try {
      const n = native.queryPatch(this.client.engineHandle(), handle, patchDoc);
      if (n < 0) throw new Error(`queryPatch failed: ${native.lastError()}`);
      return n;
    } finally {
      native.docFree(patchDoc);
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
