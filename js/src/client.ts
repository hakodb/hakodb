import { loadNativeBindings, type NativeBindings } from './native';

export type Primitive = string | number | boolean | null | Uint8Array;
export type FireLiteDocData = Record<string, Primitive>;

export interface FireLiteClientOptions {
  libraryPath?: string;
  native?: NativeBindings;
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

function ensureOk(code: number, native: NativeBindings, ctx: string): void {
  if (code !== 0) {
    throw new Error(`${ctx}: ${native.lastError()}`);
  }
}

function parseDocJson(json: string | null): FireLiteDocData {
  if (!json) return {};
  const parsed = JSON.parse(json) as FireLiteDocData;
  return parsed;
}

function parseQueryRows(json: string | null): FireLiteDocData[] {
  if (!json) return [];
  const parsed = JSON.parse(json);
  return Array.isArray(parsed) ? (parsed as FireLiteDocData[]) : [];
}

function insertField(native: NativeBindings, handle: unknown, key: string, value: Primitive): void {
  if (typeof value === 'string') {
    ensureOk(native.docInsertStr(handle, key, value), native, `docInsertStr(${key})`);
    return;
  }
  if (typeof value === 'number') {
    if (Number.isInteger(value)) {
      ensureOk(native.docInsertInt(handle, key, value), native, `docInsertInt(${key})`);
    } else {
      ensureOk(native.docInsertFloat(handle, key, value), native, `docInsertFloat(${key})`);
    }
    return;
  }
  if (typeof value === 'boolean') {
    ensureOk(native.docInsertBool(handle, key, value), native, `docInsertBool(${key})`);
    return;
  }
  if (value === null) {
    ensureOk(native.docInsertNull(handle, key), native, `docInsertNull(${key})`);
    return;
  }
  if (value instanceof Uint8Array) {
    ensureOk(native.docInsertBin(handle, key, value), native, `docInsertBin(${key})`);
    return;
  }

  throw new Error(
    `Unsupported field type for '${key}'. Supported: string, number, boolean, null, Uint8Array.`
  );
}

function toNativeDoc(native: NativeBindings, data: FireLiteDocData): unknown {
  const doc = native.docNew();
  if (!doc) {
    throw new Error(`docNew failed: ${native.lastError()}`);
  }

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
  public readonly id: string;
  public readonly exists: boolean;
  private readonly payload?: FireLiteDocData;

  constructor(id: string, exists: boolean, payload?: FireLiteDocData) {
    this.id = id;
    this.exists = exists;
    this.payload = payload;
  }

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
    const engine = native.engineOpen(path);
    if (!engine) {
      throw new Error(`fl_engine_open failed: ${native.lastError()}`);
    }
    return new FireLiteClient(native, engine);
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
      ensureOk(this.native.engineInsert(this.engine, collection, docId, doc), this.native, 'fl_engine_insert');
    } finally {
      this.native.docFree(doc);
    }
  }

  async get(collection: string, docId: string): Promise<DocumentSnapshot> {
    this.assertOpen();
    const doc = this.native.engineGet(this.engine, collection, docId);
    if (!doc) {
      return new DocumentSnapshot(docId, false);
    }

    try {
      const json = this.native.docToJson(doc);
      return new DocumentSnapshot(docId, true, parseDocJson(json));
    } finally {
      this.native.docFree(doc);
    }
  }

  async delete(collection: string, docId: string): Promise<void> {
    this.assertOpen();
    ensureOk(this.native.engineDelete(this.engine, collection, docId), this.native, 'fl_engine_delete');
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
    if (!query) {
      throw new Error(`fl_query_new failed: ${this.native.lastError()}`);
    }

    try {
      for (const filter of filters) {
        if (filter.op !== '==') {
          throw new Error(`Only '==' filter is currently supported by C-FFI. Received '${filter.op}'.`);
        }
        if (typeof filter.value === 'string') {
          ensureOk(
            this.native.queryWhereEqStr(query, filter.field, filter.value),
            this.native,
            'fl_query_where_eq_str'
          );
        } else {
          ensureOk(
            this.native.queryWhereEqInt(query, filter.field, filter.value),
            this.native,
            'fl_query_where_eq_int'
          );
        }
      }

      if (order) {
        ensureOk(this.native.queryOrderBy(query, order.field, order.ascending), this.native, 'fl_query_order_by');
      }

      if (queryLimit !== undefined) {
        ensureOk(this.native.queryLimit(query, queryLimit), this.native, 'fl_query_limit');
      }

      for (const field of projection) {
        ensureOk(this.native.querySelectField(query, field), this.native, 'fl_query_select_field');
      }

      const json = this.native.queryExecute(this.engine, query);
      return parseQueryRows(json);
    } finally {
      this.native.queryFree(query);
    }
  }

  nativeBindings(): NativeBindings {
    return this.native;
  }

  engineHandle(): unknown {
    return this.engine;
  }

  private assertOpen(): void {
    if (this.isClosed) {
      throw new Error('FireLiteClient is already closed');
    }
  }
}

export class CollectionReference {
  private readonly client: FireLiteClient;
  private readonly name: string;

  constructor(client: FireLiteClient, name: string) {
    this.client = client;
    this.name = name;
  }

  doc(id: string): DocumentReference {
    return new DocumentReference(this.client, this.name, id);
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
  private readonly client: FireLiteClient;
  readonly _collection: string;
  readonly _id: string;

  constructor(client: FireLiteClient, collection: string, id: string) {
    this.client = client;
    this._collection = collection;
    this._id = id;
  }

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
  private readonly client: FireLiteClient;
  private readonly collection: string;
  private readonly filters: QueryConstraint[] = [];
  private order?: QueryOrder;
  private queryLimit?: number;
  private projection: string[] = [];

  constructor(client: FireLiteClient, collection: string) {
    this.client = client;
    this.collection = collection;
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
  private readonly client: FireLiteClient;
  private readonly native: NativeBindings;
  private readonly handle: unknown;
  private committed = false;

  constructor(client: FireLiteClient) {
    this.client = client;
    this.native = client.nativeBindings();
    this.handle = this.native.batchNew();
    if (!this.handle) {
      throw new Error(`fl_batch_new failed: ${this.native.lastError()}`);
    }
  }

  set(docRef: DocumentReference, data: FireLiteDocData): WriteBatch {
    this.ensureActive();
    const doc = toNativeDoc(this.native, data);
    try {
      ensureOk(
        this.native.batchSet(this.handle, docRef._collection, docRef._id, doc),
        this.native,
        'fl_batch_set'
      );
      return this;
    } finally {
      this.native.docFree(doc);
    }
  }

  delete(docRef: DocumentReference): WriteBatch {
    this.ensureActive();
    ensureOk(
      this.native.batchDelete(this.handle, docRef._collection, docRef._id),
      this.native,
      'fl_batch_delete'
    );
    return this;
  }

  async commit(): Promise<void> {
    this.ensureActive();
    ensureOk(this.native.batchCommit(this.client.engineHandle(), this.handle), this.native, 'fl_batch_commit');
    this.native.batchFree(this.handle);
    this.committed = true;
  }

  dispose(): void {
    if (this.committed) return;
    this.native.batchFree(this.handle);
    this.committed = true;
  }

  private ensureActive(): void {
    if (this.committed) {
      throw new Error('WriteBatch is already committed/disposed');
    }
  }
}
