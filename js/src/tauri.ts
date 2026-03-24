import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';

// 1. UPDATED: Recursive Types to match client.ts
export type FireLitePrimitive = 
  | string 
  | number 
  | boolean 
  | null 
  | Uint8Array 
  | FireLiteRecord 
  | FireLitePrimitive[];

export type FireLiteRecord = { [key: string]: FireLitePrimitive };

// v0.5.9 operators exposed by the Tauri gateway
export type FilterOperator = 
  | 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' 
  | 'match' | 'contains' | 'startsWith' | 'in' | 'notIn' | 'arrayContains' | 'arrayContainsAny';

type FireLiteOp =
  | { op: 'get'; collection: string; docId: string }
  | { op: 'set'; collection: string; docId: string; data: FireLiteRecord }
  | { op: 'delete'; collection: string; docId: string }
  | { op: 'createIndex'; collection: string; field: string }      // NEW
  | { op: 'createFtsIndex'; collection: string; field: string }   // NEW
  | {
      op: 'query';
      collection: string;
      filters: FilterInput[];
      orderBy?: OrderByInput;
      limit?: number;
      offset?: number;
      projection?: string[];
    }
  | { op: 'batch'; mutations: BatchInput[] }
  | {
      op: 'subscribe';
      listenerId: string;
      collection: string;
      filters: FilterInput[];
      orderBy?: OrderByInput;
      limit?: number;
      offset?: number;
      projection?: string[];
      eventName: string;
    }
  | { op: 'unsubscribe'; listenerId: string }
  | { op: 'aggregate'; collection: string; filters: FilterInput[]; kind: 'count' | 'sum' | 'avg'; field?: string };

type FireLiteResponse =
  | { ok: null }
  | { document: { data: FireLiteRecord | null } }
  | { queryResult: { rows: FireLiteRecord[] } }
  | { aggregateResult: { value: number } }
  | { subscriptionAck: { listenerId: string } }
  | { unsubscribed: { listenerId: string } };

interface FilterInput {
  field: string;
  op: FilterOperator;
  value: any;
}

interface OrderByInput {
  field: string;
  ascending: boolean;
}

interface BatchInput {
  mutation: 'set' | 'delete';
  collection: string;
  docId: string;
  data?: FireLiteRecord;
}

interface SnapshotPayload {
  listenerId: string;
  rows: FireLiteRecord[];
}

let listenerCounter = 0;
function nextListenerId(): string {
  listenerCounter += 1;
  return `fl_listener_${Date.now()}_${listenerCounter}`;
}

/**
 * RECURSIVE NORMALIZATION: 
 * Converts Uint8Array to number[] so it can cross the Tauri JSON bridge
 */
function normalizeValue(v: FireLitePrimitive): any {
  if (v instanceof Uint8Array) return Array.from(v);
  if (Array.isArray(v)) return v.map(normalizeValue);
  if (typeof v === 'object' && v !== null) {
    const normalized: any = {};
    for (const [key, val] of Object.entries(v)) {
      normalized[key] = normalizeValue(val);
    }
    return normalized;
  }
  return v;
}

async function exec(op: FireLiteOp): Promise<FireLiteResponse> {
  return invoke<FireLiteResponse>('firelite_exec', { op });
}

export class TauriFireLite {
  collection(name: string): TauriCollectionReference {
    return new TauriCollectionReference(name);
  }

  batch(): TauriWriteBatch {
    return new TauriWriteBatch();
  }

  async set(collection: string, docId: string, data: FireLiteRecord): Promise<void> {
    await exec({ op: 'set', collection, docId, data: normalizeValue(data) });
  }

  async get(collection: string, docId: string): Promise<TauriDocumentSnapshot> {
    const res = await exec({ op: 'get', collection, docId });
    if ('document' in res) {
      return new TauriDocumentSnapshot(docId, !!res.document.data, res.document.data ?? undefined);
    }
    throw new Error('Unexpected response shape for get');
  }

  async delete(collection: string, docId: string): Promise<void> {
    await exec({ op: 'delete', collection, docId });
  }

  // INDEX MANAGEMENT
  async createIndex(collection: string, field: string): Promise<void> {
    await exec({ op: 'createIndex', collection, field });
  }

  async createFtsIndex(collection: string, field: string): Promise<void> {
    await exec({ op: 'createFtsIndex', collection, field });
  }

  async query(
    collection: string,
    filters: FilterInput[],
    orderBy?: OrderByInput,
    limit?: number,
    offset?: number,
    projection?: string[]
  ): Promise<FireLiteRecord[]> {
    const normalizedFilters = filters.map((f) => ({
      ...f,
      value: normalizeValue(f.value)
    }));

    const res = await exec({
      op: 'query',
      collection,
      filters: normalizedFilters,
      orderBy,
      limit,
      offset,
      projection
    });

    if ('queryResult' in res) {
      return res.queryResult.rows;
    }
    throw new Error('Unexpected response shape for query');
  }

  async subscribe(
    params: {
      collection: string;
      filters: FilterInput[];
      orderBy?: OrderByInput;
      limit?: number;
      offset?: number;
      projection?: string[];
      eventName: string;
    },
    callback: (rows: FireLiteRecord[]) => void
  ): Promise<() => Promise<void>> {
    const listenerId = nextListenerId();

    const unlisten: UnlistenFn = await listen<SnapshotPayload>(params.eventName, (event) => {
      if (event.payload.listenerId !== listenerId) return;
      callback(event.payload.rows);
    });

    await exec({
      op: 'subscribe',
      listenerId,
      collection: params.collection,
      filters: params.filters.map(f => ({ ...f, value: normalizeValue(f.value) })),
      orderBy: params.orderBy,
      limit: params.limit,
      offset: params.offset,
      projection: params.projection,
      eventName: params.eventName
    });

    return async () => {
      unlisten();
      await exec({ op: 'unsubscribe', listenerId });
    };
  }
}

export class TauriCollectionReference {
  constructor(private readonly _collection: string) {}

  doc(id: string): TauriDocumentReference {
    return new TauriDocumentReference(this._collection, id);
  }

  where(field: string, op: FilterOperator, value: any): TauriQuery {
    return new TauriQuery(this._collection).where(field, op, value);
  }

  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): TauriQuery {
    return new TauriQuery(this._collection).orderBy(field, direction);
  }

  limit(limit: number): TauriQuery {
    return new TauriQuery(this._collection).limit(limit);
  }

  select(...fields: string[]): TauriQuery {
    return new TauriQuery(this._collection).select(...fields);
  }

  async createIndex(field: string): Promise<void> {
    await new TauriFireLite().createIndex(this._collection, field);
  }

  async createFtsIndex(field: string): Promise<void> {
    await new TauriFireLite().createFtsIndex(this._collection, field);
  }

  async get(): Promise<FireLiteRecord[]> {
    return new TauriQuery(this._collection).get();
  }
}

export class TauriDocumentReference {
  constructor(readonly _collection: string, readonly _docId: string) {}

  async set(data: FireLiteRecord): Promise<void> {
    await new TauriFireLite().set(this._collection, this._docId, data);
  }

  async get(): Promise<TauriDocumentSnapshot> {
    return new TauriFireLite().get(this._collection, this._docId);
  }

  async delete(): Promise<void> {
    await new TauriFireLite().delete(this._collection, this._docId);
  }
}

export class TauriDocumentSnapshot {
  constructor(
    readonly id: string,
    readonly exists: boolean,
    private readonly _data?: FireLiteRecord
  ) {}

  data(): FireLiteRecord | undefined {
    return this._data;
  }
}

export class TauriQuery {
  private readonly filters: FilterInput[] = [];
  private orderByDef?: OrderByInput;
  private limitDef?: number;
  private offsetDef?: number;
  private projectionDef?: string[];

  constructor(private readonly collection: string) {}

  where(field: string, op: FilterOperator, value: any): TauriQuery {
    this.filters.push({ field, op, value });
    return this;
  }

  startAfter(snapshot: TauriDocumentSnapshot): TauriQuery {
    void snapshot;
    throw new Error('startAfter is not currently supported by the JSON-based Tauri gateway transport');
  }

  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): TauriQuery {
    this.orderByDef = { field, ascending: direction === 'asc' };
    return this;
  }

  limit(limit: number): TauriQuery {
    this.limitDef = limit;
    return this;
  }

  offset(offset: number): TauriQuery {
    this.offsetDef = offset;
    return this;
  }

  select(...fields: string[]): TauriQuery {
    this.projectionDef = fields;
    return this;
  }

  async get(): Promise<FireLiteRecord[]> {
    return new TauriFireLite().query(
        this.collection, 
        this.filters, 
        this.orderByDef, 
        this.limitDef, 
        this.offsetDef,
        this.projectionDef
    );
  }

  async onSnapshot(callback: (rows: FireLiteRecord[]) => void): Promise<() => Promise<void>> {
    return new TauriFireLite().subscribe(
      {
        collection: this.collection,
        filters: this.filters,
        orderBy: this.orderByDef,
        limit: this.limitDef,
        offset: this.offsetDef,
        projection: this.projectionDef,
        eventName: 'firelite://snapshot'
      },
      callback
    );
  }

  async count(): Promise<number> {
    const res = await exec({ op: 'aggregate', collection: this.collection, filters: this.filters, kind: 'count' });
    return 'aggregateResult' in res ? res.aggregateResult.value : 0;
  }

  async sum(field: string): Promise<number> {
    const res = await exec({ op: 'aggregate', collection: this.collection, filters: this.filters, kind: 'sum', field });
    return 'aggregateResult' in res ? res.aggregateResult.value : 0;
  }

  async avg(field: string): Promise<number> {
    const res = await exec({ op: 'aggregate', collection: this.collection, filters: this.filters, kind: 'avg', field });
    return 'aggregateResult' in res ? res.aggregateResult.value : 0;
  }

}

export class TauriWriteBatch {
  private readonly mutations: BatchInput[] = [];

  set(docRef: TauriDocumentReference, data: FireLiteRecord): TauriWriteBatch {
    this.mutations.push({
      mutation: 'set',
      collection: docRef._collection,
      docId: docRef._docId,
      data: normalizeValue(data)
    });
    return this;
  }

  delete(docRef: TauriDocumentReference): TauriWriteBatch {
    this.mutations.push({
      mutation: 'delete',
      collection: docRef._collection,
      docId: docRef._docId
    });
    return this;
  }

  async commit(): Promise<void> {
    await exec({ op: 'batch', mutations: this.mutations });
  }
}
