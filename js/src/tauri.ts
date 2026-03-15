import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';

export type FireLitePrimitive = string | number | boolean | null | Uint8Array;
export type FireLiteRecord = Record<string, FireLitePrimitive>;

export type FilterOperator = 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte';

type FireLiteOp =
  | { op: 'get'; collection: string; docId: string }
  | { op: 'set'; collection: string; docId: string; data: FireLiteRecord }
  | { op: 'delete'; collection: string; docId: string }
  | {
      op: 'query';
      collection: string;
      filters: FilterInput[];
      orderBy?: OrderByInput;
      limit?: number;
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
      projection?: string[];
      eventName: string;
    }
  | { op: 'unsubscribe'; listenerId: string };

type FireLiteResponse =
  | { ok: null }
  | { document: { data: FireLiteRecord | null } }
  | { queryResult: { rows: FireLiteRecord[] } }
  | { subscriptionAck: { listenerId: string } }
  | { unsubscribed: { listenerId: string } };

interface FilterInput {
  field: string;
  op: FilterOperator;
  value: FireLitePrimitive;
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

function normalizeRecord(data: FireLiteRecord): Record<string, FireLitePrimitive | number[]> {
  const normalized: Record<string, FireLitePrimitive | number[]> = {};
  for (const [k, v] of Object.entries(data)) {
    normalized[k] = v instanceof Uint8Array ? Array.from(v) : v;
  }
  return normalized;
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
    await exec({ op: 'set', collection, docId, data: normalizeRecord(data) as FireLiteRecord });
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

  async query(
    collection: string,
    filters: FilterInput[],
    orderBy?: OrderByInput,
    limit?: number,
    projection?: string[]
  ): Promise<FireLiteRecord[]> {
    const normalizedFilters = filters.map((f) => ({
      ...f,
      value: f.value instanceof Uint8Array ? Array.from(f.value) : f.value
    }));

    const res = await exec({
      op: 'query',
      collection,
      filters: normalizedFilters,
      orderBy,
      limit,
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
      filters: params.filters,
      orderBy: params.orderBy,
      limit: params.limit,
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

  where(field: string, op: FilterOperator, value: FireLitePrimitive): TauriQuery {
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

  async get(): Promise<FireLiteRecord[]> {
    return new TauriFireLite().query(this._collection, []);
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
  private projectionDef?: string[];

  constructor(private readonly collection: string) {}

  where(field: string, op: FilterOperator, value: FireLitePrimitive): TauriQuery {
    this.filters.push({ field, op, value });
    return this;
  }

  orderBy(field: string, direction: 'asc' | 'desc' = 'asc'): TauriQuery {
    this.orderByDef = { field, ascending: direction === 'asc' };
    return this;
  }

  limit(limit: number): TauriQuery {
    this.limitDef = limit;
    return this;
  }

  select(...fields: string[]): TauriQuery {
    this.projectionDef = fields;
    return this;
  }

  async get(): Promise<FireLiteRecord[]> {
    return new TauriFireLite().query(this.collection, this.filters, this.orderByDef, this.limitDef, this.projectionDef);
  }

  async onSnapshot(callback: (rows: FireLiteRecord[]) => void): Promise<() => Promise<void>> {
    return new TauriFireLite().subscribe(
      {
        collection: this.collection,
        filters: this.filters,
        orderBy: this.orderByDef,
        limit: this.limitDef,
        projection: this.projectionDef,
        eventName: 'firelite://snapshot'
      },
      callback
    );
  }
}

export class TauriWriteBatch {
  private readonly mutations: BatchInput[] = [];

  set(docRef: TauriDocumentReference, data: FireLiteRecord): TauriWriteBatch {
    this.mutations.push({
      mutation: 'set',
      collection: docRef._collection,
      docId: docRef._docId,
      data: normalizeRecord(data) as FireLiteRecord
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
