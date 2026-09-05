// ponytail: Firestore-shaped FireLite shim (539 lines) is YAGNI with one impl; inline db.exec when second backend appears
import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';
import { decode } from "@msgpack/msgpack";

// --- Types & Interfaces ---
export type FireLitePrimitive = 
  | string | number | boolean | null | Uint8Array | Date
  | { [key: string]: FireLitePrimitive } 
  | FireLitePrimitive[];

export type FireLiteRecord = Record<string, any>;

export type FilterOperator = 
  | 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' 
  | 'match' | 'matchPrefix' | 'contains' | 'startsWith' | 'in' | 'notIn' 
  | 'arrayContains' | 'arrayContainsAny';

export interface SetOptions {
    merge?: boolean;
}

export type AggregateKind = 'count' | 'sum' | 'avg';

export interface AuditEntry {
    op: string;
    collection: string;
    doc_id?: string; // Corrected to snake_case for Rust
    ok: boolean;
}

// --- Add these interfaces for the Delta Protocol ---
interface DeltaChange {
    kind: 'full' | 'update' | 'delete';
    doc_id: string;
    data?: any;
}

interface DeltaPayload {
    listener_id: string;
    changes: DeltaChange[];
}

function symToOp(sym: string): string {
    switch (sym) {
        case '==': return 'eq';
        case '!=': return 'ne';
        case '>': return 'gt';
        case '>=': return 'gte';
        case '<': return 'lt';
        case '<=': return 'lte';
        // Rust FilterOperator deserializes with rename_all = "snake_case",
        // so camelCase ops used at the call site are mapped to snake_case.
        case 'matchPrefix': return 'match_prefix';
        case 'startsWith': return 'starts_with';
        case 'notIn': return 'not_in';
        case 'arrayContains': return 'array_contains';
        case 'arrayContainsAny': return 'array_contains_any';
        default: return sym;
    }
}

// --- Internal Utilities ---
function generateId() {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

function normalizeValue(v: any): any {
    if (v === null || typeof v !== 'object') return v; // Fast path for primitives
    
    if (v instanceof Uint8Array) {
        return v;
    }
    if (v instanceof Date) return v.getTime() * 1000;
    
    if (Array.isArray(v)) {
        return v.map(normalizeValue);
    }

    const out: any = {};
    for (const key in v) {
        out[key] = normalizeValue(v[key]);
    }
    return out;
}

async function exec(op: any): Promise<any> {
    // Note: The 'op' field inside the payload is the variant tag
    // The other fields must match the Rust struct fields (snake_case)
    const bytes = await invoke<number[]>('firelite_exec', { op });
    const res = decode(new Uint8Array(bytes)) as any;
    if (res?.error) throw new Error(res.error);
    return res;
}

// --- Core Classes ---
export class FireLite {}

export class DocumentReference {
    constructor(public readonly collectionPath: string, public readonly id: string) {}
    get path() { return `${this.collectionPath}/${this.id}`; }
}

export class CollectionReference {
    readonly type = 'collection' as const;
    constructor(public readonly path: string) {}
}

export class CollectionGroupReference {
    readonly type = 'collectionGroup' as const;
    constructor(public readonly id: string) {}
}

export class DocumentSnapshot {
    public readonly id: string; 
    public readonly _time: number;
    private _cachedData?: FireLiteRecord;

    constructor(
        id: string, 
        private readonly _exists: boolean, 
        private readonly _data?: FireLiteRecord,
        private readonly _ref?: DocumentReference
    ) {
        // Use the ID from data if the provided ID is null (common in queries)
        this.id = id;
        this._time = (_data as any)?._time || 0;
    }
    
    get ref() { return this._ref; }
    exists() { return this._exists; }

    data(): FireLiteRecord | undefined { 
        if (!this._data) return undefined;
        
        if (!this._cachedData) {
            this._cachedData = { ...this._data, id: this.id };
        }
        return this._cachedData;
    }

}

export interface DocumentChange {
    type: 'added' | 'modified' | 'removed';
    doc: DocumentSnapshot;
}

export class QuerySnapshot {
    constructor(public readonly docs: DocumentSnapshot[], private readonly _changes: DocumentChange[] = []) {}
    get empty() { return this.docs.length === 0; }
    get size() { return this.docs.length; }
    docChanges() { return this._changes; }
    forEach(callback: (doc: DocumentSnapshot) => void) { this.docs.forEach(callback); }
}

export type QueryConstraintType = 'where' | 'order_by' | 'limit' | 'offset' | 'select' | 'start_at' | 'start_after' | 'end_at' | 'end_before' | 'or' | 'defer_blobs';

export class QueryConstraint {
    constructor(public readonly type: QueryConstraintType, public readonly data: any) {}
}

export class Query {
    readonly type = 'query' as const;
    constructor(
        public readonly colRef: CollectionReference | CollectionGroupReference, 
        public readonly constraints: QueryConstraint[] = []
    ) {}
}

// --- API Implementation ---
export const getFirestore = () => new FireLite();

export const collection = (_db: any, path: string) => new CollectionReference(path);

export const doc = (_db: any, colOrPath: string | CollectionReference, id?: string) => {
    if (typeof colOrPath === 'string') {
        const segments = colOrPath.split('/').filter(Boolean);
        if (id) return new DocumentReference(colOrPath, id);

        const docId = segments.pop()!;
        const colPath = segments.join('/');
        return new DocumentReference(colPath, docId);
        // return new DocumentReference(segments[0], segments[1]);
    }
    return new DocumentReference(colOrPath.path, id!);
};

// --- Write Operations ---
export const addDoc = async (colRef: CollectionReference, data: FireLiteRecord) => {
    // Send an empty string as doc_id to trigger Rust-side generation
    const res = await exec({ 
        op: 'set', 
        collection: colRef.path, 
        doc_id: data.id||"", 
        data: normalizeValue(data) 
    });
    // The Rust backend now returns the generated ID in the response
    return new DocumentReference(colRef.path, res.id);
};

export const setDoc = async (ref: DocumentReference, data: FireLiteRecord, options?: SetOptions) => {
    const op = options?.merge ? 'patch' : 'set';
    await exec({ op, collection: ref.collectionPath, doc_id: ref.id, data: normalizeValue(data) });
};

export const updateDoc = async (ref: DocumentReference, data: Partial<FireLiteRecord>) => {
    await exec({ op: 'patch', collection: ref.collectionPath, doc_id: ref.id, data: normalizeValue(data) });
};

export const updateDocs = async (
    q: Query | CollectionReference | CollectionGroupReference, 
    data: Partial<FireLiteRecord>
) => {
    const params = buildQueryParams(q);
    const res = await exec({ 
        op: 'query', 
        action: { patch: { data: normalizeValue(data) } }, 
        ...params 
    });
    return res.bulk_action_result.count;
};

export const deleteDoc = async (ref: DocumentReference) => {
    await exec({ op: 'delete', collection: ref.collectionPath, doc_id: ref.id });
};

export const deleteDocs = async (
    q: Query | CollectionReference | CollectionGroupReference
) => {
    const params = buildQueryParams(q);
    const res = await exec({ 
        op: 'query', 
        action: 'delete', 
        ...params 
    });
    return res.bulk_action_result.count;
};

export const getDoc = async (ref: DocumentReference) => {
    const res = await exec({ op: 'get', collection: ref.collectionPath, doc_id: ref.id });
    const data = res.document?.data;
    return new DocumentSnapshot(ref.id, !!data, data ?? undefined, ref);
};

// --- Querying ---
export const query = (colRef: CollectionReference | CollectionGroupReference, ...constraints: QueryConstraint[]) => 
    new Query(colRef, constraints);

export const where = (field: string, op: FilterOperator, value: any) => new QueryConstraint('where', { field, op, value });
export const or = (...constraints: QueryConstraint[]) => new QueryConstraint('or', constraints);
export const orderBy = (field: string, direction: 'asc' | 'desc' = 'asc') => new QueryConstraint('order_by', { field, ascending: direction === 'asc' });
export const limit = (n: number) => new QueryConstraint('limit', n);
export const offset = (n: number) => new QueryConstraint('offset', n);
export const select = (...fields: string[]) => new QueryConstraint('select', fields);
export const startAt = (...values: any[]) => new QueryConstraint('start_at', values);
export const startAfter = (...values: any[]) => new QueryConstraint('start_after', values);
export const endAt = (...values: any[]) => new QueryConstraint('end_at', values);
export const endBefore = (...values: any[]) => new QueryConstraint('end_before', values);
// Blob-backed fields come back as __blob__ placeholders (no blob-file
// reads); resolve per doc with a getDoc. List views over image docs.
export const deferBlobs = () => new QueryConstraint('defer_blobs', true);

export const getDocs = async (q: Query | CollectionReference | CollectionGroupReference) => {
    const params = buildQueryParams(q);
    const res = await exec({ 
        op: 'query', 
        action: 'fetch', // Explicitly request data
        ...params 
    });
    const docs = res.query_result.rows.map((r: any) => new DocumentSnapshot(r.id || generateId(), true, r));
    return new QuerySnapshot(docs);
};

// --- Aggregations ---
export const getCountFromServer = async (q: Query | CollectionReference | CollectionGroupReference) => {
    const params = buildQueryParams(q);
    const res = await exec({ op: 'aggregate', kind: 'count', ...params });
    return { data: () => ({ count: res.aggregate_result.value }) };
};

export function onSnapshot(
    ref: DocumentReference,
    onNext: (snapshot: DocumentSnapshot) => void,
    onError?: (error: any) => void
): () => void;

export function onSnapshot(
    ref: Query | CollectionReference | CollectionGroupReference,
    onNext: (snapshot: QuerySnapshot) => void,
    onError?: (error: any) => void
): () => void;

export function onSnapshot(
    q: Query | CollectionReference | CollectionGroupReference | DocumentReference, 
    onNext: (snapshot: any) => void,
    onError?: (error: any) => void
) {
    const isDoc = q instanceof DocumentReference;
    const listener_id = `fl_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const event_name = `firelite://snapshot/${listener_id}`;
    const params = buildQueryParams(q);

    // 1. Local state (Raw values)
    const localCache = new Map<string, any>();
    let unlisten: UnlistenFn;

    const start = async () => {
        try {
            unlisten = await listen<Uint8Array>(event_name, (event) => {

                // const payload = decode(event.payload) as DeltaPayload;
                const payload = decode(new Uint8Array(event.payload)) as DeltaPayload;
                const { changes } = payload;
                
                // const { changes } = event.payload;
                let hasChanged = false;

                changes.forEach(change => {
                    const { kind, doc_id, data } = change;
                    const existing = localCache.get(doc_id);
                    
                    const oldTime = existing?._time || 0;
                    const newTime = data?._time || 0;

                    if (kind === 'full') {
                        localCache.clear();
                        (data as any[]).forEach(r => localCache.set(r.id.toString(), r));
                        hasChanged = true;
                    } 
                    else if (kind === 'update') {
                        // LWW when the delta carries a timestamp; deltas without
                        // one are always accepted so the UI can never be left
                        // stranded on a stale row.
                        if (newTime === 0 || newTime >= oldTime) {
                            localCache.set(doc_id, data);
                            hasChanged = true;
                        }
                    } 
                    else if (kind === 'delete') {
                        // Same rule as updates: never strand stale rows.
                        if (!existing || newTime === 0 || newTime >= oldTime) {
                            localCache.delete(doc_id);
                            hasChanged = true;
                        }
                    }
                });

                if (!hasChanged) return;

                // 2. TERMINAL ACTION: Emit to UI
                if (isDoc) {
                    const docData = localCache.get((q as DocumentReference).id);
                    onNext(new DocumentSnapshot((q as DocumentReference).id, !!docData, docData, q as DocumentReference));
                } else {
                    // Optimized Re-sorting: Only if order_by is present
                    let results = Array.from(localCache.values());
                    
                    if (params.order_by && Array.isArray(params.order_by)) {
                        results.sort((a, b) => {
                            for (const order of params.order_by||[]) {
                                const { field, ascending } = order;
                                const valA = a[field];
                                const valB = b[field];

                                if (valA === valB) continue;

                                const cmp = valA < valB ? -1 : 1;
                                return ascending ? cmp : -cmp;
                            }
                            return 0;
                        });
                    }

                    if (params.offset || params.limit) {
                        const start = params.offset || 0;
                        results = results.slice(start, params.limit ? start + params.limit : undefined);
                    }

                    const snapshots = results.map(r => new DocumentSnapshot(r.id.toString(), true, r));
                    onNext(new QuerySnapshot(snapshots));
                }
            });

            await exec({ op: 'subscribe', listener_id, event_name, ...params });
        } catch (err) {
            onError?.(err);
        }
    };

    start();
    return () => {
        if (unlisten) unlisten();
        exec({ op: 'unsubscribe', listener_id }).catch(() => {});
    };
}

// --- Transactions & Batches ---
export const writeBatch = (_db: FireLite) => {
    const mutations: any[] = [];
    return {
        set: (ref: DocumentReference, data: FireLiteRecord) => 
            mutations.push({ mutation: 'set', collection: ref.collectionPath, doc_id: ref.id, data: normalizeValue(data) }),
        update: (ref: DocumentReference, data: Partial<FireLiteRecord>) => 
            mutations.push({ mutation: 'patch', collection: ref.collectionPath, doc_id: ref.id, data: normalizeValue(data) }),
        delete: (ref: DocumentReference) => 
            mutations.push({ mutation: 'delete', collection: ref.collectionPath, doc_id: ref.id }),
        commit: async () => exec({ op: 'batch', mutations })
    };
};

export const runTransaction = async (
  _db: FireLite,
  updateFunction: (transaction: any) => Promise<any>
) => {
  const mutations: any[] = [];

  const transaction = {
    get: async (ref: DocumentReference) => {
      return await getDoc(ref);
    },

    set: (ref: DocumentReference, data: any) =>
      mutations.push({
        mutation: 'set',
        collection: ref.collectionPath,
        doc_id: ref.id,
        data: normalizeValue(data)
      }),

    update: (ref: DocumentReference, data: any) =>
      mutations.push({
        mutation: 'patch',
        collection: ref.collectionPath,
        doc_id: ref.id,
        data: normalizeValue(data)
      }),

    delete: (ref: DocumentReference) =>
      mutations.push({
        mutation: 'delete',
        collection: ref.collectionPath,
        doc_id: ref.id
      })
  };

  const result = await updateFunction(transaction);

  await exec({
    op: 'batch',
    mutations
  });

  return result;
};

// --- Indexing ---
export const createIndex = async (colOrPath: string | CollectionReference, field: string) => {
    const collection = typeof colOrPath === 'string' ? colOrPath : colOrPath.path;
    return exec({ op: 'create_index', collection, field });
};

export const createFtsIndex = async (colOrPath: string | CollectionReference, field: string) => {
    const collection = typeof colOrPath === 'string' ? colOrPath : colOrPath.path;
    return exec({ op: 'create_fts_index', collection, field });
};

export const createCompositeIndex = async (
  colOrPath: string | CollectionReference,
  fields: { field: string; desc?: boolean }[]
) => {
  const collection =
    typeof colOrPath === 'string' ? colOrPath : colOrPath.path;

  return exec({
    op: 'create_composite_index',
    collection,
    fields
  });
};

export const listIndexes = async (collection?: string) => {
  const res = await exec({
    op: 'list_indexes',
    collection
  });
  return res.indexes.list;
};

export const snapshotIndices = async () =>
  exec({ op: 'snapshot_indices' });

// --- Admin ---
export const listCollections = async () => {
    const res = await exec({ op: 'list_collections' });
    return res.collections.names;
};

export const getStats = async () => {
    const res = await exec({ op: 'get_stats' });
    return res.stats.details;
};


function buildQueryParams(q: Query | CollectionReference | CollectionGroupReference | DocumentReference) {
    // 1. Handle single DocumentReference (e.g., doc(db, 'users', '123'))
    if (q instanceof DocumentReference) {
        return {
            collection: q.collectionPath,
            doc_id_filter: q.id, // Explicit ID filter for the Rust loop
            filters: [],
            or_groups: undefined,
            order_by: undefined,
            limit: undefined,
            offset: undefined,
            projection: undefined,
            start_at: undefined,
            start_after: undefined,
            end_at: undefined,
            end_before: undefined,
            defer_blobs: false
        };
    }

    // 2. Normalize input: if it's a raw CollectionRef, wrap it in a Query object
    const queryObj = (q as any).constraints ? (q as Query) : new Query(q as any);
    
    const filters: any[] = [];
    const or_groups: any[][] = [];
    let order_by: any[] = [];
    let limit: number | undefined = undefined;
    let offset: number | undefined = undefined;
    let projection: string[] | undefined = undefined;
    let start_at: any[] | undefined = undefined;
    let start_after: any[] | undefined = undefined;
    let end_at: any[] | undefined = undefined;
    let end_before: any[] | undefined = undefined;
    let defer_blobs = false;

    // 3. Extract all constraints from the query object
    for (const c of queryObj.constraints) {
        switch (c.type) {
            case 'where': 
                filters.push({ 
                    field: c.data.field, 
                    op: symToOp(c.data.op), 
                    value: normalizeValue(c.data.value) 
                }); 
                break;
            case 'or': 
                or_groups.push(c.data.map((cc: any) => ({ 
                    field: cc.data.field, 
                    op: symToOp(cc.data.op), 
                    value: normalizeValue(cc.data.value) 
                }))); 
                break;
            case 'order_by': order_by.push(c.data); break;
            case 'limit': limit = c.data; break;
            case 'offset': offset = c.data; break;
            case 'select': projection = c.data; break;
            case 'start_at': start_at = c.data.map(normalizeValue); break;
            case 'start_after': start_after = c.data.map(normalizeValue); break;
            case 'end_at': end_at = c.data.map(normalizeValue); break;
            case 'end_before': end_before = c.data.map(normalizeValue); break;
            case 'defer_blobs': defer_blobs = true; break;
        }
    }

    // 4. Resolve the collection name/path
    const collection = queryObj.colRef instanceof CollectionReference 
        ? queryObj.colRef.path 
        : (queryObj.colRef as any).id;

    // 5. Return the payload structured for the Rust QueryInput struct
    return {
        collection,
        doc_id_filter: undefined, // Not used for collection-wide queries
        filters,
        or_groups: or_groups.length > 0 ? or_groups : undefined,
        order_by: order_by.length > 0 ? order_by : undefined,
        limit,
        offset,
        projection,
        start_at,
        start_after,
        end_at,
        end_before,
        defer_blobs
    };
}


export const getSumFromServer = async (
  q: Query | CollectionReference | CollectionGroupReference,
  field: string
) => {
  const params = buildQueryParams(q);
  const res = await exec({
    op: 'aggregate',
    kind: 'sum',
    field,
    ...params
  });
  return { data: () => ({ value: res.aggregate_result.value }) };
};

export const getAverageFromServer = async (
  q: Query | CollectionReference | CollectionGroupReference,
  field: string
) => {
  const params = buildQueryParams(q);
  const res = await exec({
    op: 'aggregate',
    kind: 'avg',
    field,
    ...params
  });
  return { data: () => ({ value: res.aggregate_result.value }) };
};

export const setDurability = (mode: 'always' | 'interval' | 'manual' | 'on_commit') => {
    const modes = { always: 0, interval: 1, manual: 2, on_commit: 3 };
    return exec({ op: 'set_durability', mode: modes[mode] });
};

export const setCompression = (enabled: boolean, level: number = 3) => {
    return exec({ op: 'set_compression', enabled, level });
};

export const getAuditLog = async (): Promise<AuditEntry[]> => {
    const res = await exec({ op: 'get_audit_log' });
    return res.audit_log.entries;
};