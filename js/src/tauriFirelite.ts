import { invoke } from '@tauri-apps/api/core';
import { listen, type UnlistenFn } from '@tauri-apps/api/event';

// --- Types & Interfaces ---
export type FireLitePrimitive = 
  | string | number | boolean | null | Uint8Array | Date
  | { [key: string]: FireLitePrimitive } 
  | FireLitePrimitive[];

export type FireLiteRecord = { [key: string]: FireLitePrimitive };

export type FilterOperator = 
  | 'eq' | 'ne' | 'gt' | 'gte' | 'lt' | 'lte' 
  | 'match' | 'contains' | 'startsWith' | 'in' | 'notIn' 
  | 'arrayContains' | 'arrayContainsAny';



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

function symToOp(sym: string): FilterOperator {
    switch (sym) {
        case '==': return 'eq';
        case '!=': return 'ne';
        case '>': return 'gt';
        case '>=': return 'gte';
        case '<': return 'lt';
        case '<=': return 'lte';
        default: return sym as FilterOperator;
    }
}

// --- Internal Utilities ---
function generateId() {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

function normalizeValue(v: any): any {
    if (v instanceof Uint8Array) return Array.from(v);
    if (v instanceof Date) return v.getTime() * 1000; 
    if (Array.isArray(v)) return v.map(normalizeValue);
    if (typeof v === 'object' && v !== null) {
        if (v instanceof DocumentSnapshot) return v.data(); 
        return Object.fromEntries(Object.entries(v).map(([k, val]) => [k, normalizeValue(val)]));
    }
    return v;
}

async function exec(op: any): Promise<any> {
    // Note: The 'op' field inside the payload is the variant tag
    // The other fields must match the Rust struct fields (snake_case)
    const res = await invoke<any>('firelite_exec', { op });
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
    constructor(
        public readonly id: string, 
        private readonly _exists: boolean, 
        private readonly _data?: FireLiteRecord,
        private readonly _ref?: DocumentReference
    ) {}
    exists() { return this._exists; }
    data() { return this._data; }
    get ref() { return this._ref || new DocumentReference('', this.id); }
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

export type QueryConstraintType = 'where' | 'order_by' | 'limit' | 'offset' | 'select' | 'start_at' | 'start_after' | 'end_at' | 'end_before' | 'or';

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
        return new DocumentReference(segments[0], segments[1]);
    }
    return new DocumentReference(colOrPath.path, id!);
};

// --- Write Operations ---
export const addDoc = async (colRef: CollectionReference, data: FireLiteRecord) => {
    const id = generateId();
    const ref = new DocumentReference(colRef.path, id);
    await setDoc(ref, data);
    return ref;
};

export const setDoc = async (ref: DocumentReference, data: FireLiteRecord) => {
    await exec({ op: 'set', collection: ref.collectionPath, doc_id: ref.id, data: normalizeValue(data) });
};

export const updateDoc = async (ref: DocumentReference, data: Partial<FireLiteRecord>) => {
    await exec({ op: 'patch', collection: ref.collectionPath, doc_id: ref.id, data: normalizeValue(data) });
};

export const deleteDoc = async (ref: DocumentReference) => {
    await exec({ op: 'delete', collection: ref.collectionPath, doc_id: ref.id });
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

export const getDocs = async (q: Query | CollectionReference | CollectionGroupReference) => {
    const params = buildQueryParams(q);
    const res = await exec({ op: 'query', ...params });
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

    // Persistent state for this specific listener
    let localCache = new Map<string, any>();
    let unlisten: UnlistenFn;

    const start = async () => {
        try {
            unlisten = await listen<DeltaPayload>(event_name, (event) => {
                const { changes } = event.payload;
                const documentChanges: DocumentChange[] = [];

                // 1. Process the Batch of Deltas
                changes.forEach(change => {
                    const { kind, doc_id, data } = change;

                    if (kind === 'full') {
                        localCache.clear();
                        const rows = data as any[];
                        rows.forEach(r => {
                            const id = (r.id || r._id || doc_id).toString();
                            localCache.set(id, r);
                            documentChanges.push({ type: 'added', doc: new DocumentSnapshot(id, true, r) });
                        });
                    } 
                    else if (kind === 'update') {
                        const type = localCache.has(doc_id) ? 'modified' : 'added';
                        localCache.set(doc_id, data);
                        documentChanges.push({ type, doc: new DocumentSnapshot(doc_id, true, data) });
                    } 
                    else if (kind === 'delete') {
                        if (localCache.has(doc_id)) {
                            const oldData = localCache.get(doc_id);
                            localCache.delete(doc_id);
                            documentChanges.push({ type: 'removed', doc: new DocumentSnapshot(doc_id, true, oldData) });
                        }
                    }
                });

                // 2. Branch Logic: Document vs Query
                if (isDoc) {
                    const docId = (q as DocumentReference).id;
                    const docData = localCache.get(docId);
                    onNext(new DocumentSnapshot(docId, !!docData, docData));
                } 
                else {
                    // 3. Handle Lists (Sorting & Pagination)
                    let docs = Array.from(localCache.values()).map(r => new DocumentSnapshot(r.id, true, r));

                    // Variant 4: Client-side OrderBy
                    if (params.order_by) {
                        const { field, ascending } = params.order_by;
                        docs.sort((a, b) => {
                            const valA = a.data()?.[field] ?? '';
                            const valB = b.data()?.[field] ?? '';
                            if (valA === valB) return 0;
                            const cmp = valA < valB ? -1 : 1;
                            return ascending ? cmp : -cmp;
                        });
                    }

                    // Variant 4: Client-side Limit & Offset (Pagination)
                    if (params.offset || params.limit) {
                        const start = params.offset || 0;
                        const end = params.limit ? start + params.limit : docs.length;
                        docs = docs.slice(start, end);
                    }

                    // Variant 3: DocChanges
                    // Trigger callback with the list and the specific delta metadata
                    onNext(new QuerySnapshot(docs, documentChanges));
                }
            });

            // Register the subscription in the Rust backend
            await exec({
                op: 'subscribe',
                listener_id,
                event_name,
                ...params
            });
        } catch (err) {
            if (onError) onError(err);
            else console.error("FireLite Subscription Error:", err);
        }
    };

    start();

    // Return Unsubscribe Function
    return () => {
        if (unlisten) unlisten();
        exec({ op: 'unsubscribe', listener_id }).catch(() => {});
    };
}

// --- Transactions & Batches ---
export const writeBatch = () => {
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
            end_before: undefined
        };
    }

    // 2. Normalize input: if it's a raw CollectionRef, wrap it in a Query object
    const queryObj = (q as any).constraints ? (q as Query) : new Query(q as any);
    
    const filters: any[] = [];
    const or_groups: any[][] = [];
    let order_by: any = undefined;
    let limit: number | undefined = undefined;
    let offset: number | undefined = undefined;
    let projection: string[] | undefined = undefined;
    let start_at: any[] | undefined = undefined;
    let start_after: any[] | undefined = undefined;
    let end_at: any[] | undefined = undefined;
    let end_before: any[] | undefined = undefined;

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
            case 'order_by': order_by = c.data; break;
            case 'limit': limit = c.data; break;
            case 'offset': offset = c.data; break;
            case 'select': projection = c.data; break;
            case 'start_at': start_at = c.data.map(normalizeValue); break;
            case 'start_after': start_after = c.data.map(normalizeValue); break;
            case 'end_at': end_at = c.data.map(normalizeValue); break;
            case 'end_before': end_before = c.data.map(normalizeValue); break;
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
        order_by,
        limit,
        offset,
        projection,
        start_at,
        start_after,
        end_at,
        end_before
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