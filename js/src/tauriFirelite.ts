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
    docId?: string;
    ok: boolean;
}

// --- Internal Utilities ---

function generateId() {
    return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

function normalizeValue(v: any): any {
    if (v instanceof Uint8Array) return Array.from(v);
    if (v instanceof Date) return v.getTime() * 1000; // Convert to micros for Rust
    if (Array.isArray(v)) return v.map(normalizeValue);
    if (typeof v === 'object' && v !== null) {
        if (v instanceof DocumentSnapshot) return v.data(); // Allow passing snapshots to cursors
        return Object.fromEntries(Object.entries(v).map(([k, val]) => [k, normalizeValue(val)]));
    }
    return v;
}

async function exec(op: any): Promise<any> {
    const res = await invoke('firelite_exec', { op });
    if (res?.error) throw new Error(res.error);
    return res;
}

// --- Firestore Core Classes ---

export class FireLite {}

export class DocumentReference {
    constructor(public readonly collectionPath: string, public readonly id: string) {}
    get path() { return `${this.collectionPath}/${this.id}`; }
}

export class CollectionReference {
    constructor(public readonly path: string) {}
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

// --- Query Building ---

export type QueryConstraintType = 'where' | 'orderBy' | 'limit' | 'offset' | 'select' | 'startAt' | 'startAfter' | 'endAt' | 'endBefore' | 'or';

export class QueryConstraint {
    constructor(
        public readonly type: QueryConstraintType,
        public readonly data: any
    ) {}
}

export class Query {
    constructor(
        public readonly colRef: CollectionReference, 
        public readonly constraints: QueryConstraint[] = []
    ) {}
}

// --- API Implementation ---

export const getFirestore = () => new FireLite();

export const collection = (db: FireLite | DocumentReference, path: string) => {
    if (db instanceof DocumentReference) return new CollectionReference(`${db.path}/${path}`);
    return new CollectionReference(path);
};

export const doc = (db: FireLite | CollectionReference, colOrId: string, id?: string) => {
    if (db instanceof CollectionReference) return new DocumentReference(db.path, colOrId);
    if (id) return new DocumentReference(colOrId, id);
    const parts = colOrId.split('/');
    return new DocumentReference(parts[0], parts[1]);
};

// --- Write Operations ---

export const addDoc = async (colRef: CollectionReference, data: FireLiteRecord) => {
    const id = generateId();
    const ref = new DocumentReference(colRef.path, id);
    await setDoc(ref, data);
    return ref;
};

export const setDoc = async (ref: DocumentReference, data: FireLiteRecord) => {
    await exec({ op: 'set', collection: ref.collectionPath, docId: ref.id, data: normalizeValue(data) });
};

export const updateDoc = async (ref: DocumentReference, data: Partial<FireLiteRecord>) => {
    await exec({ op: 'patch', collection: ref.collectionPath, docId: ref.id, data: normalizeValue(data) });
};

export const deleteDoc = async (ref: DocumentReference) => {
    await exec({ op: 'delete', collection: ref.collectionPath, docId: ref.id });
};

export const getDoc = async (ref: DocumentReference) => {
    const res = await exec({ op: 'get', collection: ref.collectionPath, docId: ref.id });
    const data = res.document?.data;
    return new DocumentSnapshot(ref.id, !!data, data ?? undefined, ref);
};

// --- Querying ---

export const query = (colRef: CollectionReference, ...constraints: QueryConstraint[]) => new Query(colRef, constraints);

export const where = (field: string, op: FilterOperator, value: any) => new QueryConstraint('where', { field, op, value });

export const or = (...constraints: QueryConstraint[]) => new QueryConstraint('or', constraints);

export const orderBy = (field: string, direction: 'asc' | 'desc' = 'asc') => new QueryConstraint('orderBy', { field, ascending: direction === 'asc' });

export const limit = (n: number) => new QueryConstraint('limit', n);

export const offset = (n: number) => new QueryConstraint('offset', n);

export const select = (...fields: string[]) => new QueryConstraint('select', fields);

export const startAt = (...values: any[]) => new QueryConstraint('startAt', values);
export const startAfter = (...values: any[]) => new QueryConstraint('startAfter', values);
export const endAt = (...values: any[]) => new QueryConstraint('endAt', values);
export const endBefore = (...values: any[]) => new QueryConstraint('endBefore', values);

export const getDocs = async (q: Query) => {
    const params = buildQueryParams(q);
    const res = await exec({ op: 'query', ...params });
    const docs = res.queryResult.rows.map((r: any) => new DocumentSnapshot(r.id || generateId(), true, r));
    return new QuerySnapshot(docs);
};

// --- Aggregations ---

export const getCountFromServer = async (q: Query) => {
    const params = buildQueryParams(q);
    const res = await exec({ op: 'aggregate', kind: 'count', ...params });
    return { data: () => ({ count: res.aggregateResult.value }) };
};

export const getSumFromServer = async (q: Query, field: string) => {
    const params = buildQueryParams(q);
    const res = await exec({ op: 'aggregate', kind: 'sum', field, ...params });
    return { data: () => ({ value: res.aggregateResult.value }) };
};

export const getAverageFromServer = async (q: Query, field: string) => {
    const params = buildQueryParams(q);
    const res = await exec({ op: 'aggregate', kind: 'avg', field, ...params });
    return { data: () => ({ value: res.aggregateResult.value }) };
};

// --- Real-time Snapshots ---

export const onSnapshot = (q: Query, callback: (snapshot: QuerySnapshot) => void) => {
    const listenerId = `fl_${Date.now()}_${Math.random().toString(36).slice(2)}`;
    const eventName = `firelite://snapshot/${listenerId}`;
    const params = buildQueryParams(q);

    let unlisten: UnlistenFn;
    let lastRowsJson = "[]";

    const start = async () => {
        unlisten = await listen<{ rows: any[] }>(eventName, (event) => {
            const rows = event.payload.rows || [];
            const currentRowsJson = JSON.stringify(rows);
            
            // Basic change tracking
            const oldRows = JSON.parse(lastRowsJson);
            const changes = computeChanges(oldRows, rows);
            
            lastRowsJson = currentRowsJson;
            const docs = rows.map(r => new DocumentSnapshot(r.id, true, r));
            callback(new QuerySnapshot(docs, changes));
        });

        await exec({
            op: 'subscribe',
            listenerId,
            eventName,
            ...params
        });
    };

    start();

    return () => {
        if (unlisten) unlisten();
        exec({ op: 'unsubscribe', listenerId });
    };
};

// --- Transactions & Batches ---

export const writeBatch = () => {
    const mutations: any[] = [];
    return {
        set: (ref: DocumentReference, data: FireLiteRecord) => mutations.push({ mutation: 'set', collection: ref.collectionPath, docId: ref.id, data: normalizeValue(data) }),
        update: (ref: DocumentReference, data: Partial<FireLiteRecord>) => mutations.push({ mutation: 'patch', collection: ref.collectionPath, docId: ref.id, data: normalizeValue(data) }),
        delete: (ref: DocumentReference) => mutations.push({ mutation: 'delete', collection: ref.collectionPath, doc_id: ref.id }),
        commit: async () => exec({ op: 'batch', mutations })
    };
};

export const runTransaction = async (db: FireLite, updateFunction: (transaction: any) => Promise<any>) => {
    // Note: Rust side serializable transactions are more restrictive. 
    // This implementation wraps the logic in a Batch for atomicity.
    const reads: any[] = [];
    const mutations: any[] = [];
    
    const transaction = {
        get: async (ref: DocumentReference) => {
            const doc = await getDoc(ref);
            reads.push({ ref, version: Date.now() }); // Optimistic concurrency placeholder
            return doc;
        },
        set: (ref: DocumentReference, data: any) => mutations.push({ mutation: 'set', collection: ref.collectionPath, docId: ref.id, data: normalizeValue(data) }),
        update: (ref: DocumentReference, data: any) => mutations.push({ mutation: 'patch', collection: ref.collectionPath, docId: ref.id, data: normalizeValue(data) }),
        delete: (ref: DocumentReference) => mutations.push({ mutation: 'delete', collection: ref.collectionPath, docId: ref.id })
    };

    const result = await updateFunction(transaction);
    await exec({ op: 'batch', mutations });
    return result;
};

// --- Indexing ----
export const createIndex = async (collection: string, field: string) =>
    exec({ op: 'createIndex', collection, field });

export const createFtsIndex = async (collection: string, field: string) =>
    exec({ op: 'createFtsIndex', collection, field });

export const createCompositeIndex = async (
    collection: string,
    fields: { field: string, desc?: boolean }[]
) => exec({ op: 'createCompositeIndex', collection, fields });

export const listIndexes = async (collection?: string) => {
    const res = await exec({ op: 'listIndexes', collection });
    return res.indexes.list;
};

export const snapshotIndices = async () =>
    exec({ op: 'snapshotIndices' });


// --- Engine / Admin Operations ---

export const listCollections = async () => {
    const res = await exec({ op: 'listCollections' });
    return res.collections.names;
};

export const getStats = async () => {
    const res = await exec({ op: 'getStats' });
    return res.stats.details;
};

export const compactEngine = async () => exec({ op: 'compact' });

export const backupEngine = async (path: string) => {
    await exec({ op: 'backup', path });
};

export const getAuditLog = async (): Promise<AuditEntry[]> => {
    const res = await exec({ op: 'getAuditLog' });
    return res.auditLog.entries;
};

export const setDurabilityMode = async (mode: 'Always' | 'Interval' | 'Manual' | 'OnCommit') => {
    const map = { Interval: 1, Manual: 2, OnCommit: 3, Always: 0 };
    await exec({ op: 'setDurability', mode: map[mode] });
};

export const setCompression = async (enabled: boolean, level: number = 3) => {
    await exec({ op: 'setCompression', enabled, level });
};

// --- Private Helpers ---

function buildQueryParams(q: Query) {
    const filters: any[] = [];
    const orGroups: any[][] = [];
    let orderBy: any = undefined;
    let limit: number | undefined = undefined;
    let offset: number | undefined = undefined;
    let projection: string[] | undefined = undefined;
    let startAt: any[] | undefined = undefined;
    let startAfter: any[] | undefined = undefined;
    let endAt: any[] | undefined = undefined;
    let endBefore: any[] | undefined = undefined;

    for (const c of q.constraints) {
        switch (c.type) {
            case 'where': filters.push({ field: c.data.field, op: c.data.op, value: normalizeValue(c.data.value) }); break;
            case 'or': orGroups.push(c.data.map((cc: any) => ({ field: cc.data.field, op: cc.data.op, value: normalizeValue(cc.data.value) }))); break;
            case 'orderBy': orderBy = c.data; break;
            case 'limit': limit = c.data; break;
            case 'offset': offset = c.data; break;
            case 'select': projection = c.data; break;
            case 'startAt': startAt = c.data.map(normalizeValue); break;
            case 'startAfter': startAfter = c.data.map(normalizeValue); break;
            case 'endAt': endAt = c.data.map(normalizeValue); break;
            case 'endBefore': endBefore = c.data.map(normalizeValue); break;
        }
    }

    return {
        collection: q.colRef.path,
        filters,
        orGroups: orGroups.length > 0 ? orGroups : undefined,
        orderBy,
        limit,
        offset,
        projection,
        startAt,
        startAfter,
        endAt,
        endBefore
    };
}

function computeChanges(oldRows: any[], newRows: any[]): DocumentChange[] {
    const changes: DocumentChange[] = [];
    const oldMap = new Map(oldRows.map(r => [r.id, r]));
    const newMap = new Map(newRows.map(r => [r.id, r]));

    newMap.forEach((newDoc, id) => {
        const oldDoc = oldMap.get(id);
        if (!oldDoc) {
            changes.push({ type: 'added', doc: new DocumentSnapshot(id, true, newDoc) });
        } else if (JSON.stringify(oldDoc) !== JSON.stringify(newDoc)) {
            changes.push({ type: 'modified', doc: new DocumentSnapshot(id, true, newDoc) });
        }
    });

    oldMap.forEach((oldDoc, id) => {
        if (!newMap.has(id)) {
            changes.push({ type: 'removed', doc: new DocumentSnapshot(id, true, oldDoc) });
        }
    });

    return changes;
}
