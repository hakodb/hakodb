package firelite

/*
#cgo CXXFLAGS: -std=c++17
#cgo LDFLAGS: -L../../target/debug -lfirelite
#include <stdlib.h>
#include <stdint.h>
#include "firelite_c.h"

extern void firelite_watch_bridge(char* collection, char* path, int32_t kind, void* user_data);
static inline void firelite_watch_bridge_const(const char* collection, const char* path, int32_t kind, void* user_data) {
	firelite_watch_bridge((char*)collection, (char*)path, kind, user_data);
}
static inline FL_Watch* firelite_watch_bridge_register(FL_Engine* engine, const char* collection, void* user_data) {
	return fl_engine_watch(engine, collection, firelite_watch_bridge_const, user_data);
}
extern bool fireliteWalkBridge(char* id, uintptr_t id_len, uint8_t* bytes, uintptr_t bytes_len, void* user_data);
static inline int64_t firelite_walk_register(FL_Engine* engine, const FL_Query* query, void* user_data) {
	// ponytail: the Go bridge takes non-const pointers (cgo has no const);
	// it never mutates — the cast keeps the public typedef const-correct.
	return fl_cursor_walk(engine, query, (FlWalkCallback)fireliteWalkBridge, user_data);
}
extern bool fireliteViewWalkBridge(char* id, uintptr_t id_len, FL_ViewDoc* view, void* user_data);
static inline int64_t firelite_view_walk_register(FL_Engine* engine, const FL_Query* query, void* user_data) {
	// ponytail: same const-cast shaping as the byte walk above; the engine
	// lends the view for the call, Go must not retain the handle.
	return fl_cursor_walk_view(engine, query, (FlViewWalkCallback)fireliteViewWalkBridge, user_data);
}
*/
import "C"

import (
	"encoding/json"
	"errors"
	"fmt"
	"runtime/cgo"
	"unsafe"
)

type (
	Engine      struct{ ptr *C.FL_Engine }
	Config      struct{ ptr *C.FL_Config }
	Doc         struct{ ptr *C.FL_Doc }
	Array       struct{ ptr *C.FL_Array }
	Query       struct{ ptr *C.FL_Query }
	Batch       struct{ ptr *C.FL_Batch }
	Transaction struct{ ptr *C.FL_Transaction }
	NetSyncer   struct{ ptr *C.FL_NetSyncer }
	ResultSet   struct{ ptr *C.FL_ResultSet }
	RawDoc      struct{ ptr *C.FL_RawDoc }
	RawResultSet struct{ ptr *C.FL_RawResultSet }
	ViewDoc     struct{ ptr *C.FL_ViewDoc }
	CloudSync   struct{ ptr *C.FL_CloudSync }
	Watch       struct {
		ptr    *C.FL_Watch
		handle cgo.Handle
	}
)

type DurabilityMode int32

const (
	DurabilityAlways   DurabilityMode = 0
	DurabilityInterval DurabilityMode = 1
	DurabilityManual   DurabilityMode = 2
	DurabilityOnCommit DurabilityMode = 3
)

type CloudSyncMode int32

const (
	CloudSyncServer CloudSyncMode = 0
	CloudSyncClient CloudSyncMode = 1
)

type SnapshotKind int32

const (
	SnapshotInsert SnapshotKind = 1
	SnapshotDelete SnapshotKind = 2
)

type Reference struct {
	Collection string
	DocID      string
}

type ServerTimestamp struct{}

type TimestampMicros int64

type SnapshotCallback func(collection, path string, kind SnapshotKind)

func lastError() string {
	err := C.fl_last_error()
	if err == nil {
		return "unknown ffi error"
	}
	return C.GoString(err)
}

func checkStatus(op string, status C.int32_t) error {
	if status == 0 {
		return nil
	}
	return fmt.Errorf("%s failed: %s", op, lastError())
}

func cString(v string) (*C.char, func()) {
	cv := C.CString(v)
	return cv, func() { C.free(unsafe.Pointer(cv)) }
}

func Open(path string) (*Engine, error) {
	cp, free := cString(path)
	defer free()
	ptr := C.fl_engine_open(cp)
	if ptr == nil {
		return nil, fmt.Errorf("fl_engine_open failed: %s", lastError())
	}
	return &Engine{ptr: ptr}, nil
}

func OpenWithConfig(path string, cfg *Config) (*Engine, error) {
	if cfg == nil || cfg.ptr == nil {
		return nil, errors.New("config is nil")
	}
	cp, free := cString(path)
	defer free()
	ptr := C.fl_engine_open_with_config(cp, cfg.ptr)
	cfg.ptr = nil
	if ptr == nil {
		return nil, fmt.Errorf("fl_engine_open_with_config failed: %s", lastError())
	}
	return &Engine{ptr: ptr}, nil
}

func (e *Engine) Close() {
	if e != nil && e.ptr != nil {
		C.fl_engine_free(e.ptr)
		e.ptr = nil
	}
}

func NewConfig() *Config { return &Config{ptr: C.fl_config_new()} }
func (c *Config) Free() {
	if c != nil && c.ptr != nil {
		C.fl_config_free(c.ptr)
		c.ptr = nil
	}
}

func (c *Config) SetDurability(mode DurabilityMode) {
	C.fl_config_set_durability(c.ptr, C.int32_t(mode))
}
func (c *Config) SetQueryWorkers(count uintptr) {
	C.fl_config_set_query_workers(c.ptr, C.uintptr_t(count))
}
func (c *Config) SetCompression(enabled bool, level int32) {
	C.fl_config_set_compression(c.ptr, C.bool(enabled), C.int32_t(level))
}

// SetBackgroundMaintenance holds the 5s maintenance tick (checkpoint,
// compaction, purge, snapshots) for deterministic benchmarks or hard
// latency bounds. Engine stays correct; files grow until re-enabled.
func (c *Config) SetBackgroundMaintenance(enabled bool) {
	C.fl_config_set_background_maintenance(c.ptr, C.bool(enabled))
}

func (c *Config) SetEncryptionKey(key string) {
	ck, free := cString(key)
	defer free()
	C.fl_config_set_encryption_key(c.ptr, ck)
}

func (c *Config) SetAuditLog(enabled bool, path string) {
	cp, free := cString(path)
	defer free()
	C.fl_config_set_audit_log(c.ptr, C.bool(enabled), cp)
}

func (c *Config) SetMemoryLimits(mmapSize, maxInlined uintptr) {
	C.fl_config_set_memory_limits(c.ptr, C.uintptr_t(mmapSize), C.uintptr_t(maxInlined))
}

func (c *Config) SetStorageTuning(pageSize, compactionThreshold, groupCommitMaxOps uintptr) {
	C.fl_config_set_storage_tuning(c.ptr, C.uintptr_t(pageSize), C.uintptr_t(compactionThreshold), C.uintptr_t(groupCommitMaxOps))
}

// SetEncryptedCollections marks specific collections for at-rest encryption.
func (c *Config) SetEncryptedCollections(collections ...string) error {
	payload, err := json.Marshal(collections)
	if err != nil {
		return err
	}
	cj, free := cString(string(payload))
	defer free()
	return checkStatus("fl_config_set_encrypted_collections", C.fl_config_set_encrypted_collections(c.ptr, cj))
}

func (c *Config) SetBlobThreshold(thresholdBytes uintptr) {
	C.fl_config_set_blob_threshold(c.ptr, C.uintptr_t(thresholdBytes))
}

// SetWALReserveBytes sets the WAL headroom reservation (0 = off).
func (c *Config) SetWALReserveBytes(bytes uint64) {
	C.fl_config_set_wal_reserve_bytes(c.ptr, C.uint64_t(bytes))
}

func NewDoc() *Doc { return &Doc{ptr: C.fl_doc_new()} }
func (d *Doc) Free() {
	if d != nil && d.ptr != nil {
		C.fl_doc_free(d.ptr)
		d.ptr = nil
	}
}

func (d *Doc) ToJSON() (string, error) {
	ptr := C.fl_doc_to_json(d.ptr)
	if ptr == nil {
		return "", fmt.Errorf("fl_doc_to_json failed: %s", lastError())
	}
	defer C.fl_string_free(ptr)
	return C.GoString(ptr), nil
}

func (d *Doc) InsertString(key, value string) error {
	ck, fk := cString(key)
	cv, fv := cString(value)
	defer fk()
	defer fv()
	return checkStatus("fl_doc_insert_str", C.fl_doc_insert_str(d.ptr, ck, cv))
}
func (d *Doc) InsertInt(key string, value int64) error {
	ck, fk := cString(key)
	defer fk()
	return checkStatus("fl_doc_insert_int", C.fl_doc_insert_int(d.ptr, ck, C.int64_t(value)))
}
func (d *Doc) InsertFloat(key string, value float64) error {
	ck, fk := cString(key)
	defer fk()
	return checkStatus("fl_doc_insert_float", C.fl_doc_insert_float(d.ptr, ck, C.double(value)))
}
func (d *Doc) InsertBool(key string, value bool) error {
	ck, fk := cString(key)
	defer fk()
	return checkStatus("fl_doc_insert_bool", C.fl_doc_insert_bool(d.ptr, ck, C.bool(value)))
}
func (d *Doc) InsertNull(key string) error {
	ck, fk := cString(key)
	defer fk()
	return checkStatus("fl_doc_insert_null", C.fl_doc_insert_null(d.ptr, ck))
}
func (d *Doc) InsertTimestamp(key string, micros int64) error {
	ck, fk := cString(key)
	defer fk()
	return checkStatus("fl_doc_insert_timestamp", C.fl_doc_insert_timestamp(d.ptr, ck, C.int64_t(micros)))
}
func (d *Doc) InsertServerTimestamp(key string) error {
	ck, fk := cString(key)
	defer fk()
	return checkStatus("fl_doc_insert_server_timestamp", C.fl_doc_insert_server_timestamp(d.ptr, ck))
}
func (d *Doc) InsertBinary(key string, data []byte) error {
	ck, fk := cString(key)
	defer fk()
	if len(data) == 0 {
		return checkStatus("fl_doc_insert_bin", C.fl_doc_insert_bin(d.ptr, ck, nil, 0))
	}
	return checkStatus("fl_doc_insert_bin", C.fl_doc_insert_bin(d.ptr, ck, (*C.uint8_t)(unsafe.Pointer(&data[0])), C.uintptr_t(len(data))))
}
func (d *Doc) InsertDoc(key string, child *Doc) error {
	ck, fk := cString(key)
	defer fk()
	if child == nil || child.ptr == nil {
		return errors.New("child doc is nil")
	}
	return checkStatus("fl_doc_insert_doc", C.fl_doc_insert_doc(d.ptr, ck, child.ptr))
}
func (d *Doc) InsertArray(key string, arr *Array) error {
	ck, fk := cString(key)
	defer fk()
	if arr == nil || arr.ptr == nil {
		return errors.New("array is nil")
	}
	ptr := arr.ptr
	arr.ptr = nil
	return checkStatus("fl_doc_insert_array", C.fl_doc_insert_array(d.ptr, ck, ptr))
}
func (d *Doc) InsertReference(key, targetCollection, targetID string) error {
	ck, fk := cString(key)
	cc, fc := cString(targetCollection)
	ci, fi := cString(targetID)
	defer fk()
	defer fc()
	defer fi()
	return checkStatus("fl_doc_insert_reference", C.fl_doc_insert_reference(d.ptr, ck, cc, ci))
}

func NewArray() *Array { return &Array{ptr: C.fl_array_new()} }
func (a *Array) Free() {
	if a != nil && a.ptr != nil {
		C.fl_array_free(a.ptr)
		a.ptr = nil
	}
}
func (a *Array) AppendString(v string) error {
	cv, free := cString(v)
	defer free()
	return checkStatus("fl_array_append_str", C.fl_array_append_str(a.ptr, cv))
}
func (a *Array) AppendInt(v int64) error {
	return checkStatus("fl_array_append_int", C.fl_array_append_int(a.ptr, C.int64_t(v)))
}
func (a *Array) AppendDoc(d *Doc) error {
	if d == nil || d.ptr == nil {
		return errors.New("doc is nil")
	}
	return checkStatus("fl_array_append_doc", C.fl_array_append_doc(a.ptr, d.ptr))
}

func (e *Engine) Set(collection, docID string, doc *Doc) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_engine_insert", C.fl_engine_insert(e.ptr, cc, ci, doc.ptr))
}

// InsertTake moves doc into the engine without cloning (no deep copy).
// The Doc handle is always consumed — do not use or free it afterwards.
func (e *Engine) InsertTake(collection, docID string, doc *Doc) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	err := checkStatus("fl_engine_insert_take", C.fl_engine_insert_take(e.ptr, cc, ci, doc.ptr))
	doc.ptr = nil
	return err
}

// ResolveBlobs hydrates deferred blob fields of a query-returned doc.
func (e *Engine) ResolveBlobs(collection string, doc *Doc) error {
	cc, fc := cString(collection)
	defer fc()
	return checkStatus("fl_doc_resolve_blobs", C.fl_doc_resolve_blobs(e.ptr, cc, doc.ptr))
}

func (e *Engine) GetDoc(collection, docID string) (*Doc, error) {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	ptr := C.fl_engine_get(e.ptr, cc, ci)
	if ptr == nil {
		return nil, nil
	}
	return &Doc{ptr: ptr}, nil
}

func (e *Engine) Delete(collection, docID string) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_engine_delete", C.fl_engine_delete(e.ptr, cc, ci))
}

// DeleteLocal marks the key so no sync tailer or handshake ever transmits
// it, then deletes normally (fresh tombstone keeps the version clock ahead).
func (e *Engine) DeleteLocal(collection, docID string) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_engine_delete_local", C.fl_engine_delete_local(e.ptr, cc, ci))
}

// SetCollectionLocal marks a collection local-only (never syncs) or, with
// local=false, rejoins it to sync.
func (e *Engine) SetCollectionLocal(collection string, local bool) error {
	cc, fc := cString(collection)
	defer fc()
	l := C.int(0)
	if local {
		l = 1
	}
	return checkStatus("fl_engine_set_collection_local", C.fl_engine_set_collection_local(e.ptr, cc, l))
}

// ReplicateKey opts a key back into replication (future ops only).
func (e *Engine) ReplicateKey(collection, docID string) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_engine_replicate_key", C.fl_engine_replicate_key(e.ptr, cc, ci))
}

// ReplicateCollection opts a whole collection back into replication.
func (e *Engine) ReplicateCollection(collection string) error {
	cc, fc := cString(collection)
	defer fc()
	return checkStatus("fl_engine_replicate_collection", C.fl_engine_replicate_collection(e.ptr, cc))
}

// VacuumCollection purges a collection's tombstones. Emits nothing (never
// replicates); the next handshake pulls peer state (restore-on-rejoin).
func (e *Engine) VacuumCollection(collection string) (int32, error) {
	cc, fc := cString(collection)
	defer fc()
	n := C.fl_engine_vacuum_collection(e.ptr, cc)
	if n < 0 {
		return 0, fmt.Errorf("fl_engine_vacuum_collection failed: %s", lastError())
	}
	return int32(n), nil
}

func (e *Engine) Patch(collection, docID string, updates *Doc) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_engine_patch", C.fl_engine_patch(e.ptr, cc, ci, updates.ptr))
}

func (e *Engine) GetByReference(doc *Doc, fieldKey string) (*Doc, error) {
	ck, fk := cString(fieldKey)
	defer fk()
	ptr := C.fl_engine_get_by_ref(e.ptr, doc.ptr, ck)
	if ptr == nil {
		return nil, nil
	}
	return &Doc{ptr: ptr}, nil
}

func (e *Engine) Backup(path string) error {
	cp, free := cString(path)
	defer free()
	return checkStatus("fl_engine_backup", C.fl_engine_backup(e.ptr, cp))
}
func (e *Engine) Compact() error { return checkStatus("fl_engine_compact", C.fl_engine_compact(e.ptr)) }

// IsIndexesReady reports whether background index construction has completed.
func (e *Engine) IsIndexesReady() bool {
	return bool(C.fl_engine_is_indexes_ready(e.ptr))
}

// ListIndexes returns the raw JSON index listing for a collection (or all collections when empty).
func (e *Engine) ListIndexes(collection string) (string, error) {
	cc, free := cString(collection)
	defer free()
	return ownedCStringJSON(func() *C.char { return C.fl_engine_list_indexes(e.ptr, cc) })
}

func (e *Engine) ListCollections() ([]string, error) {
	s, err := ownedCStringJSON(func() *C.char { return C.fl_engine_list_collections(e.ptr) })
	if err != nil {
		return nil, err
	}
	var cols []string
	if err := json.Unmarshal([]byte(s), &cols); err != nil {
		return nil, err
	}
	return cols, nil
}
func (e *Engine) StatsJSON() (string, error) {
	return ownedCStringJSON(func() *C.char { return C.fl_engine_get_stats(e.ptr) })
}
func (e *Engine) AuditLogJSON() (string, error) {
	return ownedCStringJSON(func() *C.char { return C.fl_engine_get_audit_log(e.ptr) })
}

func (e *Engine) NewNetSyncer(name, roomKey string) (*NetSyncer, error) {
	cn, fn := cString(name)
	cr, fr := cString(roomKey)
	defer fn()
	defer fr()
	ptr := C.fl_net_syncer_new(e.ptr, cn, cr)
	if ptr == nil {
		return nil, fmt.Errorf("fl_net_syncer_new failed: %s", lastError())
	}
	return &NetSyncer{ptr: ptr}, nil
}

func (n *NetSyncer) Start(port uint16) error {
	return checkStatus("fl_net_syncer_start", C.fl_net_syncer_start(n.ptr, C.uint16_t(port)))
}

// Discovery transports for LAN mesh: 0 = mDNS (desktop default), 1 = UDP
// broadcast (mobile default, no multicast), 2 = both (mixed groups — a
// desktop joining mobile peers must opt into both or broadcast).
const (
	DiscoveryMdns      = 0
	DiscoveryBroadcast = 1
	DiscoveryBoth      = 2
)

// SetDiscoveryMode selects discovery transports. Takes effect at Start.
func (n *NetSyncer) SetDiscoveryMode(mode int) error {
	return checkStatus("fl_net_syncer_set_discovery", C.fl_net_syncer_set_discovery(n.ptr, C.int(mode)))
}

func (n *NetSyncer) StatusJSON() (string, error) {
	return ownedCStringJSON(func() *C.char { return C.fl_net_syncer_status(n.ptr) })
}

func (n *NetSyncer) Free() {
	if n != nil && n.ptr != nil {
		C.fl_net_syncer_free(n.ptr)
		n.ptr = nil
	}
}

// NewCloudSync creates a bi-directional cloud sync handle.
// mode: CloudSyncServer (0) or CloudSyncClient (1).
// roomName identifies the room; roomKey is the room's security key. On the
// server, room collections are stored as <roomName>_<collection>.
func (e *Engine) NewCloudSync(mode CloudSyncMode, clientID, roomName, roomKey, authToken string) (*CloudSync, error) {
	ci, fi := cString(clientID)
	rn, frn := cString(roomName)
	cr, fr := cString(roomKey)
	ct, ft := cString(authToken)
	defer fi()
	defer frn()
	defer fr()
	defer ft()
	ptr := C.fl_cloud_sync_new(e.ptr, C.int32_t(mode), ci, rn, cr, ct)
	if ptr == nil {
		return nil, fmt.Errorf("fl_cloud_sync_new failed: %s", lastError())
	}
	return &CloudSync{ptr: ptr}, nil
}

// NewCloudSyncServer creates a room-agnostic cloud SERVER ("big cloud server
// storage"). It is not bound to any room: it accepts and persists any
// (roomName, roomKey) pair its clients ask for, stores each room's collections
// under its own storage prefix and relays sync only to the members of that room.
func (e *Engine) NewCloudSyncServer(serverID, authToken string) (*CloudSync, error) {
	si, fsi := cString(serverID)
	ct, ft := cString(authToken)
	defer fsi()
	defer ft()
	ptr := C.fl_cloud_sync_server_new(e.ptr, si, ct)
	if ptr == nil {
		return nil, fmt.Errorf("fl_cloud_sync_server_new failed: %s", lastError())
	}
	return &CloudSync{ptr: ptr}, nil
}

// NewCloudSyncClient creates an offline-first cloud CLIENT bound to a room of
// the caller's choosing. The client picks the room (roomName + roomKey) and
// later picks the server via Start.
func (e *Engine) NewCloudSyncClient(clientID, roomName, roomKey, authToken string) (*CloudSync, error) {
	ci, fi := cString(clientID)
	rn, frn := cString(roomName)
	cr, fr := cString(roomKey)
	ct, ft := cString(authToken)
	defer fi()
	defer frn()
	defer fr()
	defer ft()
	ptr := C.fl_cloud_sync_client_new(e.ptr, ci, rn, cr, ct)
	if ptr == nil {
		return nil, fmt.Errorf("fl_cloud_sync_client_new failed: %s", lastError())
	}
	return &CloudSync{ptr: ptr}, nil
}

// Start connects a cloud sync client (ws:// or wss://) or binds the cloud sync server (host:port).
func (s *CloudSync) Start(address string) error {
	ca, free := cString(address)
	defer free()
	return checkStatus("fl_cloud_sync_start", C.fl_cloud_sync_start(s.ptr, ca))
}

func (s *CloudSync) Status() (string, error) {
	return ownedCStringJSON(func() *C.char { return C.fl_cloud_sync_status(s.ptr) })
}

func (s *CloudSync) Stop() {
	if s != nil && s.ptr != nil {
		C.fl_cloud_sync_stop(s.ptr)
	}
}

func (s *CloudSync) Free() {
	if s != nil && s.ptr != nil {
		C.fl_cloud_sync_free(s.ptr)
		s.ptr = nil
	}
}

func (e *Engine) InsertSubDoc(col, id, subCol, subID string, doc *Doc) error {
	cc, fc := cString(col)
	ci, fi := cString(id)
	cs, fs := cString(subCol)
	csi, fsi := cString(subID)
	defer fc()
	defer fi()
	defer fs()
	defer fsi()
	return checkStatus("fl_engine_insert_subdoc", C.fl_engine_insert_subdoc(e.ptr, cc, ci, cs, csi, doc.ptr))
}

func (e *Engine) CreateIndex(collection string, fieldsJSON string) (uint32, error) {
	cc, fc := cString(collection)
	cj, fj := cString(fieldsJSON)
	defer fc()
	defer fj()
	v := C.fl_engine_create_index(e.ptr, cc, cj)
	if v == 0 {
		return 0, fmt.Errorf("fl_engine_create_index failed: %s", lastError())
	}
	return uint32(v), nil
}
func (e *Engine) CreateSimpleIndex(collection, field string) error {
	cc, fc := cString(collection)
	cf, ff := cString(field)
	defer fc()
	defer ff()
	return checkStatus("fl_engine_create_simple_index", C.fl_engine_create_simple_index(e.ptr, cc, cf))
}
func (e *Engine) CreateFTSIndex(collection, field string) error {
	cc, fc := cString(collection)
	cf, ff := cString(field)
	defer fc()
	defer ff()
	return checkStatus("fl_engine_create_fts_index", C.fl_engine_create_fts_index(e.ptr, cc, cf))
}
func (e *Engine) SnapshotIndices() error {
	return checkStatus("fl_engine_snapshot_indices", C.fl_engine_snapshot_indices(e.ptr))
}

func NewBatch() *Batch { return &Batch{ptr: C.fl_batch_new()} }
func (b *Batch) Free() {
	if b != nil && b.ptr != nil {
		C.fl_batch_free(b.ptr)
		b.ptr = nil
	}
}
func (b *Batch) Set(collection, docID string, doc *Doc) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_batch_set", C.fl_batch_set(b.ptr, cc, ci, doc.ptr))
}
func (b *Batch) Delete(collection, docID string) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_batch_delete", C.fl_batch_delete(b.ptr, cc, ci))
}
func (e *Engine) CommitBatch(batch *Batch) error {
	return checkStatus("fl_batch_commit", C.fl_batch_commit(e.ptr, batch.ptr))
}

func (e *Engine) BeginTransaction() (*Transaction, error) {
	ptr := C.fl_transaction_begin(e.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("fl_transaction_begin failed: %s", lastError())
	}
	return &Transaction{ptr: ptr}, nil
}
func (t *Transaction) Free() {
	if t != nil && t.ptr != nil {
		C.fl_transaction_free(t.ptr)
		t.ptr = nil
	}
}
func (e *Engine) TxGet(t *Transaction, collection, docID string) (*Doc, error) {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	ptr := C.fl_transaction_get(e.ptr, t.ptr, cc, ci)
	if ptr == nil {
		return nil, nil
	}
	return &Doc{ptr: ptr}, nil
}
func (t *Transaction) Set(collection, docID string, doc *Doc) error {
	cc, fc := cString(collection)
	ci, fi := cString(docID)
	defer fc()
	defer fi()
	return checkStatus("fl_transaction_set", C.fl_transaction_set(t.ptr, cc, ci, doc.ptr))
}
func (e *Engine) CommitTransaction(t *Transaction) error {
	return checkStatus("fl_transaction_commit", C.fl_transaction_commit(e.ptr, t.ptr))
}

func NewQuery(collection string) *Query {
	cc, free := cString(collection)
	defer free()
	return &Query{ptr: C.fl_query_new(cc)}
}
func (q *Query) Free() {
	if q != nil && q.ptr != nil {
		C.fl_query_free(q.ptr)
		q.ptr = nil
	}
}
func (q *Query) WhereEqString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_eq_str", C.fl_query_where_eq_str(q.ptr, cf, cv))
}
func (q *Query) WhereEqBool(field string, value bool) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_eq_bool", C.fl_query_where_eq_bool(q.ptr, cf, C.bool(value)))
}
func (q *Query) WhereEqInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_eq_int", C.fl_query_where_eq_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereNeString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_ne_str", C.fl_query_where_ne_str(q.ptr, cf, cv))
}
func (q *Query) WhereNeInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_ne_int", C.fl_query_where_ne_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereGtString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_gt_str", C.fl_query_where_gt_str(q.ptr, cf, cv))
}
func (q *Query) WhereGtInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_gt_int", C.fl_query_where_gt_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereGteString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_gte_str", C.fl_query_where_gte_str(q.ptr, cf, cv))
}
func (q *Query) WhereGteInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_gte_int", C.fl_query_where_gte_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereLtString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_lt_str", C.fl_query_where_lt_str(q.ptr, cf, cv))
}
func (q *Query) WhereLtInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_lt_int", C.fl_query_where_lt_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereLteString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_lte_str", C.fl_query_where_lte_str(q.ptr, cf, cv))
}
func (q *Query) WhereLteInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_lte_int", C.fl_query_where_lte_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereOrString(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_or_str", C.fl_query_where_or_str(q.ptr, cf, cv))
}
func (q *Query) WhereOrInt(field string, value int64) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_where_or_int", C.fl_query_where_or_int(q.ptr, cf, C.int64_t(value)))
}
func (q *Query) WhereIn(field string, arr *Array) error {
	cf, ff := cString(field)
	defer ff()
	ptr := arr.ptr
	arr.ptr = nil
	return checkStatus("fl_query_where_in", C.fl_query_where_in(q.ptr, cf, ptr))
}
func (q *Query) WhereNotIn(field string, arr *Array) error {
	cf, ff := cString(field)
	defer ff()
	ptr := arr.ptr
	arr.ptr = nil
	return checkStatus("fl_query_where_not_in", C.fl_query_where_not_in(q.ptr, cf, ptr))
}
func (q *Query) WhereArrayContainsAny(field string, arr *Array) error {
	cf, ff := cString(field)
	defer ff()
	ptr := arr.ptr
	arr.ptr = nil
	return checkStatus("fl_query_where_array_contains_any", C.fl_query_where_array_contains_any(q.ptr, cf, ptr))
}
func (q *Query) WhereArrayContains(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_array_contains", C.fl_query_where_array_contains(q.ptr, cf, cv))
}
func (q *Query) WhereMatch(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_match", C.fl_query_where_match(q.ptr, cf, cv))
}
func (q *Query) WhereMatchPrefix(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_match_prefix", C.fl_query_where_match_prefix(q.ptr, cf, cv))
}
func (q *Query) WhereContains(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_contains", C.fl_query_where_contains(q.ptr, cf, cv))
}
func (q *Query) WhereStartsWith(field, value string) error {
	cf, ff := cString(field)
	cv, fv := cString(value)
	defer ff()
	defer fv()
	return checkStatus("fl_query_where_starts_with", C.fl_query_where_starts_with(q.ptr, cf, cv))
}
func (q *Query) OrderBy(field string, ascending bool) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_order_by", C.fl_query_order_by(q.ptr, cf, C.bool(ascending)))
}
func (q *Query) Limit(v uintptr) error {
	return checkStatus("fl_query_limit", C.fl_query_limit(q.ptr, C.uintptr_t(v)))
}
func (q *Query) Offset(v uintptr) error {
	return checkStatus("fl_query_offset", C.fl_query_offset(q.ptr, C.uintptr_t(v)))
}
func (q *Query) SelectField(field string) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_select_field", C.fl_query_select_field(q.ptr, cf))
}

// DeferBlobs returns blob-backed fields as placeholders (no blob reads).
func (q *Query) DeferBlobs(deferBlobs bool) error {
	var d C.int
	if deferBlobs {
		d = 1
	}
	return checkStatus("fl_query_defer_blobs", C.fl_query_defer_blobs(q.ptr, d))
}
func (q *Query) StartAfter(anchor *Doc) error {
	return checkStatus("fl_query_start_after", C.fl_query_start_after(q.ptr, anchor.ptr))
}
func (q *Query) StartAt(anchor *Doc) error {
	return checkStatus("fl_query_start_at", C.fl_query_start_at(q.ptr, anchor.ptr))
}
func (q *Query) EndAt(anchor *Doc) error {
	return checkStatus("fl_query_end_at", C.fl_query_end_at(q.ptr, anchor.ptr))
}
func (q *Query) EndBefore(anchor *Doc) error {
	return checkStatus("fl_query_end_before", C.fl_query_end_before(q.ptr, anchor.ptr))
}
func (q *Query) AggregateCount() error {
	return checkStatus("fl_query_aggregate_count", C.fl_query_aggregate_count(q.ptr))
}
func (q *Query) AggregateSum(field string) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_aggregate_sum", C.fl_query_aggregate_sum(q.ptr, cf))
}
func (q *Query) AggregateAvg(field string) error {
	cf, ff := cString(field)
	defer ff()
	return checkStatus("fl_query_aggregate_avg", C.fl_query_aggregate_avg(q.ptr, cf))
}
func (e *Engine) ExecuteQuery(q *Query) (string, error) {
	return ownedCStringJSON(func() *C.char { return C.fl_query_execute(e.ptr, q.ptr) })
}
func (e *Engine) ExecuteAggregation(q *Query) (string, error) {
	return ownedCStringJSON(func() *C.char { return C.fl_query_execute_aggregation(e.ptr, q.ptr) })
}

// Delete executes the query and deletes all matching documents.
// Returns the number of deleted documents.
func (e *Engine) DeleteWhere(q *Query) (int32, error) {
	n := C.fl_query_delete(e.ptr, q.ptr)
	if n < 0 {
		return 0, fmt.Errorf("fl_query_delete failed: %s", lastError())
	}
	return int32(n), nil
}

// DeleteWhereLocal marks every match so the wipe never leaves this device,
// then deletes. Returns the number of deleted documents.
func (e *Engine) DeleteWhereLocal(q *Query) (int32, error) {
	n := C.fl_query_delete_local(e.ptr, q.ptr)
	if n < 0 {
		return 0, fmt.Errorf("fl_query_delete_local failed: %s", lastError())
	}
	return int32(n), nil
}

// PatchWhere executes the query and applies the updates from 'updates' to all matches.
// Returns the number of updated documents.
func (e *Engine) PatchWhere(q *Query, updates *Doc) (int32, error) {
	if updates == nil || updates.ptr == nil {
		return 0, errors.New("patch doc is nil")
	}
	n := C.fl_query_patch(e.ptr, q.ptr, updates.ptr)
	if n < 0 {
		return 0, fmt.Errorf("fl_query_patch failed: %s", lastError())
	}
	return int32(n), nil
}

// ExecuteQueryToHandles runs the query and returns the result as native doc handles,
// avoiding the JSON serialization round-trip.
func (e *Engine) ExecuteQueryToHandles(q *Query) (*ResultSet, error) {
	ptr := C.fl_query_execute_to_handles(e.ptr, q.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("fl_query_execute_to_handles failed: %s", lastError())
	}
	return &ResultSet{ptr: ptr}, nil
}

func (r *ResultSet) Count() uintptr {
	if r == nil || r.ptr == nil {
		return 0
	}
	return uintptr(C.fl_result_set_count(r.ptr))
}

// GetDoc returns the doc handle at the given index. The returned Doc is owned by the
// ResultSet and must not be freed separately.
func (r *ResultSet) GetDoc(index uintptr) (*Doc, error) {
	if r == nil || r.ptr == nil {
		return nil, errors.New("result set is nil")
	}
	ptr := C.fl_result_set_get_doc(r.ptr, C.uintptr_t(index))
	if ptr == nil {
		return nil, nil
	}
	return &Doc{ptr: ptr}, nil
}

func (r *ResultSet) Free() {
	if r != nil && r.ptr != nil {
		C.fl_result_set_free(r.ptr)
		r.ptr = nil
	}
}

// ToJSON renders the whole result set as one JSON array string in a single
// call (no per-doc round trips). Byte-identical to joining per-doc JSON.
func (r *ResultSet) ToJSON() (string, error) {
	if r == nil || r.ptr == nil {
		return "", errors.New("result set is nil")
	}
	ptr := C.fl_result_set_to_json(r.ptr)
	if ptr == nil {
		return "", fmt.Errorf("fl_result_set_to_json failed: %s", lastError())
	}
	defer C.fl_string_free(ptr)
	return C.GoString(ptr), nil
}

// ExecuteQueryRaw runs the query and returns pinned storage bytes per row
// instead of decoded docs (v0.8.3+). Bytes are opaque storage encoding:
// hash, count, export, or resolve them with RawDoc.ToDoc.
func (e *Engine) ExecuteQueryRaw(q *Query) (*RawResultSet, error) {
	ptr := C.fl_query_execute_raw(e.ptr, q.ptr)
	if ptr == nil {
		return nil, fmt.Errorf("fl_query_execute_raw failed: %s", lastError())
	}
	return &RawResultSet{ptr: ptr}, nil
}

func (r *RawResultSet) Count() uintptr {
	if r == nil || r.ptr == nil {
		return 0
	}
	return uintptr(C.fl_rawresult_count(r.ptr))
}

// GetRawDoc returns the raw row at the given index. Borrowed by the
// result set like ResultSet.GetDoc: do not free, free the set first.
func (r *RawResultSet) GetRawDoc(index uintptr) (*RawDoc, error) {
	if r == nil || r.ptr == nil {
		return nil, errors.New("raw result set is nil")
	}
	ptr := C.fl_rawresult_get(r.ptr, C.uintptr_t(index))
	if ptr == nil {
		return nil, nil
	}
	return &RawDoc{ptr: ptr}, nil
}

func (r *RawResultSet) Free() {
	if r != nil && r.ptr != nil {
		C.fl_rawresult_free(r.ptr)
		r.ptr = nil
	}
}

// ID returns the row's doc id (borrowed view copied out).
func (d *RawDoc) ID() (string, error) {
	if d == nil || d.ptr == nil {
		return "", errors.New("raw doc is nil")
	}
	var ln C.uintptr_t
	ptr := C.fl_rawdoc_id(d.ptr, &ln)
	if ptr == nil {
		return "", fmt.Errorf("fl_rawdoc_id failed: %s", lastError())
	}
	return C.GoStringN(ptr, C.int(ln)), nil
}

// Bytes returns a copy of the row's storage-encoded bytes.
func (d *RawDoc) Bytes() ([]byte, error) {
	if d == nil || d.ptr == nil {
		return nil, errors.New("raw doc is nil")
	}
	var ln C.uintptr_t
	ptr := C.fl_rawdoc_bytes(d.ptr, &ln)
	if ptr == nil {
		return nil, fmt.Errorf("fl_rawdoc_bytes failed: %s", lastError())
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(ln)), nil
}

// StartAfterRaw binds the next page's cursor from a raw row (no decode).
func (q *Query) StartAfterRaw(anchor *RawDoc) error {
	return checkStatus("fl_query_start_after_raw", C.fl_query_start_after_raw(q.ptr, anchor.ptr))
}

// ToDoc resolves a raw row into a decoded Doc (blobs inflated).
func (d *RawDoc) ToDoc(e *Engine, collection string) (*Doc, error) {
	cc, free := cString(collection)
	defer free()
	ptr := C.fl_rawdoc_to_doc(e.ptr, d.ptr, cc)
	if ptr == nil {
		return nil, fmt.Errorf("fl_rawdoc_to_doc failed: %s", lastError())
	}
	return &Doc{ptr: ptr}, nil
}

// WalkCallback receives one row per call (id + storage bytes); return
// false to stop early. Bytes are only valid for the call duration.
type WalkCallback func(id string, bytes []byte) bool

//export fireliteWalkBridge
func fireliteWalkBridge(id *C.char, idLen C.uintptr_t, bytes *C.uint8_t, bytesLen C.uintptr_t, userData unsafe.Pointer) C.bool {
	h := cgo.Handle(userData)
	cb, ok := h.Value().(WalkCallback)
	if !ok {
		return C.bool(false)
	}
	return C.bool(cb(C.GoStringN(id, C.int(idLen)), C.GoBytes(unsafe.Pointer(bytes), C.int(bytesLen))))
}

// CursorWalk runs the query as a single zero-alloc walk (v0.8.6+),
// invoking callback per row. Returns rows visited.
func (e *Engine) CursorWalk(q *Query, callback WalkCallback) (int64, error) {
	if callback == nil {
		return -1, errors.New("walk callback is nil")
	}
	h := cgo.NewHandle(callback)
	defer h.Delete()
	n := C.firelite_walk_register(e.ptr, q.ptr, unsafe.Pointer(h))
	if n < 0 {
		return -1, fmt.Errorf("fl_cursor_walk failed: %s", lastError())
	}
	return int64(n), nil
}

// ViewDoc pins storage bytes for lazy per-field pulls (v0.8.11+): no
// decode, no owned construction. Borrowed views never inflate blobs.
func (e *Engine) GetView(collection, docID string) (*ViewDoc, error) {
	cc, freeC := cString(collection)
	defer freeC()
	ci, freeI := cString(docID)
	defer freeI()
	ptr := C.fl_view_get(e.ptr, cc, ci)
	if ptr == nil {
		return nil, nil
	}
	return &ViewDoc{ptr: ptr}, nil
}

func (d *ViewDoc) Free() {
	if d != nil && d.ptr != nil {
		C.fl_view_free(d.ptr)
		d.ptr = nil
	}
}

func (d *ViewDoc) FieldCount() uintptr {
	if d == nil || d.ptr == nil {
		return 0
	}
	return uintptr(C.fl_view_field_count(d.ptr))
}

func (d *ViewDoc) HasField(key string) bool {
	if d == nil || d.ptr == nil {
		return false
	}
	ck, free := cString(key)
	defer free()
	return bool(C.fl_view_has_field(d.ptr, ck))
}

func (d *ViewDoc) GetInt(key string) (int64, bool) {
	if d == nil || d.ptr == nil {
		return 0, false
	}
	ck, free := cString(key)
	defer free()
	var out C.int64_t
	if !bool(C.fl_view_get_int(d.ptr, ck, &out)) {
		return 0, false
	}
	return int64(out), true
}

func (d *ViewDoc) GetFloat(key string) (float64, bool) {
	if d == nil || d.ptr == nil {
		return 0, false
	}
	ck, free := cString(key)
	defer free()
	var out C.double
	if !bool(C.fl_view_get_float(d.ptr, ck, &out)) {
		return 0, false
	}
	return float64(out), true
}

// GetBool returns (value, ok): ok is false when missing or not a bool.
func (d *ViewDoc) GetBool(key string) (bool, bool) {
	if d == nil || d.ptr == nil {
		return false, false
	}
	ck, free := cString(key)
	defer free()
	switch v := C.fl_view_get_bool(d.ptr, ck); v {
	case 1:
		return true, true
	case 0:
		return false, true
	default:
		return false, false
	}
}

// GetString returns a copy of a String field's bytes.
func (d *ViewDoc) GetString(key string) (string, bool) {
	if d == nil || d.ptr == nil {
		return "", false
	}
	ck, free := cString(key)
	defer free()
	var ln C.uintptr_t
	ptr := C.fl_view_get_str(d.ptr, ck, &ln)
	if ptr == nil {
		return "", false
	}
	return C.GoStringN(ptr, C.int(ln)), true
}

// GetBytes returns a copy of a Binary field's bytes.
func (d *ViewDoc) GetBytes(key string) ([]byte, bool) {
	if d == nil || d.ptr == nil {
		return nil, false
	}
	ck, free := cString(key)
	defer free()
	var ln C.uintptr_t
	ptr := C.fl_view_get_bytes(d.ptr, ck, &ln)
	if ptr == nil {
		return nil, false
	}
	return C.GoBytes(unsafe.Pointer(ptr), C.int(ln)), true
}

// ToDoc fully decodes the pinned bytes into an owned Doc.
func (d *ViewDoc) ToDoc(docID string) (*Doc, error) {
	if d == nil || d.ptr == nil {
		return nil, errors.New("view doc is nil")
	}
	ci, free := cString(docID)
	defer free()
	ptr := C.fl_view_to_doc(d.ptr, ci)
	if ptr == nil {
		return nil, fmt.Errorf("fl_view_to_doc failed: %s", lastError())
	}
	return &Doc{ptr: ptr}, nil
}

// ViewWalkCallback receives one row per call as a borrowed view handle
// (valid for the call only); return false to stop early.
type ViewWalkCallback func(id string, view *ViewDoc) bool

//export fireliteViewWalkBridge
func fireliteViewWalkBridge(id *C.char, idLen C.uintptr_t, view *C.FL_ViewDoc, userData unsafe.Pointer) C.bool {
	h := cgo.Handle(userData)
	cb, ok := h.Value().(ViewWalkCallback)
	if !ok {
		return C.bool(false)
	}
	return C.bool(cb(C.GoStringN(id, C.int(idLen)), &ViewDoc{ptr: view}))
}

// CursorWalkView runs the query lending each row as a view (v0.8.11+).
// The ViewDoc handles are borrowed: valid only inside the callback.
func (e *Engine) CursorWalkView(q *Query, callback ViewWalkCallback) (int64, error) {
	if callback == nil {
		return -1, errors.New("walk callback is nil")
	}
	h := cgo.NewHandle(callback)
	defer h.Delete()
	n := C.firelite_view_walk_register(e.ptr, q.ptr, unsafe.Pointer(h))
	if n < 0 {
		return -1, fmt.Errorf("fl_cursor_walk_view failed: %s", lastError())
	}
	return int64(n), nil
}

func ownedCStringJSON(fn func() *C.char) (string, error) {
	ptr := fn()
	if ptr == nil {
		return "", errors.New(lastError())
	}
	defer C.fl_string_free(ptr)
	return C.GoString(ptr), nil
}

//export firelite_watch_bridge
func firelite_watch_bridge(collection *C.char, path *C.char, kind C.int32_t, userData unsafe.Pointer) {
	h := cgo.Handle(userData)
	cb, ok := h.Value().(SnapshotCallback)
	if !ok {
		return
	}
	cb(C.GoString(collection), C.GoString(path), SnapshotKind(kind))
}

func (e *Engine) Watch(collection string, callback SnapshotCallback) (*Watch, error) {
	cc, free := cString(collection)
	defer free()
	h := cgo.NewHandle(callback)
	ptr := C.firelite_watch_bridge_register(e.ptr, cc, unsafe.Pointer(h))
	if ptr == nil {
		h.Delete()
		return nil, fmt.Errorf("fl_engine_watch failed: %s", lastError())
	}
	return &Watch{ptr: ptr, handle: h}, nil
}

func (w *Watch) Close() {
	if w == nil {
		return
	}
	if w.ptr != nil {
		C.fl_watch_free(w.ptr)
		w.ptr = nil
	}
	if w.handle != 0 {
		w.handle.Delete()
		w.handle = 0
	}
}

// Firestore-style high-level facade

type Client struct{ engine *Engine }

func OpenClient(path string) (*Client, error) {
	e, err := Open(path)
	if err != nil {
		return nil, err
	}
	return &Client{engine: e}, nil
}

func (c *Client) Close()                                { c.engine.Close() }
func (c *Client) Collection(name string) *CollectionRef { return &CollectionRef{client: c, name: name} }
func (c *Client) Batch() *WriteBatch                    { return &WriteBatch{client: c, batch: NewBatch()} }

func (c *Client) RunTransaction(fn func(tx *Tx) error) error {
	txHandle, err := c.engine.BeginTransaction()
	if err != nil {
		return err
	}
	defer txHandle.Free()
	tx := &Tx{engine: c.engine, tx: txHandle}
	if err := fn(tx); err != nil {
		return err
	}
	return c.engine.CommitTransaction(txHandle)
}

// Compact triggers storage compaction on the underlying engine.
func (c *Client) Compact() error { return c.engine.Compact() }

// IndexesReady reports whether background indexes have finished building.
func (c *Client) IndexesReady() bool { return c.engine.IsIndexesReady() }

// CloudSync creates a bi-directional cloud sync handle for this engine.
func (c *Client) CloudSync(mode CloudSyncMode, clientID, roomName, roomKey, authToken string) (*CloudSync, error) {
	return c.engine.NewCloudSync(mode, clientID, roomName, roomKey, authToken)
}

// CloudSyncServer creates a room-agnostic cloud SERVER for this engine.
func (c *Client) CloudSyncServer(serverID, authToken string) (*CloudSync, error) {
	return c.engine.NewCloudSyncServer(serverID, authToken)
}

// CloudSyncClient creates an offline-first cloud CLIENT bound to a room.
func (c *Client) CloudSyncClient(clientID, roomName, roomKey, authToken string) (*CloudSync, error) {
	return c.engine.NewCloudSyncClient(clientID, roomName, roomKey, authToken)
}

// SnapshotIndices forces a durable snapshot of the in-memory index metadata.
func (c *Client) SnapshotIndices() error { return c.engine.SnapshotIndices() }

// ListIndexes returns the raw JSON listing of indexes for the collection.
func (c *Client) ListIndexes(collection string) (string, error) {
	return c.engine.ListIndexes(collection)
}

type CollectionRef struct {
	client *Client
	name   string
}

type DocumentRef struct {
	client     *Client
	collection string
	id         string
}

type DocumentSnapshot struct {
	Exists bool
	Data   map[string]any
}

func (c *CollectionRef) Doc(id string) *DocumentRef {
	return &DocumentRef{client: c.client, collection: c.name, id: id}
}

func (d *DocumentRef) Set(data map[string]any) error {
	doc, err := mapToDoc(data)
	if err != nil {
		return err
	}
	defer doc.Free()
	return d.client.engine.Set(d.collection, d.id, doc)
}

func (d *DocumentRef) Update(data map[string]any) error {
	updates, err := mapToDoc(data)
	if err != nil {
		return err
	}
	defer updates.Free()
	return d.client.engine.Patch(d.collection, d.id, updates)
}

func (d *DocumentRef) Delete() error { return d.client.engine.Delete(d.collection, d.id) }

func (d *DocumentRef) Get() (*DocumentSnapshot, error) {
	doc, err := d.client.engine.GetDoc(d.collection, d.id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return &DocumentSnapshot{Exists: false}, nil
	}
	defer doc.Free()
	jsonText, err := doc.ToJSON()
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(jsonText), &payload); err != nil {
		return nil, err
	}
	return &DocumentSnapshot{Exists: true, Data: payload}, nil
}

func (c *CollectionRef) Where(field, op string, value any) *QueryRef {
	q := &QueryRef{client: c.client, collection: c.name}
	_ = q.Where(field, op, value)
	return q
}

func (c *CollectionRef) OrderBy(field string, ascending bool) *QueryRef {
	q := &QueryRef{client: c.client, collection: c.name}
	_ = q.OrderBy(field, ascending)
	return q
}

func (c *CollectionRef) Get() ([]map[string]any, error) {
	return (&QueryRef{client: c.client, collection: c.name}).Get()
}

type QueryRef struct {
	client     *Client
	collection string
	ops        []func(*Query) error
}

func (q *QueryRef) Where(field, op string, value any) *QueryRef {
	q.ops = append(q.ops, func(raw *Query) error {
		switch op {
		case "==":
			switch v := value.(type) {
			case string:
				return raw.WhereEqString(field, v)
			case bool:
				return raw.WhereEqBool(field, v)
			case int:
				return raw.WhereEqInt(field, int64(v))
			case int64:
				return raw.WhereEqInt(field, v)
			default:
				return fmt.Errorf("unsupported == type %T", value)
			}
		case "!=", ">", ">=", "<", "<=":
			switch v := value.(type) {
			case string:
				switch op {
				case "!=":
					return raw.WhereNeString(field, v)
				case ">":
					return raw.WhereGtString(field, v)
				case ">=":
					return raw.WhereGteString(field, v)
				case "<":
					return raw.WhereLtString(field, v)
				default:
					return raw.WhereLteString(field, v)
				}
			case int:
				return applyNumericWhere(raw, field, op, int64(v))
			case int64:
				return applyNumericWhere(raw, field, op, v)
			default:
				return fmt.Errorf("unsupported %s type %T", op, value)
			}
		case "in", "not-in", "array-contains-any":
			arr, err := anyToArray(value)
			if err != nil {
				return err
			}
			switch op {
			case "in":
				return raw.WhereIn(field, arr)
			case "not-in":
				return raw.WhereNotIn(field, arr)
			default:
				return raw.WhereArrayContainsAny(field, arr)
			}
		case "array-contains":
			return raw.WhereArrayContains(field, fmt.Sprint(value))
		case "match":
			return raw.WhereMatch(field, fmt.Sprint(value))
		case "contains":
			return raw.WhereContains(field, fmt.Sprint(value))
		case "startsWith":
			return raw.WhereStartsWith(field, fmt.Sprint(value))
		default:
			return fmt.Errorf("unsupported operator %q", op)
		}
	})
	return q
}

func applyNumericWhere(raw *Query, field, op string, value int64) error {
	switch op {
	case "!=":
		return raw.WhereNeInt(field, value)
	case ">":
		return raw.WhereGtInt(field, value)
	case ">=":
		return raw.WhereGteInt(field, value)
	case "<":
		return raw.WhereLtInt(field, value)
	default:
		return raw.WhereLteInt(field, value)
	}
}

func (q *QueryRef) OrderBy(field string, ascending bool) *QueryRef {
	q.ops = append(q.ops, func(raw *Query) error { return raw.OrderBy(field, ascending) })
	return q
}
func (q *QueryRef) Limit(v int) *QueryRef {
	q.ops = append(q.ops, func(raw *Query) error { return raw.Limit(uintptr(v)) })
	return q
}
func (q *QueryRef) Offset(v int) *QueryRef {
	q.ops = append(q.ops, func(raw *Query) error { return raw.Offset(uintptr(v)) })
	return q
}
func (q *QueryRef) Select(fields ...string) *QueryRef {
	q.ops = append(q.ops, func(raw *Query) error {
		for _, f := range fields {
			if err := raw.SelectField(f); err != nil {
				return err
			}
		}
		return nil
	})
	return q
}

// DeferBlobs returns blob-backed fields as placeholders (no blob reads).
func (q *QueryRef) DeferBlobs() *QueryRef {
	q.ops = append(q.ops, func(raw *Query) error { return raw.DeferBlobs(true) })
	return q
}

func (q *QueryRef) Get() ([]map[string]any, error) {
	raw := NewQuery(q.collection)
	defer raw.Free()
	for _, op := range q.ops {
		if err := op(raw); err != nil {
			return nil, err
		}
	}
	jsonText, err := q.client.engine.ExecuteQuery(raw)
	if err != nil {
		return nil, err
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(jsonText), &rows); err != nil {
		return nil, err
	}
	return rows, nil
}

// Delete executes the query and removes every matching document. Returns the deleted count.
func (q *QueryRef) Delete() (int32, error) {
	raw := NewQuery(q.collection)
	defer raw.Free()
	for _, op := range q.ops {
		if err := op(raw); err != nil {
			return 0, err
		}
	}
	return q.client.engine.DeleteWhere(raw)
}

// Patch applies the given field updates to every matching document. Returns the updated count.
func (q *QueryRef) Patch(data map[string]any) (int32, error) {
	raw := NewQuery(q.collection)
	defer raw.Free()
	for _, op := range q.ops {
		if err := op(raw); err != nil {
			return 0, err
		}
	}
	updates, err := mapToDoc(data)
	if err != nil {
		return 0, err
	}
	defer updates.Free()
	return q.client.engine.PatchWhere(raw, updates)
}

type WriteBatch struct {
	client *Client
	batch  *Batch
}

func (b *WriteBatch) Set(docRef *DocumentRef, data map[string]any) *WriteBatch {
	doc, err := mapToDoc(data)
	if err != nil {
		panic(err)
	}
	defer doc.Free()
	if err := b.batch.Set(docRef.collection, docRef.id, doc); err != nil {
		panic(err)
	}
	return b
}

func (b *WriteBatch) Delete(docRef *DocumentRef) *WriteBatch {
	if err := b.batch.Delete(docRef.collection, docRef.id); err != nil {
		panic(err)
	}
	return b
}

func (b *WriteBatch) Commit() error {
	defer b.batch.Free()
	return b.client.engine.CommitBatch(b.batch)
}

type Tx struct {
	engine *Engine
	tx     *Transaction
}

func (t *Tx) Get(docRef *DocumentRef) (*DocumentSnapshot, error) {
	doc, err := t.engine.TxGet(t.tx, docRef.collection, docRef.id)
	if err != nil {
		return nil, err
	}
	if doc == nil {
		return &DocumentSnapshot{Exists: false}, nil
	}
	defer doc.Free()
	js, err := doc.ToJSON()
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal([]byte(js), &payload); err != nil {
		return nil, err
	}
	return &DocumentSnapshot{Exists: true, Data: payload}, nil
}

func (t *Tx) Set(docRef *DocumentRef, data map[string]any) error {
	doc, err := mapToDoc(data)
	if err != nil {
		return err
	}
	defer doc.Free()
	return t.tx.Set(docRef.collection, docRef.id, doc)
}

func mapToDoc(data map[string]any) (*Doc, error) {
	doc := NewDoc()
	for k, v := range data {
		if err := insertAny(doc, k, v); err != nil {
			doc.Free()
			return nil, err
		}
	}
	return doc, nil
}

func anyToArray(value any) (*Array, error) {
	arr := NewArray()
	vals, ok := value.([]any)
	if !ok {
		switch v := value.(type) {
		case []string:
			for _, s := range v {
				if err := arr.AppendString(s); err != nil {
					return nil, err
				}
			}
			return arr, nil
		case []int:
			for _, i := range v {
				if err := arr.AppendInt(int64(i)); err != nil {
					return nil, err
				}
			}
			return arr, nil
		case []int64:
			for _, i := range v {
				if err := arr.AppendInt(i); err != nil {
					return nil, err
				}
			}
			return arr, nil
		default:
			return nil, fmt.Errorf("expected array value, got %T", value)
		}
	}
	for _, item := range vals {
		switch v := item.(type) {
		case string:
			if err := arr.AppendString(v); err != nil {
				return nil, err
			}
		case int:
			if err := arr.AppendInt(int64(v)); err != nil {
				return nil, err
			}
		case int64:
			if err := arr.AppendInt(v); err != nil {
				return nil, err
			}
		default:
			return nil, fmt.Errorf("unsupported array value %T", v)
		}
	}
	return arr, nil
}

func insertAny(doc *Doc, key string, value any) error {
	switch v := value.(type) {
	case nil:
		return doc.InsertNull(key)
	case string:
		return doc.InsertString(key, v)
	case bool:
		return doc.InsertBool(key, v)
	case int:
		return doc.InsertInt(key, int64(v))
	case int32:
		return doc.InsertInt(key, int64(v))
	case int64:
		return doc.InsertInt(key, v)
	case float32:
		return doc.InsertFloat(key, float64(v))
	case float64:
		return doc.InsertFloat(key, v)
	case []byte:
		return doc.InsertBinary(key, v)
	case map[string]any:
		child, err := mapToDoc(v)
		if err != nil {
			return err
		}
		defer child.Free()
		return doc.InsertDoc(key, child)
	case []any:
		arr := NewArray()
		defer arr.Free()
		for _, item := range v {
			switch t := item.(type) {
			case string:
				if err := arr.AppendString(t); err != nil {
					return err
				}
			case int:
				if err := arr.AppendInt(int64(t)); err != nil {
					return err
				}
			case int64:
				if err := arr.AppendInt(t); err != nil {
					return err
				}
			case map[string]any:
				d, err := mapToDoc(t)
				if err != nil {
					return err
				}
				defer d.Free()
				if err := arr.AppendDoc(d); err != nil {
					return err
				}
			default:
				return fmt.Errorf("unsupported array item %T", t)
			}
		}
		return doc.InsertArray(key, arr)
	case TimestampMicros:
		return doc.InsertTimestamp(key, int64(v))
	case ServerTimestamp:
		return doc.InsertServerTimestamp(key)
	case Reference:
		return doc.InsertReference(key, v.Collection, v.DocID)
	default:
		return fmt.Errorf("unsupported value for key %q: %T", key, v)
	}
}
