unit FireLite;

{$mode objfpc}{$H+}
{$macro on}

interface

uses
  Classes, SysUtils, fpjson, jsonparser, SyncObjs, ctypes, FireLiteRaw;

type
  EFireLiteError = class(Exception);

  TFLDurabilityMode = (dmAlways, dmInterval, dmManual, dmOnCommit);

  TFLCloudSyncMode = (csmServer, csmClient);

  TFLDiscoveryMode = (dmMdns = 0, dmBroadcast = 1, dmBoth = 2);

  TOnSnapshotCallback = procedure(const JsonSnapshot: string) of object;

  IFLSubscription = interface
    ['{91E1FC85-5B6F-4F66-B070-8698E120AF7E}']
    procedure Stop;
  end;

  TFireLite = class;
  TFLCollection = class;
  TFLDocument = class;
  TFLArray = class;
  TFLQuery = class;
  TFLRawDoc = class;
  TFLRawResultSet = class;
  TFLViewDoc = class;
  TFLBatch = class;
  TFLTransaction = class;
  TFLDocumentRef = class;
  TFLCloudSync = class;

  { TFLArray: Builder for List/Array types }
  TFLArray = class
  private
    FHandle: PFL_Array;
    FOwned: Boolean;
    procedure EnsureHandle;
  public
    constructor Create;
    destructor Destroy; override;
    function AppendStr(const Value: string): TFLArray;
    function AppendInt(Value: Int64): TFLArray;
    function AppendDoc(ADoc: TFLDocument): TFLArray;
    property Handle: PFL_Array read FHandle;
  end;

  { TFLConfig: Advanced Configuration Builder }
  TFLConfig = class
  private
    FHandle: PFL_Config;
  public
    constructor Create;
    destructor Destroy; override;
    function SetDurability(Mode: TFLDurabilityMode): TFLConfig;
    function SetEncryptionKey(const Key: string): TFLConfig;
    function SetEncryptedCollections(const Collections: array of string): TFLConfig;
    function SetAuditLog(Enabled: Boolean; const LogPath: string = ''): TFLConfig;
    function SetQueryWorkers(Count: NativeUInt): TFLConfig;
    function SetMemoryLimits(MMapSize, MaxInlinedBytes: NativeUInt): TFLConfig;
    function SetStorageTuning(PageSize, CompactionThreshold, GroupCommitMaxOps: NativeUInt): TFLConfig;
    function SetBlobThreshold(ThresholdBytes: NativeUInt): TFLConfig;
    function SetCompression(Enabled: Boolean; Level: Integer = 3): TFLConfig;
    property Handle: PFL_Config read FHandle;
  end;

  { TFLDocument: Binary Document Model }
  TFLDocument = class
  private
    FHandle: PFL_Doc;
    FOwned: Boolean;
  public
    constructor Create; overload;
    constructor CreateFromHandle(AHandle: PFL_Doc; AOwned: Boolean); overload;
    destructor Destroy; override;

    function InsertStr(const Key, Value: string): TFLDocument;
    function InsertInt(const Key: string; Value: Int64): TFLDocument;
    function InsertFloat(const Key: string; Value: Double): TFLDocument;
    function InsertBool(const Key: string; Value: Boolean): TFLDocument;
    function InsertNull(const Key: string): TFLDocument;
    function InsertBin(const Key: string; Data: PByte; Len: NativeUInt): TFLDocument;
    function InsertTimestamp(const Key: string; Micros: Int64): TFLDocument;
    function InsertServerTimestamp(const Key: string): TFLDocument;
    function InsertDoc(const Key: string; ADoc: TFLDocument): TFLDocument;
    function InsertArray(const Key: string; AArray: TFLArray): TFLDocument;
    function InsertRef(const Key, TargetCol, TargetID: string): TFLDocument;

    class function FromJSON(const Obj: TJSONObject): TFLDocument;
    function ToJSON: string;
    property Handle: PFL_Doc read FHandle;
  end;

  { TFLBatch: Atomic Write Operations }
  TFLBatch = class
  private
    FDBHandle: PFL_Engine;
    FHandle: PFL_Batch;
    FCommitted: Boolean;
  public
    constructor Create(ADBHandle: PFL_Engine);
    destructor Destroy; override;
    function SetDoc(const Col, ID: string; Doc: TFLDocument): TFLBatch;
    function Delete(const Col, ID: string): TFLBatch;
    procedure Commit;
  end;

  { TFLTransaction: Serializable RMW }
  TFLTransaction = class
  private
    FDBHandle: PFL_Engine;
    FHandle: PFL_Transaction;
  public
    constructor Create(ADBHandle: PFL_Engine);
    destructor Destroy; override;
    function Get(const Col, ID: string): TFLDocument;
    procedure SetDoc(const Col, ID: string; Doc: TFLDocument);
    procedure Commit;
  end;

  { TFLRawDoc: borrowed row of a raw result set (v0.8.3+). The wrapper is
    yours to free; the native handle belongs to the TFLRawResultSet. }
  TFLRawDoc = class
  private
    FDB: TFireLite;
    FHandle: PFL_RawDoc;
  public
    constructor CreateBorrowed(ADB: TFireLite; AHandle: PFL_RawDoc);
    function Id: string;
    function Bytes: TBytes;
    { Decode into an owned TFLDocument (blobs inflated). Free the result. }
    function Resolve(const Collection: string): TFLDocument;
    property Handle: PFL_RawDoc read FHandle;
  end;

  { TFLRawResultSet: pinned storage bytes per row (v0.8.3+). }
  TFLRawResultSet = class
  private
    FDB: TFireLite;
    FHandle: PFL_RawResultSet;
  public
    constructor Create(ADB: TFireLite; AHandle: PFL_RawResultSet);
    destructor Destroy; override;
    function Count: NativeUInt;
    { Borrowed row — valid until Free/Destroy. Free the wrapper, not the handle. }
    function Get(Index: NativeUInt): TFLRawDoc;
    procedure Free;
    property Handle: PFL_RawResultSet read FHandle;
  end;

  { TFLViewDoc: owned pinned-bytes handle with lazy typed pulls (v0.8.11+).
    No decode, no owned construction; strict scalar matches. Free it. }
  TFLViewDoc = class
  private
    FHandle: PFL_ViewDoc;
  public
    constructor Create(AHandle: PFL_ViewDoc);
    destructor Destroy; override;
    function FieldCount: NativeUInt;
    function HasField(const Key: string): Boolean;
    function GetInt(const Key: string; out Value: Int64): Boolean;
    function GetFloat(const Key: string; out Value: Double): Boolean;
    function GetBool(const Key: string; out Value: Boolean): Boolean;
    function GetStr(const Key: string): string;
    function GetBytes(const Key: string): TBytes;
    { Full owned decode. Free the result. }
    function ToDoc(const DocID: string): TFLDocument;
    property Handle: PFL_ViewDoc read FHandle;
  end;

  { TFLQuery: Optimized Parallel Query Engine }
  TFLQuery = class
  private
    FDB: TFireLite;
    FCollection: string;
    FWhereStr: array of record Field, Value, Op: string; end;
    FWhereInt: array of record Field: string; Value: Int64; Op: string; end;
    FWhereBool: array of record Field: string; Value: Boolean; end;
    FWhereIn: array of record Field: string; Data: TJSONArray; end;
    FWhereNotIn: array of record Field: string; Data: TJSONArray; end;
    FWhereArrayContainsAny: array of record Field: string; Data: TJSONArray; end;
    FOrderByField: string;
    FOrderByAsc: Boolean;
FLimit, FOffset: NativeUInt;
FHasLimit, FHasOffset: Boolean;
FDeferBlobs: Boolean;
    FSelectFields: TStringList;
    FStartAt, FStartAfter, FEndAt, FEndBefore: PFL_Doc;
    FStartAfterRaw: PFL_RawDoc;
    FWhereOrStr: array of record Field, Value: string; end;
    FWhereOrInt: array of record Field: string; Value: Int64; end;

    function BuildNativeQuery: PFL_Query;
  public
    constructor Create(ADB: TFireLite; const ACollection: string);
    destructor Destroy; override;

    function WhereEqStr(const Field, Value: string): TFLQuery;
    function WhereEqBool(const Field: string; Value: Boolean): TFLQuery;
    function WhereEqInt(const Field: string; Value: Int64): TFLQuery;
    function WhereNeStr(const Field, Value: string): TFLQuery;
    function WhereNeInt(const Field: string; Value: Int64): TFLQuery;
    function WhereGtStr(const Field, Value: string): TFLQuery;
    function WhereGtInt(const Field: string; Value: Int64): TFLQuery;
    function WhereGteStr(const Field, Value: string): TFLQuery;
    function WhereGteInt(const Field: string; Value: Int64): TFLQuery;
    function WhereLtStr(const Field, Value: string): TFLQuery;
    function WhereLtInt(const Field: string; Value: Int64): TFLQuery;
    function WhereLteStr(const Field, Value: string): TFLQuery;
    function WhereLteInt(const Field: string; Value: Int64): TFLQuery;
    function WhereIn(const Field: string; const Values: array of const): TFLQuery;
    function WhereNotIn(const Field: string; const Values: array of const): TFLQuery;
    function ArrayContains(const Field, Value: string): TFLQuery;
    function ArrayContainsAny(const Field: string; const Values: array of const): TFLQuery;
    function Match(const Field, Value: string): TFLQuery;
    function MatchPrefix(const Field, Value: string): TFLQuery;
    function Contains(const Field, Value: string): TFLQuery;
    function StartsWith(const Field, Value: string): TFLQuery;
    
    function OrderBy(const Field: string; Ascending: Boolean = True): TFLQuery;
    function Limit(ACount: NativeUInt): TFLQuery;
    function Offset(ACount: NativeUInt): TFLQuery;
    function Select(const Fields: array of string): TFLQuery;
{ Blob fields come back as placeholders (no blob reads); resolve per doc. }
function DeferBlobs(Defer: Boolean = True): TFLQuery;

    function StartAt(ASnapshot: TFLDocument): TFLQuery;
    function StartAfter(ASnapshot: TFLDocument): TFLQuery;
    function StartAfterRaw(ARaw: TFLRawDoc): TFLQuery;
    function EndAt(ASnapshot: TFLDocument): TFLQuery;
    function EndBefore(ASnapshot: TFLDocument): TFLQuery;

    function WhereOrStr(const Field, Value: string): TFLQuery;
    function WhereOrInt(const Field: string; Value: Int64): TFLQuery;

    function Count: Int64;
    function Sum(const Field: string): Double;
    function Avg(const Field: string): Double;

    function GetJSON: string;
    { Raw execution (v0.8.3+): pinned bytes per row. Free the result. }
    function ExecuteRaw: TFLRawResultSet;
    { Zero-alloc walk (v0.8.6+): one call per scan. Returns rows visited. }
    function Walk(Callback: TFL_WalkCallback; UserData: Pointer): Int64;
    { Lazy view walk (v0.8.11+): each row lent as a borrowed view handle. }
    function WalkView(Callback: TFL_ViewWalkCallback; UserData: Pointer): Int64;
    function Delete: Int64;
    function DeleteLocal: Int64;
    function Patch(Doc: TFLDocument): Int64;
    function OnSnapshot(const Callback: TOnSnapshotCallback; QueueToMainThread: Boolean = True): IFLSubscription;
  end;

  TFLDocumentRef = class
  private
    FDB: TFireLite;
    FCollection, FDocID: string;
  public
    constructor Create(ADB: TFireLite; const ACollection, ADocID: string);
    procedure SetDoc(const Doc: TFLDocument);
    function Get: TFLDocument;
    procedure Delete;
    procedure DeleteLocal;
  end;

  TFLCollection = class
  private
    FDB: TFireLite;
    FName: string;
  public
    constructor Create(ADB: TFireLite; const AName: string);
    function Doc(const DocID: string): TFLDocumentRef;
    function Query: TFLQuery;
    
    { Shortcut Methods }
    function WhereEqStr(const Field, Value: string): TFLQuery;
    function WhereEqInt(const Field: string; Value: Int64): TFLQuery;
    function Match(const Field, Value: string): TFLQuery;
    function Limit(ACount: NativeUInt): TFLQuery;

    procedure CreateIndex(const Field: string);
    procedure CreateFTSIndex(const Field: string);
    procedure CreateCompositeIndex(const Fields: array of string);
    function ListIndexes: string;
  end;

  TFLNetSyncer = class
  private
    FHandle: PFL_NetSyncer;
  public
    constructor Create(ADBHandle: PFL_Engine; const Name, RoomKey: string);
    destructor Destroy; override;
    procedure Start(APort: Word);
    procedure SetDiscoveryMode(AMode: TFLDiscoveryMode);
    function StatusJSON: string;
  end;

  TFLCloudSync = class
  private
    FHandle: PFL_CloudSync;
  public
    constructor Create(ADBHandle: PFL_Engine; Mode: TFLCloudSyncMode; const ClientID, RoomName, RoomKey, AuthToken: string);
    constructor CreateServer(ADBHandle: PFL_Engine; const ServerID, AuthToken: string);
    constructor CreateClient(ADBHandle: PFL_Engine; const ClientID, RoomName, RoomKey, AuthToken: string);
    destructor Destroy; override;
    procedure Start(const Address: string);
    function StatusJSON: string;
    procedure Stop;
  end;

  TFireLite = class
  private
    FHandle: PFL_Engine;
  public
    constructor Create(const DBPath: string); overload;
    constructor Create(const DBPath: string; AConfig: TFLConfig); overload;
    destructor Destroy; override;
    function Collection(const Name: string): TFLCollection;
    function ListCollections: TStringList;
    function ListIndexes(const ACollection: string): string;
    function GetStats: string;
    function GetAuditLog: string;
    function Backup(const Path: string): Integer;
    procedure Compact;
    function IsIndexesReady: Boolean;
    { Borrowed point view (v0.8.11+): lazy pulls, no decode. Free it.
      Returns nil when missing. }
    function GetView(const Col, ID: string): TFLViewDoc;
    procedure SnapshotIndices;
    function InsertSubDoc(const Col, ID, SubCol, SubID: string; Doc: TFLDocument): Integer;
    function GetByRef(Doc: TFLDocument; const FieldKey: string): TFLDocument;
    procedure CreateCompositeIndex(const ACollection: string; const Fields: array of string);
    function StartBatch: TFLBatch;
    function StartTransaction: TFLTransaction;
    procedure SetCollectionLocal(const ACollection: string; Local: Boolean);
    procedure ReplicateKey(const ACollection, ADocID: string);
    procedure ReplicateCollection(const ACollection: string);
    function VacuumCollection(const ACollection: string): Integer;
    function CreateNetSyncer(const Name, RoomKey: string): TFLNetSyncer;
    function CreateCloudSyncer(Mode: TFLCloudSyncMode; const ClientID, RoomName, RoomKey, AuthToken: string): TFLCloudSync;
    function CreateCloudServerSyncer(const ServerID, AuthToken: string): TFLCloudSync;
    function CreateCloudClientSyncer(const ClientID, RoomName, RoomKey, AuthToken: string): TFLCloudSync;
    property Handle: PFL_Engine read FHandle;
  end;

implementation

{ Internal Helpers }

function ConsumeCString(P: PChar): string;
begin
  if P = nil then Exit('');
  Result := string(P);
  fl_string_free(P);
end;

procedure CheckStatus(Code: cint32; const Context: string);
var P: PChar;
begin
  if Code <> 0 then begin
    P := fl_last_error;
    raise EFireLiteError.CreateFmt('%s failed: %s', [Context, string(P)]);
  end;
end;

{ TFLArray }

constructor TFLArray.Create;
begin FHandle := fl_array_new; FOwned := True; end;

destructor TFLArray.Destroy;
begin if FOwned and (FHandle <> nil) then fl_array_free(FHandle); inherited; end;

procedure TFLArray.EnsureHandle;
begin if FHandle = nil then raise EFireLiteError.Create('Array handle consumed'); end;

function TFLArray.AppendStr(const Value: string): TFLArray;
begin EnsureHandle; fl_array_append_str(FHandle, PChar(Value)); Result := Self; end;

function TFLArray.AppendInt(Value: Int64): TFLArray;
begin EnsureHandle; fl_array_append_int(FHandle, Value); Result := Self; end;

function TFLArray.AppendDoc(ADoc: TFLDocument): TFLArray;
begin EnsureHandle; fl_array_append_doc(FHandle, ADoc.Handle); Result := Self; end;

{ TFLConfig }

constructor TFLConfig.Create;
begin
  inherited Create;
  FHandle := fl_config_new;
end;

destructor TFLConfig.Destroy;
begin
  if FHandle <> nil then fl_config_free(FHandle);
  inherited;
end;

function TFLConfig.SetDurability(Mode: TFLDurabilityMode): TFLConfig;
begin
  fl_config_set_durability(FHandle, Ord(Mode));
  Result := Self;
end;

function TFLConfig.SetEncryptionKey(const Key: string): TFLConfig;
begin
  fl_config_set_encryption_key(FHandle, PChar(Key));
  Result := Self;
end;

function TFLConfig.SetAuditLog(Enabled: Boolean; const LogPath: string): TFLConfig;
begin
  if LogPath = '' then
    fl_config_set_audit_log(FHandle, Enabled, nil)
  else
    fl_config_set_audit_log(FHandle, Enabled, PChar(LogPath));
  Result := Self;
end;

function TFLConfig.SetQueryWorkers(Count: NativeUInt): TFLConfig;
begin
  fl_config_set_query_workers(FHandle, Count);
  Result := Self;
end;

function TFLConfig.SetMemoryLimits(MMapSize, MaxInlinedBytes: NativeUInt): TFLConfig;
begin
  fl_config_set_memory_limits(FHandle, MMapSize, MaxInlinedBytes);
  Result := Self;
end;

function TFLConfig.SetEncryptedCollections(const Collections: array of string): TFLConfig;
var
  I: Integer;
  S: string;
begin
  S := '[';
  for I := Low(Collections) to High(Collections) do begin
    if I > Low(Collections) then S := S + ',';
    S := S + '"' + Collections[I] + '"';
  end;
  S := S + ']';
  CheckStatus(fl_config_set_encrypted_collections(FHandle, PChar(S)), 'SetEncryptedCollections');
  Result := Self;
end;

function TFLConfig.SetStorageTuning(PageSize, CompactionThreshold, GroupCommitMaxOps: NativeUInt): TFLConfig;
begin
  fl_config_set_storage_tuning(FHandle, PageSize, CompactionThreshold, GroupCommitMaxOps);
  Result := Self;
end;

function TFLConfig.SetBlobThreshold(ThresholdBytes: NativeUInt): TFLConfig;
begin
  fl_config_set_blob_threshold(FHandle, ThresholdBytes);
  Result := Self;
end;

function TFLConfig.SetCompression(Enabled: Boolean; Level: Integer): TFLConfig;
begin
  fl_config_set_compression(FHandle, Enabled, Level);
  Result := Self;
end;

{ TFLDocument }

constructor TFLDocument.Create;
begin FHandle := fl_doc_new; FOwned := True; end;

constructor TFLDocument.CreateFromHandle(AHandle: PFL_Doc; AOwned: Boolean);
begin FHandle := AHandle; FOwned := AOwned; end;

destructor TFLDocument.Destroy;
begin if FOwned and (FHandle <> nil) then fl_doc_free(FHandle); inherited; end;

function TFLDocument.InsertStr(const Key, Value: string): TFLDocument;
begin fl_doc_insert_str(FHandle, PChar(Key), PChar(Value)); Result := Self; end;

function TFLDocument.InsertInt(const Key: string; Value: Int64): TFLDocument;
begin fl_doc_insert_int(FHandle, PChar(Key), Value); Result := Self; end;

function TFLDocument.InsertFloat(const Key: string; Value: Double): TFLDocument;
begin fl_doc_insert_float(FHandle, PChar(Key), Value); Result := Self; end;

function TFLDocument.InsertBool(const Key: string; Value: Boolean): TFLDocument;
begin fl_doc_insert_bool(FHandle, PChar(Key), Value); Result := Self; end;

function TFLDocument.InsertNull(const Key: string): TFLDocument;
begin fl_doc_insert_null(FHandle, PChar(Key)); Result := Self; end;

function TFLDocument.InsertBin(const Key: string; Data: PByte; Len: NativeUInt): TFLDocument;
begin fl_doc_insert_bin(FHandle, PChar(Key), Data, Len); Result := Self; end;

function TFLDocument.InsertTimestamp(const Key: string; Micros: Int64): TFLDocument;
begin fl_doc_insert_timestamp(FHandle, PChar(Key), Micros); Result := Self; end;

function TFLDocument.InsertServerTimestamp(const Key: string): TFLDocument;
begin fl_doc_insert_server_timestamp(FHandle, PChar(Key)); Result := Self; end;

function TFLDocument.InsertDoc(const Key: string; ADoc: TFLDocument): TFLDocument;
begin fl_doc_insert_doc(FHandle, PChar(Key), ADoc.Handle); Result := Self; end;

function TFLDocument.InsertArray(const Key: string; AArray: TFLArray): TFLDocument;
begin
  CheckStatus(fl_doc_insert_array(FHandle, PChar(Key), AArray.Handle), 'InsertArray');
  AArray.FHandle := nil; // Handled by Rust ownership
  Result := Self;
end;

function TFLDocument.InsertRef(const Key, TargetCol, TargetID: string): TFLDocument;
begin fl_doc_insert_reference(FHandle, PChar(Key), PChar(TargetCol), PChar(TargetID)); Result := Self; end;

class function TFLDocument.FromJSON(const Obj: TJSONObject): TFLDocument;
var 
  I, J: Integer; 
  Key: string; 
  Data: TJSONData;
  SubArr: TFLArray;
begin
  Result := TFLDocument.Create;
  for I := 0 to Obj.Count - 1 do begin
    Key := Obj.Names[I]; Data := Obj.Items[I];
    case Data.JSONType of
      jtNull: Result.InsertNull(Key);
      jtBoolean: Result.InsertBool(Key, Data.AsBoolean);
      jtNumber: if Pos('.', Data.AsJSON) > 0 then Result.InsertFloat(Key, Data.AsFloat) else Result.InsertInt(Key, Data.AsInt64);
      jtString: Result.InsertStr(Key, Data.AsString);
      jtObject: Result.InsertDoc(Key, TFLDocument.FromJSON(TJSONObject(Data)));
      jtArray: begin
        SubArr := TFLArray.Create;
        for J := 0 to TJSONArray(Data).Count - 1 do begin
           if TJSONArray(Data).Items[J].JSONType = jtObject then
             SubArr.AppendDoc(TFLDocument.FromJSON(TJSONObject(TJSONArray(Data).Items[J])))
           else if TJSONArray(Data).Items[J].JSONType = jtNumber then
             SubArr.AppendInt(TJSONArray(Data).Items[J].AsInt64)
           else
             SubArr.AppendStr(TJSONArray(Data).Items[J].AsString);
        end;
        Result.InsertArray(Key, SubArr);
      end;
    end;
  end;
end;

function TFLDocument.ToJSON: string;
begin Result := ConsumeCString(fl_doc_to_json(FHandle)); end;

{ TFLBatch }

constructor TFLBatch.Create(ADBHandle: PFL_Engine);
begin inherited Create; FDBHandle := ADBHandle; FHandle := fl_batch_new; end;

destructor TFLBatch.Destroy;
begin if (FHandle <> nil) and not FCommitted then fl_batch_free(FHandle); inherited; end;

function TFLBatch.SetDoc(const Col, ID: string; Doc: TFLDocument): TFLBatch;
begin CheckStatus(fl_batch_set(FHandle, PChar(Col), PChar(ID), Doc.Handle), 'BatchSet'); Result := Self; end;

function TFLBatch.Delete(const Col, ID: string): TFLBatch;
begin CheckStatus(fl_batch_delete(FHandle, PChar(Col), PChar(ID)), 'BatchDelete'); Result := Self; end;

procedure TFLBatch.Commit;
begin CheckStatus(fl_batch_commit(FDBHandle, FHandle), 'BatchCommit'); FCommitted := True; end;

{ TFLTransaction }

constructor TFLTransaction.Create(ADBHandle: PFL_Engine);
begin inherited Create; FDBHandle := ADBHandle; FHandle := fl_transaction_begin(FDBHandle); end;

destructor TFLTransaction.Destroy;
begin if FHandle <> nil then fl_transaction_free(FHandle); inherited; end;

function TFLTransaction.Get(const Col, ID: string): TFLDocument;
var H: PFL_Doc;
begin
  H := fl_transaction_get(FDBHandle, FHandle, PChar(Col), PChar(ID));
  if H = nil then Exit(nil);
  Result := TFLDocument.CreateFromHandle(H, True);
end;

procedure TFLTransaction.SetDoc(const Col, ID: string; Doc: TFLDocument);
begin CheckStatus(fl_transaction_set(FHandle, PChar(Col), PChar(ID), Doc.Handle), 'TxSet'); end;

procedure TFLTransaction.Commit;
begin CheckStatus(fl_transaction_commit(FDBHandle, FHandle), 'TxCommit'); end;

{ TFLQuery }

constructor TFLQuery.Create(ADB: TFireLite; const ACollection: string);
begin FDB := ADB; FCollection := ACollection; FSelectFields := TStringList.Create; end;

destructor TFLQuery.Destroy;
var I: Integer; begin
  FSelectFields.Free;
  for I := Low(FWhereIn) to High(FWhereIn) do FWhereIn[I].Data.Free;
  for I := Low(FWhereNotIn) to High(FWhereNotIn) do FWhereNotIn[I].Data.Free;
  for I := Low(FWhereArrayContainsAny) to High(FWhereArrayContainsAny) do FWhereArrayContainsAny[I].Data.Free;
  inherited;
end;

function TFLQuery.WhereEqStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := '=='; Result := Self; end;

function TFLQuery.WhereEqBool(const Field: string; Value: Boolean): TFLQuery;
var L: Integer; begin L := Length(FWhereBool); SetLength(FWhereBool, L + 1); FWhereBool[L].Field := Field; FWhereBool[L].Value := Value; Result := Self; end;

function TFLQuery.WhereEqInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; FWhereInt[L].Op := 'eq'; Result := Self; end;

function TFLQuery.WhereNeStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'ne'; Result := Self; end;

function TFLQuery.WhereNeInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; FWhereInt[L].Op := 'ne'; Result := Self; end;

function TFLQuery.WhereGtStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'gt'; Result := Self; end;

function TFLQuery.WhereGtInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; FWhereInt[L].Op := 'gt'; Result := Self; end;

function TFLQuery.WhereGteStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'gte'; Result := Self; end;

function TFLQuery.WhereGteInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; FWhereInt[L].Op := 'gte'; Result := Self; end;

function TFLQuery.WhereLtStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'lt'; Result := Self; end;

function TFLQuery.WhereLtInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; FWhereInt[L].Op := 'lt'; Result := Self; end;

function TFLQuery.WhereLteStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'lte'; Result := Self; end;

function TFLQuery.WhereLteInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; FWhereInt[L].Op := 'lte'; Result := Self; end;

function TFLQuery.Match(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'match'; Result := Self; end;

function TFLQuery.MatchPrefix(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'match_prefix'; Result := Self; end;

function TFLQuery.Contains(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'contains'; Result := Self; end;

function TFLQuery.StartsWith(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'starts_with'; Result := Self; end;

function TFLQuery.WhereIn(const Field: string; const Values: array of const): TFLQuery;
var L, I: Integer;
begin
  L := Length(FWhereIn); SetLength(FWhereIn, L + 1);
  FWhereIn[L].Field := Field; FWhereIn[L].Data := TJSONArray.Create;
  for I := Low(Values) to High(Values) do begin
    case Values[I].VType of
      vtInteger: FWhereIn[L].Data.Add(Values[I].VInteger);
      vtInt64: FWhereIn[L].Data.Add(Values[I].VInt64^);
      vtAnsiString: FWhereIn[L].Data.Add(string(Values[I].VAnsiString));
    end;
  end;
  Result := Self;
end;

function TFLQuery.WhereNotIn(const Field: string; const Values: array of const): TFLQuery;
var L, I: Integer;
begin
  L := Length(FWhereNotIn); SetLength(FWhereNotIn, L + 1);
  FWhereNotIn[L].Field := Field; FWhereNotIn[L].Data := TJSONArray.Create;
  for I := Low(Values) to High(Values) do begin
    case Values[I].VType of
      vtInteger: FWhereNotIn[L].Data.Add(Values[I].VInteger);
      vtInt64: FWhereNotIn[L].Data.Add(Values[I].VInt64^);
      vtAnsiString: FWhereNotIn[L].Data.Add(string(Values[I].VAnsiString));
    end;
  end;
  Result := Self;
end;

function TFLQuery.ArrayContains(const Field, Value: string): TFLQuery;
var L: Integer;
begin
  L := Length(FWhereStr); SetLength(FWhereStr, L + 1);
  FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'array_contains';
  Result := Self;
end;

function TFLQuery.ArrayContainsAny(const Field: string; const Values: array of const): TFLQuery;
var L, I: Integer;
begin
  L := Length(FWhereArrayContainsAny); SetLength(FWhereArrayContainsAny, L + 1);
  FWhereArrayContainsAny[L].Field := Field; FWhereArrayContainsAny[L].Data := TJSONArray.Create;
  for I := Low(Values) to High(Values) do begin
    case Values[I].VType of
      vtInteger: FWhereArrayContainsAny[L].Data.Add(Values[I].VInteger);
      vtInt64: FWhereArrayContainsAny[L].Data.Add(Values[I].VInt64^);
      vtAnsiString: FWhereArrayContainsAny[L].Data.Add(string(Values[I].VAnsiString));
    end;
  end;
  Result := Self;
end;

function TFLQuery.OrderBy(const Field: string; Ascending: Boolean): TFLQuery;
begin FOrderByField := Field; FOrderByAsc := Ascending; Result := Self; end;

function TFLQuery.Limit(ACount: NativeUInt): TFLQuery;
begin FLimit := ACount; FHasLimit := True; Result := Self; end;

function TFLQuery.Offset(ACount: NativeUInt): TFLQuery;
begin FOffset := ACount; FHasOffset := True; Result := Self; end;

function TFLQuery.DeferBlobs(Defer: Boolean): TFLQuery;
begin FDeferBlobs := Defer; Result := Self; end;

function TFLQuery.StartAt(ASnapshot: TFLDocument): TFLQuery;
begin
  if ASnapshot <> nil then FStartAt := ASnapshot.Handle;
  Result := Self;
end;

function TFLQuery.StartAfter(ASnapshot: TFLDocument): TFLQuery;
begin
  if ASnapshot <> nil then FStartAfter := ASnapshot.Handle;
  Result := Self;
end;

function TFLQuery.EndAt(ASnapshot: TFLDocument): TFLQuery;
begin
  if ASnapshot <> nil then FEndAt := ASnapshot.Handle;
  Result := Self;
end;

function TFLQuery.EndBefore(ASnapshot: TFLDocument): TFLQuery;
begin
  if ASnapshot <> nil then FEndBefore := ASnapshot.Handle;
  Result := Self;
end;

function TFLQuery.Select(const Fields: array of string): TFLQuery;
var I: Integer; begin FSelectFields.Clear; for I := Low(Fields) to High(Fields) do FSelectFields.Add(Fields[I]); Result := Self; end;

function TFLQuery.BuildNativeQuery: PFL_Query;
var I, J: Integer; TmpArr: PFL_Array;
begin
  Result := fl_query_new(PChar(FCollection));
  try
    for I := Low(FWhereStr) to High(FWhereStr) do begin
      if FWhereStr[I].Op = 'match' then fl_query_where_match(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'match_prefix' then fl_query_where_match_prefix(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'contains' then fl_query_where_contains(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'starts_with' then fl_query_where_starts_with(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'ne' then fl_query_where_ne_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'gt' then fl_query_where_gt_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'gte' then fl_query_where_gte_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'lt' then fl_query_where_lt_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'lte' then fl_query_where_lte_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'array_contains' then fl_query_where_array_contains(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else fl_query_where_eq_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value));
    end;
    for I := Low(FWhereInt) to High(FWhereInt) do begin
      if FWhereInt[I].Op = 'ne' then fl_query_where_ne_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value)
      else if FWhereInt[I].Op = 'gt' then fl_query_where_gt_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value)
      else if FWhereInt[I].Op = 'gte' then fl_query_where_gte_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value)
      else if FWhereInt[I].Op = 'lt' then fl_query_where_lt_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value)
      else if FWhereInt[I].Op = 'lte' then fl_query_where_lte_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value)
      else fl_query_where_eq_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value);
    end;
    for I := Low(FWhereBool) to High(FWhereBool) do begin
      fl_query_where_eq_bool(Result, PChar(FWhereBool[I].Field), FWhereBool[I].Value);
    end;
    for I := Low(FWhereOrStr) to High(FWhereOrStr) do begin
      fl_query_where_or_str(Result, PChar(FWhereOrStr[I].Field), PChar(FWhereOrStr[I].Value));
    end;
    for I := Low(FWhereOrInt) to High(FWhereOrInt) do begin
      fl_query_where_or_int(Result, PChar(FWhereOrInt[I].Field), FWhereOrInt[I].Value);
    end;
    for I := Low(FWhereIn) to High(FWhereIn) do begin
      TmpArr := fl_array_new;
      for J := 0 to FWhereIn[I].Data.Count-1 do
        if FWhereIn[I].Data.Items[J].JSONType = jtNumber then fl_array_append_int(TmpArr, FWhereIn[I].Data.Items[J].AsInt64)
        else fl_array_append_str(TmpArr, PChar(FWhereIn[I].Data.Items[J].AsString));
      fl_query_where_in(Result, PChar(FWhereIn[I].Field), TmpArr);
    end;
    for I := Low(FWhereNotIn) to High(FWhereNotIn) do begin
      TmpArr := fl_array_new;
      for J := 0 to FWhereNotIn[I].Data.Count-1 do
        if FWhereNotIn[I].Data.Items[J].JSONType = jtNumber then fl_array_append_int(TmpArr, FWhereNotIn[I].Data.Items[J].AsInt64)
        else fl_array_append_str(TmpArr, PChar(FWhereNotIn[I].Data.Items[J].AsString));
      fl_query_where_not_in(Result, PChar(FWhereNotIn[I].Field), TmpArr);
    end;
    for I := Low(FWhereArrayContainsAny) to High(FWhereArrayContainsAny) do begin
      TmpArr := fl_array_new;
      for J := 0 to FWhereArrayContainsAny[I].Data.Count-1 do
        if FWhereArrayContainsAny[I].Data.Items[J].JSONType = jtNumber then fl_array_append_int(TmpArr, FWhereArrayContainsAny[I].Data.Items[J].AsInt64)
        else fl_array_append_str(TmpArr, PChar(FWhereArrayContainsAny[I].Data.Items[J].AsString));
      fl_query_where_array_contains_any(Result, PChar(FWhereArrayContainsAny[I].Field), TmpArr);
    end;
    
    if FStartAt <> nil then fl_query_start_at(Result, FStartAt);
    if FStartAfter <> nil then fl_query_start_after(Result, FStartAfter);
    if FStartAfterRaw <> nil then fl_query_start_after_raw(Result, FStartAfterRaw);
    if FEndAt <> nil then fl_query_end_at(Result, FEndAt);
    if FEndBefore <> nil then fl_query_end_before(Result, FEndBefore);

    if FOrderByField <> '' then fl_query_order_by(Result, PChar(FOrderByField), FOrderByAsc);
    if FHasLimit then fl_query_limit(Result, FLimit);
    if FHasOffset then fl_query_offset(Result, FOffset);
    for I := 0 to FSelectFields.Count - 1 do fl_query_select_field(Result, PChar(FSelectFields[I]));
if FDeferBlobs then fl_query_defer_blobs(Result, 1);
  except fl_query_free(Result); raise; end;
end;

function TFLQuery.Count: Int64;
var Q: PFL_Query; J: TJSONObject; begin
  Q := BuildNativeQuery; try fl_query_aggregate_count(Q);
  J := TJSONObject(TJSONParser.Create(ConsumeCString(fl_query_execute_aggregation(FDB.Handle, Q))).Parse);
  Result := J.Get('count', 0); J.Free; finally fl_query_free(Q); end;
end;

function TFLQuery.Sum(const Field: string): Double;
var Q: PFL_Query; J: TJSONObject; begin
  Q := BuildNativeQuery; try fl_query_aggregate_sum(Q, PChar(Field));
  J := TJSONObject(TJSONParser.Create(ConsumeCString(fl_query_execute_aggregation(FDB.Handle, Q))).Parse);
  Result := J.Get('sum_'+Field, 0.0); J.Free; finally fl_query_free(Q); end;
end;

function TFLQuery.Avg(const Field: string): Double;
var Q: PFL_Query; J: TJSONObject; begin
  Q := BuildNativeQuery; try fl_query_aggregate_avg(Q, PChar(Field));
  J := TJSONObject(TJSONParser.Create(ConsumeCString(fl_query_execute_aggregation(FDB.Handle, Q))).Parse);
  Result := J.Get('avg_'+Field, 0.0); J.Free; finally fl_query_free(Q); end;
end;

function TFLQuery.GetJSON: string;
var Q: PFL_Query; begin Q := BuildNativeQuery; try Result := ConsumeCString(fl_query_execute(FDB.Handle, Q)); finally fl_query_free(Q); end; end;

{ TFLRawDoc }

constructor TFLRawDoc.CreateBorrowed(ADB: TFireLite; AHandle: PFL_RawDoc);
begin FDB := ADB; FHandle := AHandle; end;

function TFLRawDoc.Id: string;
var P: PChar; L: SizeUInt;
begin
  P := fl_rawdoc_id(FHandle, @L);
  if (P = nil) or (L = 0) then Exit('');
  SetString(Result, P, L);
end;

function TFLRawDoc.Bytes: TBytes;
var P: PByte; L: SizeUInt;
begin
  P := fl_rawdoc_bytes(FHandle, @L);
  if P = nil then Exit(nil);
  SetLength(Result, L);
  if L > 0 then Move(P^, Result[0], L);
end;

function TFLRawDoc.Resolve(const Collection: string): TFLDocument;
var H: PFL_Doc;
begin
  H := fl_rawdoc_to_doc(FDB.Handle, FHandle, PChar(Collection));
  if H = nil then raise EFireLiteError.Create('fl_rawdoc_to_doc failed: ' + string(fl_last_error));
  Result := TFLDocument.CreateFromHandle(H, True);
end;

{ TFLRawResultSet }

constructor TFLRawResultSet.Create(ADB: TFireLite; AHandle: PFL_RawResultSet);
begin FDB := ADB; FHandle := AHandle; end;

destructor TFLRawResultSet.Destroy;
begin Free; inherited; end;

function TFLRawResultSet.Count: NativeUInt;
begin Result := fl_rawresult_count(FHandle); end;

function TFLRawResultSet.Get(Index: NativeUInt): TFLRawDoc;
var H: PFL_RawDoc;
begin
  H := fl_rawresult_get(FHandle, Index);
  if H = nil then raise EFireLiteError.Create('raw row out of range');
  Result := TFLRawDoc.CreateBorrowed(FDB, H);
end;

procedure TFLRawResultSet.Free;
begin if FHandle <> nil then begin fl_rawresult_free(FHandle); FHandle := nil; end; end;

function TFLQuery.StartAfterRaw(ARaw: TFLRawDoc): TFLQuery;
begin FStartAfterRaw := ARaw.Handle; Result := Self; end;

function TFLQuery.ExecuteRaw: TFLRawResultSet;
var Q: PFL_Query; H: PFL_RawResultSet;
begin
  Q := BuildNativeQuery; try
    H := fl_query_execute_raw(FDB.Handle, Q);
    if H = nil then raise EFireLiteError.Create('fl_query_execute_raw failed: ' + string(fl_last_error));
    Result := TFLRawResultSet.Create(FDB, H);
  finally fl_query_free(Q); end;
end;

function TFLQuery.Walk(Callback: TFL_WalkCallback; UserData: Pointer): Int64;
var Q: PFL_Query;
begin
  Q := BuildNativeQuery; try
    Result := fl_cursor_walk(FDB.Handle, Q, Callback, UserData);
    if Result < 0 then raise EFireLiteError.Create('fl_cursor_walk failed: ' + string(fl_last_error));
  finally fl_query_free(Q); end;
end;

function TFLQuery.WalkView(Callback: TFL_ViewWalkCallback; UserData: Pointer): Int64;
var Q: PFL_Query;
begin
  Q := BuildNativeQuery; try
    Result := fl_cursor_walk_view(FDB.Handle, Q, Callback, UserData);
    if Result < 0 then raise EFireLiteError.Create('fl_cursor_walk_view failed: ' + string(fl_last_error));
  finally fl_query_free(Q); end;
end;

{ TFLViewDoc }

constructor TFLViewDoc.Create(AHandle: PFL_ViewDoc);
begin FHandle := AHandle; end;

destructor TFLViewDoc.Destroy;
begin if FHandle <> nil then fl_view_free(FHandle); inherited; end;

function TFLViewDoc.FieldCount: NativeUInt;
begin Result := fl_view_field_count(FHandle); end;

function TFLViewDoc.HasField(const Key: string): Boolean;
begin Result := fl_view_has_field(FHandle, PChar(Key)); end;

function TFLViewDoc.GetInt(const Key: string; out Value: Int64): Boolean;
var V: Int64;
begin
  Result := fl_view_get_int(FHandle, PChar(Key), @V);
  if Result then Value := V;
end;

function TFLViewDoc.GetFloat(const Key: string; out Value: Double): Boolean;
var V: Double;
begin
  Result := fl_view_get_float(FHandle, PChar(Key), @V);
  if Result then Value := V;
end;

function TFLViewDoc.GetBool(const Key: string; out Value: Boolean): Boolean;
var R: cint32;
begin
  R := fl_view_get_bool(FHandle, PChar(Key));
  Result := R >= 0;
  if Result then Value := R <> 0;
end;

function TFLViewDoc.GetStr(const Key: string): string;
var P: PChar; L: SizeUInt;
begin
  P := fl_view_get_str(FHandle, PChar(Key), @L);
  if (P = nil) or (L = 0) then Exit('');
  SetString(Result, P, L);
end;

function TFLViewDoc.GetBytes(const Key: string): TBytes;
var P: PByte; L: SizeUInt;
begin
  P := fl_view_get_bytes(FHandle, PChar(Key), @L);
  if P = nil then Exit(nil);
  SetLength(Result, L);
  if L > 0 then Move(P^, Result[0], L);
end;

function TFLViewDoc.ToDoc(const DocID: string): TFLDocument;
var H: PFL_Doc;
begin
  H := fl_view_to_doc(FHandle, PChar(DocID));
  if H = nil then raise EFireLiteError.Create('fl_view_to_doc failed: ' + string(fl_last_error));
  Result := TFLDocument.CreateFromHandle(H, True);
end;

function TFLQuery.WhereOrStr(const Field, Value: string): TFLQuery;
begin
  SetLength(FWhereOrStr, Length(FWhereOrStr) + 1);
  FWhereOrStr[High(FWhereOrStr)].Field := Field;
  FWhereOrStr[High(FWhereOrStr)].Value := Value;
  Result := Self;
end;

function TFLQuery.WhereOrInt(const Field: string; Value: Int64): TFLQuery;
begin
  SetLength(FWhereOrInt, Length(FWhereOrInt) + 1);
  FWhereOrInt[High(FWhereOrInt)].Field := Field;
  FWhereOrInt[High(FWhereOrInt)].Value := Value;
  Result := Self;
end;

function TFLQuery.Delete: Int64;
var Q: PFL_Query;
begin
  Q := BuildNativeQuery; try
    Result := fl_query_delete(FDB.Handle, Q);
  finally fl_query_free(Q); end;
end;

function TFLQuery.DeleteLocal: Int64;
var Q: PFL_Query;
begin
  Q := BuildNativeQuery; try
    Result := fl_query_delete_local(FDB.Handle, Q);
  finally fl_query_free(Q); end;
end;

function TFLQuery.Patch(Doc: TFLDocument): Int64;
var Q: PFL_Query;
begin
  Q := BuildNativeQuery; try
    Result := fl_query_patch(FDB.Handle, Q, Doc.Handle);
  finally fl_query_free(Q); end;
end;

type
  TFLPollingThread = class(TThread)
  private
    FQuery: TFLQuery;
    FCallback: TOnSnapshotCallback;
    FQueueToMain: Boolean;
    FInterval: Cardinal;
    procedure DoCallback;
  protected
    procedure Execute; override;
  public
    constructor Create(AQuery: TFLQuery; ACallback: TOnSnapshotCallback; AQueueToMain: Boolean; AInterval: Cardinal);
  end;

  TFLPollingSubscription = class(TInterfacedObject, IFLSubscription)
  private
    FThread: TFLPollingThread;
  public
    constructor Create(AThread: TFLPollingThread);
    procedure Stop;
    destructor Destroy; override;
  end;

constructor TFLPollingThread.Create(AQuery: TFLQuery; ACallback: TOnSnapshotCallback; AQueueToMain: Boolean; AInterval: Cardinal);
begin
  inherited Create(False);
  FQuery := AQuery; FCallback := ACallback; FQueueToMain := AQueueToMain; FInterval := AInterval;
  FreeOnTerminate := False;
end;

procedure TFLPollingThread.DoCallback;
begin
  FCallback(FQuery.GetJSON);
end;

procedure TFLPollingThread.Execute;
begin
  while not Terminated do begin
    try
      if FQueueToMain then
        Queue(@DoCallback)
      else
        DoCallback;
    except end;
    Sleep(FInterval);
  end;
end;

constructor TFLPollingSubscription.Create(AThread: TFLPollingThread);
begin
  inherited Create;
  FThread := AThread;
end;

procedure TFLPollingSubscription.Stop;
begin
  FThread.Terminate;
end;

destructor TFLPollingSubscription.Destroy;
begin
  FThread.Terminate;
  FThread.WaitFor;
  FThread.Free;
  inherited;
end;

function TFLQuery.OnSnapshot(const Callback: TOnSnapshotCallback; QueueToMainThread: Boolean): IFLSubscription;
begin
  Result := TFLPollingSubscription.Create(TFLPollingThread.Create(Self, Callback, QueueToMainThread, 1000));
end;

{ TFLDocumentRef }

constructor TFLDocumentRef.Create(ADB: TFireLite; const ACollection, ADocID: string);
begin
  inherited Create;
  FDB := ADB; FCollection := ACollection; FDocID := ADocID;
end;

procedure TFLDocumentRef.SetDoc(const Doc: TFLDocument);
begin
  CheckStatus(fl_engine_insert(FDB.Handle, PChar(FCollection), PChar(FDocID), Doc.Handle), 'DocRefSet');
end;

function TFLDocumentRef.Get: TFLDocument;
var H: PFL_Doc;
begin
  H := fl_engine_get(FDB.Handle, PChar(FCollection), PChar(FDocID));
  if H = nil then Exit(nil);
  Result := TFLDocument.CreateFromHandle(H, True);
end;

procedure TFLDocumentRef.Delete;
begin
  CheckStatus(fl_engine_delete(FDB.Handle, PChar(FCollection), PChar(FDocID)), 'DocRefDelete');
end;

procedure TFLDocumentRef.DeleteLocal;
begin
  CheckStatus(fl_engine_delete_local(FDB.Handle, PChar(FCollection), PChar(FDocID)), 'DocRefDeleteLocal');
end;

{ TFLCollection }

constructor TFLCollection.Create(ADB: TFireLite; const AName: string); begin inherited Create; FDB := ADB; FName := AName; end;
function TFLCollection.Doc(const DocID: string): TFLDocumentRef; begin Result := TFLDocumentRef.Create(FDB, FName, DocID); end;
function TFLCollection.Query: TFLQuery; begin Result := TFLQuery.Create(FDB, FName); end;
function TFLCollection.WhereEqStr(const Field, Value: string): TFLQuery; begin Result := Query.WhereEqStr(Field, Value); end;
function TFLCollection.WhereEqInt(const Field: string; Value: Int64): TFLQuery; begin Result := Query.WhereEqInt(Field, Value); end;
function TFLCollection.Match(const Field, Value: string): TFLQuery; begin Result := Query.Match(Field, Value); end;
function TFLCollection.Limit(ACount: NativeUInt): TFLQuery; begin Result := Query.Limit(ACount); end;

{ TFLNetSyncer }

constructor TFLNetSyncer.Create(ADBHandle: PFL_Engine; const Name, RoomKey: string);
begin
  inherited Create;
  FHandle := fl_net_syncer_new(ADBHandle, PChar(Name), PChar(RoomKey));
  if FHandle = nil then
    raise Exception.Create('CreateNetSyncer failed: ' + string(fl_last_error));
end;

destructor TFLNetSyncer.Destroy;
begin
  if FHandle <> nil then fl_net_syncer_free(FHandle);
  inherited;
end;

procedure TFLNetSyncer.Start(APort: Word);
begin
  CheckStatus(fl_net_syncer_start(FHandle, APort), 'NetSyncStart');
end;

procedure TFLNetSyncer.SetDiscoveryMode(AMode: TFLDiscoveryMode);
begin
  CheckStatus(fl_net_syncer_set_discovery(FHandle, Ord(AMode)), 'NetSyncSetDiscovery');
end;

function TFLNetSyncer.StatusJSON: string;
begin
  Result := ConsumeCString(fl_net_syncer_status(FHandle));
end;

{ TFLCloudSync }

constructor TFLCloudSync.Create(ADBHandle: PFL_Engine; Mode: TFLCloudSyncMode; const ClientID, RoomName, RoomKey, AuthToken: string);
begin
  inherited Create;
  FHandle := fl_cloud_sync_new(ADBHandle, Ord(Mode), PChar(ClientID), PChar(RoomName), PChar(RoomKey), PChar(AuthToken));
  if FHandle = nil then
    raise Exception.Create('CreateCloudSyncer failed: ' + string(fl_last_error));
end;

constructor TFLCloudSync.CreateServer(ADBHandle: PFL_Engine; const ServerID, AuthToken: string);
begin
  inherited Create;
  FHandle := fl_cloud_sync_server_new(ADBHandle, PChar(ServerID), PChar(AuthToken));
  if FHandle = nil then
    raise Exception.Create('CreateCloudServerSyncer failed: ' + string(fl_last_error));
end;

constructor TFLCloudSync.CreateClient(ADBHandle: PFL_Engine; const ClientID, RoomName, RoomKey, AuthToken: string);
begin
  inherited Create;
  FHandle := fl_cloud_sync_client_new(ADBHandle, PChar(ClientID), PChar(RoomName), PChar(RoomKey), PChar(AuthToken));
  if FHandle = nil then
    raise Exception.Create('CreateCloudClientSyncer failed: ' + string(fl_last_error));
end;

destructor TFLCloudSync.Destroy;
begin
  if FHandle <> nil then fl_cloud_sync_free(FHandle);
  inherited;
end;

procedure TFLCloudSync.Start(const Address: string);
begin
  CheckStatus(fl_cloud_sync_start(FHandle, PChar(Address)), 'CloudSyncStart');
end;

function TFLCloudSync.StatusJSON: string;
begin
  Result := ConsumeCString(fl_cloud_sync_status(FHandle));
end;

procedure TFLCloudSync.Stop;
begin
  fl_cloud_sync_stop(FHandle);
end;
procedure TFLCollection.CreateIndex(const Field: string); begin CheckStatus(fl_engine_create_simple_index(FDB.Handle, PChar(FName), PChar(Field)), 'CreateIndex'); end;
procedure TFLCollection.CreateFTSIndex(const Field: string); begin CheckStatus(fl_engine_create_fts_index(FDB.Handle, PChar(FName), PChar(Field)), 'CreateFTSIndex'); end;

procedure TFLCollection.CreateCompositeIndex(const Fields: array of string);
var I: Integer; S: string;
begin
  S := '[';
  for I := Low(Fields) to High(Fields) do begin
    if I > Low(Fields) then S := S + ',';
    S := S + '{"field":"' + Fields[I] + '","desc":false}';
  end;
  S := S + ']';
  CheckStatus(fl_engine_create_index(FDB.Handle, PChar(FName), PChar(S)), 'CreateCompositeIndex');
end;

function TFLCollection.ListIndexes: string;
begin Result := ConsumeCString(fl_engine_list_indexes(FDB.Handle, PChar(FName))); end;

{ TFireLite }

constructor TFireLite.Create(const DBPath: string); begin inherited Create; FHandle := fl_engine_open(PChar(DBPath)); end;
constructor TFireLite.Create(const DBPath: string; AConfig: TFLConfig); begin inherited Create; FHandle := fl_engine_open_with_config(PChar(DBPath), AConfig.Handle); AConfig.FHandle := nil; end;
destructor TFireLite.Destroy; begin if FHandle <> nil then fl_engine_free(FHandle); inherited; end;
function TFireLite.Collection(const Name: string): TFLCollection; begin Result := TFLCollection.Create(Self, Name); end;
function TFireLite.StartBatch: TFLBatch; begin Result := TFLBatch.Create(FHandle); end;
function TFireLite.StartTransaction: TFLTransaction; begin Result := TFLTransaction.Create(FHandle); end;
procedure TFireLite.SetCollectionLocal(const ACollection: string; Local: Boolean);
var L: cint32;
begin
  if Local then L := 1 else L := 0;
  CheckStatus(fl_engine_set_collection_local(FHandle, PChar(ACollection), L), 'SetCollectionLocal');
end;
procedure TFireLite.ReplicateKey(const ACollection, ADocID: string);
begin
  CheckStatus(fl_engine_replicate_key(FHandle, PChar(ACollection), PChar(ADocID)), 'ReplicateKey');
end;
procedure TFireLite.ReplicateCollection(const ACollection: string);
begin
  CheckStatus(fl_engine_replicate_collection(FHandle, PChar(ACollection)), 'ReplicateCollection');
end;
function TFireLite.VacuumCollection(const ACollection: string): Integer;
var R: cint32;
begin
  R := fl_engine_vacuum_collection(FHandle, PChar(ACollection));
  if R < 0 then CheckStatus(R, 'VacuumCollection');
  Result := R;
end;
function TFireLite.CreateNetSyncer(const Name, RoomKey: string): TFLNetSyncer; begin Result := TFLNetSyncer.Create(FHandle, Name, RoomKey); end;
function TFireLite.CreateCloudSyncer(Mode: TFLCloudSyncMode; const ClientID, RoomName, RoomKey, AuthToken: string): TFLCloudSync; begin Result := TFLCloudSync.Create(FHandle, Mode, ClientID, RoomName, RoomKey, AuthToken); end;
function TFireLite.CreateCloudServerSyncer(const ServerID, AuthToken: string): TFLCloudSync; begin Result := TFLCloudSync.CreateServer(FHandle, ServerID, AuthToken); end;
function TFireLite.CreateCloudClientSyncer(const ClientID, RoomName, RoomKey, AuthToken: string): TFLCloudSync; begin Result := TFLCloudSync.CreateClient(FHandle, ClientID, RoomName, RoomKey, AuthToken); end;
function TFireLite.Backup(const Path: string): Integer; begin Result := fl_engine_backup(FHandle, PChar(Path)); end;
procedure TFireLite.Compact; begin CheckStatus(fl_engine_compact(FHandle), 'Compact'); end;
function TFireLite.IsIndexesReady: Boolean; begin Result := fl_engine_is_indexes_ready(FHandle); end;

function TFireLite.GetView(const Col, ID: string): TFLViewDoc;
var H: PFL_ViewDoc;
begin
  H := fl_view_get(FHandle, PChar(Col), PChar(ID));
  if H = nil then Exit(nil);
  Result := TFLViewDoc.Create(H);
end;
procedure TFireLite.SnapshotIndices; begin CheckStatus(fl_engine_snapshot_indices(FHandle), 'SnapshotIndices'); end;
function TFireLite.ListIndexes(const ACollection: string): string; begin Result := ConsumeCString(fl_engine_list_indexes(FHandle, PChar(ACollection))); end;
function TFireLite.GetAuditLog: string; begin Result := ConsumeCString(fl_engine_get_audit_log(FHandle)); end;
function TFireLite.InsertSubDoc(const Col, ID, SubCol, SubID: string; Doc: TFLDocument): Integer;
begin Result := fl_engine_insert_subdoc(FHandle, PChar(Col), PChar(ID), PChar(SubCol), PChar(SubID), Doc.Handle); end;
function TFireLite.GetByRef(Doc: TFLDocument; const FieldKey: string): TFLDocument;
var H: PFL_Doc;
begin
  H := fl_engine_get_by_ref(FHandle, Doc.Handle, PChar(FieldKey));
  if H = nil then Exit(nil);
  Result := TFLDocument.CreateFromHandle(H, True);
end;
procedure TFireLite.CreateCompositeIndex(const ACollection: string; const Fields: array of string);
var I: Integer; S: string;
begin
  S := '[';
  for I := Low(Fields) to High(Fields) do begin
    if I > Low(Fields) then S := S + ',';
    S := S + '{"field":"' + Fields[I] + '","desc":false}';
  end;
  S := S + ']';
  CheckStatus(fl_engine_create_index(FHandle, PChar(ACollection), PChar(S)), 'CreateCompositeIndex');
end;
function TFireLite.ListCollections: TStringList; var S: string; P: TJSONParser; A: TJSONArray; I: Integer; begin Result := TStringList.Create; S := ConsumeCString(fl_engine_list_collections(FHandle)); if S = '' then Exit; P := TJSONParser.Create(S); try A := TJSONArray(P.Parse); for I := 0 to A.Count - 1 do Result.Add(A.Strings[I]); finally P.Free; end; end;
function TFireLite.GetStats: string; begin Result := ConsumeCString(fl_engine_get_stats(FHandle)); end;

end.
