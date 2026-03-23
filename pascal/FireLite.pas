unit FireLite;

{$mode objfpc}{$H+}
{$macro on}

interface

uses
  Classes, SysUtils, fpjson, jsonparser, SyncObjs, ctypes, FireLiteRaw;

type
  EFireLiteError = class(Exception);

  TFLDurabilityMode = (dmAlways, dmInterval, dmManual, dmOnCommit);

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
  TFLBatch = class;
  TFLTransaction = class;
  TFLDocumentRef = class;

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
    function SetAuditLog(Enabled: Boolean; const LogPath: string = ''): TFLConfig;
    function SetQueryWorkers(Count: NativeUInt): TFLConfig;
    function SetMemoryLimits(MMapSize, MaxInlinedBytes: NativeUInt): TFLConfig;
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
    function Set(const Col, ID: string; Doc: TFLDocument): TFLBatch;
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
    procedure Set(const Col, ID: string; Doc: TFLDocument);
    procedure Commit;
  end;

  { TFLQuery: Optimized Parallel Query Engine }
  TFLQuery = class
  private
    FDB: TFireLite;
    FCollection: string;
    FWhereStr: array of record Field, Value, Op: string; end;
    FWhereInt: array of record Field: string; Value: Int64; end;
    FWhereIn: array of record Field: string; Data: TJSONArray; end;
    FOrderByField: string;
    FOrderByAsc: Boolean;
    FLimit, FOffset: NativeUInt;
    FHasLimit, FHasOffset: Boolean;
    FStartAfter: PFL_Doc;
    FSelectFields: TStringList;

    function BuildNativeQuery: PFL_Query;
  public
    constructor Create(ADB: TFireLite; const ACollection: string);
    destructor Destroy; override;

    function WhereEqStr(const Field, Value: string): TFLQuery;
    function WhereEqInt(const Field: string; Value: Int64): TFLQuery;
    function WhereIn(const Field: string; const Values: array of const): TFLQuery;
    function Match(const Field, Value: string): TFLQuery;
    function Contains(const Field, Value: string): TFLQuery;
    function StartsWith(const Field, Value: string): TFLQuery;
    
    function OrderBy(const Field: string; Ascending: Boolean = True): TFLQuery;
    function Limit(ACount: NativeUInt): TFLQuery;
    function Offset(ACount: NativeUInt): TFLQuery;
    function StartAfter(ASnapshot: TFLDocument): TFLQuery;
    function [Select](const Fields: array of string): TFLQuery;

    function Count: Int64;
    function Sum(const Field: string): Double;
    function Avg(const Field: string): Double;

    function GetJSON: string;
    function OnSnapshot(const Callback: TOnSnapshotCallback; QueueToMainThread: Boolean = True): IFLSubscription;
  end;

  TFLDocumentRef = class
  private
    FDB: TFireLite;
    FCollection, FDocID: string;
  public
    constructor Create(ADB: TFireLite; const ACollection, ADocID: string);
    procedure [Set](const Doc: TFLDocument);
    function Get: TFLDocument;
    procedure Delete;
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
    function GetStats: string;
    function StartBatch: TFLBatch;
    function StartTransaction: TFLTransaction;
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

function TFLConfig.SetCompression(Enabled: Boolean; Level: Integer): TFLConfig;
begin
  if Enabled then
    fl_config_set_storage_tuning(FHandle, 4096, 1024, NativeUInt(Level), 256)
  else
    fl_config_set_storage_tuning(FHandle, 4096, 1024, 0, 256);
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

function TFLBatch.Set(const Col, ID: string; Doc: TFLDocument): TFLBatch;
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

procedure TFLTransaction.Set(const Col, ID: string; Doc: TFLDocument);
begin CheckStatus(fl_transaction_set(FHandle, PChar(Col), PChar(ID), Doc.Handle), 'TxSet'); end;

procedure TFLTransaction.Commit;
begin CheckStatus(fl_transaction_commit(FDBHandle, FHandle), 'TxCommit'); FHandle := nil; end;

{ TFLQuery }

constructor TFLQuery.Create(ADB: TFireLite; const ACollection: string);
begin FDB := ADB; FCollection := ACollection; FSelectFields := TStringList.Create; end;

destructor TFLQuery.Destroy;
var I: Integer; begin
  FSelectFields.Free;
  for I := Low(FWhereIn) to High(FWhereIn) do FWhereIn[I].Data.Free;
  inherited;
end;

function TFLQuery.WhereEqStr(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := '=='; Result := Self; end;

function TFLQuery.WhereEqInt(const Field: string; Value: Int64): TFLQuery;
var L: Integer; begin L := Length(FWhereInt); SetLength(FWhereInt, L + 1); FWhereInt[L].Field := Field; FWhereInt[L].Value := Value; Result := Self; end;

function TFLQuery.Match(const Field, Value: string): TFLQuery;
var L: Integer; begin L := Length(FWhereStr); SetLength(FWhereStr, L + 1); FWhereStr[L].Field := Field; FWhereStr[L].Value := Value; FWhereStr[L].Op := 'match'; Result := Self; end;

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

function TFLQuery.OrderBy(const Field: string; Ascending: Boolean): TFLQuery;
begin FOrderByField := Field; FOrderByAsc := Ascending; Result := Self; end;

function TFLQuery.Limit(ACount: NativeUInt): TFLQuery;
begin FLimit := ACount; FHasLimit := True; Result := Self; end;

function TFLQuery.Offset(ACount: NativeUInt): TFLQuery;
begin FOffset := ACount; FHasOffset := True; Result := Self; end;

function TFLQuery.StartAfter(ASnapshot: TFLDocument): TFLQuery;
begin FStartAfter := ASnapshot.Handle; Result := Self; end;

function TFLQuery.Select(const Fields: array of string): TFLQuery;
var I: Integer; begin FSelectFields.Clear; for I := Low(Fields) to High(Fields) do FSelectFields.Add(Fields[I]); Result := Self; end;

function TFLQuery.BuildNativeQuery: PFL_Query;
var I, J: Integer; TmpArr: PFL_Array;
begin
  Result := fl_query_new(PChar(FCollection));
  try
    for I := Low(FWhereStr) to High(FWhereStr) do begin
      if FWhereStr[I].Op = 'match' then fl_query_where_match(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'contains' then fl_query_where_contains(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else if FWhereStr[I].Op = 'starts_with' then fl_query_where_starts_with(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value))
      else fl_query_where_eq_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value));
    end;
    for I := Low(FWhereInt) to High(FWhereInt) do fl_query_where_eq_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value);
    for I := Low(FWhereIn) to High(FWhereIn) do begin
      TmpArr := fl_array_new;
      for J := 0 to FWhereIn[I].Data.Count-1 do
        if FWhereIn[I].Data.Items[J].JSONType = jtNumber then fl_array_append_int(TmpArr, FWhereIn[I].Data.Items[J].AsInt64)
        else fl_array_append_str(TmpArr, PChar(FWhereIn[I].Data.Items[J].AsString));
      fl_query_where_in(Result, PChar(FWhereIn[I].Field), TmpArr);
    end;
    if FStartAfter <> nil then fl_query_start_after(Result, FStartAfter);
    if FOrderByField <> '' then fl_query_order_by(Result, PChar(FOrderByField), FOrderByAsc);
    if FHasLimit then fl_query_limit(Result, FLimit);
    if FHasOffset then fl_query_offset(Result, FOffset);
    for I := 0 to FSelectFields.Count - 1 do fl_query_select_field(Result, PChar(FSelectFields[I]));
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

{ TFLCollection }

constructor TFLCollection.Create(ADB: TFireLite; const AName: string); begin inherited Create; FDB := ADB; FName := AName; end;
function TFLCollection.Doc(const DocID: string): TFLDocumentRef; begin Result := TFLDocumentRef.Create(FDB, FName, DocID); end;
function TFLCollection.Query: TFLQuery; begin Result := TFLQuery.Create(FDB, FName); end;
function TFLCollection.WhereEqStr(const Field, Value: string): TFLQuery; begin Result := Query.WhereEqStr(Field, Value); end;
function TFLCollection.WhereEqInt(const Field: string; Value: Int64): TFLQuery; begin Result := Query.WhereEqInt(Field, Value); end;
function TFLCollection.Match(const Field, Value: string): TFLQuery; begin Result := Query.Match(Field, Value); end;
function TFLCollection.Limit(ACount: NativeUInt): TFLQuery; begin Result := Query.Limit(ACount); end;
procedure TFLCollection.CreateIndex(const Field: string); begin CheckStatus(fl_engine_create_simple_index(FDB.Handle, PChar(FName), PChar(Field)), 'CreateIndex'); end;
procedure TFLCollection.CreateFTSIndex(const Field: string); begin CheckStatus(fl_engine_create_fts_index(FDB.Handle, PChar(FName), PChar(Field)), 'CreateFTSIndex'); end;

{ TFireLite }

constructor TFireLite.Create(const DBPath: string); begin inherited Create; FHandle := fl_engine_open(PChar(DBPath)); end;
constructor TFireLite.Create(const DBPath: string; AConfig: TFLConfig); begin inherited Create; FHandle := fl_engine_open_with_config(PChar(DBPath), AConfig.Handle); AConfig.FHandle := nil; end;
destructor TFireLite.Destroy; begin if FHandle <> nil then fl_engine_free(FHandle); inherited; end;
function TFireLite.Collection(const Name: string): TFLCollection; begin Result := TFLCollection.Create(Self, Name); end;
function TFireLite.StartBatch: TFLBatch; begin Result := TFLBatch.Create(FHandle); end;
function TFireLite.StartTransaction: TFLTransaction; begin Result := TFLTransaction.Create(FHandle); end;
function TFireLite.ListCollections: TStringList; var S: string; P: TJSONParser; A: TJSONArray; I: Integer; begin Result := TStringList.Create; S := ConsumeCString(fl_engine_list_collections(FHandle)); if S = '' then Exit; P := TJSONParser.Create(S); try A := TJSONArray(P.Parse); for I := 0 to A.Count - 1 do Result.Add(A.Strings[I]); finally P.Free; end; end;
function TFireLite.GetStats: string; begin Result := ConsumeCString(fl_engine_get_stats(FHandle)); end;

end.
