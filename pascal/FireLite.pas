unit FireLite;

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, fpjson, jsonparser, SyncObjs, FireLiteRaw;

type
  EFireLiteError = class(Exception);

  TOnSnapshotCallback = procedure(const JsonSnapshot: string) of object;

  IFLSubscription = interface
    ['{91E1FC85-5B6F-4F66-B070-8698E120AF7E}']
    procedure Stop;
  end;

  TFireLite = class;
  TFLCollection = class;
  TFLDocument = class;
  TFLDocumentRef = class;
  TFLQuery = class;
  TFLBatch = class;
  TFLTransaction = class;

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

    class function FromJSON(const Obj: TJSONObject): TFLDocument;
    function ToJSON: string;

    property Handle: PFL_Doc read FHandle;
  end;

  TFLBatch = class
  private
    FDB: TFireLite;
    FHandle: PFL_Batch;
    FClosed: Boolean;
    procedure EnsureOpen;
  public
    constructor Create(ADB: TFireLite);
    destructor Destroy; override;

    function [Set](const Collection, DocID: string; const Doc: TFLDocument): TFLBatch;
    function Delete(const Collection, DocID: string): TFLBatch;
    procedure Commit;
  end;

  TFLTransaction = class
  private
    FBatch: TFLBatch;
  public
    constructor Create(ADB: TFireLite);
    destructor Destroy; override;

    function [Set](const Collection, DocID: string; const Doc: TFLDocument): TFLTransaction;
    function Delete(const Collection, DocID: string): TFLTransaction;
    procedure Commit;
  end;

  TFLQuery = class
  private
    FDB: TFireLite;
    FCollection: string;
    FWhereStr: array of record Field, Value: string; end;
    FWhereInt: array of record Field: string; Value: Int64; end;
    FOrderByField: string;
    FOrderByAsc: Boolean;
    FLimit: NativeUInt;
    FHasLimit: Boolean;
    FSelectFields: TStringList;

    function BuildNativeQuery: PFL_Query;
    procedure ApplyProjection(Q: PFL_Query);
  public
    constructor Create(ADB: TFireLite; const ACollection: string);
    destructor Destroy; override;

    function WhereEqStr(const Field, Value: string): TFLQuery;
    function WhereEqInt(const Field: string; Value: Int64): TFLQuery;
    function OrderBy(const Field: string; Ascending: Boolean = True): TFLQuery;
    function Limit(ACount: NativeUInt): TFLQuery;
    function [Select](const Fields: array of string): TFLQuery;

    function GetJSON: string;
    function OnSnapshot(const Callback: TOnSnapshotCallback;
      PollIntervalMs: Cardinal = 250; QueueToMainThread: Boolean = True): IFLSubscription;
  end;

  TFLDocumentRef = class
  private
    FDB: TFireLite;
    FCollection: string;
    FDocID: string;
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
    function WhereEqStr(const Field, Value: string): TFLQuery;
    function WhereEqInt(const Field: string; Value: Int64): TFLQuery;
    function OrderBy(const Field: string; Ascending: Boolean = True): TFLQuery;
    function Limit(ACount: NativeUInt): TFLQuery;
    function [Select](const Fields: array of string): TFLQuery;
  end;

  TFireLite = class
  private
    FHandle: PFL_Engine;
  public
    constructor Create(const DBPath: string);
    destructor Destroy; override;

    function Collection(const Name: string): TFLCollection;
    function StartBatch: TFLBatch;
    function StartTransaction: TFLTransaction;

    property Handle: PFL_Engine read FHandle;
  end;

implementation

type
  TFLSnapshotPoller = class(TThread, IFLSubscription)
  private
    FLock: TCriticalSection;
    FStopped: Boolean;
    FQueueToMainThread: Boolean;
    FQuery: TFLQuery;
    FCallback: TOnSnapshotCallback;
    FIntervalMs: Cardinal;
    FLastJSON: string;
    FPendingJSON: string;

    procedure Deliver;
    function IsStopped: Boolean;
  protected
    procedure Execute; override;
  public
    constructor Create(AQuery: TFLQuery; const ACallback: TOnSnapshotCallback;
      APollIntervalMs: Cardinal; AQueueToMainThread: Boolean);
    destructor Destroy; override;
    procedure Stop;
  end;

function LastFireLiteError: string;
var
  P: PChar;
begin
  P := fl_last_error;
  if P = nil then
    Exit('unknown firelite error');
  Result := string(P);
end;

procedure CheckStatus(Code: cint32; const Context: string);
begin
  if Code <> 0 then
    raise EFireLiteError.CreateFmt('%s failed: %s', [Context, LastFireLiteError]);
end;

function ConsumeCString(P: PChar): string;
begin
  if P = nil then
    Exit('');
  Result := string(P);
  fl_string_free(P);
end;

{ TFLDocument }

constructor TFLDocument.Create;
begin
  inherited Create;
  FHandle := fl_doc_new;
  FOwned := True;
  if FHandle = nil then
    raise EFireLiteError.Create('fl_doc_new failed');
end;

constructor TFLDocument.CreateFromHandle(AHandle: PFL_Doc; AOwned: Boolean);
begin
  inherited Create;
  FHandle := AHandle;
  FOwned := AOwned;
end;

destructor TFLDocument.Destroy;
begin
  if FOwned and (FHandle <> nil) then
    fl_doc_free(FHandle);
  inherited Destroy;
end;

function TFLDocument.InsertStr(const Key, Value: string): TFLDocument;
begin
  CheckStatus(fl_doc_insert_str(FHandle, PChar(Key), PChar(Value)), 'fl_doc_insert_str');
  Result := Self;
end;

function TFLDocument.InsertInt(const Key: string; Value: Int64): TFLDocument;
begin
  CheckStatus(fl_doc_insert_int(FHandle, PChar(Key), Value), 'fl_doc_insert_int');
  Result := Self;
end;

function TFLDocument.InsertFloat(const Key: string; Value: Double): TFLDocument;
begin
  CheckStatus(fl_doc_insert_float(FHandle, PChar(Key), Value), 'fl_doc_insert_float');
  Result := Self;
end;

function TFLDocument.InsertBool(const Key: string; Value: Boolean): TFLDocument;
var
  B: cbool;
begin
  if Value then B := 1 else B := 0;
  CheckStatus(fl_doc_insert_bool(FHandle, PChar(Key), B), 'fl_doc_insert_bool');
  Result := Self;
end;

function TFLDocument.InsertNull(const Key: string): TFLDocument;
begin
  CheckStatus(fl_doc_insert_null(FHandle, PChar(Key)), 'fl_doc_insert_null');
  Result := Self;
end;

class function TFLDocument.FromJSON(const Obj: TJSONObject): TFLDocument;
var
  I: Integer;
  Key: string;
  Data: TJSONData;
begin
  Result := TFLDocument.Create;
  for I := 0 to Obj.Count - 1 do
  begin
    Key := Obj.Names[I];
    Data := Obj.Items[I];
    case Data.JSONType of
      jtNull: Result.InsertNull(Key);
      jtBoolean: Result.InsertBool(Key, Data.AsBoolean);
      jtNumber:
        if Pos('.', Data.AsJSON) > 0 then
          Result.InsertFloat(Key, Data.AsFloat)
        else
          Result.InsertInt(Key, Data.AsInt64);
      jtString: Result.InsertStr(Key, Data.AsString);
    else
      raise EFireLiteError.CreateFmt('Unsupported JSON type for key "%s"', [Key]);
    end;
  end;
end;

function TFLDocument.ToJSON: string;
begin
  Result := ConsumeCString(fl_doc_to_json(FHandle));
end;

{ TFLBatch }

constructor TFLBatch.Create(ADB: TFireLite);
begin
  inherited Create;
  FDB := ADB;
  FHandle := fl_batch_new;
  if FHandle = nil then
    raise EFireLiteError.Create('fl_batch_new failed');
end;

destructor TFLBatch.Destroy;
begin
  if (FHandle <> nil) and not FClosed then
    fl_batch_free(FHandle);
  inherited Destroy;
end;

procedure TFLBatch.EnsureOpen;
begin
  if FClosed then
    raise EFireLiteError.Create('batch already closed');
end;

function TFLBatch.Set(const Collection, DocID: string; const Doc: TFLDocument): TFLBatch;
begin
  EnsureOpen;
  CheckStatus(fl_batch_set(FHandle, PChar(Collection), PChar(DocID), Doc.Handle), 'fl_batch_set');
  Result := Self;
end;

function TFLBatch.Delete(const Collection, DocID: string): TFLBatch;
begin
  EnsureOpen;
  CheckStatus(fl_batch_delete(FHandle, PChar(Collection), PChar(DocID)), 'fl_batch_delete');
  Result := Self;
end;

procedure TFLBatch.Commit;
begin
  EnsureOpen;
  CheckStatus(fl_batch_commit(FDB.Handle, FHandle), 'fl_batch_commit');
  fl_batch_free(FHandle);
  FHandle := nil;
  FClosed := True;
end;

{ TFLTransaction }

constructor TFLTransaction.Create(ADB: TFireLite);
begin
  inherited Create;
  FBatch := TFLBatch.Create(ADB);
end;

destructor TFLTransaction.Destroy;
begin
  FBatch.Free;
  inherited Destroy;
end;

function TFLTransaction.Set(const Collection, DocID: string; const Doc: TFLDocument): TFLTransaction;
begin
  FBatch.Set(Collection, DocID, Doc);
  Result := Self;
end;

function TFLTransaction.Delete(const Collection, DocID: string): TFLTransaction;
begin
  FBatch.Delete(Collection, DocID);
  Result := Self;
end;

procedure TFLTransaction.Commit;
begin
  FBatch.Commit;
end;

{ TFLQuery }

constructor TFLQuery.Create(ADB: TFireLite; const ACollection: string);
begin
  inherited Create;
  FDB := ADB;
  FCollection := ACollection;
  FOrderByAsc := True;
  FHasLimit := False;
  FSelectFields := TStringList.Create;
end;

destructor TFLQuery.Destroy;
begin
  FSelectFields.Free;
  inherited Destroy;
end;

function TFLQuery.WhereEqStr(const Field, Value: string): TFLQuery;
var
  L: SizeInt;
begin
  L := Length(FWhereStr);
  SetLength(FWhereStr, L + 1);
  FWhereStr[L].Field := Field;
  FWhereStr[L].Value := Value;
  Result := Self;
end;

function TFLQuery.WhereEqInt(const Field: string; Value: Int64): TFLQuery;
var
  L: SizeInt;
begin
  L := Length(FWhereInt);
  SetLength(FWhereInt, L + 1);
  FWhereInt[L].Field := Field;
  FWhereInt[L].Value := Value;
  Result := Self;
end;

function TFLQuery.OrderBy(const Field: string; Ascending: Boolean): TFLQuery;
begin
  FOrderByField := Field;
  FOrderByAsc := Ascending;
  Result := Self;
end;

function TFLQuery.Limit(ACount: NativeUInt): TFLQuery;
begin
  FLimit := ACount;
  FHasLimit := True;
  Result := Self;
end;

function TFLQuery.Select(const Fields: array of string): TFLQuery;
var
  I: Integer;
begin
  FSelectFields.Clear;
  for I := Low(Fields) to High(Fields) do
    FSelectFields.Add(Fields[I]);
  Result := Self;
end;

procedure TFLQuery.ApplyProjection(Q: PFL_Query);
var
  I: Integer;
begin
  for I := 0 to FSelectFields.Count - 1 do
    CheckStatus(fl_query_select_field(Q, PChar(FSelectFields[I])), 'fl_query_select_field');
end;

function TFLQuery.BuildNativeQuery: PFL_Query;
var
  I: Integer;
  Asc: cbool;
begin
  Result := fl_query_new(PChar(FCollection));
  if Result = nil then
    raise EFireLiteError.Create('fl_query_new failed');

  try
    for I := 0 to High(FWhereStr) do
      CheckStatus(
        fl_query_where_eq_str(Result, PChar(FWhereStr[I].Field), PChar(FWhereStr[I].Value)),
        'fl_query_where_eq_str'
      );

    for I := 0 to High(FWhereInt) do
      CheckStatus(
        fl_query_where_eq_int(Result, PChar(FWhereInt[I].Field), FWhereInt[I].Value),
        'fl_query_where_eq_int'
      );

    if FOrderByField <> '' then
    begin
      if FOrderByAsc then Asc := 1 else Asc := 0;
      CheckStatus(fl_query_order_by(Result, PChar(FOrderByField), Asc), 'fl_query_order_by');
    end;

    if FHasLimit then
      CheckStatus(fl_query_limit(Result, FLimit), 'fl_query_limit');

    ApplyProjection(Result);
  except
    fl_query_free(Result);
    raise;
  end;
end;

function TFLQuery.GetJSON: string;
var
  Q: PFL_Query;
  OutStr: PChar;
begin
  Q := BuildNativeQuery;
  try
    OutStr := fl_query_execute(FDB.Handle, Q);
    if OutStr = nil then
      raise EFireLiteError.CreateFmt('fl_query_execute failed: %s', [LastFireLiteError]);
    Result := ConsumeCString(OutStr);
  finally
    fl_query_free(Q);
  end;
end;

function TFLQuery.OnSnapshot(const Callback: TOnSnapshotCallback;
  PollIntervalMs: Cardinal; QueueToMainThread: Boolean): IFLSubscription;
begin
  Result := TFLSnapshotPoller.Create(Self, Callback, PollIntervalMs, QueueToMainThread);
end;

{ TFLDocumentRef }

constructor TFLDocumentRef.Create(ADB: TFireLite; const ACollection, ADocID: string);
begin
  inherited Create;
  FDB := ADB;
  FCollection := ACollection;
  FDocID := ADocID;
end;

procedure TFLDocumentRef.Set(const Doc: TFLDocument);
begin
  CheckStatus(
    fl_engine_insert(FDB.Handle, PChar(FCollection), PChar(FDocID), Doc.Handle),
    'fl_engine_insert'
  );
end;

function TFLDocumentRef.Get: TFLDocument;
var
  D: PFL_Doc;
begin
  D := fl_engine_get(FDB.Handle, PChar(FCollection), PChar(FDocID));
  if D = nil then
    Exit(nil);
  Result := TFLDocument.CreateFromHandle(D, True);
end;

procedure TFLDocumentRef.Delete;
begin
  CheckStatus(fl_engine_delete(FDB.Handle, PChar(FCollection), PChar(FDocID)), 'fl_engine_delete');
end;

{ TFLCollection }

constructor TFLCollection.Create(ADB: TFireLite; const AName: string);
begin
  inherited Create;
  FDB := ADB;
  FName := AName;
end;

function TFLCollection.Doc(const DocID: string): TFLDocumentRef;
begin
  Result := TFLDocumentRef.Create(FDB, FName, DocID);
end;

function TFLCollection.WhereEqStr(const Field, Value: string): TFLQuery;
begin
  Result := TFLQuery.Create(FDB, FName).WhereEqStr(Field, Value);
end;

function TFLCollection.WhereEqInt(const Field: string; Value: Int64): TFLQuery;
begin
  Result := TFLQuery.Create(FDB, FName).WhereEqInt(Field, Value);
end;

function TFLCollection.OrderBy(const Field: string; Ascending: Boolean): TFLQuery;
begin
  Result := TFLQuery.Create(FDB, FName).OrderBy(Field, Ascending);
end;

function TFLCollection.Limit(ACount: NativeUInt): TFLQuery;
begin
  Result := TFLQuery.Create(FDB, FName).Limit(ACount);
end;

function TFLCollection.Select(const Fields: array of string): TFLQuery;
begin
  Result := TFLQuery.Create(FDB, FName).Select(Fields);
end;

{ TFireLite }

constructor TFireLite.Create(const DBPath: string);
begin
  inherited Create;
  FHandle := fl_engine_open(PChar(DBPath));
  if FHandle = nil then
    raise EFireLiteError.CreateFmt('fl_engine_open failed: %s', [LastFireLiteError]);
end;

destructor TFireLite.Destroy;
begin
  if FHandle <> nil then
    fl_engine_free(FHandle);
  inherited Destroy;
end;

function TFireLite.Collection(const Name: string): TFLCollection;
begin
  Result := TFLCollection.Create(Self, Name);
end;

function TFireLite.StartBatch: TFLBatch;
begin
  Result := TFLBatch.Create(Self);
end;

function TFireLite.StartTransaction: TFLTransaction;
begin
  Result := TFLTransaction.Create(Self);
end;

{ TFLSnapshotPoller }

constructor TFLSnapshotPoller.Create(AQuery: TFLQuery; const ACallback: TOnSnapshotCallback;
  APollIntervalMs: Cardinal; AQueueToMainThread: Boolean);
begin
  inherited Create(True);
  FreeOnTerminate := False;
  FLock := TCriticalSection.Create;
  FStopped := False;
  FQuery := AQuery;
  FCallback := ACallback;
  FIntervalMs := APollIntervalMs;
  FQueueToMainThread := AQueueToMainThread;
  Start;
end;

destructor TFLSnapshotPoller.Destroy;
begin
  Stop;
  WaitFor;
  FLock.Free;
  inherited Destroy;
end;

function TFLSnapshotPoller.IsStopped: Boolean;
begin
  FLock.Acquire;
  try
    Result := FStopped;
  finally
    FLock.Release;
  end;
end;

procedure TFLSnapshotPoller.Stop;
begin
  FLock.Acquire;
  try
    FStopped := True;
  finally
    FLock.Release;
  end;
end;

procedure TFLSnapshotPoller.Deliver;
begin
  if Assigned(FCallback) then
    FCallback(FPendingJSON);
end;

procedure TFLSnapshotPoller.Execute;
var
  Current: string;
begin
  while (not Terminated) and (not IsStopped) do
  begin
    try
      Current := FQuery.GetJSON;
      if Current <> FLastJSON then
      begin
        FLastJSON := Current;
        FPendingJSON := Current;
        if FQueueToMainThread then
          TThread.Queue(nil, @Deliver)
        else
          Deliver;
      end;
    except
      // keep polling even if a transient error occurs
    end;

    Sleep(FIntervalMs);
  end;
end;

end.
