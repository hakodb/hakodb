unit FireLiteComponent;

{ FireLite for Lazarus
  =====================

  TFireLiteComponent is a drop-on-form (or drop-on-datamodule) TComponent
  wrapper around the high-level TFireLite engine. It exposes the most common
  settings as published properties so you can configure a FireLite database
  from the Object Inspector with no code.

  It also exposes the NetSync and CloudSync configuration so you can start
  LAN / Cloud replication with a single method call.

  Runtime usage (no form needed):
    var C: TFireLiteComponent;
    C := TFireLiteComponent.Create(nil);
    C.DatabasePath := 'app.db';
    C.Open;
    C.Collection('users').Doc('u1').SetDoc(Doc);
    C.StartNetSync;   // uses NetSyncName/RoomKey/Port
    C.StartCloudSync; // uses CloudSyncMode/ClientID/RoomKey/AuthToken/Address
    C.Close;
    C.Free;
}

{$mode objfpc}{$H+}

interface

uses
  Classes, FireLite;

type

  { TFireLiteComponent : lazarus component wrapper around TFireLite }
  TFireLiteComponent = class(TComponent)
  private
    FDatabasePath: string;
    FNetSyncEnabled: Boolean;
    FNetSyncName: string;
    FNetSyncRoomKey: string;
    FNetSyncPort: Word;
    FNetSyncDiscovery: TFLDiscoveryMode;
    FCloudSyncEnabled: Boolean;
    FCloudSyncMode: TFLCloudSyncMode;
    FCloudSyncClientID: string;
    FCloudSyncRoomName: string;
    FCloudSyncRoomKey: string;
    FCloudSyncAuthToken: string;
    FCloudSyncAddress: string;
    FLite: TFireLite;
    FNetSyncer: TFLNetSyncer;
    FCloudSyncer: TFLCloudSync;
    FOpened: Boolean;
    procedure EnsureOpen;
  public
    constructor Create(AOwner: TComponent); override;
    destructor Destroy; override;

    { Opens the database stored at DatabasePath (creates it if missing). }
    procedure Open;
    { Closes the database and releases every owned handle. }
    procedure Close;
    { True once Open() has been called successfully. }
    property IsOpen: Boolean read FOpened;
    { The underlying high-level engine. }
    property Lite: TFireLite read FLite;

    { Start LAN (net-sync) replication using NetSyncName / NetSyncRoomKey / NetSyncPort. }
    procedure StartNetSync;
    { Stop the currently running NetSync replication (if any). }
    procedure StopNetSync;
    { Start cloud replication using the CloudSync* configuration. }
    procedure StartCloudSync;
    { Stop the currently running CloudSync replication (if any). }
    procedure StopCloudSync;

    { Create a standalone NetSyncer (not tied to the published properties). }
    function CreateNetSyncer(const AName, ARoomKey: string): TFLNetSyncer;
    { Create a standalone CloudSyncer (not tied to the published properties). }
    function CreateCloudSyncer(AMode: TFLCloudSyncMode; const AClientID, ARoomName, ARoomKey, AAuthToken: string): TFLCloudSync;
    { Create a room-agnostic cloud SERVER (not tied to the published properties). }
    function CreateCloudServerSyncer(const AServerID, AAuthToken: string): TFLCloudSync;
    { Create an offline-first cloud CLIENT for a room (not tied to the published properties). }
    function CreateCloudClientSyncer(const AClientID, ARoomName, ARoomKey, AAuthToken: string): TFLCloudSync;

    function Collection(const AName: string): TFLCollection;
    function ListCollections: TStringList;
    function GetStats: string;
    procedure Compact;
  published
    { Path to the FireLite database file. }
    property DatabasePath: string read FDatabasePath write FDatabasePath;

    { --- NetSync (LAN replication) configuration --- }
    { Master switch: the NetSync* options below are inert until enabled. }
    property NetSyncEnabled: Boolean read FNetSyncEnabled write FNetSyncEnabled default False;
    property NetSyncName: string read FNetSyncName write FNetSyncName;
    property NetSyncRoomKey: string read FNetSyncRoomKey write FNetSyncRoomKey;
    property NetSyncPort: Word read FNetSyncPort write FNetSyncPort default 4456;
    property NetSyncDiscovery: TFLDiscoveryMode read FNetSyncDiscovery write FNetSyncDiscovery default dmMdns;

    { --- CloudSync configuration --- }
    { Master switch: the CloudSync* options below are inert until enabled. }
    property CloudSyncEnabled: Boolean read FCloudSyncEnabled write FCloudSyncEnabled default False;
    property CloudSyncMode: TFLCloudSyncMode read FCloudSyncMode write FCloudSyncMode default csmClient;
    property CloudSyncClientID: string read FCloudSyncClientID write FCloudSyncClientID;
    property CloudSyncRoomName: string read FCloudSyncRoomName write FCloudSyncRoomName;
    property CloudSyncRoomKey: string read FCloudSyncRoomKey write FCloudSyncRoomKey;
    property CloudSyncAuthToken: string read FCloudSyncAuthToken write FCloudSyncAuthToken;
    property CloudSyncAddress: string read FCloudSyncAddress write FCloudSyncAddress;
  end;

implementation

{ TFireLiteComponent }

constructor TFireLiteComponent.Create(AOwner: TComponent);
begin
  inherited Create(AOwner);
  FNetSyncEnabled := False;
  FNetSyncPort := 4456;
  FNetSyncDiscovery := dmMdns;
  FCloudSyncEnabled := False;
  FCloudSyncMode := csmClient;
end;

destructor TFireLiteComponent.Destroy;
begin
  FNetSyncer.Free;
  FCloudSyncer.Free;
  FLite.Free;
  inherited Destroy;
end;

procedure TFireLiteComponent.EnsureOpen;
begin
  if not FOpened then
    raise EFireLiteError.Create('TFireLiteComponent.Open must be called first');
end;

procedure TFireLiteComponent.Open;
begin
  if FOpened then Exit;
  if FDatabasePath = '' then
    raise EFireLiteError.Create('TFireLiteComponent.DatabasePath is empty');
  FLite := TFireLite.Create(FDatabasePath);
  FOpened := True;
end;

procedure TFireLiteComponent.Close;
begin
  if not FOpened then Exit;
  FNetSyncer.Free;
  FNetSyncer := nil;
  FCloudSyncer.Free;
  FCloudSyncer := nil;
  FLite.Free;
  FLite := nil;
  FOpened := False;
end;

procedure TFireLiteComponent.StartNetSync;
begin
  EnsureOpen;
  if not FNetSyncEnabled then
    raise EFireLiteError.Create('NetSyncEnabled is False: enable it before StartNetSync');
  FNetSyncer.Free;
  FNetSyncer := FLite.CreateNetSyncer(FNetSyncName, FNetSyncRoomKey);
  FNetSyncer.SetDiscoveryMode(FNetSyncDiscovery);
  FNetSyncer.Start(FNetSyncPort);
end;

procedure TFireLiteComponent.StopNetSync;
begin
  FNetSyncer.Free;
  FNetSyncer := nil;
end;

procedure TFireLiteComponent.StartCloudSync;
begin
  EnsureOpen;
  if not FCloudSyncEnabled then
    raise EFireLiteError.Create('CloudSyncEnabled is False: enable it before StartCloudSync');
  FCloudSyncer.Free;
  FCloudSyncer := FLite.CreateCloudSyncer(FCloudSyncMode, FCloudSyncClientID,
    FCloudSyncRoomName, FCloudSyncRoomKey, FCloudSyncAuthToken);
  FCloudSyncer.Start(FCloudSyncAddress);
end;

procedure TFireLiteComponent.StopCloudSync;
begin
  FCloudSyncer.Free;
  FCloudSyncer := nil;
end;

function TFireLiteComponent.CreateNetSyncer(const AName, ARoomKey: string): TFLNetSyncer;
begin
  EnsureOpen;
  Result := FLite.CreateNetSyncer(AName, ARoomKey);
end;

function TFireLiteComponent.CreateCloudSyncer(AMode: TFLCloudSyncMode;
  const AClientID, ARoomName, ARoomKey, AAuthToken: string): TFLCloudSync;
begin
  EnsureOpen;
  Result := FLite.CreateCloudSyncer(AMode, AClientID, ARoomName, ARoomKey, AAuthToken);
end;

function TFireLiteComponent.CreateCloudServerSyncer(const AServerID, AAuthToken: string): TFLCloudSync;
begin
  EnsureOpen;
  Result := FLite.CreateCloudServerSyncer(AServerID, AAuthToken);
end;

function TFireLiteComponent.CreateCloudClientSyncer(const AClientID, ARoomName, ARoomKey, AAuthToken: string): TFLCloudSync;
begin
  EnsureOpen;
  Result := FLite.CreateCloudClientSyncer(AClientID, ARoomName, ARoomKey, AAuthToken);
end;

function TFireLiteComponent.Collection(const AName: string): TFLCollection;
begin
  EnsureOpen;
  Result := FLite.Collection(AName);
end;

function TFireLiteComponent.ListCollections: TStringList;
begin
  EnsureOpen;
  Result := FLite.ListCollections;
end;

function TFireLiteComponent.GetStats: string;
begin
  EnsureOpen;
  Result := FLite.GetStats;
end;

procedure TFireLiteComponent.Compact;
begin
  EnsureOpen;
  FLite.Compact;
end;

initialization
  { Required so TFireLiteComponent can be streamed from .lfm files at runtime. }
  RegisterClass(TFireLiteComponent);

end.
