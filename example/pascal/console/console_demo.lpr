program console_demo;

{ FireLite embedded document database - Pascal (FPC) console example.

  Compile:
    fpc -Fu..\..\..\pascal console_demo.lpr

  Run (make sure the FireLite shared library is findable):
    Windows : copy ..\..\..\target\release\firelite.dll  (next to console_demo.exe)
    Linux   : LD_LIBRARY_PATH=../../target/release ./console_demo
    macOS   : DYLD_LIBRARY_PATH=../../target/release ./console_demo

  Note: the NetSync section needs the library built with the net-sync feature:
    cargo build --release --features net-sync,cloud-sync
}

{$mode objfpc}{$H+}

uses
  SysUtils, FireLite;

var
  DB: TFireLite;
  Users: TFLCollection;
  Ref: TFLDocumentRef;
  Doc, Got: TFLDocument;
  Q: TFLQuery;
  Batch: TFLBatch;
  Tx: TFLTransaction;
  Syncer: TFLNetSyncer;
begin
  DB := TFireLite.Create('demo.db');
  try
    Users := DB.Collection('users');
    try
      { --- Insert a document --- }
      Doc := TFLDocument.Create;
      try
        Doc.InsertStr('name', 'Alice').InsertInt('age', 32).InsertBool('active', True);
        Ref := Users.Doc('u1');
        try
          Ref.SetDoc(Doc);
        finally
          Ref.Free;
        end;
      finally
        Doc.Free;
      end;
      WriteLn('inserted u1');

      { --- Read it back --- }
      Ref := Users.Doc('u1');
      try
        Got := Ref.Get;
        try
          if Got <> nil then WriteLn('u1 -> ', Got.ToJSON);
        finally
          Got.Free;
        end;
      finally
        Ref.Free;
      end;

      { --- Query --- }
      Q := Users.WhereEqInt('age', 32);
      try
        WriteLn('query(age=32): ', Q.GetJSON);
      finally
        Q.Free;
      end;

      { --- Aggregation --- }
      Q := Users.Query;
      try
        WriteLn('count(users) = ', Q.Count);
      finally
        Q.Free;
      end;

      { --- Atomic batch --- }
      Batch := DB.StartBatch;
      try
        Doc := TFLDocument.Create;
        try
          Doc.InsertStr('name', 'Bob').InsertInt('age', 27);
          Batch.SetDoc('users', 'u2', Doc);
        finally
          Doc.Free;
        end;
        Batch.Delete('users', 'u1');
        Batch.Commit;
        WriteLn('batch committed (u2 added, u1 deleted)');
      finally
        Batch.Free;
      end;

      { --- Serializable transaction --- }
      Tx := DB.StartTransaction;
      try
        Got := Tx.Get('users', 'u2');
        try
          if Got <> nil then WriteLn('tx read u2 -> ', Got.ToJSON);
        finally
          Got.Free;
        end;
        Tx.Commit;
        WriteLn('transaction committed');
      finally
        Tx.Free;
      end;
    finally
      Users.Free;
    end;

    { --- NetSync (LAN replication) - needs net-sync feature build.
          Starting needs a Tokio host runtime; from FPC it degrades gracefully. }
    Syncer := DB.CreateNetSyncer('demo-room', 'secret-key');
    try
      try
        Syncer.Start(4456);
        WriteLn('net sync status: ', Syncer.StatusJSON);
      except
        on E: Exception do
          WriteLn('net sync skipped: ', E.Message);
      end;
    finally
      Syncer.Free;
    end;

    WriteLn('done');
  finally
    DB.Free;
  end;
end.
