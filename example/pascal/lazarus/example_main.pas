unit example_main;

{ FireLite Lazarus minimal demo form. }

{$mode objfpc}{$H+}

interface

uses
  Classes, SysUtils, Forms, Controls, Graphics, Dialogs, StdCtrls,
  FireLite, FireLiteComponent;

type

  { TForm1 }

  TForm1 = class(TForm)
    btnRun: TButton;
    FireLite1: TFireLiteComponent;
    Memo1: TMemo;
    procedure btnRunClick(Sender: TObject);
  end;

var
  Form1: TForm1;

implementation

{$R *.lfm}

{ TForm1 }

procedure TForm1.btnRunClick(Sender: TObject);
var
  Users: TFLCollection;
  Ref: TFLDocumentRef;
  Doc, Got: TFLDocument;
  Q: TFLQuery;
begin
  Memo1.Lines.Clear;
  try
    if not FireLite1.IsOpen then
      FireLite1.Open;
    Memo1.Lines.Add('opened ' + FireLite1.DatabasePath);

    Users := FireLite1.Collection('users');
    try
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
      Memo1.Lines.Add('inserted u1');

      Ref := Users.Doc('u1');
      try
        Got := Ref.Get;
        try
          if Got <> nil then Memo1.Lines.Add('u1 -> ' + Got.ToJSON);
        finally
          Got.Free;
        end;
      finally
        Ref.Free;
      end;

      Q := Users.Query;
      try
        Memo1.Lines.Add('count(users) = ' + IntToStr(Q.Count));
      finally
        Q.Free;
      end;
    finally
      Users.Free;
    end;

    { NetSync + CloudSync are configured via the Object Inspector
      (NetSyncName / NetSyncRoomKey / NetSyncPort / CloudSync*) and started
      with one call each. Requires the net-sync/cloud-sync feature build. }
    // FireLite1.StartNetSync;
    // FireLite1.StartCloudSync;

    Memo1.Lines.Add('stats: ' + FireLite1.GetStats);
    Memo1.Lines.Add('done');
  except
    on E: Exception do
      Memo1.Lines.Add('ERROR: ' + E.Message);
  end;
end;

end.
