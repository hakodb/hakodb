program example;

{ FireLite Lazarus minimal demo.
  Open example.lpi in Lazarus (or run: lazbuild example.lpi), then press the
  "Run demo" button. Demonstrates the TFireLiteComponent dropped on a form
  plus NetSync / CloudSync configuration from the Object Inspector.
}

{$mode objfpc}{$H+}

uses
  {$IFDEF UNIX}
  cthreads,
  {$ENDIF}
  Interfaces, // this includes the LCL widgetset
  Forms,
  example_main;

{$R *.res}

begin
  RequireDerivedFormResource := True;
  Application.Scaled := True;
  Application.Initialize;
  Application.CreateForm(TForm1, Form1);
  Application.Run;
end.
