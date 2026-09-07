unit FireLitePkgReg;

{ Design-time registration for the FireLite Lazarus package.

  Installs TFireLiteComponent on the "FireLite" tab of the component palette.
  The Register() procedure is only invoked by the Lazarus IDE when the
  package is installed; this unit also compiles fine as a plain runtime unit
  (RegisterComponents is provided by the Classes unit in the RTL), so it can
  be built with fpc alone.
}

{$mode objfpc}{$H+}

interface

uses
  Classes, LResources;

procedure Register;

implementation

uses
  FireLiteComponent;

procedure Register;
begin
  RegisterComponents('FireLite', [TFireLiteComponent]);
end;

initialization
  { Palette icon for TFireLiteComponent (resource name = lowercase class
    name). Built from tfirelitecomponent.xpm via lazres; replace the .xpm
    and re-run lazres to change the icon. }
  {$I tfirelitecomponent.lrs}

end.
