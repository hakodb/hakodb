---
title: Lazarus / Free Pascal
---

Use `pascal/FireLiteRaw.pas` for low-level ABI imports and `pascal/FireLite.pas` for OO usage.

## Hello World

```pascal
var
  DB: TFireLite;
  Doc: TFLDocument;
begin
  DB := TFireLite.Create('./data.firelite');
  try
    Doc := TFLDocument.Create
      .InsertStr('name', 'alice')
      .InsertInt('age', 30);
    try
      DB.Collection('users').Doc('u1').Set(Doc);
    finally
      Doc.Free;
    end;
  finally
    DB.Free;
  end;
end;
```
