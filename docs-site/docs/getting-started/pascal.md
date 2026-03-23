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

## v0.5.6 highlights (Pascal)

- Advanced config builder (`TFLConfig`) for durability, audit log, encryption, worker, and memory tuning.
- Query FTS methods: `Match`, `Contains`, `StartsWith`.
- Query pagination/filter extensions: `WhereIn`, `StartAfter`, `Offset`.
- Aggregates from query builder: `Count`, `Sum`, `Avg`.
- Index helpers on collections: `CreateIndex` and `CreateFTSIndex`.
