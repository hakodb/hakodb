# FireLite Storage Format

This document describes the on-disk binary storage format used by FireLite.

The format is designed to be:

* simple
* append-only
* crash safe
* efficient for sequential writes

---

# File Layout

A FireLite database file contains:

```
[File Header]
[Record]
[Record]
[Record]
...
```

All records are appended sequentially.

---

# File Header

The file header identifies the database format.

Example layout:

```
magic_number
version
flags
```

Example values:

```
magic_number: "FIRELITE"
version: 1
```

The header allows FireLite to verify file compatibility.

---

# Record Format

Each record stores a document operation.

Record layout:

```
record_length
key_length
key
document_length
binary_document
```

Fields are stored sequentially.

---

# Record Fields

## Record Length

Total length of the record in bytes.

Used to skip corrupted records during recovery.

---

## Key

The key uniquely identifies the document.

Example:

```
users:123
orders:999
```

Key format:

```
collection:document_id
```

---

## Binary Document

Documents are stored using FireLite's internal binary format.

Example document:

```
{
  "name": "Alice",
  "age": 25
}
```

Binary representation contains:

```
document_length
field_count
field_data
```

---

# Field Encoding

Each field is stored as:

```
key_length
key
type
value
```

---

# Supported Types

Type identifiers:

```
0x01 string
0x02 integer
0x03 float
0x04 boolean
0x05 null
0x06 object
0x07 array
```

Values are encoded in binary form.

---

# Example Record

Example document:

```
users:123
{
  "name": "Alice",
  "age": 25
}
```

Record layout:

```
record_length
key_length
"users:123"
document_length
[field_count=2]

field1:
"name"
string
"Alice"

field2:
"age"
int
25
```

---

# Index Reconstruction

During startup FireLite rebuilds the in-memory index.

Process:

1. read records sequentially
2. store latest offset for each key
3. discard obsolete records

Result:

```
users:123 → offset
orders:999 → offset
```

---

# Deletion Records

Deletion is stored as a tombstone record.

Example:

```
DELETE users:123
```

Encoded as:

```
record_length
key_length
key
document_length = 0
```

---

# Compaction

Compaction rewrites the storage file.

Steps:

1. scan all records
2. keep latest document per key
3. write new storage file
4. replace old file

This removes obsolete records and reduces disk usage.

---

# Compatibility

Future versions may extend the storage format.

Backward compatibility is maintained using:

* versioned headers
* feature flags
