# FireLite Binary Document Format

This document describes the internal binary document representation used by FireLite.

FireLite exposes a JSON API to developers but internally stores documents using an optimized binary encoding.

Goals of this format:

* faster document access
* smaller storage footprint
* predictable memory layout
* efficient indexing
* minimal parsing overhead

---

# Design Overview

External representation:

JSON document.

Example:

```json
{
  "name": "Alice",
  "age": 25,
  "active": true
}
```

Internal representation:

Binary encoded document.

```text
document_length
field_count
field_table
data_section
```

This structure allows FireLite to access fields without scanning the entire document.

---

# Document Layout

Binary document structure:

```text
+-------------------+
| document_length   |
+-------------------+
| field_count       |
+-------------------+
| field_offset[0]   |
| field_offset[1]   |
| field_offset[n]   |
+-------------------+
| field data        |
+-------------------+
```

Explanation:

* **document_length**: total document size
* **field_count**: number of fields
* **field_offset table**: offset for each field
* **field data**: actual encoded fields

Field offsets allow direct lookup.

---

# Field Encoding

Each field is encoded as:

```text
key_length
key_bytes
value_type
value_bytes
```

Example layout:

```text
4
"name"
STRING
"Alice"
```

---

# Value Types

Supported value types:

| Type ID | Type    |
| ------- | ------- |
| 0x01    | String  |
| 0x02    | Integer |
| 0x03    | Float   |
| 0x04    | Boolean |
| 0x05    | Null    |
| 0x06    | Object  |
| 0x07    | Array   |

Objects and arrays use nested document encoding.

---

# Example Encoding

Example JSON:

```json
{
  "name": "Alice",
  "age": 25
}
```

Binary layout:

```text
document_length
field_count = 2

offset_0
offset_1

field0:
key_len
"name"
type = string
"Alice"

field1:
key_len
"age"
type = int
25
```

---

# Field Lookup

To retrieve a field:

1. read field count
2. iterate offset table
3. read field key
4. compare with target key

Future optimizations may include:

* sorted field tables
* field hash tables

---

# Nested Documents

Nested JSON objects are encoded as embedded binary documents.

Example:

```json
{
  "user": {
    "name": "Alice"
  }
}
```

Encoding:

```text
field:
"user"
type = object
embedded document
```

---

# Arrays

Arrays are encoded as sequential values.

Example:

```json
{
  "scores": [10, 20, 30]
}
```

Binary representation:

```text
field:
"scores"
type = array
count = 3
values
```

---

# Advantages

Binary encoding provides several benefits:

* avoids repeated JSON parsing
* smaller disk footprint
* faster field access
* predictable memory layout

This approach allows FireLite to maintain a simple JSON API while achieving efficient storage.
