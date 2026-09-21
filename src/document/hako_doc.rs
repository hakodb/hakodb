use crate::document::value::Value;
use crate::util::varint::*;
use std::collections::HashMap;
use std::sync::Arc;
use std::cell::RefCell;

const MAGIC: u8 = 0xF1;
const VERSION: u8 = 5; 

thread_local! {
    static ENCODE_BUF: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128 * 1024));
    static FIELD_NAMES: RefCell<HashMap<String, Arc<str>>> = RefCell::new(HashMap::new());
}

/// ponytail: field names repeat across every document ("tenant", "score",
/// ...) but `Arc::from` allocated per field per decode. Intern them in a
/// thread-local pool: hits are a hash + refcount bump, zero alloc. Capped so
/// pathological schemas (unbounded distinct keys) degrade to plain Arc
/// instead of growing a leak vector.
pub(crate) fn intern_field(key: &str) -> Arc<str> {
    FIELD_NAMES.with(|pool| {
        let mut p = pool.borrow_mut();
        if let Some(a) = p.get(key) {
            return a.clone();
        }
        if p.len() < 4096 {
            let a: Arc<str> = Arc::from(key);
            p.insert(key.to_string(), a.clone());
            a
        } else {
            Arc::from(key)
        }
    })
}

#[derive(Debug, Clone, PartialEq, Default, serde::Serialize, serde::Deserialize)]
pub struct HakoDoc {
    pub fields: Vec<(Arc<str>, Value)>,
    pub _time: i64,
}

impl HakoDoc {
    pub fn get(&self, key: &str) -> Option<&Value> {
        self.fields.binary_search_by(|(k, _)| k.as_ref().cmp(key))
            .ok()
            .map(|pos| &self.fields[pos].1)
    }

    pub fn insert(&mut self, key: impl Into<String>, value: Value) {
        let key_str = key.into();
        match self.fields.binary_search_by(|(k, _)| k.as_ref().cmp(&key_str)) {
            Ok(pos) => self.fields[pos].1 = value,
            Err(pos) => self.fields.insert(pos, (Arc::from(key_str), value)),
        }
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(512);
        self.encode_into(&mut out);
        out
    }

    pub fn encode_buffered(&self) -> Vec<u8> {
        ENCODE_BUF.with(|buf| {
            let mut b = buf.borrow_mut();
            b.clear();
            self.encode_into(&mut b);
            b.to_vec() 
        })
    }

    pub fn encode_into(&self, out: &mut Vec<u8>) {
        out.push(MAGIC);
        out.push(VERSION);
        out.extend_from_slice(&self._time.to_le_bytes()); 
        out.extend_from_slice(&(self.fields.len() as u16).to_le_bytes()); 
        
        for (key, value) in &self.fields {
            out.push(key.len() as u8);
            out.extend_from_slice(key.as_bytes());
            Self::encode_value_to(value, out);
        }
    }

    pub fn decode(bytes: &[u8]) -> Option<Self> {
        let view = HakoDocView::new(bytes)?;
        // ponytail: size the vec from the header count — the old push-grown
        // vec always paid one backing alloc (+ regrow for wide docs) per
        // decode. The count is right there in the view.
        let mut doc = HakoDoc::default();
        doc._time = view._time;
        doc.fields = Vec::with_capacity(view.fields_count as usize);
        let mut it = view.iter();
        for (key, tag, data) in it.by_ref() {
            doc.fields.push((intern_field(key), decode_value(tag, data)?));
        }
        // ponytail: strict framing — a buffer that ends mid-document or
        // carries trailing garbage decodes to None, never to a silently
        // short doc. All producers (WAL, segments, snapshots) store exact
        // slices, so well-formed data always consumes exactly.
        if it.remaining() != 0 || it.position() != bytes.len() {
            return None;
        }
        Some(doc)
    }

    pub fn decode_projected(bytes: &[u8], projection: &[String]) -> Option<Self> {
        let view = HakoDocView::new(bytes)?;
        let mut doc = HakoDoc::default();
        doc._time = view._time;
        doc.fields = Vec::with_capacity(view.fields_count as usize);
        let mut it = view.iter();
        for (key, tag, data) in it.by_ref() {
            if projection.is_empty() || projection.iter().any(|p| p == key) {
                doc.fields.push((intern_field(key), decode_value(tag, data)?));
            }
        }
        if it.remaining() != 0 || it.position() != bytes.len() {
            return None;
        }
        Some(doc)
    }

    pub fn get_logical_time(&self) -> i64 { self._time }

    pub fn to_json(&self) -> serde_json::Value {
        let mut map = serde_json::Map::new();
        map.insert("_time".to_string(), serde_json::json!(self._time));
        for (k, v) in &self.fields {
            map.insert(k.to_string(), v.to_json());
        }
        serde_json::Value::Object(map)
    }

    pub fn to_json_with_id(&self, id: &str) -> serde_json::Value {
        let mut json = self.to_json();
        if let Some(obj) = json.as_object_mut() {
            obj.insert("id".to_string(), serde_json::Value::String(id.to_string()));
        }
        json
    }

    pub(crate) fn encode_value_to(v: &Value, out: &mut Vec<u8>) {
        match v {
            // --- SUPER TAGS (1 Byte Total) ---
            Value::Null => out.push(0xC0),
            Value::ServerTimestamp => out.push(0xC1),
            Value::Bool(true) => out.push(0xC2),
            Value::Bool(false) => out.push(0xC3),
            
            Value::Int(v) if *v >= 0 && *v <= 15 => {
                out.push(0x10 | (*v as u8));
            }

            Value::String(s) if s.len() <= 7 => {
                out.push(0x40 | (s.len() as u8));
                out.extend_from_slice(s.as_bytes());
            }

            // --- STANDARD TAGS ---
            Value::Int(v) => {
                out.push(3);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]); 
                encode_varint(zigzag_encode(*v), out);
                let len = (out.len() - start - 4) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Float(v) => {
                out.push(4);
                out.extend_from_slice(&8u32.to_le_bytes());
                out.extend_from_slice(&v.to_le_bytes());
            }
            Value::String(v) => {
                out.push(5);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v.as_bytes());
            }
            Value::Binary(v) => {
                out.push(6);
                out.extend_from_slice(&(v.len() as u32).to_le_bytes());
                out.extend_from_slice(v);
            }
            Value::Timestamp(v) => {
                out.push(7);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]); 
                encode_varint(zigzag_encode(*v), out);
                let len = (out.len() - start - 4) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Map(fields) => {
                out.push(8);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]); 
                let body_start = out.len();
                out.extend_from_slice(&(fields.len() as u16).to_le_bytes());
                for (k, v) in fields {
                    out.push(k.len() as u8);
                    out.extend_from_slice(k.as_bytes());
                    Self::encode_value_to(v, out);
                }
                let len = (out.len() - body_start) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Array(items) => {
                out.push(9);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]);
                let body_start = out.len();
                out.extend_from_slice(&(items.len() as u32).to_le_bytes());
                for item in items { Self::encode_value_to(item, out); }
                let len = (out.len() - body_start) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::Reference { collection, doc_id } => {
                out.push(10);
                let start = out.len();
                out.extend_from_slice(&[0u8; 4]);
                let body_start = out.len();
                out.push(collection.len() as u8);
                out.extend_from_slice(collection.as_bytes());
                out.push(doc_id.len() as u8);
                out.extend_from_slice(doc_id.as_bytes());
                let len = (out.len() - body_start) as u32;
                out[start..start+4].copy_from_slice(&len.to_le_bytes());
            }
            Value::BlobLink { offset, len } => {
                out.push(11);
                out.extend_from_slice(&12u32.to_le_bytes());
                out.extend_from_slice(&offset.to_le_bytes());
                out.extend_from_slice(&len.to_le_bytes());
            }
        }
    }
}

pub struct HakoDocView<'a> {
    bytes: &'a [u8],
    fields_count: u16,
    pub _time: i64,
}

impl<'a> HakoDocView<'a> {
    pub fn new(bytes: &'a [u8]) -> Option<Self> {
        if bytes.len() < 12 || bytes[0] != MAGIC || bytes[1] != VERSION { return None; }
        let _time = i64::from_le_bytes(bytes[2..10].try_into().ok()?);
        let fields_count = u16::from_le_bytes(bytes[10..12].try_into().ok()?);
        Some(Self { bytes, fields_count, _time })
    }
    pub fn iter(&self) -> HakoDocIter<'a> {
        HakoDocIter { bytes: self.bytes, pos: 12, remaining: self.fields_count }
    }
    /// Raw framing walk: yields key BYTES without UTF-8 validation, plus
    /// tag and value slice. For filter matching only — byte-compare keys,
    /// never materialize. Skipped (non-matching) fields cost pointer bumps
    /// alone; validation happens exactly once, on the owned-decode path
    /// that accepted rows take anyway (so corrupt keys still reject: the
    /// body scan below fails them, same outcome as validating here).
    pub fn iter_raw(&self) -> HakoDocRawIter<'a> {
        HakoDocRawIter { bytes: self.bytes, pos: 12, remaining: self.fields_count }
    }
    /// Field count from the header, without walking the framing.
    pub(crate) fn field_count(&self) -> usize {
        self.fields_count as usize
    }
}

pub struct BorrowedValue<'a> {
    pub tag: u8,
    pub data: &'a [u8],
}
impl<'a> BorrowedValue<'a> {
    pub fn to_owned_value(&self) -> Option<Value> { decode_value(self.tag, self.data) }
    pub fn as_f64(&self) -> Option<f64> {
        if (self.tag & 0xF0) == 0x10 { return Some((self.tag & 0x0F) as f64); }
        match self.tag {
            3 => { 
                let mut p = 4; // Skip length header
                let v = decode_varint(self.data, &mut p)?;
                Some(zigzag_decode(v) as f64)
            }
            4 => Some(f64::from_le_bytes(self.data.get(4..12)?.try_into().ok()?)),
            _ => None,
        }
    }
}

pub struct HakoDocIter<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: u16,
}

/// Owned handle to a borrowed document (v0.8.10+): pins the storage bytes
/// with an Arc and parses field values LAZILY on access. The sqlite3_stmt
/// analog — a cursor position plus pull-on-touch reads, with none of the
/// owned-decode cost (no fields Vec, no interning, no value allocs until
/// a field is actually pulled).
///
/// Get one via `Hako::get_view` (point read) or `Hako::walk_view`
/// (scan). `get` walks the framing with `skip_value` (no decode of
/// skipped fields); `to_owned_doc` fully decodes when you want it all.
#[derive(Clone)]
pub struct DocView {
    bytes: Arc<Vec<u8>>,
    time: i64,
    fields_count: u16,
}

impl DocView {
    /// Pin already-resident bytes. Validates the framing header (magic,
    /// version, length floor) — full field walks happen per access, all
    /// bounds-checked, with strict decode ruling in `to_owned_doc`.
    /// (A full up-front validation pass cost a second framing walk per
    /// row, ~2x on scans, for corrupt data storage never holds.)
    pub fn new(bytes: Arc<Vec<u8>>) -> Option<Self> {
        let view = HakoDocView::new(&bytes)?;
        Some(Self { time: view._time, fields_count: view.fields_count, bytes })
    }

    /// Number of top-level fields.
    pub fn len(&self) -> usize {
        self.fields_count as usize
    }

    pub fn is_empty(&self) -> bool {
        self.fields_count == 0
    }

    /// Logical timestamp carried by the encoding.
    pub fn time(&self) -> i64 {
        self.time
    }

    /// Raw storage bytes (opaque encoding — hash/count/export freely).
    pub fn as_bytes(&self) -> &[u8] {
        &self.bytes
    }

    /// Pull one field by name: linear framing walk, decode on hit only.
    /// Skipped fields cost a `skip_value` pointer bump each, never a decode.
    pub fn get(&self, field: &str) -> Option<Value> {
        let mut pos = 12usize;
        let bytes: &[u8] = &self.bytes;
        for _ in 0..self.fields_count {
            let k_len = *bytes.get(pos)? as usize;
            pos += 1;
            let key = std::str::from_utf8(bytes.get(pos..pos + k_len)?).ok()?;
            pos += k_len;
            let tag = *bytes.get(pos)?;
            pos += 1;
            let start = pos;
            skip_value(tag, bytes, &mut pos)?;
            if key == field {
                return decode_value(tag, &bytes[start..pos]);
            }
        }
        None
    }

    /// Pull a nested value by dotted path (`profile.sub_03`): descends
    /// through Map values only, decoding nothing but the terminal —
    /// siblings, ancestors, and skipped subtrees cost framing skips.
    /// Single-segment paths behave exactly like [`Self::get`]. Returns
    /// None on any miss, on descent into a non-Map value, on empty
    /// segments, or on corrupt framing.
    pub fn get_path(&self, path: &str) -> Option<Value> {
        let mut segs = path.split('.');
        let first = segs.next()?;
        if first.is_empty() {
            return None;
        }
        // Top level uses the same walk as `get`, but keeps the raw value
        // span so descent avoids a decode-then-re-encode round trip.
        let bytes: &[u8] = &self.bytes;
        let mut pos = 12usize;
        let mut cur: Option<(u8, &[u8])> = None;
        for _ in 0..self.fields_count {
            let k_len = *bytes.get(pos)? as usize;
            pos += 1;
            let key = std::str::from_utf8(bytes.get(pos..pos + k_len)?).ok()?;
            pos += k_len;
            let tag = *bytes.get(pos)?;
            pos += 1;
            let start = pos;
            skip_value(tag, bytes, &mut pos)?;
            if key == first {
                cur = Some((tag, &bytes[start..pos]));
                break;
            }
        }
        let (mut tag, mut span) = cur?;
        // One Map level per remaining segment; the terminal decodes once.
        // Key validation mirrors `get`: every key the traversal touches
        // must be valid UTF-8, corrupt keys reject.
        for seg in segs {
            if seg.is_empty() || tag != 8 {
                return None;
            }
            // Map body: [len u32][count u16][entries].
            let count = u16::from_le_bytes(span.get(4..6)?.try_into().ok()?) as usize;
            let mut p = 6usize;
            let mut hit = None;
            for _ in 0..count {
                let k_len = *span.get(p)? as usize;
                p += 1;
                let key = span.get(p..p + k_len)?;
                p += k_len;
                let t = *span.get(p)?;
                p += 1;
                let s = p;
                let mut q = s;
                skip_value(t, span, &mut q)?;
                std::str::from_utf8(key).ok()?;
                if key == seg.as_bytes() {
                    hit = Some((t, &span[s..q]));
                    break;
                }
                p = q;
            }
            (tag, span) = hit?;
        }
        decode_value(tag, span)
    }

    /// Iterate `(key, borrowed value)` pairs without materializing.
    pub fn iter(&self) -> impl Iterator<Item = (&str, BorrowedValue<'_>)> + '_ {
        let bytes: &[u8] = &self.bytes;
        let mut pos = 12usize;
        let mut remaining = self.fields_count;
        std::iter::from_fn(move || {
            if remaining == 0 {
                return None;
            }
            let k_len = *bytes.get(pos)? as usize;
            pos += 1;
            let key = std::str::from_utf8(bytes.get(pos..pos + k_len)?).ok()?;
            pos += k_len;
            let tag = *bytes.get(pos)?;
            pos += 1;
            let start = pos;
            skip_value(tag, bytes, &mut pos)?;
            remaining -= 1;
            Some((key, BorrowedValue { tag, data: &bytes[start..pos] }))
        })
    }

    /// Escape hatch: full owned decode.
    pub fn to_owned_doc(&self) -> Option<HakoDoc> {
        HakoDoc::decode(&self.bytes)
    }
}

impl<'a> HakoDocIter<'a> {
    /// Fields not yet consumed. After a full pass this must be 0 —
    /// `decode` rejects buffers that end mid-document instead of
    /// returning a silently short doc.
    pub fn remaining(&self) -> u16 {
        self.remaining
    }
    /// Byte offset after the last consumed value. A well-formed buffer is
    /// consumed exactly (`decode` rejects trailing garbage the same way).
    pub fn position(&self) -> usize {
        self.pos
    }
}

impl<'a> Iterator for HakoDocIter<'a> {
    type Item = (&'a str, u8, &'a [u8]);    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 { return None; }
        let k_len = *self.bytes.get(self.pos)? as usize;
        self.pos += 1;
        let key = std::str::from_utf8(self.bytes.get(self.pos..self.pos + k_len)?).ok()?;
        self.pos += k_len;
        let tag = *self.bytes.get(self.pos)?;
        self.pos += 1;

        let start = self.pos;
        skip_value(tag, self.bytes, &mut self.pos)?;
        let data = &self.bytes[start..self.pos];

        self.remaining -= 1;
        Some((key, tag, data))
    }
}

/// Unvalidating framing cursor: identical traversal to [`HakoDocIter`]
/// minus the per-key UTF-8 check. Match-only callers (filter byte-compare)
/// use this; any row they accept is validated downstream by the owned
/// decode, so skipping validation here changes no outcome.
pub struct HakoDocRawIter<'a> {
    bytes: &'a [u8],
    pos: usize,
    remaining: u16,
}

impl<'a> Iterator for HakoDocRawIter<'a> {
    type Item = (&'a [u8], u8, &'a [u8]);
    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 { return None; }
        let k_len = *self.bytes.get(self.pos)? as usize;
        self.pos += 1;
        let key = self.bytes.get(self.pos..self.pos + k_len)?;
        self.pos += k_len;
        let tag = *self.bytes.get(self.pos)?;
        self.pos += 1;

        let start = self.pos;
        skip_value(tag, self.bytes, &mut self.pos)?;
        let data = &self.bytes[start..self.pos];

        self.remaining -= 1;
        Some((key, tag, data))
    }
}

pub(crate) fn skip_value(tag: u8, bytes: &[u8], pos: &mut usize) -> Option<()> {
    if tag >= 0xC0 && tag <= 0xC3 { return Some(()); }
    if (tag & 0xF0) == 0x10 { return Some(()); }
    if (tag & 0xF8) == 0x40 {
        *pos += (tag & 0x07) as usize;
        // ponytail: the inline-length arm above is the only unchecked
        // advance — a corrupt tag can overshoot the buffer, and every
        // caller slices [start..pos] afterwards. Fail here instead of
        // panicking there (found via dotted-path fuzzing).
        if *pos > bytes.len() { return None; }
        return Some(());
    }

    match tag {
        3 | 4 | 5 | 6 | 7 | 8 | 9 | 10 | 11 => {
            // let v_len = u32::from_le_bytes(bytes.get(*pos..*pos + 4)?.try_into().ok()?) as usize;
            // *pos += 4 + v_len;
            // Some(())
            if *pos + 4 > bytes.len() { return None; }
            let v_len = u32::from_le_bytes(bytes[*pos..*pos + 4].try_into().ok()?) as usize;
            *pos += 4;
            // Ensure the claimed length actually exists in the buffer
            if *pos + v_len > bytes.len() { return None; }
            *pos += v_len;
            Some(())
        }
        _ => None,
    }
}

pub(crate) fn decode_value(tag: u8, bytes: &[u8]) -> Option<Value> {
    if tag == 0xC0 { return Some(Value::Null); }
    if tag == 0xC1 { return Some(Value::ServerTimestamp); }
    if tag == 0xC2 { return Some(Value::Bool(true)); }
    if tag == 0xC3 { return Some(Value::Bool(false)); }
    if (tag & 0xF0) == 0x10 { return Some(Value::Int((tag & 0x0F) as i64)); }
    if (tag & 0xF8) == 0x40 {
        let len = (tag & 0x07) as usize;
        return Some(Value::String(String::from_utf8(bytes.get(..len)?.to_vec()).ok()?));
    }

    match tag {
        3 | 7 => {
            let mut p = 4; 
            let val = decode_varint(bytes, &mut p)?;
            if tag == 3 { Some(Value::Int(zigzag_decode(val))) } 
            else { Some(Value::Timestamp(zigzag_decode(val))) }
        }
        4 => Some(Value::Float(f64::from_le_bytes(bytes.get(4..12)?.try_into().ok()?))),
        5 => {
            let v_len = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
            Some(Value::String(String::from_utf8(bytes.get(4..4+v_len)?.to_vec()).ok()?))
        }
        6 => {
            let v_len = u32::from_le_bytes(bytes.get(..4)?.try_into().ok()?) as usize;
            Some(Value::Binary(bytes.get(4..4+v_len)?.to_vec()))
        }
        8 => {
            let mut p = 4;
            let count = u16::from_le_bytes(bytes.get(p..p + 2)?.try_into().ok()?);
            p += 2;
            let mut fields = Vec::with_capacity(count as usize);
            for _ in 0..count {
                let k_len = *bytes.get(p)? as usize;
                p += 1;
                let key = std::str::from_utf8(bytes.get(p..p + k_len)?).ok()?;
                p += k_len;
                let inner_tag = *bytes.get(p)?;
                p += 1;
                let start = p;
                skip_value(inner_tag, bytes, &mut p)?;
                fields.push((intern_field(key), decode_value(inner_tag, &bytes[start..p])?));
            }
            Some(Value::Map(fields))
        }
        9 => {
            let mut p = 4;
            let count = u32::from_le_bytes(bytes.get(p..p + 4)?.try_into().ok()?) as usize;
            p += 4;
            let mut items = Vec::with_capacity(count);
            for _ in 0..count {
                let tag = *bytes.get(p)?;
                p += 1;
                let start = p;
                skip_value(tag, bytes, &mut p)?;
                items.push(decode_value(tag, &bytes[start..p])?);
            }
            Some(Value::Array(items))
        }
        10 => {
            let mut p = 4;
            let c_len = *bytes.get(p)? as usize;
            p += 1;
            let collection = std::str::from_utf8(bytes.get(p..p+c_len)?).ok()?.to_string();
            p += c_len;
            let d_len = *bytes.get(p)? as usize;
            p += 1;
            let doc_id = std::str::from_utf8(bytes.get(p..p+d_len)?).ok()?.to_string();
            Some(Value::Reference { collection, doc_id })
        }
        11 => {
            let offset = u64::from_le_bytes(bytes.get(4..12)?.try_into().ok()?);
            let len = u32::from_le_bytes(bytes.get(12..16)?.try_into().ok()?);
            Some(Value::BlobLink { offset, len })
        }
        _ => Some(Value::Null),
    }
}