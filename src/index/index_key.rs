use crate::document::value::Value;
use std::cell::RefCell;

thread_local! {
    /// Reused encode buffer for index maintenance (`index_document` encodes
    /// every indexed field per write). Borrowed per field, never escapes.
    pub static ENC_SCRATCH: RefCell<Vec<u8>> = RefCell::new(Vec::with_capacity(128));
}

pub fn encode_scalar(value: &Value) -> Vec<u8> {
    let mut out = Vec::new();
    encode_scalar_into(value, &mut out);
    out
}

/// ponytail: borrow-friendly form — encode into a caller-provided buffer
/// (e.g. a reused thread-local scratch) instead of allocating per call.
/// Byte-identical output to `encode_scalar`.
pub fn encode_scalar_into(value: &Value, out: &mut Vec<u8>) {
    match value {
        Value::Null => out.push(0),
        Value::Bool(v) => { out.push(1); out.push(*v as u8); }
        Value::Int(v) => {
            out.push(2);
            out.extend(v.to_be_bytes());
        }
        Value::Float(v) => {
            out.push(3);
            out.extend(v.to_bits().to_be_bytes());
        }
        Value::String(v) => encode_str_scalar_into(v, out),
        Value::Binary(v) => {
            out.push(5);
            out.extend((v.len() as u32).to_be_bytes());
            out.extend(v);
        }
        Value::Timestamp(v) => {
            out.push(6);
            out.extend(v.to_be_bytes());
        }
        Value::ServerTimestamp => out.push(0), // Fallback to Null
        Value::Map(_) => out.push(8),
        Value::Array(_) => out.push(9),
        Value::Reference { collection, doc_id } => {
            out.reserve(1 + 1 + collection.len() + 1 + doc_id.len());
            out.push(10); // Tag 10
            out.push(collection.len() as u8);
            out.extend_from_slice(collection.as_bytes());
            out.push(doc_id.len() as u8);
            out.extend_from_slice(doc_id.as_bytes());
        }
        Value::BlobLink { offset, len } => {
            out.push(11); // Tag 11
            out.extend_from_slice(&offset.to_be_bytes());
            out.extend_from_slice(&len.to_be_bytes());
        }
    }
}

/// String scalar encoding shared by `encode_scalar_into` and the id fast
/// path in index maintenance (avoids building a temp `Value::String`).
pub fn encode_str_scalar_into(s: &str, out: &mut Vec<u8>) {
    out.push(4);
    out.extend((s.len() as u32).to_be_bytes());
    out.extend(s.as_bytes());
}

/// Borrowed fast path: encode a stored (tag, value-bytes) pair straight
/// into a secondary-index key, byte-identical to decoding then calling
/// `encode_scalar_into`. Returns false on truncated/exotic input — the
/// caller falls back to the owned path (same outcome, never a wrong key).
pub fn encode_raw_scalar(tag: u8, data: &[u8], out: &mut Vec<u8>) -> bool {
    match tag {
        0xC0 => out.push(0),
        0xC1 => out.push(0), // ServerTimestamp falls back to Null, like owned
        0xC2 => out.extend_from_slice(&[1, 1]),
        0xC3 => out.extend_from_slice(&[1, 0]),
        _ if (tag & 0xF0) == 0x10 => {
            out.push(2);
            out.extend(((tag & 0x0F) as i64).to_be_bytes());
        }
        _ if (tag & 0xF8) == 0x40 => {
            let len = (tag & 0x07) as usize;
            let s = match data.get(..len) {
                Some(s) => s,
                None => return false,
            };
            // Same strictness as owned decode: invalid UTF-8 rejects.
            if std::str::from_utf8(s).is_err() {
                return false;
            }
            out.push(4);
            out.extend((s.len() as u32).to_be_bytes());
            out.extend_from_slice(s);
        }
        3 | 7 => {
            let mut p = 4;
            let raw = match crate::util::varint::decode_varint(data, &mut p) {
                Some(v) => v,
                None => return false,
            };
            let v = crate::util::varint::zigzag_decode(raw);
            out.push(if tag == 3 { 2 } else { 6 });
            out.extend(v.to_be_bytes());
        }
        4 => {
            let b: [u8; 8] = match data.get(4..12).and_then(|s| <[u8; 8]>::try_from(s).ok()) {
                Some(b) => b,
                None => return false,
            };
            // from_le storage then to_be key matches the owned roundtrip.
            out.push(3);
            out.extend(f64::from_le_bytes(b).to_bits().to_be_bytes());
        }
        5 | 6 => {
            let v_len = match data.get(..4) {
                Some(h) => u32::from_le_bytes([h[0], h[1], h[2], h[3]]) as usize,
                None => return false,
            };
            let body = match data.get(4..4 + v_len) {
                Some(s) => s,
                None => return false,
            };
            if tag == 5 && std::str::from_utf8(body).is_err() {
                return false;
            }
            out.push(if tag == 5 { 4 } else { 5 });
            out.extend((body.len() as u32).to_be_bytes());
            out.extend_from_slice(body);
        }
        8 | 9 => {
            // Compound values decode recursively; mirroring that walk
            // here would reimplement decode_value, so fall back to owned
            // (same outcome, rare in secondary indexes).
            return false;
        }
        10 => {
            let mut p = 4;
            let c_len = match data.get(p).copied() {
                Some(v) => v as usize,
                None => return false,
            };
            p += 1;
            let col = match data.get(p..p + c_len) {
                Some(s) => s,
                None => return false,
            };
            p += c_len;
            let d_len = match data.get(p).copied() {
                Some(v) => v as usize,
                None => return false,
            };
            p += 1;
            let doc = match data.get(p..p + d_len) {
                Some(s) => s,
                None => return false,
            };
            if std::str::from_utf8(col).is_err() || std::str::from_utf8(doc).is_err() {
                return false;
            }
            if col.len() > 255 || doc.len() > 255 {
                return false;
            }
            out.push(10);
            out.push(col.len() as u8);
            out.extend_from_slice(col);
            out.push(doc.len() as u8);
            out.extend_from_slice(doc);
        }
        11 => {
            let (off, len) = match (data.get(4..12), data.get(12..16)) {
                (Some(o), Some(l)) => (o, l),
                _ => return false,
            };
            out.push(11);
            out.extend_from_slice(&u64::from_le_bytes(off.try_into().unwrap()).to_be_bytes());
            out.extend_from_slice(&u32::from_le_bytes(len.try_into().unwrap()).to_be_bytes());
        }
        _ => {
            // Unknown tag decodes to Null in owned (catch-all arm).
            out.push(0);
        }
    }
    true
}

pub fn decode_scalar_as_f64(bytes: &[u8]) -> Option<f64> {
    let tag = *bytes.first()?;
    match tag {
        2 => { // Int
            let b: [u8; 8] = bytes.get(1..9)?.try_into().ok()?;
            Some(i64::from_be_bytes(b) as f64)
        }
        3 => { // Float
            let b: [u8; 8] = bytes.get(1..9)?.try_into().ok()?;
            Some(f64::from_bits(u64::from_be_bytes(b)))
        }
        _ => None
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::hako_doc::decode_value;

    /// Borrowed encoding must equal owned decode-then-encode for every
    /// scalar shape, and both must fail together on truncations.
    fn assert_equiv(tag: u8, data: &[u8]) {
        let mut raw_out = Vec::new();
        let raw_ok = encode_raw_scalar(tag, data, &mut raw_out);
        let owned = decode_value(tag, data);
        match owned {
            Some(v) => {
                assert!(raw_ok, "raw must succeed where owned does (tag {tag})");
                let mut owned_out = Vec::new();
                encode_scalar_into(&v, &mut owned_out);
                assert_eq!(raw_out, owned_out, "byte-identical keys (tag {tag})");
            }
            None => assert!(!raw_ok, "raw must fail where owned does (tag {tag})"),
        }
    }

    #[test]
    fn raw_scalar_matches_owned_across_shapes() {
        // Null / bool / small ints / short strings.
        assert_equiv(0xC0, &[]);
        assert_equiv(0xC2, &[]);
        assert_equiv(0xC3, &[]);
        assert_equiv(0x10, &[]);
        assert_equiv(0x1F, &[]);
        assert_equiv(0x40, &[]);
        assert_equiv(0x47, b"1234567");
        // Varint int / timestamp (zigzag of 7 / -3).
        assert_equiv(3, &[14, 0, 0, 0]);
        assert_equiv(7, &[5, 0, 0, 0]);
        // Float 1.5le, strings, binary.
        let mut f = vec![0u8, 0, 0, 0];
        f.extend_from_slice(&1.5f64.to_le_bytes());
        assert_equiv(4, &f);
        let mut s = vec![3u8, 0, 0, 0];
        s.extend_from_slice(b"abc");
        assert_equiv(5, &s);
        let mut b = vec![2u8, 0, 0, 0];
        b.extend_from_slice(b"hi");
        assert_equiv(6, &b);
        // Map / array fall back (see encoder).
        assert_equiv(8, &[]);
        assert_equiv(9, &[]);
        // Reference col/id, bloblink.
        let mut r = vec![0u8, 0, 0, 0, 2u8];
        r.extend_from_slice(b"co");
        r.push(3u8);
        r.extend_from_slice(b"doc");
        assert_equiv(10, &r);
        let mut bl = vec![0u8, 0, 0, 0];
        bl.extend_from_slice(&9u64.to_le_bytes());
        bl.extend_from_slice(&4u32.to_le_bytes());
        assert_equiv(11, &bl);
        // Truncation sweep: both sides fail together at every length.
        let full = s.clone();
        for n in 0..full.len() {
            assert_equiv(5, &full[..n]);
        }
        for n in 0..bl.len() {
            assert_equiv(11, &bl[..n]);
        }
        for n in 0..r.len() {
            assert_equiv(10, &r[..n]);
        }
        assert_equiv(0x47, b"12");
        // Unknown tag falls back.
        assert_equiv(0xFF, b"zzz");
    }
}
