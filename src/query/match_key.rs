//! Comparison-key cache for the fused residual-filter page.
//!
//! The scan loop used to re-encode a filter's comparison value inside the
//! per-row matching pass, although that value is a query constant and its
//! encoding is therefore identical for every row of the scan.
//!
//! This module encodes each value **once per fused page**, using the same codec
//! the storage writer uses (`HakoDoc::encode_value_to`), and hands the result
//! to the matcher. The loop then compares the borrowed stored bytes against the
//! cached bytes instead of rebuilding them.
//!
//! Gates (deliberately narrower than "any Eq"):
//!
//! * `Operator::Eq` only. `Ne` keeps the semantic comparator because a byte
//!   miss proves "different bytes", not "different value" - cross-type values
//!   such as `Int(7)` and `String("7")` compare equal semantically.
//! * Only [`byte_safe`] values participate. `Float` is excluded
//!   (`-0.0 == 0.0` on different bits); `Map`/`Array` are excluded because their
//!   field order is not canonical.
//! * Only an **exact storage tag** match may take the byte verdict. If the
//!   stored value uses another tag (e.g. stored `Int`, filter `String("7")`),
//!   the matcher falls back to [`crate::query::filter::compare_values`]. A miss
//!   must never become a rejection on its own.
//!
//! An empty key slot means "not eligible". Every call site falls back to the
//! old encode-and-compare path for that position, so a plan built by hand
//! (tests, older snapshot) stays correct: it merely pays the old per-row
//! encode, never a wrong comparison.

use smallvec::SmallVec;

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::query::filter::{Filter, Operator};

/// Canonical `(tag, body)` storage encoding of one comparison value.
///
/// Super-tag scalars (null, bool, an int `0..=15`, a string up to 7 bytes) fit
/// inline; longer values allocate one buffer **per plan**, not per row.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct MatchKey(SmallVec<[u8; 8]>);

impl MatchKey {
    #[inline]
    pub fn as_bytes(&self) -> &[u8] {
        &self.0
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Storage tag byte, or `None` when this value has no encoded form.
    #[inline]
    pub fn tag(&self) -> Option<u8> {
        self.0.first().copied()
    }

    /// Bytes after the tag byte.
    #[inline]
    pub fn body(&self) -> &[u8] {
        if self.0.is_empty() {
            &[]
        } else {
            &self.0[1..]
        }
    }

    /// Byte equality **is** value equality when the stored side carries the
    /// same canonical tag: the storage codec is deterministic per type, and
    /// these are the types [`byte_safe`] admits.
    #[inline]
    pub fn matches(&self, stored_tag: u8, stored_body: &[u8]) -> bool {
        self.tag() == Some(stored_tag) && self.body() == stored_body
    }

    /// Encode with the storage codec. The scratch buffer is reused, so this
    /// allocates only when a value is longer than `SmallVec`'s inline
    /// capacity (8 bytes here); the copy then lands inline or spills once.
    pub(crate) fn encode(v: &Value) -> Self {
        let mut heap = Vec::with_capacity(16);
        HakoDoc::encode_value_to(v, &mut heap);
        Self(SmallVec::from_slice(&heap))
    }
}

/// Value types with a canonical byte encoding: same value <=> same bytes.
///
/// `Float` is excluded (`-0.0 == 0.0` on different bits), and `Map`/`Array` are
/// excluded because their field order is not canonical. This is the single gate
/// used by the executor's fused lane and its compile-time gate, so the two
/// cannot disagree
/// about which values may be compared byte-wise.
#[inline]
pub fn byte_safe(v: &Value) -> bool {
    !matches!(v, Value::Float(_) | Value::Map(_) | Value::Array(_))
}

/// Encode a filter's comparison bytes, or `None` when the filter must keep the
/// semantic comparator. `Ne` is deliberately excluded: see module docs.
#[inline]
pub fn encode_for_filter(op: &Operator, v: &Value) -> Option<MatchKey> {
    if *op != Operator::Eq || !byte_safe(v) {
        return None;
    }
    Some(MatchKey::encode(v))
}

/// True when every predicate can be decided by comparing encoded bytes with no
/// semantic fallback:
///
/// * no dotted paths (`profile.city` needs a Map descent, not a byte compare);
/// * no `id` / `_time` (they live in the document header, not the body);
/// * only `Eq` (ordering and membership need `compare_values`);
/// * values must be [`byte_safe`] (no `Float` bit aliasing, no `Map`/`Array`
///   field-order ambiguity).
///
/// This is the eligibility gate used by the executor's fused page lane. A plan
/// that fails it keeps the generic matcher.
#[inline]
pub fn can_preverify(filters: &[Filter]) -> bool {
    filters.iter().all(|f| {
        !f.field.contains('.')
            && f.field != "id"
            && f.field != "_time"
            && f.op == Operator::Eq
            && byte_safe(&f.value)
    })
}

/// Per-fused-page key list, parallel to `QueryPlan::filters`.
#[derive(Debug, Clone, Default)]
pub struct MatchKeys {
    pub filters: Vec<MatchKey>,
}

impl MatchKeys {
    /// Compile the key list only when it covers the **whole** predicate list;
    /// otherwise return `None` and the caller keeps the generic matcher.
    ///
    /// A partially-filled cache is not useful: the matcher needs a byte verdict
    /// for every conjunct before it may skip a decode, and one semantic
    /// predicate in the list forces the decode for the row anyway.
    pub fn compile_fast_eq(filters: &[Filter]) -> Option<Self> {
        if filters.is_empty() || !can_preverify(filters) {
            return None;
        }
        let mut keys = Vec::with_capacity(filters.len());
        for f in filters {
            keys.push(encode_for_filter(&f.op, &f.value)?);
        }
        Some(Self { filters: keys })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn eq(v: Value) -> Option<MatchKey> {
        encode_for_filter(&Operator::Eq, &v)
    }

    #[test]
    fn eq_byte_safe_gets_a_key() {
        let k = eq(Value::String("Andi".into())).expect("eq string is byte safe");
        assert_eq!(k.body(), b"Andi");
        assert_eq!(k.tag(), Some(0x40 | 4));
    }

    #[test]
    fn long_string_still_encodes() {
        let k = eq(Value::String("Al".repeat(32))).expect("long string has a standard tag");
        assert_eq!(k.tag(), Some(5));
        assert_eq!(k.body().len(), 68); // u32 len + 64 bytes
    }

    #[test]
    fn ne_float_map_array_do_not_enter_the_fast_lane() {
        assert!(encode_for_filter(&Operator::Ne, &Value::String("a".into())).is_none());
        assert!(encode_for_filter(&Operator::Eq, &Value::Float(1.0)).is_none());
        assert!(eq(Value::Map(Vec::new())).is_none());
        assert!(eq(Value::Array(Vec::new())).is_none());
    }

    #[test]
    fn small_scalars_do_not_allocate() {
        for v in [Value::Null, Value::Bool(true), Value::Bool(false), Value::Int(9)] {
            let k = eq(v.clone()).expect("scalar is byte safe");
            assert!(!k.0.spilled(), "scalar key must stay inline: {v:?}");
        }
    }

    /// The cache must agree with the storage codec byte-for-byte, otherwise a
    /// plan-built key could claim a match the document never stored.
    #[test]
    fn key_is_exactly_the_stored_document_encoding() {
        for v in [
            Value::Null,
            Value::Bool(true),
            Value::Int(9),
            Value::Int(9_000),
            Value::String("Andi".into()),
            Value::String("x".repeat(64)),
        ] {
            let mut doc = HakoDoc::default();
            doc.fields.push(("field".into(), v.clone()));
            let encoded = doc.encode();
            let key = eq(v.clone()).expect("byte-safe value");
            // document: MAGIC, VERSION, time(8), count(2), key_len, key reservoir,
            // then the value encoding exactly as cached.
            let mut pos = 12;
            let k_len = encoded[pos] as usize;
            pos += 1 + k_len;
            assert_eq!(&encoded[pos..], key.as_bytes(), "codec drift for {v:?}");
        }
    }

    /// An ineligible position keeps an empty key, and a mixed list must refuse
    /// the whole cache rather than return a partially usable one.
    #[test]
    fn plan_layout_keeps_empty_slots() {
        let eq_filter = Filter {
            field: "a".into(),
            op: Operator::Eq,
            value: Value::String("x".into()),
        };
        let float_filter = Filter {
            field: "b".into(),
            op: Operator::Eq,
            value: Value::Float(1.5),
        };
        let ne_filter = Filter {
            field: "c".into(),
            op: Operator::Ne,
            value: Value::Bool(true),
        };
        assert!(can_preverify(std::slice::from_ref(&eq_filter)));
        assert!(!can_preverify(&[float_filter]));
        assert!(!can_preverify(&[ne_filter]));
        assert!(MatchKeys::compile_fast_eq(&[eq_filter]).is_some());
    }
}
