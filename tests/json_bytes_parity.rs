//! Byte-parity: `HakoDoc::write_json` must emit exactly what `to_json()` +
//! `serde_json::to_vec` produce, across shapes that stress ordering,
//! escaping, nesting, and numeric edges. Any divergence fails loudly.
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use std::sync::Arc;

fn check(doc: &HakoDoc) {
    let expected = serde_json::to_vec(&doc.to_json()).unwrap();
    let mut got = Vec::new();
    doc.write_json(&mut got);
    assert_eq!(got, expected, "mismatch for doc ts={}", doc._time);
    assert_eq!(doc.to_json_bytes(), expected);
}

fn doc_with_time(fields: Vec<(&str, Value)>, ts: i64) -> HakoDoc {
    let mut d = HakoDoc::default();
    for (k, v) in fields {
        d.insert(k, v);
    }
    d._time = ts;
    d
}

#[test]
fn empty_doc_is_just_time() {
    check(&doc_with_time(vec![], 0));
    check(&doc_with_time(vec![], -5));
    check(&doc_with_time(vec![], i64::MAX));
}

#[test]
fn time_merges_at_sorted_position() {
    // Keys around "_time" in byte order: digits/uppercase sort before it.
    check(&doc_with_time(
        vec![
            ("0abc", Value::Int(1)),
            ("Zebra", Value::Int(2)),
            ("_time", Value::String("field-wins".into())),
            ("apple", Value::Int(3)),
        ],
        99,
    ));
    // _time last when all keys sort below it.
    check(&doc_with_time(vec![("a", Value::Int(1))], 7));
    // _time first when all keys sort above it.
    check(&doc_with_time(vec![("zzz", Value::Int(1))], 7));
}

#[test]
fn string_escapes_match_serde() {
    let tricky = "a\"b\\c\nd\re\tf\x08g\x0Ch\x00i\x1Fj\x7f😀é";
    check(&doc_with_time(vec![("s", Value::String(tricky.into()))], 1));
    // Key escaping too.
    let mut d = HakoDoc::default();
    d.insert("k\"ey\\", Value::Int(1));
    d._time = 2;
    check(&d);
}

#[test]
fn numerics_match_serde() {
    check(&doc_with_time(
        vec![
            ("zero", Value::Int(0)),
            ("neg", Value::Int(-1)),
            ("min", Value::Int(i64::MIN)),
            ("max", Value::Int(i64::MAX)),
            ("pi", Value::Float(3.14159)),
            ("big", Value::Float(1e100)),
            ("negzero", Value::Float(-0.0)),
            ("nan", Value::Float(f64::NAN)),
            ("inf", Value::Float(f64::INFINITY)),
            ("ninf", Value::Float(f64::NEG_INFINITY)),
            ("t", Value::Timestamp(1790635278743865)),
        ],
        3,
    ));
}

#[test]
fn shapes_match_serde() {
    // Nested maps inserted out of order (writer must sort like BTreeMap).
    let mut inner = Vec::new();
    inner.push((Arc::from("z"), Value::Int(1)));
    inner.push((Arc::from("a"), Value::Int(2)));
    check(&doc_with_time(
        vec![
            ("nul", Value::Null),
            ("st", Value::ServerTimestamp),
            ("b", Value::Bool(true)),
            ("bf", Value::Bool(false)),
            ("arr", Value::Array(vec![Value::Int(1), Value::String("x".into()), Value::Null])),
            ("nested", Value::Array(vec![Value::Map(inner.clone())])),
            ("map", Value::Map(inner)),
            ("bin", Value::Binary(vec![0, 1, 255, 254])),
            ("binempty", Value::Binary(vec![])),
            ("r", Value::Reference { collection: "posts".into(), doc_id: "a\"b".into() }),
            ("bl", Value::BlobLink { offset: 123456789012345, len: 999 }),
            ("emptystr", Value::String("".into())),
            ("emptyarr", Value::Array(vec![])),
        ],
        42,
    ));
}

#[test]
fn writer_appends_to_existing_buffer() {
    let d = doc_with_time(vec![("a", Value::Int(1))], 5);
    let mut out = b"prefix:".to_vec();
    d.write_json(&mut out);
    let expected = serde_json::to_vec(&d.to_json()).unwrap();
    assert_eq!(&out[7..], &expected[..]);
    assert_eq!(&out[..7], b"prefix:");
}
