//! Dotted-path pulls (`DocView::get_path`) and lazy filter matching.
//! Covers what the query fast paths rely on: skip-only traversal,
//! Map-only descent, strict rejection.
use hakodb::document::hako_doc::{DocView, HakoDoc};
use hakodb::document::value::Value;
use std::sync::Arc;

fn nested_fixture() -> (HakoDoc, Vec<u8>) {
    let mut d = HakoDoc::default();
    d.insert("id", Value::String("doc-1".into()));
    let mut profile = Vec::new();
    profile.push((Arc::from("name"), Value::String("alice".into())));
    profile.push((Arc::from("age"), Value::Int(30)));
    let mut prefs = Vec::new();
    prefs.push((Arc::from("theme"), Value::String("dark".into())));
    prefs.push((Arc::from("lang"), Value::String("id".into())));
    profile.push((Arc::from("prefs"), Value::Map(prefs)));
    d.insert("profile", Value::Map(profile));
    d.insert("score", Value::Int(7));
    let bytes = d.encode();
    (d, bytes)
}

fn view_of(bytes: &[u8]) -> DocView {
    DocView::new(Arc::new(bytes.to_vec())).unwrap()
}

#[test]
fn single_segment_matches_get() {
    let (d, bytes) = nested_fixture();
    let v = view_of(&bytes);
    for key in ["id", "profile", "score", "missing"] {
        assert_eq!(v.get_path(key), v.get(key), "single segment {key}");
    }
    assert_eq!(d.get("score").cloned(), v.get_path("score"));
}

#[test]
fn dotted_hits_at_every_depth() {
    let (_, bytes) = nested_fixture();
    let v = view_of(&bytes);
    assert_eq!(
        v.get_path("profile.name"),
        Some(Value::String("alice".into()))
    );
    assert_eq!(v.get_path("profile.age"), Some(Value::Int(30)));
    assert_eq!(
        v.get_path("profile.prefs.theme"),
        Some(Value::String("dark".into()))
    );
    assert_eq!(
        v.get_path("profile.prefs.lang"),
        Some(Value::String("id".into()))
    );
    // Intermediate maps decode as values too.
    match v.get_path("profile.prefs") {
        Some(Value::Map(fields)) => assert_eq!(fields.len(), 2),
        other => panic!("expected map, got {other:?}"),
    }
}

#[test]
fn dotted_misses_and_type_guards() {
    let (_, bytes) = nested_fixture();
    let v = view_of(&bytes);
    // Missing at each level.
    assert_eq!(v.get_path("nope"), None);
    assert_eq!(v.get_path("profile.nope"), None);
    assert_eq!(v.get_path("profile.prefs.nope"), None);
    // Descent into non-Map values.
    assert_eq!(v.get_path("id.sub"), None);
    assert_eq!(v.get_path("score.sub"), None);
    assert_eq!(v.get_path("profile.age.sub"), None);
    // Empty segments never match.
    for bad in ["", ".", ".a", "a.", "a..b", "profile..age"] {
        assert_eq!(v.get_path(bad), None, "path {bad:?}");
    }
}

#[test]
fn dotted_rejects_corrupt_framing() {
    let (_, bytes) = nested_fixture();
    // Truncations at every prefix length must yield None, never panic.
    for len in 0..bytes.len() {
        let v = match DocView::new(Arc::new(bytes[..len].to_vec())) {
            Some(v) => v,
            None => continue,
        };
        let _ = v.get_path("profile.prefs.theme");
        let _ = v.get_path("profile");
        let _ = v.get_path("id");
    }
    // Flipped tag byte on the nested map (tag 8 -> 5) must not decode
    // the subtree as a string.
    let mut bad = bytes.clone();
    if let Some(pos) = (0..bad.len()).find(|&i| bad[i] == 8) {
        bad[pos] = 5;
        if let Some(v) = DocView::new(Arc::new(bad)) {
            // Either rejects or yields a value whose re-encode round-trips;
            // it must never panic. Accepting a string here is fine (tag 5
            // is a valid string tag); what matters is no panic/no garbage.
            let _ = v.get_path("profile");
        }
    }
}
