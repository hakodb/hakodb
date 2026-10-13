//! Phase A graph traversal (issue #53): backlink tracking, BFS in all
//! directions, depth caps, cycles, deletes, persistence, determinism.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::traverse::{TraverseDir, TraverseSpec, MAX_TRAVERSE_DEPTH};

fn open_db(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Manual;
    Hako::open(dir, cfg).expect("open")
}

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-trav-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn refdoc(target_col: &str, target_id: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert(
        "link",
        Value::Reference { collection: target_col.into(), doc_id: target_id.into() },
    );
    doc
}

fn blank() -> HakoDoc {
    HakoDoc::default()
}

fn spec(dir: TraverseDir, depth: usize, limit: usize) -> TraverseSpec {
    TraverseSpec { direction: dir, edge_field: None, max_depth: depth, limit }
}

/// Chain a->b->c plus c->a cycle; users collection untracked.
fn seed_chain(db: &Hako) {
    db.put("docs", "a", &refdoc("docs", "b")).unwrap();
    db.put("docs", "b", &refdoc("docs", "c")).unwrap();
    db.put("docs", "c", &refdoc("docs", "a")).unwrap();
    db.put("docs", "plain", &blank()).unwrap();
    db.put("users", "u1", &refdoc("docs", "a")).unwrap();
}

fn wait(db: &Hako) {
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
}

#[test]
fn outgoing_walks_chain_and_stops_at_cycles() {
    let dir = tmp("out");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    seed_chain(&db);
    wait(&db);
    // a -> b -> c -> a(visited): 2 hits, cycle terminates.
    let hits = db.find_paths("docs", "a", &spec(TraverseDir::Outgoing, 10, 100)).unwrap();
    let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["b", "c"]);
    assert!(hits.iter().all(|h| h.collection == "docs"));
    assert_eq!(hits[0].depth, 1);
    assert_eq!(hits[1].depth, 2);
    // Depth cap truncates.
    let hits = db.find_paths("docs", "a", &spec(TraverseDir::Outgoing, 1, 100)).unwrap();
    assert_eq!(hits.len(), 1);
    assert_eq!(hits[0].id, "b");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn incoming_needs_tracking_and_walks_backlinks() {
    let dir = tmp("in");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    seed_chain(&db);
    wait(&db);
    // Who points at b? a (docs.a.link). BFS continues: a's inbound
    // (c + users.u1) land at depth 2.
    let hits = db.find_paths("docs", "b", &spec(TraverseDir::Incoming, 10, 100)).unwrap();
    let mut got: Vec<(&str, &str, usize)> = hits
        .iter()
        .map(|h| (h.collection.as_str(), h.id.as_str(), h.depth))
        .collect();
    got.sort();
    assert_eq!(got, vec![("docs", "a", 1), ("docs", "c", 2), ("users", "u1", 2)]);
    let hits = db.find_paths("docs", "a", &spec(TraverseDir::Incoming, 10, 100)).unwrap();
    let mut ids: Vec<(&str, &str)> = hits.iter().map(|h| (h.collection.as_str(), h.id.as_str())).collect();
    ids.sort();
    assert_eq!(ids, vec![("docs", "b"), ("docs", "c"), ("users", "u1")]);
    // Untracked start + incoming = loud error, not silent empty.
    let bad = db.find_paths("users", "u1", &spec(TraverseDir::Incoming, 5, 10));
    assert!(bad.is_err(), "must demand create_traverse_index");
    // Outgoing from untracked works (no state needed) — and follows
    // through tracked collections too (u1 -> a -> b -> c).
    let hits = db.find_paths("users", "u1", &spec(TraverseDir::Outgoing, 5, 10)).unwrap();
    let ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    assert_eq!(ids, vec!["a", "b", "c"]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn both_directions_dedupes_and_limits() {
    let dir = tmp("both");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    seed_chain(&db);
    wait(&db);
    let hits = db.find_paths("docs", "a", &spec(TraverseDir::Both, 10, 100)).unwrap();
    // Level 1: b (out), c + u1 (in). Level 2 adds nothing new
    // (b->c visited, c->a anchor, u1->a anchor). No self-hit on `a`.
    let mut got: Vec<(&str, &str, usize)> = hits
        .iter()
        .map(|h| (h.collection.as_str(), h.id.as_str(), h.depth))
        .collect();
    got.sort();
    assert_eq!(got, vec![("docs", "b", 1), ("docs", "c", 1), ("users", "u1", 1)]);
    // Limit truncates deterministically (sorted level prefix).
    let hits = db.find_paths("docs", "a", &spec(TraverseDir::Both, 10, 2)).unwrap();
    assert_eq!(hits.len(), 2);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn edge_field_filter_and_validation() {
    let dir = tmp("edge");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    let mut multi = HakoDoc::default();
    multi.insert("link", Value::Reference { collection: "docs".into(), doc_id: "x".into() });
    multi.insert("other", Value::Reference { collection: "docs".into(), doc_id: "y".into() });
    db.put("docs", "m", &multi).unwrap();
    db.put("docs", "x", &blank()).unwrap();
    db.put("docs", "y", &blank()).unwrap();
    wait(&db);
    let all = db.find_paths("docs", "m", &spec(TraverseDir::Outgoing, 5, 10)).unwrap();
    assert_eq!(all.len(), 2);
    let one = db
        .find_paths(
            "docs",
            "m",
            &TraverseSpec { direction: TraverseDir::Outgoing, edge_field: Some("other".into()), max_depth: 5, limit: 10 },
        )
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].id, "y");
    // depth 0 and over-cap are hard errors, never silent clamps.
    assert!(db.find_paths("docs", "m", &spec(TraverseDir::Outgoing, 0, 10)).is_err());
    assert!(db
        .find_paths("docs", "m", &spec(TraverseDir::Outgoing, MAX_TRAVERSE_DEPTH + 1, 10))
        .is_err());
    assert!(db.find_paths("docs", "m", &spec(TraverseDir::Outgoing, 5, 0)).is_err());
    // Missing anchor = empty (fail-closed).
    assert!(db.find_paths("docs", "ghost", &spec(TraverseDir::Outgoing, 5, 10)).unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn deletes_clean_both_directions() {
    let dir = tmp("del");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    db.put("docs", "a", &refdoc("docs", "b")).unwrap();
    db.put("docs", "b", &blank()).unwrap();
    wait(&db);
    assert_eq!(db.find_paths("docs", "b", &spec(TraverseDir::Incoming, 5, 10)).unwrap().len(), 1);
    db.delete("docs", "a").unwrap();
    wait(&db);
    assert!(db.find_paths("docs", "b", &spec(TraverseDir::Incoming, 5, 10)).unwrap().is_empty());
    // Target deleted: inbound gone too.
    db.put("docs", "c", &refdoc("docs", "d")).unwrap();
    db.put("docs", "d", &blank()).unwrap();
    wait(&db);
    db.delete("docs", "d").unwrap();
    wait(&db);
    // Forward edge to a deleted doc liveness-filters to empty.
    assert!(db.find_paths("docs", "c", &spec(TraverseDir::Outgoing, 5, 10)).unwrap().is_empty());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn defs_survive_restart_and_results_are_deterministic() {
    let dir = tmp("restart");
    let run = |db: &Hako| {
        db.find_paths("docs", "a", &spec(TraverseDir::Both, 10, 100))
            .unwrap()
            .into_iter()
            .map(|h| (h.collection, h.id, h.depth))
            .collect::<Vec<_>>()
    };
    {
        let db = open_db(&dir);
        db.create_traverse_index("docs").unwrap();
        seed_chain(&db);
        wait(&db);
        db.flush().unwrap();
    }
    {
        let db = open_db(&dir);
        wait(&db);
        let first = run(&db);
        assert!(!first.is_empty(), "backlinks must rebuild on open");
        let second = run(&db);
        assert_eq!(first, second, "traversal order must be deterministic");
        let listed = db.list_indexes(Some("docs"));
        assert!(listed.traverse.contains(&"docs".to_string()), "def must persist");
    }
    let _ = std::fs::remove_dir_all(&dir);
}
