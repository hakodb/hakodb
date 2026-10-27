//! Phase A graph traversal (issue #53): backlink tracking, BFS in all
//! directions, depth caps, cycles, deletes, persistence, determinism.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::traverse::{TraverseDir, TraverseSpec, MAX_TRAVERSE_DEPTH_CAP};

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
    TraverseSpec { direction: dir, edge_field: None, max_depth: depth, limit, via_edge: None, weight_field: None, node_match: None }
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
    // Full discovery paths (#140-1): start-inclusive, one link per hop.
    assert_eq!(
        hits[0].path,
        vec![("docs".to_string(), "a".to_string()), ("docs".to_string(), "b".to_string())]
    );
    assert_eq!(
        hits[1].path,
        vec![
            ("docs".to_string(), "a".to_string()),
            ("docs".to_string(), "b".to_string()),
            ("docs".to_string(), "c".to_string())
        ]
    );
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
            &TraverseSpec { direction: TraverseDir::Outgoing, edge_field: Some("other".into()), max_depth: 5, limit: 10, via_edge: None, weight_field: None, node_match: None },
        )
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].id, "y");
    // depth 0 and over-cap are hard errors, never silent clamps.
    assert!(db.find_paths("docs", "m", &spec(TraverseDir::Outgoing, 0, 10)).is_err());
    assert!(db
        .find_paths("docs", "m", &spec(TraverseDir::Outgoing, MAX_TRAVERSE_DEPTH_CAP + 1, 10))
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

fn wspec(dir: TraverseDir, depth: usize, limit: usize) -> TraverseSpec {
    TraverseSpec {
        direction: dir,
        edge_field: None,
        max_depth: depth,
        limit,
        via_edge: Some("rel".into()),
        weight_field: Some("w".into()),
        node_match: None,
    }
}

fn edge_doc(from_col: &str, from: &str, to_col: &str, to: &str, w: serde_json::Value) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert("_from", Value::Reference { collection: from_col.into(), doc_id: from.into() });
    doc.insert("_to", Value::Reference { collection: to_col.into(), doc_id: to.into() });
    let wv = match &w {
        serde_json::Value::Number(n) => n.as_f64().map(Value::Float).unwrap_or(Value::Null),
        _ => Value::Null,
    };
    doc.insert("w", wv);
    doc
}

/// Phase B: a-b-c chain exists ONLY as edge docs (no inline refs).
/// Dijkstra follows cheapest cost; BFS ignores weights.
fn seed_edges(db: &Hako) {
    db.create_edge_index("rel").unwrap();
    // a -1-> b -1-> c, plus a -10-> c direct (BFS prefers 1 hop, Dijkstra prefers 1+1).
    db.put("rel", "e1", &edge_doc("n", "a", "n", "b", serde_json::json!(1.0))).unwrap();
    db.put("rel", "e2", &edge_doc("n", "b", "n", "c", serde_json::json!(1.0))).unwrap();
    db.put("rel", "e3", &edge_doc("n", "a", "n", "c", serde_json::json!(10.0))).unwrap();
    for id in ["a", "b", "c"] {
        db.put("n", id, &blank()).unwrap();
    }
}

#[test]
fn dijkstra_prefers_cheap_over_short() {
    let dir = tmp("dijkstra");
    let db = open_db(&dir);
    seed_edges(&db);
    wait(&db);
    // BFS: a -> {b, c} (both depth 1, direct edges win by hop count).
    let bfs = db
        .find_paths("n", "a", &TraverseSpec {
            direction: TraverseDir::Outgoing,
            edge_field: None,
            max_depth: 5,
            limit: 10,
            via_edge: Some("rel".into()),
            weight_field: None,
            node_match: None,
        })
        .unwrap();
    let mut ids: Vec<&str> = bfs.iter().map(|h| h.id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["b", "c"]);
    assert!(bfs.iter().all(|h| (h.cost - h.depth as f64).abs() < 1e-9), "BFS cost == hops");
    // Dijkstra: c via b costs 2 < direct 10 — c comes with cost 2.
    let dj = db.find_paths("n", "a", &wspec(TraverseDir::Outgoing, 5, 10)).unwrap();
    let costs: Vec<(&str, f64)> = dj.iter().map(|h| (h.id.as_str(), h.cost)).collect();
    assert_eq!(costs.len(), 2);
    assert_eq!(costs[0].0, "b");
    assert!((costs[0].1 - 1.0).abs() < 1e-9);
    assert_eq!(costs[1].0, "c");
    assert!((costs[1].1 - 2.0).abs() < 1e-9, "cheapest path a-b-c, not direct: {costs:?}");
    // Cost order holds across bigger gaps: limit 1 takes cheapest only.
    let one = db
        .find_paths(
            "n",
            "a",
            &TraverseSpec { limit: 1, ..wspec(TraverseDir::Outgoing, 5, 10) },
        )
        .unwrap();
    assert_eq!(one.len(), 1);
    assert_eq!(one[0].id, "b");
    // Weighted paths follow the cheapest route (#140-1): c's path is the
    // a-b-c chain, not the direct 10-weight edge.
    let c_hit = dj.iter().find(|h| h.id == "c").unwrap();
    assert_eq!(
        c_hit.path,
        vec![
            ("n".to_string(), "a".to_string()),
            ("n".to_string(), "b".to_string()),
            ("n".to_string(), "c".to_string())
        ]
    );
    let b_hit = dj.iter().find(|h| h.id == "b").unwrap();
    assert_eq!(b_hit.path, vec![("n".to_string(), "a".to_string()), ("n".to_string(), "b".to_string())]);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn edge_weights_default_and_clamp() {
    let dir = tmp("weights");
    let db = open_db(&dir);
    db.create_edge_index("rel").unwrap();
    // Missing weight -> 1.0; negative -> clamped 0.
    db.put("rel", "e1", &edge_doc("n", "a", "n", "b", serde_json::json!(serde_json::Value::Null)))
        .unwrap();
    let mut negdoc = HakoDoc::default();
    negdoc.insert("_from", Value::Reference { collection: "n".into(), doc_id: "b".into() });
    negdoc.insert("_to", Value::Reference { collection: "n".into(), doc_id: "c".into() });
    negdoc.insert("w", Value::Float(-5.0));
    db.put("rel", "e2", &negdoc).unwrap();
    for id in ["a", "b", "c"] {
        db.put("n", id, &blank()).unwrap();
    }
    wait(&db);
    let dj = db.find_paths("n", "a", &wspec(TraverseDir::Outgoing, 5, 10)).unwrap();
    let costs: Vec<(&str, f64)> = dj.iter().map(|h| (h.id.as_str(), h.cost)).collect();
    assert_eq!(costs.len(), 2);
    assert!((costs[0].1 - 1.0).abs() < 1e-9, "missing weight counts 1.0: {costs:?}");
    assert!((costs[1].1 - 1.0).abs() < 1e-9, "negative clamps to 0, total 1.0: {costs:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn edge_incoming_walks_reverse() {
    let dir = tmp("edgein");
    let db = open_db(&dir);
    seed_edges(&db);
    wait(&db);
    // Incoming to c via edges: b (e2) and a (e3).
    let hits = db.find_paths("n", "c", &wspec(TraverseDir::Incoming, 5, 10)).unwrap();
    let mut ids: Vec<&str> = hits.iter().map(|h| h.id.as_str()).collect();
    ids.sort();
    assert_eq!(ids, vec!["a", "b"]);
    let _ = std::fs::remove_dir_all(&dir);
}

// ===== Phase C (#156): filtered traversal, 256 depth cap, simple paths =====

/// a -(kind:start, link)-> b -(kind:hop, link)-> c -(kind:hop, link)->
/// d -(kind:stop). Matching nodes visited AND expanded; non-matching
/// pruned. The start node is expanded regardless (`kind: start`).
fn seed_kinds(db: &Hako) {
    let mut a = HakoDoc::default();
    a.insert("kind", Value::String("start".into()));
    a.insert("link", Value::Reference { collection: "docs".into(), doc_id: "b".into() });
    let mut b = HakoDoc::default();
    b.insert("kind", Value::String("hop".into()));
    b.insert("link", Value::Reference { collection: "docs".into(), doc_id: "c".into() });
    let mut c = HakoDoc::default();
    c.insert("kind", Value::String("hop".into()));
    c.insert("link", Value::Reference { collection: "docs".into(), doc_id: "d".into() });
    let mut d = HakoDoc::default();
    d.insert("kind", Value::String("stop".into()));
    db.put("docs", "a", &a).unwrap();
    db.put("docs", "b", &b).unwrap();
    db.put("docs", "c", &c).unwrap();
    db.put("docs", "d", &d).unwrap();
}

fn nodespec(field: &str, want: &str) -> TraverseSpec {
    TraverseSpec {
        node_match: Some((field.into(), Value::String(want.into()))),
        ..spec(TraverseDir::Outgoing, 5, 10)
    }
}

fn rows_of(hits: &[hakodb::engine::TraverseHit]) -> Vec<(String, usize, Vec<String>)> {
    hits.iter()
        .map(|h| {
            (
                h.id.clone(),
                h.depth,
                h.path.iter().map(|(c, i)| format!("{c}/{i}")).collect(),
            )
        })
        .collect()
}

fn p(items: &[&str]) -> Vec<String> {
    items.iter().map(|s| s.to_string()).collect()
}

#[test]
fn node_match_filters_and_prunes_descent() {
    let dir = tmp("nmatch");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    seed_kinds(&db);
    wait(&db);
    // b and c match ("hop"); d does not and must not be reached (pruned
    // descent, not post-filter).
    let hits = db.find_paths("docs", "a", &nodespec("kind", "hop")).unwrap();
    let want: Vec<(String, usize, Vec<String>)> = vec![
        ("b".into(), 1, p(&["docs/a", "docs/b"])),
        ("c".into(), 2, p(&["docs/a", "docs/b", "docs/c"])),
    ];
    assert_eq!(rows_of(&hits), want);
    // A filter nothing satisfies prunes the first neighbor: empty walk.
    assert!(db.find_paths("docs", "a", &nodespec("kind", "never")).unwrap().is_empty());
    // Unfiltered walk sees d — the filter changed reachability.
    let plain = db.find_paths("docs", "a", &spec(TraverseDir::Outgoing, 5, 10)).unwrap();
    assert_eq!(
        plain.iter().map(|h| h.id.as_str()).collect::<Vec<_>>(),
        vec!["b", "c", "d"]
    );
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn depth_cap_allows_256_functionally_and_rejects_257() {
    let dir = tmp("cap");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    // Real 255-edge chain x000 -> x255: the cap must WORK, not just validate.
    for i in 0..256usize {
        let mut doc = HakoDoc::default();
        if i + 1 < 256 {
            doc.insert(
                "link",
                Value::Reference { collection: "docs".into(), doc_id: format!("x{:03}", i + 1) },
            );
        }
        db.put("docs", &format!("x{i:03}"), &doc).unwrap();
    }
    wait(&db);
    let hits = db.find_paths("docs", "x000", &spec(TraverseDir::Outgoing, 256, 300)).unwrap();
    let last = hits.last().expect("deep chain must have hits");
    assert_eq!(last.id, "x255");
    assert_eq!(last.depth, 255);
    assert_eq!(last.path.len(), 256);
    // Above the cap or zero: hard error, never a silent clamp.
    assert!(db.find_paths("docs", "x000", &spec(TraverseDir::Outgoing, 257, 300)).is_err());
    assert!(db.find_paths("docs", "x000", &spec(TraverseDir::Outgoing, 0, 300)).is_err());
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn find_simple_paths_routes_not_node_hits() {
    let dir = tmp("simple");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    // Diamond: a -> {b, d} -> c, distinct inline ref fields.
    let mut a = HakoDoc::default();
    a.insert("r1", Value::Reference { collection: "docs".into(), doc_id: "b".into() });
    a.insert("r2", Value::Reference { collection: "docs".into(), doc_id: "d".into() });
    let mut b = HakoDoc::default();
    b.insert("r1", Value::Reference { collection: "docs".into(), doc_id: "c".into() });
    let mut d = HakoDoc::default();
    d.insert("r1", Value::Reference { collection: "docs".into(), doc_id: "c".into() });
    db.put("docs", "a", &a).unwrap();
    db.put("docs", "b", &b).unwrap();
    db.put("docs", "c", &blank()).unwrap();
    db.put("docs", "d", &d).unwrap();
    wait(&db);
    let paths = db
        .find_simple_paths("docs", "a", &spec(TraverseDir::Outgoing, 5, 10))
        .unwrap();
    let want: Vec<(String, usize, Vec<String>)> = vec![
        ("a".into(), 0, p(&["docs/a"])),
        ("b".into(), 1, p(&["docs/a", "docs/b"])),
        ("d".into(), 1, p(&["docs/a", "docs/d"])),
        ("c".into(), 2, p(&["docs/a", "docs/b", "docs/c"])),
        ("c".into(), 2, p(&["docs/a", "docs/d", "docs/c"])),
    ];
    assert_eq!(rows_of(&paths), want);
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn find_simple_paths_never_repeats_nodes_and_respects_limit() {
    let dir = tmp("simplecyc");
    let db = open_db(&dir);
    db.create_traverse_index("docs").unwrap();
    seed_chain(&db); // a -> b -> c -> a cycle
    wait(&db);
    let paths = db
        .find_simple_paths("docs", "a", &spec(TraverseDir::Outgoing, 10, 100))
        .unwrap();
    assert_eq!(
        paths.iter().map(|h| h.path.len()).collect::<Vec<_>>(),
        vec![1, 2, 3],
        "cycle closes: no path may revisit a node"
    );
    // limit caps results, keeping the cheapest.
    let two = db
        .find_simple_paths("docs", "a", &spec(TraverseDir::Outgoing, 10, 2))
        .unwrap();
    assert_eq!(two.len(), 2);
    assert_eq!(two[1].id, "b");
    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn find_simple_paths_weighted_orders_by_cost() {
    let dir = tmp("simplew");
    let db = open_db(&dir);
    seed_edges(&db); // a -1-> b -1-> c, plus a -10-> c (edge docs, via "rel", weight "w")
    wait(&db);
    let paths = db
        .find_simple_paths("n", "a", &wspec(TraverseDir::Outgoing, 5, 10))
        .unwrap();
    let want: Vec<(String, usize, Vec<String>)> = vec![
        ("a".into(), 0, p(&["n/a"])),
        ("b".into(), 1, p(&["n/a", "n/b"])),
        ("c".into(), 2, p(&["n/a", "n/b", "n/c"])),
        ("c".into(), 1, p(&["n/a", "n/c"])),
    ];
    assert_eq!(rows_of(&paths), want);
    // The direct 10-weight route is costlier than the chain: 2 < 10.
    assert!(paths[3].cost > paths[2].cost);
    let _ = std::fs::remove_dir_all(&dir);
}
