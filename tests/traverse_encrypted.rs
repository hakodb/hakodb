//! Issue #209: find_paths returned [] on encrypted engines. The walk's
//! edge/doc decode path must work identically on encrypted storage.
use hakodb::config::{DurabilityMode, HakoConfig};
use hakodb::document::hako_doc::HakoDoc;
use hakodb::document::value::Value;
use hakodb::engine::Hako;
use hakodb::index::traverse::{TraverseDir, TraverseSpec};

fn tmp(label: &str) -> std::path::PathBuf {
    std::env::temp_dir().join(format!(
        "fl-test-trav-enc-{label}-{}",
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos()
    ))
}

fn open_enc(dir: &std::path::Path) -> Hako {
    let mut cfg = HakoConfig::default();
    cfg.durability_mode = DurabilityMode::Interval;
    cfg.encryption_key = Some("master-key-2026".into());
    Hako::open(dir, cfg).expect("open")
}

fn refdoc(target_col: &str, target_id: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert(
        "link",
        Value::Reference { collection: target_col.into(), doc_id: target_id.into() },
    );
    doc
}

fn edgedoc(from_col: &str, from: &str, to: &str) -> HakoDoc {
    let mut doc = HakoDoc::default();
    doc.insert(
        "_from",
        Value::Reference { collection: from_col.into(), doc_id: from.into() },
    );
    doc.insert(
        "_to",
        Value::Reference { collection: from_col.into(), doc_id: to.into() },
    );
    doc
}

fn spec(dir: TraverseDir, depth: usize, limit: usize) -> TraverseSpec {
    TraverseSpec { direction: dir, edge_field: None, max_depth: depth, limit, via_edge: None, weight_field: None, node_match: None, edge_type: None, edge_match: None, include_path: true, include_via: false }
}

fn wait(db: &Hako) {
    assert!(db.await_quiescent(std::time::Duration::from_secs(60)));
}

/// Ref-chain walk on a globally encrypted engine (issue #209).
#[test]
fn encrypted_engine_walks_ref_chain() {
    let dir = tmp("ref");
    let db = open_enc(&dir);
    db.create_traverse_index("docs").unwrap();
    db.put("docs", "a", &refdoc("docs", "b")).unwrap();
    db.put("docs", "b", &refdoc("docs", "c")).unwrap();
    db.put("docs", "c", &refdoc("docs", "a")).unwrap();
    wait(&db);
    let hits = db.find_paths("docs", "a", &spec(TraverseDir::Outgoing, 4, 50)).unwrap();
    assert!(!hits.is_empty(), "encrypted engine must walk the ref chain");
}

/// Edge-collection (via) walk on a globally encrypted engine, mirroring
/// the FFI repro (edge index + _from/_to refs + hops).
#[test]
fn encrypted_engine_walks_edge_collection() {
    let dir = tmp("edge");
    let db = open_enc(&dir);
    db.create_edge_index("edges").unwrap();
    db.create_traverse_index("nodes").unwrap();
    for i in 0..8 {
        let doc = refdoc("nodes", &format!("n_{}", (i * 7 + 3) % 8));
        db.put("nodes", &format!("n_{i}"), &doc).unwrap();
    }
    for i in 0..8 {
        let doc = edgedoc("nodes", &format!("n_{i}"), &format!("n_{}", (i * 7 + 3) % 8));
        db.put("edges", &format!("x_{i}"), &doc).unwrap();
    }
    wait(&db);
    let via_spec = TraverseSpec { via_edge: Some("edges".into()), ..spec(TraverseDir::Outgoing, 4, 50) };
    let hits = db.find_paths("nodes", "n_0", &via_spec).unwrap();
    assert!(!hits.is_empty(), "encrypted engine must walk edge collections");
}

/// Scale mirror of the hakobench Enc_Comp repro (200 nodes + 200 edge
/// docs in BATCH commits, indexes created BEFORE writes, both readiness
/// signals honored, frozen-FFI query shape: incoming, no edge field, no
/// via). The walk must not race the encrypted index maintenance:
/// is_indexes_ready + quiescence together must imply walkable rows.
#[test]
fn encrypted_engine_bulk_graph_never_misses() {
    for _ in 0..10 {
        let dir = tmp("bulk");
        let db = open_enc(&dir);
        let mut nodes = hakodb::engine::Transaction { mutations: Vec::new() };
        for i in 0..200 {
            let mut d = HakoDoc::default();
            d.insert("name", Value::String(format!("node {i}")));
            nodes.put("nodes", &format!("n_{i}"), d);
        }
        nodes.commit(&db).unwrap();
        let mut edges = hakodb::engine::Transaction { mutations: Vec::new() };
        for i in 0..200 {
            let d = edgedoc("nodes", &format!("n_{i}"), &format!("n_{}", (i * 7 + 3) % 200));
            edges.put("edges", &format!("x_{i}"), d);
        }
        edges.commit(&db).unwrap();
        // The FFI repro creates indexes AFTER the writes: the backfill
        // must decode the encrypted docs. This is the racy lane (#209).
        db.create_edge_index("edges").unwrap();
        db.create_traverse_index("nodes").unwrap();
        let mut ready = false;
        for _ in 0..600 {
            if db.is_indexes_ready() {
                ready = true;
                break;
            }
            std::thread::sleep(std::time::Duration::from_millis(20));
        }
        assert!(ready, "indexes must become ready (issue #209 setup)");
        wait(&db);
        let hits = db
            .find_paths("nodes", "n_0", &spec(TraverseDir::Incoming, 4, 50))
            .unwrap();
        if hits.is_empty() {
            // Distinguish "maintenance still pending" from "rows missing
            // for good": retry after a settle window.
            std::thread::sleep(std::time::Duration::from_millis(500));
            let after = db
                .find_paths("nodes", "n_0", &spec(TraverseDir::Incoming, 4, 50))
                .unwrap();
            let st = db.quiescence_status();
            panic!(
                "issue #209: walk empty though ready+quiescent (retry-after-500ms: {} hits, status: {st:?})",
                after.len()
            );
        }
    }
}
