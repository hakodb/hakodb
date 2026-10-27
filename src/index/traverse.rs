//! Graph traversal over document references (issue #53, Phase A).
//!
//! Edges are the existing `Value::Reference` values — no new storage for
//! edge data. This module holds the only derived graph state: the
//! **backlink map** (reverse lookup: who points at this doc), rebuilt
//! from documents via hooks, never snapshotted (same rule as HNSW).
//!
//! Cost rule: tracking is opt-in per target collection
//! (`create_traverse_index`). Docs without references cost one field
//! scan; docs whose referees are all untracked cost one set lookup per
//! ref. Overwrites don't remove (no old doc in the hook): pushes dedupe
//! and traversal re-verifies edges against storage, so stale edges are
//! tolerated exactly like secondary-index over-approximation. Deletes
//! clean both directions through `remove_doc` (the old doc IS available
//! on that path).

use hashbrown::{HashMap, HashSet};

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;

/// Default depth guidance: traversals always terminate (cycles via the
/// visited set, depth via the cap below). Generous — real queries stop far
/// earlier.
pub const MAX_TRAVERSE_DEPTH: usize = 32;
/// Hard caller-facing cap (#156 Phase C): depth may be raised up to this
/// (memory of the walk is O(visited) — the bound exists so a deep walk on
/// a wide graph cannot balloon unbounded; default guidance stays 32).
pub const MAX_TRAVERSE_DEPTH_CAP: usize = 256;

/// One inbound edge: a `(field)` of `(src_col/src_id)` points at the key.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Backlink {
    pub src_col: String,
    pub src_id: String,
    pub field: String,
}

/// Reverse-edge store + tracked-target registry.
#[derive(Debug, Default)]
pub struct BacklinkIndex {
    tracked: HashSet<String>,
    edges: HashMap<String, HashMap<String, Vec<Backlink>>>,
}

impl BacklinkIndex {
    /// Opt a collection in as a traversal target (idempotent).
    pub fn track(&mut self, collection: &str) {
        self.tracked.insert(collection.to_string());
    }

    pub fn is_tracked(&self, collection: &str) -> bool {
        self.tracked.contains(collection)
    }

    pub fn is_empty(&self) -> bool {
        self.tracked.is_empty()
    }

    /// Sorted tracked collections (persistence + listing).
    pub fn tracked_cols(&self) -> Vec<String> {
        let mut out: Vec<String> = self.tracked.iter().cloned().collect();
        out.sort();
        out
    }

    /// Hook: index one doc's outbound refs into backlinks of TRACKED
    /// target collections. Blind-add with contains-dedupe (overwrites
    /// don't remove — see module docs).
    pub fn index_doc(&mut self, src_col: &str, src_id: &str, doc: &HakoDoc) {
        if self.tracked.is_empty() {
            return;
        }
        for (field, value) in doc.fields.iter() {
            let (tgt, tid) = match value {
                Value::Reference { collection: tgt, doc_id: tid } => (tgt, tid),
                _ => continue,
            };
            if !self.tracked.contains(tgt.as_str()) {
                continue;
            }
            let edges = self
                .edges
                .entry(tgt.clone())
                .or_default()
                .entry(tid.clone())
                .or_default();
            if !edges.iter().any(|e| {
                e.src_col == src_col && e.src_id == src_id && e.field.as_str() == &**field
            }) {
                edges.push(Backlink {
                    src_col: src_col.to_string(),
                    src_id: src_id.to_string(),
                    field: field.to_string(),
                });
            }
        }
    }

    /// Hook: drop edges touching a deleted doc — outbound (from its refs)
    /// and inbound (others pointing at it).
    pub fn remove_doc(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
        if self.edges.is_empty() {
            return;
        }
        for value in doc.fields.iter().map(|(_, v)| v) {
            if let Value::Reference { collection: tgt, doc_id: tid } = value {
                if let Some(map) = self.edges.get_mut(tgt) {
                    if let Some(edges) = map.get_mut(tid) {
                        edges.retain(|e| !(e.src_col == collection && e.src_id == doc_id));
                    }
                    if map.get(tid).is_some_and(|e| e.is_empty()) {
                        map.remove(tid);
                    }
                }
                if self.edges.get(tgt).is_some_and(|m| m.is_empty()) {
                    self.edges.remove(tgt);
                }
            }
        }
        if let Some(map) = self.edges.get_mut(collection) {
            map.remove(doc_id);
        }
        if self
            .edges
            .get(collection)
            .is_some_and(|m| m.is_empty())
        {
            self.edges.remove(collection);
        }
    }

    /// Inbound edges to one doc (caller verifies against storage —
    /// overwrites leave stale entries, same tolerance as secondary).
    pub fn inbound(&self, tgt_col: &str, tgt_id: &str) -> Option<&Vec<Backlink>> {
        self.edges.get(tgt_col).and_then(|m| m.get(tgt_id))
    }

    /// Stop tracking `collection` and drop every edge touching it.
    /// Mirrors `clear_collection`, but the registration goes too.
    pub fn untrack(&mut self, collection: &str) -> bool {
        self.clear_collection(collection);
        self.tracked.remove(collection)
    }

    /// Drop derived state touching `collection` (registrations stay —
    /// rescan refills, same rule as every other family).
    pub fn clear_collection(&mut self, collection: &str) {
        self.edges.remove(collection);
        let mut empty_maps = Vec::new();
        for (tgt, map) in self.edges.iter_mut() {
            for edges in map.values_mut() {
                edges.retain(|e| e.src_col != collection);
            }
            map.retain(|_, edges| !edges.is_empty());
            if map.is_empty() {
                empty_maps.push(tgt.clone());
            }
        }
        for tgt in empty_maps {
            self.edges.remove(&tgt);
        }
    }

    #[cfg(test)]
    pub fn edge_count(&self) -> usize {
        self.edges.values().map(|m| m.values().map(Vec::len).sum::<usize>()).sum()
    }
}

/// Traversal direction (Ffi u32: 0 = outgoing, 1 = incoming, 2 = both).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TraverseDir {
    Outgoing,
    Incoming,
    Both,
}

impl TraverseDir {
    pub fn from_u32(v: u32) -> crate::error::Result<Self> {
        match v {
            0 => Ok(TraverseDir::Outgoing),
            1 => Ok(TraverseDir::Incoming),
            2 => Ok(TraverseDir::Both),
            _ => Err(crate::error::HakoError::Corrupt(format!(
                "unknown traverse direction {v} (0=outgoing, 1=incoming, 2=both)"
            ))),
        }
    }

    pub fn uses_outgoing(self) -> bool {
        matches!(self, TraverseDir::Outgoing | TraverseDir::Both)
    }

    pub fn uses_incoming(self) -> bool {
        matches!(self, TraverseDir::Incoming | TraverseDir::Both)
    }
}

/// What to walk: direction, optional single edge field (None = any ref
/// field), depth bound, hit cap. Depth 0 is a hard error (nothing to
/// walk); above MAX_TRAVERSE_DEPTH is a hard error (never silently
/// clamp — the caller asked for something unbounded-adjacent).
///
/// Phase B virtues: `via_edge` names an edge collection (docs with `_from`/`_to`
/// references) whose edges join the walk alongside inline refs;
/// `weight_field` (requires `via_edge`) switches ordering from BFS to
/// Dijkstra over the named numeric prop (missing/non-numeric/negative
/// weights count 1.0/1.0/clamped-0 — documented, never silent-skip).
///
/// Phase C: `node_match` = filtered traversal — only nodes whose `field`
/// equals `Value` are visited AND expanded (the walk prunes descent into
/// non-matching nodes, so it can end up *unreachable* through filtered
/// territory — intended). The start node is always expanded regardless.
/// Comparison reuses `Value::PartialEq` (cross-type numeric aware:
/// Int(5) == Float(5.0)).
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraverseSpec {
    pub direction: TraverseDir,
    pub edge_field: Option<String>,
    pub max_depth: usize,
    pub limit: usize,
    pub via_edge: Option<String>,
    pub weight_field: Option<String>,
    pub node_match: Option<(String, Value)>,
    /// Edge predicates (#156 Phase F-1, #140-7): both apply to EDGE-COLLECTION
    /// hops only (via_edge) — an `edge_type` names the relationship routing
    /// (e.g. ("type", "follows")); an `edge_match` filters on any edge
    /// property. Both must hold when both are set. Inline-ref hops have no
    /// edge doc and are exempt (mirrors the start-node exemption).
    pub edge_type: Option<(String, Value)>,
    pub edge_match: Option<(String, Value)>,
    /// G1 (#168): via projection is opt-in. When false (the default) the
    /// walk skips the per-node discovery-edge map and the extra backtrack
    /// entirely — `TraverseHit.via` stays empty and FFI rows carry the
    /// v0.19.3 shape byte-for-byte (the "via" key is omitted). The via
    /// chain costs a map insert + 2 String clones per node plus a second
    /// per-hit backtrack — measured −27% BFS_qps when always-on (#167
    /// bench kajian), hence opt-in rather than always-on.
    pub include_via: bool,
}

impl TraverseSpec {
    pub fn validate(&self) -> crate::error::Result<()> {
        if self.max_depth == 0 || self.max_depth > MAX_TRAVERSE_DEPTH_CAP {
            return Err(crate::error::HakoError::Corrupt(format!(
                "traverse depth {} out of range 1..={MAX_TRAVERSE_DEPTH_CAP} (guidance: {MAX_TRAVERSE_DEPTH})",
                self.max_depth
            )));
        }
        if self.limit == 0 {
            return Err(crate::error::HakoError::Corrupt(
                "traverse limit must be >= 1".to_string(),
            ));
        }
        Ok(())
    }
}
