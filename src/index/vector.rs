//! NoSQL-native vector similarity (P0: exact KNN, no ANN structure).
//!
//! Embeddings ride `Value::Binary` as LE `f32[]` — no new `Value` variant,
//! small vectors stay `PutInlined` so the index reads them with zero blob
//! resolution. One metric per index, fixed dim, validated at creation.
//! P1 (HNSW) plugs into the same `VectorDef` registry; P0 exact doubles as
//! its recall oracle.

use crate::error::{HakoError, Result};

/// Distance metric. One per index (like one definition per FTS field).
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Metric {
    Cosine,
    L2,
}

impl Metric {
    pub fn from_u32(v: u32) -> Result<Self> {
        match v {
            0 => Ok(Metric::Cosine),
            1 => Ok(Metric::L2),
            _ => Err(HakoError::Corrupt(format!("unknown vector metric {v} (0=cosine, 1=l2)"))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
        }
    }
}

/// P0 registration: definition only (no RAM structure yet — the P0 scan is
/// brute-force exact). P1's HNSW lives behind this same registry entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorDef {
    pub dim: u32,
    pub metric: Metric,
}

/// Max dims per index (Turso allows 65536; embedded starts tighter —
/// loosen when a consumer shows up with bigger models).
pub const MAX_DIM: u32 = 4096;

pub fn validate_dim(dim: u32) -> Result<()> {
    if dim == 0 || dim > MAX_DIM {
        return Err(HakoError::Corrupt(format!(
            "vector dim {dim} out of range 1..={MAX_DIM}"
        )));
    }
    Ok(())
}

/// Decode LE f32s from a `Value::Binary` payload. Length (not dim) is
/// checked here; dim agreement with the index is the caller's job.
pub fn decode_f32s(bytes: &[u8]) -> Result<Vec<f32>> {
    if bytes.len() % 4 != 0 {
        return Err(HakoError::Corrupt(format!(
            "vector bytes {} not a multiple of 4",
            bytes.len()
        )));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| f32::from_le_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

/// Encode f32s to the LE Binary carrier.
pub fn encode_f32s(v: &[f32]) -> Vec<u8> {
    let mut out = Vec::with_capacity(v.len() * 4);
    for x in v {
        out.extend_from_slice(&x.to_le_bytes());
    }
    out
}

/// Cosine distance in [0, 2]. Zero-norm inputs (degenerate zero vectors)
/// score 1.0 — maximally dissimilar, never NaN (NaN would poison top-K
/// heaps via total_cmp).
pub fn cosine_distance(a: &[f32], b: &[f32]) -> f64 {
    debug_assert_eq!(a.len(), b.len());
    let mut dot = 0.0f64;
    let mut na = 0.0f64;
    let mut nb = 0.0f64;
    for i in 0..a.len() {
        let (x, y) = (a[i] as f64, b[i] as f64);
        dot += x * y;
        na += x * x;
        nb += y * y;
    }
    let denom = na.sqrt() * nb.sqrt();
    if denom == 0.0 {
        return 1.0;
    }
    1.0 - dot / denom
}

/// Squared Euclidean distance (sqrt-free: ordering-identical to L2, and
/// top-K only needs order).
pub fn l2_distance_squared(a: &[f32], b: &[f32]) -> f64 {
    debug_assert_eq!(a.len(), b.len());
    let mut acc = 0.0f64;
    for i in 0..a.len() {
        let d = a[i] as f64 - b[i] as f64;
        acc += d * d;
    }
    acc
}

/// Dispatch on the index metric. Length mismatch is a caller bug (the
/// planner gates dim before any row is scored); saturate to +inf so a bad
/// row sorts last instead of panicking a query worker.
pub fn distance(metric: Metric, a: &[f32], b: &[f32]) -> f64 {
    if a.len() != b.len() {
        return f64::INFINITY;
    }
    match metric {
        Metric::Cosine => cosine_distance(a, b),
        Metric::L2 => l2_distance_squared(a, b),
    }
}

// ---------------------------------------------------------------------------
// P1: HNSW (in-RAM, per collection+field).
//
// From-scratch (~200 lines) instead of a new dependency: the algorithm is
// small, the tree already depends on nothing ANN-shaped, and a fixed-seed
// xorshift keeps inserts/searches bit-deterministic for tests. Literate
// parameters (M=16, ef_construction=200) — standard values, not tuned.
//
// Correctness contract with the engine:
// - Deletes are EXTERNAL: nodes are never removed here; the executor
//   filters candidates against storage liveness (same rule as the P0
//   exact scan). Re-put updates the vector in place (links kept —
//   position may be suboptimal, never wrong).
// - `complete` gates ANN use: only a full backfill/rebuild pass sets it.
//   Anything else (fresh registration, stale snapshot skip, clear) leaves
//   brute-force exact in charge — slower, never wrong.
// - NOT persisted to ram_indexes.bin in P1 (rebuild on open; P2 may
//   persist if rebuild proves slow). Sync needs nothing: peers rebuild
//   from the docs they already replicate.

const HNSW_M: usize = 16;
const HNSW_MMAX: usize = 16;
const HNSW_MMAX0: usize = 32;
const HNSW_EF_CONSTRUCTION: usize = 200;

struct HnswNode {
    /// int8-quantized vector + scale (dequant = iv * scale). Asymmetric
    /// design: docs keep full f32 (exact rescore reads them), the graph
    /// keeps 1 byte/dim. Cosine needs no dequant at all (scale cancels —
    /// see node_dist); L2 folds the scale into one multiply per dim.
    ivec: Vec<i8>,
    scale: f32,
    /// Scale-free shape norm Σivec² (f64: 127²·4096 ≈ 6.6e7 needs the
    /// headroom). Precomputed once — every cosine call reuses it.
    sum_sq: f64,
    /// links[level] = neighbor internal idxs.
    links: Vec<Vec<usize>>,
}

/// Symmetric int8 quantization: scale = max_abs/127 so the largest
/// component maps exactly. Zero vector → all-zero + scale 1 (dequant
/// stays exactly 0, cosine guards still apply downstream).
fn quantize(v: &[f32]) -> (Vec<i8>, f32) {
    let max = v.iter().map(|x| x.abs()).fold(0.0f32, f32::max);
    if max == 0.0 {
        return (vec![0; v.len()], 1.0);
    }
    let s = max / 127.0;
    (
        v.iter()
            .map(|x| (x / s).round().clamp(-128.0, 127.0) as i8)
            .collect(),
        s,
    )
}

#[derive(Clone, Copy)]
struct Cand {
    dist: f64,
    idx: usize,
}

impl PartialEq for Cand {
    fn eq(&self, other: &Self) -> bool {
        self.dist.to_bits() == other.dist.to_bits() && self.idx == other.idx
    }
}
impl Eq for Cand {}
impl PartialOrd for Cand {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for Cand {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Max-heap by distance (farthest on top for eviction), idx
        // breaks ties so the order is total and deterministic. NaN-free
        // by construction (see cosine_distance), so total_cmp is exact.
        self.dist
            .total_cmp(&other.dist)
            .then_with(|| self.idx.cmp(&other.idx))
    }
}

pub struct HnswIndex {
    dim: usize,
    metric: Metric,
    level_mult: f64,
    nodes: Vec<HnswNode>,
    id_to_idx: std::collections::HashMap<String, usize>,
    /// Reverse map: internal idx → doc id. Stable — nodes are never
    /// removed (deletes are external), so idxs never shift.
    idx_to_id: Vec<String>,
    entry: Option<usize>,
    rng: u64,
    complete: bool,
}

impl HnswIndex {
    pub fn new(dim: usize, metric: Metric) -> Self {
        Self {
            dim,
            metric,
            level_mult: 1.0 / (HNSW_M as f64).ln(),
            nodes: Vec::new(),
            id_to_idx: std::collections::HashMap::new(),
            idx_to_id: Vec::new(),
            entry: None,
            // Fixed seed: deterministic inserts/searches across runs
            // (tests pin behavior); randomness quality is irrelevant —
            // levels only need geometric spread, not crypto.
            rng: 0x9E3779B97F4A7C15,
            complete: false,
        }
    }

    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    pub fn complete(&self) -> bool {
        self.complete
    }

    pub fn set_complete(&mut self, v: bool) {
        self.complete = v;
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
        self.id_to_idx.clear();
        self.idx_to_id.clear();
        self.entry = None;
        self.complete = false;
    }

    /// Doc id for an internal idx from `search` (caller-verified in range).
    pub fn id_of(&self, idx: usize) -> &str {
        &self.idx_to_id[idx]
    }

    fn next_random(&mut self) -> f64 {
        // xorshift64*: (0,1]-ish; clamp away from 0 so ln() stays finite.
        self.rng ^= self.rng << 13;
        self.rng ^= self.rng >> 7;
        self.rng ^= self.rng << 17;
        ((self.rng >> 11) as f64) / ((1u64 << 53) as f64).max(f64::MIN_POSITIVE)
    }

    fn random_level(&mut self) -> usize {
        (-self.next_random().ln() * self.level_mult) as usize
    }

    /// Σq² for a full-precision query (precomputed once per search/insert,
    /// reused by every distance call on the path).
    fn query_norm_sq(q: &[f32]) -> f64 {
        q.iter().map(|x| (*x as f64) * (*x as f64)).sum()
    }

    /// Query(full f32) vs node(int8): asymmetric by design — the query
    /// stays full precision, the node dequantizes implicitly. Cosine's
    /// scale cancels (shape-only); L2 folds scale into the dot term.
    /// Zero-norm either side → 1.0 (cosine) — same guard as the exact path.
    fn q_dist(&self, q: &[f32], qn: f64, idx: usize) -> f64 {
        let n = &self.nodes[idx];
        let mut dot = 0.0f64;
        for i in 0..q.len() {
            dot += q[i] as f64 * n.ivec[i] as f64;
        }
        match self.metric {
            Metric::Cosine => {
                let denom = qn.sqrt() * n.sum_sq.sqrt();
                if denom == 0.0 { 1.0 } else { 1.0 - dot / denom }
            }
            Metric::L2 => {
                let s = n.scale as f64;
                qn + s * s * n.sum_sq - 2.0 * s * dot
            }
        }
    }

    /// Node vs node on quantized bytes (integer dot, exact): drives the
    /// diversity heuristic and link shrinking without any dequant
    /// temporaries.
    fn node_dist(&self, a: usize, b: usize) -> f64 {
        let (na, nb) = (&self.nodes[a], &self.nodes[b]);
        let mut dot = 0i64;
        for i in 0..na.ivec.len() {
            dot += na.ivec[i] as i64 * nb.ivec[i] as i64;
        }
        match self.metric {
            Metric::Cosine => {
                let denom = (na.sum_sq * nb.sum_sq).sqrt();
                if denom == 0.0 { 1.0 } else { 1.0 - dot as f64 / denom }
            }
            Metric::L2 => {
                let (sa, sb) = (na.scale as f64, nb.scale as f64);
                sa * sa * na.sum_sq + sb * sb * nb.sum_sq - 2.0 * sa * sb * dot as f64
            }
        }
    }

    /// Insert or update. Wrong-dim vectors are ignored (fail-closed, same
    /// rule as the planner gate). Updates re-quantize in place and keep
    /// existing links.
    pub fn upsert(&mut self, id: &str, vec: Vec<f32>) {
        if vec.len() != self.dim {
            return;
        }
        if let Some(&idx) = self.id_to_idx.get(id) {
            let (ivec, scale) = quantize(&vec);
            let n = &mut self.nodes[idx];
            n.sum_sq = ivec.iter().map(|x| (*x as f64) * (*x as f64)).sum();
            n.ivec = ivec;
            n.scale = scale;
            return;
        }
        let level = self.random_level();
        let idx = self.nodes.len();
        let (ivec, scale) = quantize(&vec);
        let sum_sq = ivec.iter().map(|x| (*x as f64) * (*x as f64)).sum();
        self.nodes.push(HnswNode {
            ivec,
            scale,
            sum_sq,
            links: vec![Vec::new(); level + 1],
        });
        self.id_to_idx.insert(id.to_string(), idx);
        self.idx_to_id.push(id.to_string());

        let Some(mut cur) = self.entry else {
            self.entry = Some(idx);
            return;
        };
        let top = self.nodes[cur].links.len() - 1;
        // Insert-time routing uses the FULL-precision vector (better than
        // the quantized copy the node now stores).
        let qn = Self::query_norm_sq(&vec);
        // Greedy descent to the insertion levels.
        for l in ((level + 1)..=top).rev() {
            cur = self.greedy_closest(&vec, qn, cur, l);
        }
        for l in (0..=level.min(top)).rev() {
            let cand = self.search_layer(&vec, qn, &[cur], HNSW_EF_CONSTRUCTION, l);
            if cand.is_empty() {
                continue;
            }
            let m = if l == 0 { HNSW_MMAX0 } else { HNSW_MMAX };
            let neighbors = self.select_neighbors(cand, m.min(HNSW_M));
            if neighbors.is_empty() {
                continue;
            }
            cur = neighbors[0];
            let mmax = if l == 0 { HNSW_MMAX0 } else { HNSW_MMAX };
            for nb in &neighbors {
                self.add_link(idx, *nb, l);
                self.add_link(*nb, idx, l);
            }
            self.shrink_links(idx, l, mmax);
            for nb in &neighbors {
                self.shrink_links(*nb, l, mmax);
            }
        }
        if level > top {
            self.entry = Some(idx);
        }
    }

    /// Greedy single-best descent at one level (ef=1 beam).
    fn greedy_closest(&self, q: &[f32], qn: f64, entry: usize, level: usize) -> usize {
        let mut best = entry;
        let mut best_d = self.q_dist(q, qn, entry);
        // ponytail: fixed-point loop, not a worklist — one pass per
        // improvement, terminates when no neighbor is closer.
        loop {
            let mut improved = false;
            for &nb in &self.nodes[best].links[level] {
                let d = self.q_dist(q, qn, nb);
                if d < best_d {
                    best_d = d;
                    best = nb;
                    improved = true;
                }
            }
            if !improved {
                return best;
            }
        }
    }

    /// Beam search at one level from `entries`, keeping `ef` best.
    /// Returns (dist, idx) sorted ascending. `visited`/`tag` dedupe across
    /// layers without a HashSet alloc per call.
    fn search_layer(
        &self,
        q: &[f32],
        qn: f64,
        entries: &[usize],
        ef: usize,
        level: usize,
    ) -> Vec<(f64, usize)> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        let ef = ef.max(1);
        let mut visited = vec![false; self.nodes.len()];
        let mut cands: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        let mut result: BinaryHeap<Cand> = BinaryHeap::new();
        for &e in entries {
            if e < self.nodes.len() && !visited[e] {
                visited[e] = true;
                let d = self.q_dist(q, qn, e);
                cands.push(Reverse(Cand { dist: d, idx: e }));
                result.push(Cand { dist: d, idx: e });
                if result.len() > ef {
                    result.pop();
                }
            }
        }
        while let Some(Reverse(c)) = cands.pop() {
            // Stop when the closest unexpanded candidate is farther than
            // the worst kept result — nothing beyond can improve it.
            if result.len() >= ef {
                if let Some(worst) = result.peek() {
                    if c.dist > worst.dist {
                        break;
                    }
                }
            }
            for &nb in &self.nodes[c.idx].links[level] {
                if !visited[nb] {
                    visited[nb] = true;
                    let d = self.q_dist(q, qn, nb);
                    let mut push = result.len() < ef;
                    if !push {
                        if let Some(worst) = result.peek() {
                            push = d < worst.dist;
                        }
                    }
                    if push {
                        cands.push(Reverse(Cand { dist: d, idx: nb }));
                        result.push(Cand { dist: d, idx: nb });
                        if result.len() > ef {
                            result.pop();
                        }
                    }
                }
            }
        }
        let mut out: Vec<(f64, usize)> = result.into_iter().map(|c| (c.dist, c.idx)).collect();
        out.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        out
    }

    /// Standard HNSW neighbor-selection heuristic: closest first, keep a
    /// candidate only if no kept neighbor lies between it and the query
    /// (diversity over raw proximity — what keeps recall high on
    /// clustered data vs naive keep-M-closest). `cand` carries
    /// query-distances, so q itself is not needed.
    fn select_neighbors(&self, mut cand: Vec<(f64, usize)>, m: usize) -> Vec<usize> {
        cand.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        let mut out: Vec<usize> = Vec::with_capacity(m);
        'next: for (d, idx) in cand {
            for &kept in &out {
                if self.node_dist(idx, kept) < d {
                    continue 'next;
                }
            }
            out.push(idx);
            if out.len() >= m {
                break;
            }
        }
        out
    }

    fn add_link(&mut self, a: usize, b: usize, level: usize) {
        if a == b {
            return;
        }
        if !self.nodes[a].links[level].contains(&b) {
            self.nodes[a].links[level].push(b);
        }
    }

    fn shrink_links(&mut self, idx: usize, level: usize, mmax: usize) {
        if self.nodes[idx].links[level].len() <= mmax {
            return;
        }
        let cand: Vec<(f64, usize)> = self.nodes[idx].links[level]
            .iter()
            .map(|&nb| (self.node_dist(idx, nb), nb))
            .collect();
        self.nodes[idx].links[level] = self.select_neighbors(cand, mmax);
    }

    /// Top-`ef` candidate internal idxs for `query`, nearest-first.
    /// Callers map idx → doc id, verify liveness/filters, and exact-rescore.
    pub fn search(&self, query: &[f32], ef: usize) -> Vec<(f64, usize)> {
        let entry = match self.entry {
            Some(e) => e,
            None => return Vec::new(),
        };
        if query.len() != self.dim {
            return Vec::new();
        }
        let qn = Self::query_norm_sq(query);
        let top = self.nodes[entry].links.len() - 1;
        let mut cur = entry;
        for l in (1..=top).rev() {
            cur = self.greedy_closest(query, qn, cur, l);
        }
        self.search_layer(query, qn, &[cur], ef, 0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn cosine_basics() {
        let a = vec![1.0, 0.0];
        assert_eq!(cosine_distance(&a, &a), 0.0);
        // Orthogonal => 1.0.
        assert!((cosine_distance(&a, &[0.0, 1.0]) - 1.0).abs() < 1e-6);
        // Opposite => 2.0. Scale-invariant.
        assert!((cosine_distance(&a, &[-3.0, 0.0]) - 2.0).abs() < 1e-6);
        assert!((cosine_distance(&[2.0, 0.0], &[5.0, 0.0])).abs() < 1e-6);
    }

    #[test]
    fn cosine_zero_vector_never_nan() {
        let d = cosine_distance(&[0.0, 0.0], &[1.0, 2.0]);
        assert!(d.is_finite());
        assert_eq!(d, 1.0);
        assert!(cosine_distance(&[0.0], &[0.0]).is_finite());
    }

    #[test]
    fn l2_squared_ordering() {
        let q = vec![1.0, 1.0];
        // (3,4)-offset => 9+... (dx=2,dy=2) => 8; (dx=1,dy=0) => 1.
        assert_eq!(l2_distance_squared(&q, &[3.0, 3.0]), 8.0);
        assert_eq!(l2_distance_squared(&q, &[2.0, 1.0]), 1.0);
        assert_eq!(l2_distance_squared(&q, &q), 0.0);
    }

    #[test]
    fn roundtrip_bytes() {
        let v = vec![0.5, -1.25, 3.0];
        let b = encode_f32s(&v);
        assert_eq!(b.len(), 12);
        assert_eq!(decode_f32s(&b).expect("decode"), v);
        assert!(decode_f32s(&b[..5]).is_err());
    }

    #[test]
    fn dim_mismatch_sorts_last() {
        assert_eq!(distance(Metric::Cosine, &[1.0], &[1.0, 2.0]), f64::INFINITY);
    }

    #[test]
    fn metric_names() {
        assert_eq!(Metric::from_u32(0).expect("m"), Metric::Cosine);
        assert_eq!(Metric::from_u32(1).expect("m"), Metric::L2);
        assert!(Metric::from_u32(7).is_err());
        assert_eq!(Metric::Cosine.name(), "cosine");
    }

    #[test]
    fn quantize_error_bound() {
        // Half-step bound: |dequant - x| <= s/2 (+ float dust).
        let v: Vec<f32> = (0..64).map(|i| ((i * 37) % 101) as f32 / 50.0 - 1.0).collect();
        let (iv, s) = quantize(&v);
        assert_eq!(iv.len(), 64);
        assert!((s - 1.0 / 127.0).abs() < 1e-9, "max_abs=1 => s=1/127, got {s}");
        for (i, &x) in v.iter().enumerate() {
            assert!((iv[i] as f32 * s - x).abs() <= s / 2.0 + 1e-6, "i={i} x={x}");
        }
        let (z, s0) = quantize(&vec![0.0; 8]);
        assert!(z.iter().all(|&x| x == 0));
        assert_eq!(s0, 1.0);
    }

    // --- P1 HNSW ---

    fn brute_top(q: &[f32], pts: &[Vec<f32>], k: usize, m: Metric) -> Vec<usize> {
        let mut scored: Vec<(f64, usize)> = pts
            .iter()
            .enumerate()
            .map(|(i, p)| (distance(m, q, p), i))
            .collect();
        scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
        scored.into_iter().take(k).map(|(_, i)| i).collect()
    }

    /// Deterministic PRNG (xorshift64, fixed seed) — fixtures must not
    /// depend on global RNG state.
    struct FixtureRng(u64);
    impl FixtureRng {
        fn next_f32(&mut self) -> f32 {
            self.0 ^= self.0 << 13;
            self.0 ^= self.0 >> 7;
            self.0 ^= self.0 << 17;
            ((self.0 >> 11) as f64 / (1u64 << 53) as f64) as f32 * 2.0 - 1.0
        }
    }

    fn hnsw_recall(pts: &[Vec<f32>], queries: &[Vec<f32>], k: usize, m: Metric, ef: usize) -> f64 {
        let dim = pts[0].len();
        let mut h = HnswIndex::new(dim, m);
        for (i, p) in pts.iter().enumerate() {
            h.upsert(&i.to_string(), p.clone());
        }
        let mut hit = 0usize;
        for q in queries {
            let want = brute_top(q, pts, k, m);
            let got: Vec<usize> = h
                .search(q, ef)
                .into_iter()
                .take(k)
                .map(|(_, idx)| idx)
                .collect();
            for id in &got {
                if want.contains(id) {
                    hit += 1;
                }
            }
        }
        hit as f64 / (queries.len() * k) as f64
    }

    #[test]
    fn hnsw_tiny_is_exact() {
        let pts = vec![
            vec![1.0, 0.0],
            vec![0.0, 1.0],
            vec![1.0, 1.0],
            vec![10.0, 0.0],
        ];
        assert_eq!(hnsw_recall(&pts, &pts, 2, Metric::Cosine, 10), 1.0);
        assert_eq!(hnsw_recall(&pts, &pts, 4, Metric::L2, 10), 1.0);
    }

    #[test]
    fn hnsw_recall_on_random() {
        // 500 pts, dim 16, uniform in [-1,1): ANN must (near-)match the
        // P0 exact oracle. Deterministic seed — same graph every run.
        let mut rng = FixtureRng(0x12345678);
        let pts: Vec<Vec<f32>> = (0..500).map(|_| (0..16).map(|_| rng.next_f32()).collect()).collect();
        let queries: Vec<Vec<f32>> = (0..20).map(|_| (0..16).map(|_| rng.next_f32()).collect()).collect();
        let r_cos = hnsw_recall(&pts, &queries, 10, Metric::Cosine, 100);
        let r_l2 = hnsw_recall(&pts, &queries, 10, Metric::L2, 100);
        assert!(r_cos >= 0.95, "cosine recall {r_cos}");
        assert!(r_l2 >= 0.95, "l2 recall {r_l2}");
    }

    #[test]
    fn hnsw_update_and_wrong_dim() {
        let mut h = HnswIndex::new(2, Metric::L2);
        h.upsert("a", vec![0.0, 0.0]);
        h.upsert("b", vec![10.0, 0.0]);
        let top: Vec<usize> = h.search(&[0.1, 0.0], 2).into_iter().map(|(_, i)| i).collect();
        assert_eq!(top[0], h.id_to_idx["a"]);
        // Move b strictly closer to the query: update-in-place must
        // take effect (exact ties break by insertion idx, so offset it).
        h.upsert("b", vec![0.05, 0.0]);
        let top: Vec<usize> = h.search(&[0.1, 0.0], 2).into_iter().map(|(_, i)| i).collect();
        assert_eq!(top[0], h.id_to_idx["b"]);
        // Wrong dim ignored, count unchanged.
        h.upsert("c", vec![1.0, 2.0, 3.0]);
        assert_eq!(h.len(), 2);
        assert!(!h.id_to_idx.contains_key("c"));
    }

    #[test]
    fn hnsw_clear_and_determinism() {
        let mut h1 = HnswIndex::new(2, Metric::Cosine);
        let mut h2 = HnswIndex::new(2, Metric::Cosine);
        for i in 0..50 {
            let v = vec![i as f32 * 0.1, (i % 7) as f32];
            h1.upsert(&i.to_string(), v.clone());
            h2.upsert(&i.to_string(), v);
        }
        let q = vec![1.0, 1.0];
        let r1: Vec<(u64, usize)> = h1.search(&q, 5).into_iter().map(|(d, i)| (d.to_bits(), i)).collect();
        let r2: Vec<(u64, usize)> = h2.search(&q, 5).into_iter().map(|(d, i)| (d.to_bits(), i)).collect();
        assert_eq!(r1, r2, "fixed seed => identical graphs");
        h1.clear();
        assert!(h1.is_empty());
        assert!(h1.search(&q, 5).is_empty());
        assert!(!h1.complete());
    }
}
