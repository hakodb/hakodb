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
}
