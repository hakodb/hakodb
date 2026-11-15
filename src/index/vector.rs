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
    /// Inner product (#141-8): similarity = dot product. As a DISTANCE the
    /// engine exposes the negation (`-dot`), keeping the "lower is closer"
    /// contract every metric shares — including the HNSW comparators, which
    /// are metric-agnostic by construction.
    InnerProduct,
}

impl Metric {
    pub fn from_u32(v: u32) -> Result<Self> {
        match v {
            0 => Ok(Metric::Cosine),
            1 => Ok(Metric::L2),
            2 => Ok(Metric::InnerProduct),
            _ => Err(HakoError::Corrupt(format!(
                "unknown vector metric {v} (0=cosine, 1=l2, 2=inner_product)"
            ))),
        }
    }

    pub fn name(&self) -> &'static str {
        match self {
            Metric::Cosine => "cosine",
            Metric::L2 => "l2",
            Metric::InnerProduct => "inner_product",
        }
    }
}

/// P0 registration: definition only (no RAM structure yet — the P0 scan is
/// brute-force exact). P1's HNSW lives behind this same registry entry.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct VectorDef {
    pub dim: u32,
    pub metric: Metric,
    /// HNSW M override (#141-1). None = per-dim derived (hnsw_m_for_dim).
    #[serde(default)]
    pub m: Option<usize>,
    /// ef_construction override (#141-1). None = per-dim derived.
    #[serde(default)]
    pub ef_construction: Option<usize>,
    /// Query-time beam override (#141-1). None = the executor's derived
    /// formula (max_ids*8, clamped 128/256..20000).
    #[serde(default)]
    pub ef_search: Option<usize>,
    /// Binary stage-1 (#190): graph traversal ranks by popcount Hamming
    /// over sign bitsets (~16x cheaper stage-1) with the int8/f32 exact
    /// rescore untouched. Default false — int8 traversal, no behavior
    /// change. Node bitsets exist only when this is on at build time.
    #[serde(default)]
    pub binary_stage1: bool,
}

/// Configurable HNSW parameters (#141-1). `None` fields ride the derived
/// defaults; values validate/clamp when the graph builds (M clamped to
/// 16..=64, ef_construction floored at 32).
#[derive(Debug, Clone, Copy, Default)]
pub struct HnswParams {
    pub m: Option<usize>,
    pub ef_construction: Option<usize>,
    pub ef_search: Option<usize>,
    /// #190 binary stage-1 toggle (see `VectorDef::binary_stage1`).
    pub binary_stage1: bool,
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
    if !bytes.len().is_multiple_of(4) {
        return Err(HakoError::Corrupt(format!(
            "vector bytes {} not a multiple of 4",
            bytes.len()
        )));
    }
    Ok(bytes
        .as_chunks::<4>()
        .0
        .iter()
        .map(|c| f32::from_le_bytes(*c))
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
    let (dot, na, nb) = simd::cosine_parts(a, b);
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
    simd::l2_f32_f32(a, b)
}

/// Inner-product DISTANCE (#141-8): negated dot product. Dot itself is a
/// similarity (higher = closer), so the negation preserves the engine's
/// "distance, lower is closer" contract across all three metrics - the
/// HNSW comparators stay metric-agnostic. Unbounded below by design.
pub fn inner_product_distance(a: &[f32], b: &[f32]) -> f64 {
    debug_assert_eq!(a.len(), b.len());
    -simd::dot_f32_f32(a, b)
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
        Metric::InnerProduct => inner_product_distance(a, b),
    }
}

// ---------------------------------------------------------------------------
// V1 (#168 SIMD lever / #172 kajian): runtime-detected AVX2 distance kernels.
//
// Three hot dots pay for this module:
// - rescore (`cosine/l2/ip`): f32·f32 accumulated in f64 — ~20-40 calls/query
// - walk (`q_dist`): f32 query · int8 node, f64 — one per HNSW hop visited
// - construction (`node_dist`): int8·int8 — integer dot, EXACT both paths
//
// AVX2 semantics: per-element rounding stays the scalar two-step (multiply
// rounds, add rounds — Rust contracts no FMA), so lanes compute scalar-
// identical terms; only the addition tree differs (4 partial f64 lanes vs
// sequential). The integer kernel is associative-exact everywhere. Fallback
// = the original sequential loops, so a non-AVX2 machine keeps today's
// arithmetic; each build remains self-deterministic (sorted tiebreaks ride
// id ordering, not distance bits).
//
// #194: aarch64 gets a NEON mirror of the same lane schedule (8 f32 per
// iteration, 4 partial groups, fold `(l0+l2)+(l1+l3)`), so the NEON f64
// kernels are bit-identical to the AVX2 ones — cross-ISA replicas see the
// same distances. The integer kernel is exact on every path.
mod scalars {
    /// Sequential reference kernels — the ORIGINAL arithmetic, kept as the
    /// non-AVX2 execution path and as the test oracle.
    pub fn dot_f32_i8(q: &[f32], v: &[i8]) -> f64 {
        q.iter().zip(v).map(|(x, b)| *x as f64 * *b as f64).sum()
    }
    pub fn dot_f32_f32(a: &[f32], b: &[f32]) -> f64 {
        a.iter().zip(b).map(|(x, y)| *x as f64 * *y as f64).sum()
    }
    pub fn l2_f32_f32(a: &[f32], b: &[f32]) -> f64 {
        a.iter()
            .zip(b)
            .map(|(x, y)| {
                let d = *x as f64 - *y as f64;
                d * d
            })
            .sum()
    }
    pub fn cosine_parts(a: &[f32], b: &[f32]) -> (f64, f64, f64) {
        let mut dot = 0.0f64;
        let mut na = 0.0f64;
        let mut nb = 0.0f64;
        for i in 0..a.len() {
            let (x, y) = (a[i] as f64, b[i] as f64);
            dot += x * y;
            na += x * x;
            nb += y * y;
        }
        (dot, na, nb)
    }
    pub fn dot_i8_i8(a: &[i8], b: &[i8]) -> i64 {
        a.iter().zip(b).map(|(x, y)| *x as i64 * *y as i64).sum()
    }
}

#[cfg(target_arch = "x86_64")]
mod simd {
    use super::scalars;
    use std::sync::OnceLock;

    pub fn have_avx2() -> bool {
        static OK: OnceLock<bool> = OnceLock::new();
        *OK.get_or_init(|| std::arch::is_x86_feature_detected!("avx2"))
    }

    pub fn dot_f32_i8(q: &[f32], v: &[i8]) -> f64 {
        if have_avx2() {
            unsafe { return dot_f32_i8_avx2(q, v) };
        }
        scalars::dot_f32_i8(q, v)
    }
    pub fn dot_f32_f32(a: &[f32], b: &[f32]) -> f64 {
        if have_avx2() {
            unsafe { return dot_f32_f32_avx2(a, b) };
        }
        scalars::dot_f32_f32(a, b)
    }
    pub fn l2_f32_f32(a: &[f32], b: &[f32]) -> f64 {
        if have_avx2() {
            unsafe { return l2_f32_f32_avx2(a, b) };
        }
        scalars::l2_f32_f32(a, b)
    }
    pub fn cosine_parts(a: &[f32], b: &[f32]) -> (f64, f64, f64) {
        if have_avx2() {
            unsafe { return cosine_parts_avx2(a, b) };
        }
        scalars::cosine_parts(a, b)
    }
    pub fn dot_i8_i8(a: &[i8], b: &[i8]) -> i64 {
        // Exact integer paths — associativity-safe, so AVX2 is bit-identical
        // to the fallback (no reduction-order caveat, unlike the f64 lanes).
        if have_avx2() && a.len() <= 100_000 {
            unsafe { return dot_i8_i8_avx2(a, b) };
        }
        scalars::dot_i8_i8(a, b)
    }

    /// f32·int8 → f64: 8 f32 per iteration; i8 sign-extends to i32 then to
    /// f64 lanes; per lane it keeps multiply-then-add (the scalar's
    /// two-rounding steps elementwise); tail ≤ 7 uses the same scalar
    /// expressions. Horizontal fold order = (l0+l2)+(l1+l3) + tail — fp
    /// addition order differs from sequential, see the module header.
    #[target_feature(enable = "avx2")]
    unsafe fn dot_f32_i8_avx2(q: &[f32], v: &[i8]) -> f64 {
        use std::arch::x86_64::*;
        let n = q.len().min(v.len());
        let limbs = n & !7;
        let mut acc = _mm256_setzero_pd();
        let mut i = 0;
        while i < limbs {
            let x = _mm256_loadu_ps(q.as_ptr().add(i));
            let b32 = _mm256_cvtepi8_epi32(_mm_loadl_epi64(v.as_ptr().add(i) as *const __m128i));
            let xl = _mm256_cvtps_pd(_mm256_castps256_ps128(x));
            let xh = _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(x));
            let bl = _mm256_cvtepi32_pd(_mm256_castsi256_si128(b32));
            let bh = _mm256_cvtepi32_pd(_mm256_extracti128_si256::<1>(b32));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(xl, bl));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(xh, bh));
            i += 8;
        }
        let mut p = fold4(acc);
        while i < n {
            p += q[i] as f64 * v[i] as f64;
            i += 1;
        }
        p
    }

    #[target_feature(enable = "avx2")]
    unsafe fn dot_f32_f32_avx2(a: &[f32], b: &[f32]) -> f64 {
        use std::arch::x86_64::*;
        let n = a.len().min(b.len());
        let limbs = n & !7;
        let mut acc = _mm256_setzero_pd();
        let mut i = 0;
        while i < limbs {
            let x = _mm256_loadu_ps(a.as_ptr().add(i));
            let y = _mm256_loadu_ps(b.as_ptr().add(i));
            let xl = _mm256_cvtps_pd(_mm256_castps256_ps128(x));
            let xh = _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(x));
            let yl = _mm256_cvtps_pd(_mm256_castps256_ps128(y));
            let yh = _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(y));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(xl, yl));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(xh, yh));
            i += 8;
        }
        let mut p = fold4(acc);
        while i < n {
            p += a[i] as f64 * b[i] as f64;
            i += 1;
        }
        p
    }

    #[target_feature(enable = "avx2")]
    unsafe fn l2_f32_f32_avx2(a: &[f32], b: &[f32]) -> f64 {
        use std::arch::x86_64::*;
        let n = a.len().min(b.len());
        let limbs = n & !7;
        let mut acc = _mm256_setzero_pd();
        let mut i = 0;
        while i < limbs {
            let x = _mm256_loadu_ps(a.as_ptr().add(i));
            let y = _mm256_loadu_ps(b.as_ptr().add(i));
            let xl = _mm256_cvtps_pd(_mm256_castps256_ps128(x));
            let xh = _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(x));
            let yl = _mm256_cvtps_pd(_mm256_castps256_ps128(y));
            let yh = _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(y));
            let dl = _mm256_sub_pd(xl, yl);
            let dh = _mm256_sub_pd(xh, yh);
            acc = _mm256_add_pd(acc, _mm256_mul_pd(dl, dl));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(dh, dh));
            i += 8;
        }
        let mut p = fold4(acc);
        while i < n {
            let d = a[i] as f64 - b[i] as f64;
            p += d * d;
            i += 1;
        }
        p
    }

    /// (dot, ‖a‖², ‖b‖²) in one pass — three 4-lane f64 accumulators.
    #[target_feature(enable = "avx2")]
    unsafe fn cosine_parts_avx2(a: &[f32], b: &[f32]) -> (f64, f64, f64) {
        use std::arch::x86_64::*;
        let n = a.len().min(b.len());
        let limbs = n & !7;
        let (mut acc, mut ma, mut mb) = (_mm256_setzero_pd(), _mm256_setzero_pd(), _mm256_setzero_pd());
        let mut i = 0;
        while i < limbs {
            let x = _mm256_loadu_ps(a.as_ptr().add(i));
            let y = _mm256_loadu_ps(b.as_ptr().add(i));
            let (xl, xh) = (_mm256_cvtps_pd(_mm256_castps256_ps128(x)), _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(x)));
            let (yl, yh) = (_mm256_cvtps_pd(_mm256_castps256_ps128(y)), _mm256_cvtps_pd(_mm256_extractf128_ps::<1>(y)));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(xl, yl));
            acc = _mm256_add_pd(acc, _mm256_mul_pd(xh, yh));
            ma = _mm256_add_pd(ma, _mm256_mul_pd(xl, xl));
            ma = _mm256_add_pd(ma, _mm256_mul_pd(xh, xh));
            mb = _mm256_add_pd(mb, _mm256_mul_pd(yl, yl));
            mb = _mm256_add_pd(mb, _mm256_mul_pd(yh, yh));
            i += 8;
        }
        let (mut d, mut n1, mut n2) = (fold4(acc), fold4(ma), fold4(mb));
        while i < n {
            let (x, y) = (a[i] as f64, b[i] as f64);
            d += x * y;
            n1 += x * x;
            n2 += y * y;
            i += 1;
        }
        (d, n1, n2)
    }

    /// Horizontal fold of four f64 lanes: (l0+l2)+(l1+l3) — addition order
    /// differs from sequential, see the module header.
    #[inline]
    unsafe fn fold4(acc: std::arch::x86_64::__m256d) -> f64 {
        use std::arch::x86_64::*;
        let lo = _mm256_extractf128_pd::<0>(acc);
        let hi = _mm256_extractf128_pd::<1>(acc);
        let p2 = _mm_add_pd(lo, hi); // (l0+l2, l1+l3)
        let s = _mm_add_sd(p2, _mm_unpackhi_pd(p2, p2));
        _mm_cvtsd_f64(s)
    }

    /// int8·int8 exact, 32-byte chunks: i16 widen, `madd` pairs (element
    /// products ≤ 16384, pair sums ≤ 32768 — no i16/i32 overflow), i32
    /// lanes folded exactly; tail scalar. Exact for dim ≤ 100k (guarded in
    /// the dispatcher) — else the sequential path (also exact).
    #[target_feature(enable = "avx2")]
    unsafe fn dot_i8_i8_avx2(a: &[i8], b: &[i8]) -> i64 {
        use std::arch::x86_64::*;
        let n = a.len().min(b.len());
        let limbs = n & !31;
        let mut acc = _mm256_setzero_si256();
        let mut i = 0;
        while i < limbs {
            let ax = _mm256_loadu_si256(a.as_ptr().add(i) as *const __m256i);
            let bx = _mm256_loadu_si256(b.as_ptr().add(i) as *const __m256i);
            let al = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(ax));
            let bl = _mm256_cvtepi8_epi16(_mm256_castsi256_si128(bx));
            let ah = _mm256_cvtepi8_epi16(_mm256_extracti128_si256::<1>(ax));
            let bh = _mm256_cvtepi8_epi16(_mm256_extracti128_si256::<1>(bx));
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(al, bl));
            acc = _mm256_add_epi32(acc, _mm256_madd_epi16(ah, bh));
            i += 32;
        }
        // exact horizontal i32 fold
        let lo = _mm256_extracti128_si256::<0>(acc);
        let hi = _mm256_extracti128_si256::<1>(acc);
        let p = _mm_add_epi32(lo, hi);
        let p2 = _mm_add_epi32(p, _mm_shuffle_epi32::<0b01_01_01_01>(p));
        let p3 = _mm_add_epi32(p2, _mm_shuffle_epi32::<0b10_10_10_10>(p2));
        let mut tot = _mm_cvtsi128_si32(p3) as i64;
        while i < n {
            tot += a[i] as i64 * b[i] as i64;
            i += 1;
        }
        tot
    }
}

#[cfg(target_arch = "aarch64")]
mod simd {
    use super::scalars;
    use std::sync::OnceLock;

    pub fn have_neon() -> bool {
        static OK: OnceLock<bool> = OnceLock::new();
        *OK.get_or_init(|| std::arch::is_aarch64_feature_detected!("neon"))
    }

    pub fn dot_f32_i8(q: &[f32], v: &[i8]) -> f64 {
        if have_neon() {
            unsafe { return dot_f32_i8_neon(q, v) };
        }
        scalars::dot_f32_i8(q, v)
    }
    pub fn dot_f32_f32(a: &[f32], b: &[f32]) -> f64 {
        if have_neon() {
            unsafe { return dot_f32_f32_neon(a, b) };
        }
        scalars::dot_f32_f32(a, b)
    }
    pub fn l2_f32_f32(a: &[f32], b: &[f32]) -> f64 {
        if have_neon() {
            unsafe { return l2_f32_f32_neon(a, b) };
        }
        scalars::l2_f32_f32(a, b)
    }
    pub fn cosine_parts(a: &[f32], b: &[f32]) -> (f64, f64, f64) {
        if have_neon() {
            unsafe { return cosine_parts_neon(a, b) };
        }
        scalars::cosine_parts(a, b)
    }
    pub fn dot_i8_i8(a: &[i8], b: &[i8]) -> i64 {
        // Exact integer paths — associativity-safe, so NEON is bit-identical
        // to the fallback (no reduction-order caveat, unlike the f64 lanes).
        if have_neon() && a.len() <= 100_000 {
            unsafe { return dot_i8_i8_neon(a, b) };
        }
        scalars::dot_i8_i8(a, b)
    }

    /// NEON mirrors the AVX2 lane schedule exactly: 8 f32 per iteration as
    /// (e0,e1,e2,e3)+(e4..e7) f64 pairs, two f64x2 accumulators per partial
    /// group so lanes accumulate (e0 then e4), (e1 then e5), (e2 then e6),
    /// (e3 then e7) — the fold `(l0+l2)+(l1+l3)` lands on the same
    /// addition tree, making NEON bit-identical to the AVX2 kernels (and
    /// the scalar tail identical by construction).
    #[target_feature(enable = "neon")]
    unsafe fn dot_f32_f32_neon(a: &[f32], b: &[f32]) -> f64 {
        use std::arch::aarch64::*;
        let n = a.len().min(b.len());
        let limbs = n & !7;
        let (mut a01, mut a23) = (vreinterpretq_f64_f32(vdupq_n_f32(0.0)), vreinterpretq_f64_f32(vdupq_n_f32(0.0)));
        let mut i = 0;
        while i < limbs {
            let x1 = vld1q_f32(a.as_ptr().add(i));
            let x2 = vld1q_f32(a.as_ptr().add(i + 4));
            let y1 = vld1q_f32(b.as_ptr().add(i));
            let y2 = vld1q_f32(b.as_ptr().add(i + 4));
            let (xl1, xh1) = (vcvt_f64_f32(vget_low_f32(x1)), vcvt_high_f64_f32(x1));
            let (xl2, xh2) = (vcvt_f64_f32(vget_low_f32(x2)), vcvt_high_f64_f32(x2));
            let (yl1, yh1) = (vcvt_f64_f32(vget_low_f32(y1)), vcvt_high_f64_f32(y1));
            let (yl2, yh2) = (vcvt_f64_f32(vget_low_f32(y2)), vcvt_high_f64_f32(y2));
            a01 = vaddq_f64(a01, vmulq_f64(xl1, yl1)); // e0, e1
            a23 = vaddq_f64(a23, vmulq_f64(xh1, yh1)); // e2, e3
            a01 = vaddq_f64(a01, vmulq_f64(xl2, yl2)); // e4, e5
            a23 = vaddq_f64(a23, vmulq_f64(xh2, yh2)); // e6, e7
            i += 8;
        }
        let mut p = fold2(a01, a23);
        while i < n {
            p += a[i] as f64 * b[i] as f64;
            i += 1;
        }
        p
    }

    #[target_feature(enable = "neon")]
    unsafe fn l2_f32_f32_neon(a: &[f32], b: &[f32]) -> f64 {
        use std::arch::aarch64::*;
        let n = a.len().min(b.len());
        let limbs = n & !7;
        let (mut a01, mut a23) = (vreinterpretq_f64_f32(vdupq_n_f32(0.0)), vreinterpretq_f64_f32(vdupq_n_f32(0.0)));
        let mut i = 0;
        while i < limbs {
            let x1 = vld1q_f32(a.as_ptr().add(i));
            let x2 = vld1q_f32(a.as_ptr().add(i + 4));
            let y1 = vld1q_f32(b.as_ptr().add(i));
            let y2 = vld1q_f32(b.as_ptr().add(i + 4));
            let (xl1, xh1) = (vcvt_f64_f32(vget_low_f32(x1)), vcvt_high_f64_f32(x1));
            let (xl2, xh2) = (vcvt_f64_f32(vget_low_f32(x2)), vcvt_high_f64_f32(x2));
            let (yl1, yh1) = (vcvt_f64_f32(vget_low_f32(y1)), vcvt_high_f64_f32(y1));
            let (yl2, yh2) = (vcvt_f64_f32(vget_low_f32(y2)), vcvt_high_f64_f32(y2));
            a01 = vaddq_f64(a01, vmulq_f64(vsubq_f64(xl1, yl1), vsubq_f64(xl1, yl1)));
            a23 = vaddq_f64(a23, vmulq_f64(vsubq_f64(xh1, yh1), vsubq_f64(xh1, yh1)));
            a01 = vaddq_f64(a01, vmulq_f64(vsubq_f64(xl2, yl2), vsubq_f64(xl2, yl2)));
            a23 = vaddq_f64(a23, vmulq_f64(vsubq_f64(xh2, yh2), vsubq_f64(xh2, yh2)));
            i += 8;
        }
        let mut p = fold2(a01, a23);
        while i < n {
            let d = a[i] as f64 - b[i] as f64;
            p += d * d;
            i += 1;
        }
        p
    }

    /// (dot, ‖a‖², ‖b‖²) in one pass — three partial-pair groups.
    #[target_feature(enable = "neon")]
    unsafe fn cosine_parts_neon(a: &[f32], b: &[f32]) -> (f64, f64, f64) {
        use std::arch::aarch64::*;
        let n = a.len().min(b.len());
        let limbs = n & !7;
        let z = vreinterpretq_f64_f32(vdupq_n_f32(0.0));
        let ((mut d01, mut d23), (mut ma01, mut ma23), (mut mb01, mut mb23)) = ((z, z), (z, z), (z, z));
        let mut i = 0;
        while i < limbs {
            let x1 = vld1q_f32(a.as_ptr().add(i));
            let x2 = vld1q_f32(a.as_ptr().add(i + 4));
            let y1 = vld1q_f32(b.as_ptr().add(i));
            let y2 = vld1q_f32(b.as_ptr().add(i + 4));
            let (xl1, xh1) = (vcvt_f64_f32(vget_low_f32(x1)), vcvt_high_f64_f32(x1));
            let (xl2, xh2) = (vcvt_f64_f32(vget_low_f32(x2)), vcvt_high_f64_f32(x2));
            let (yl1, yh1) = (vcvt_f64_f32(vget_low_f32(y1)), vcvt_high_f64_f32(y1));
            let (yl2, yh2) = (vcvt_f64_f32(vget_low_f32(y2)), vcvt_high_f64_f32(y2));
            d01 = vaddq_f64(d01, vmulq_f64(xl1, yl1));
            d23 = vaddq_f64(d23, vmulq_f64(xh1, yh1));
            d01 = vaddq_f64(d01, vmulq_f64(xl2, yl2));
            d23 = vaddq_f64(d23, vmulq_f64(xh2, yh2));
            ma01 = vaddq_f64(ma01, vmulq_f64(xl1, xl1));
            ma23 = vaddq_f64(ma23, vmulq_f64(xh1, xh1));
            ma01 = vaddq_f64(ma01, vmulq_f64(xl2, xl2));
            ma23 = vaddq_f64(ma23, vmulq_f64(xh2, xh2));
            mb01 = vaddq_f64(mb01, vmulq_f64(yl1, yl1));
            mb23 = vaddq_f64(mb23, vmulq_f64(yh1, yh1));
            mb01 = vaddq_f64(mb01, vmulq_f64(yl2, yl2));
            mb23 = vaddq_f64(mb23, vmulq_f64(yh2, yh2));
            i += 8;
        }
        let (mut d, mut n1, mut n2) = (fold2(d01, d23), fold2(ma01, ma23), fold2(mb01, mb23));
        while i < n {
            let (x, y) = (a[i] as f64, b[i] as f64);
            d += x * y;
            n1 += x * x;
            n2 += y * y;
            i += 1;
        }
        (d, n1, n2)
    }

    /// f32·int8 → f64: i8 widens i8→i16→i32 (exact) → f32 (exact, |v| ≤ 127)
    /// → f64 per pair; the f64 side follows the shared lane schedule.
    #[target_feature(enable = "neon")]
    unsafe fn dot_f32_i8_neon(q: &[f32], v: &[i8]) -> f64 {
        use std::arch::aarch64::*;
        let n = q.len().min(v.len());
        let limbs = n & !7;
        let (mut a01, mut a23) = (vreinterpretq_f64_f32(vdupq_n_f32(0.0)), vreinterpretq_f64_f32(vdupq_n_f32(0.0)));
        let mut i = 0;
        while i < limbs {
            let x1 = vld1q_f32(q.as_ptr().add(i));
            let x2 = vld1q_f32(q.as_ptr().add(i + 4));
            let v8 = vld1_s8(v.as_ptr().add(i));
            let v16 = vmovl_s8(v8);
            let f1 = vcvtq_f32_s32(vmovl_s16(vget_low_s16(v16)));  // e0..e3 as f32
            let f2 = vcvtq_f32_s32(vmovl_s16(vget_high_s16(v16))); // e4..e7 as f32
            let (bl1, bh1) = (vcvt_f64_f32(vget_low_f32(f1)), vcvt_high_f64_f32(f1));
            let (bl2, bh2) = (vcvt_f64_f32(vget_low_f32(f2)), vcvt_high_f64_f32(f2));
            let (ql1, qh1) = (vcvt_f64_f32(vget_low_f32(x1)), vcvt_high_f64_f32(x1));
            let (ql2, qh2) = (vcvt_f64_f32(vget_low_f32(x2)), vcvt_high_f64_f32(x2));
            a01 = vaddq_f64(a01, vmulq_f64(ql1, bl1)); // e0, e1
            a23 = vaddq_f64(a23, vmulq_f64(qh1, bh1)); // e2, e3
            a01 = vaddq_f64(a01, vmulq_f64(ql2, bl2)); // e4, e5
            a23 = vaddq_f64(a23, vmulq_f64(qh2, bh2)); // e6, e7
            i += 8;
        }
        let mut p = fold2(a01, a23);
        while i < n {
            p += q[i] as f64 * v[i] as f64;
            i += 1;
        }
        p
    }

    /// int8·int8 exact, 16-byte chunks: products widen to i16 (`vmull`,
    /// each ≤ 16384), pairwise-summed into i32 lanes (`vpaddlq`/`vpadalq`,
    /// lane ≤ 2·16384 per chunk — dim ≤ 100k keeps lanes < 2³¹), folded
    /// through i64 exactly; tail scalar. Integer addition is
    /// associative-exact, so the tree never matters (see the dispatcher).
    #[target_feature(enable = "neon")]
    unsafe fn dot_i8_i8_neon(a: &[i8], b: &[i8]) -> i64 {
        use std::arch::aarch64::*;
        let n = a.len().min(b.len());
        let limbs = n & !15;
        let mut acc = vreinterpretq_s32_u32(vdupq_n_u32(0));
        let mut i = 0;
        while i < limbs {
            let ax = vld1q_s8(a.as_ptr().add(i));
            let bx = vld1q_s8(b.as_ptr().add(i));
            let pl = vmull_s8(vget_low_s8(ax), vget_low_s8(bx));
            let ph = vmull_s8(vget_high_s8(ax), vget_high_s8(bx));
            acc = vpadalq_s16(acc, pl);
            acc = vpadalq_s16(acc, ph);
            i += 16;
        }
        // exact horizontal i32 fold: pairwise to i64, then lane sum
        let p = vpaddlq_s32(acc);
        let mut tot = vaddvq_s64(p);
        while i < n {
            tot += a[i] as i64 * b[i] as i64;
            i += 1;
        }
        tot
    }

    /// Horizontal fold matching the AVX2 `fold4`: `(l0+l2)+(l1+l3)`.
    #[inline]
    unsafe fn fold2(a01: std::arch::aarch64::float64x2_t, a23: std::arch::aarch64::float64x2_t) -> f64 {
        use std::arch::aarch64::*;
        let p = vaddq_f64(a01, a23); // (l0+l2, l1+l3)
        vaddvq_f64(p)
    }
}

#[cfg(not(any(target_arch = "x86_64", target_arch = "aarch64")))]
mod simd {
    pub use super::scalars::{cosine_parts, dot_f32_f32, dot_f32_i8, dot_i8_i8, l2_f32_f32};
    pub fn have_avx2() -> bool {
        false
    }
}

// P1: HNSW (in-RAM, per collection+field).
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

// P3: parameters scale with dim (duel verdict 2026-10-10: fixed M=16
// starves high-dim graphs — recall 0.62 at D=384 vs Turso 0.92).
// M ≈ 3√D mirrors Turso's max_neighbors default; ef_construction steps
// up where int8 routing error compounds (D≥128). D≤16 keeps the old
// values exactly (3√16=12 → clamped to 16; ef 200).
const HNSW_M_MIN: usize = 16;
const HNSW_M_MAX: usize = 64;
const HNSW_EF_CONSTRUCTION_BASE: usize = 200;
const HNSW_EF_CONSTRUCTION_HIGH_DIM: usize = 300;
const HNSW_HIGH_DIM: usize = 128;

/// Neighbor budget for `dim`: clamp(3√D, 16, 64). D=32→17, D=128→34,
/// D=384→59, D=4096→64.
pub fn hnsw_m_for_dim(dim: usize) -> usize {
    let m = (3.0 * (dim as f64).sqrt()).round() as usize;
    m.clamp(HNSW_M_MIN, HNSW_M_MAX)
}

fn hnsw_ef_construction_for_dim(dim: usize) -> usize {
    if dim >= HNSW_HIGH_DIM {
        HNSW_EF_CONSTRUCTION_HIGH_DIM
    } else {
        HNSW_EF_CONSTRUCTION_BASE
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
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
    /// Sign bitset (#190): bit j = vec[j] >= 0, one word per 64 dims.
    /// Only populated when the def enables binary stage-1 (RAM stays
    /// flat for int8-only indexes); traversal ranks on popcount Hamming
    /// against the query's own sign bitset.
    bits: Vec<u64>,
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

/// Sign bitset (#190): bit j set when v[j] >= 0, one word per 64 dims.
fn sign_bits(v: &[f32]) -> Vec<u64> {
    let mut bits = vec![0u64; v.len().div_ceil(64)];
    for (j, &x) in v.iter().enumerate() {
        if x >= 0.0 {
            bits[j / 64] |= 1 << (j % 64);
        }
    }
    bits
}

/// Popcount Hamming between two same-length bitsets (#190 stage-1
/// distance): more sign agreement = lower = closer.
fn hamming(a: &[u64], b: &[u64]) -> u32 {
    a.iter().zip(b).map(|(x, y)| (x ^ y).count_ones()).sum()
}

/// Stage-1 distance mode for traversal (#190): int8 asymmetric
/// (default) or binary popcount Hamming against the query's sign
/// bitset. Insert-time link building always uses Int8 (structure
/// identical across the toggle).
enum Stage1<'a> {
    Int8,
    Binary(&'a [u64]),
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

/// Serde derives power the graph snapshot (#189): neighbor lists (not
/// vectors — the doc data stays the source of truth) ride ram_indexes.bin
/// on close/periodic snapshots, so reopen skips the full ANN rebuild.
#[derive(serde::Serialize, serde::Deserialize)]
pub struct HnswIndex {
    dim: usize,
    metric: Metric,
    /// Per-dim budgets (see hnsw_m_for_dim): M neighbors kept,
    /// Mmax/Mmax0 link caps, ef_construction beam at build.
    m: usize,
    mmax: usize,
    mmax0: usize,
    ef_construction: usize,
    level_mult: f64,
    nodes: Vec<HnswNode>,
    id_to_idx: std::collections::HashMap<String, usize>,
    /// Reverse map: internal idx → doc id. Stable — nodes are never
    /// removed (deletes are external), so idxs never shift.
    idx_to_id: Vec<String>,
    entry: Option<usize>,
    rng: u64,
    complete: bool,
    /// #190 binary stage-1: when on, `search` ranks by popcount Hamming
    /// over node/query sign bitsets; the exact rescore (int8/f32) stays
    /// untouched downstream. Insert-time link building keeps int8
    /// distances (structure identical across the toggle).
    stage1_binary: bool,
}

/// Per-node candidate list from the batch builder's phase A:
/// (level, beam results).
type NodeCands = Vec<(usize, Vec<(f64, usize)>)>;

impl HnswIndex {
    pub fn new(dim: usize, metric: Metric, params: HnswParams) -> Self {
        // Overrides (#141-1) clamp to the envelopes the derived defaults
        // use: M within 16..=64, ef_construction floored at 32.
        let m = params
            .m
            .unwrap_or_else(|| hnsw_m_for_dim(dim))
            .clamp(HNSW_M_MIN, HNSW_M_MAX);
        Self {
            dim,
            metric,
            m,
            mmax: m,
            mmax0: 2 * m,
            ef_construction: params
                .ef_construction
                .unwrap_or_else(|| hnsw_ef_construction_for_dim(dim))
                .max(32),
            level_mult: 1.0 / (m as f64).ln(),
            nodes: Vec::new(),
            id_to_idx: std::collections::HashMap::new(),
            idx_to_id: Vec::new(),
            entry: None,
            // Fixed seed: deterministic inserts/searches across runs
            // (tests pin behavior); randomness quality is irrelevant —
            // levels only need geometric spread, not crypto.
            rng: 0x9E3779B97F4A7C15,
            complete: false,
            stage1_binary: params.binary_stage1,
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

    /// Rough resident-bytes estimate (#141-7): quantized vectors (1
    /// byte/dim + scale + shape norm), neighbor lists at CAP (level 0
    /// keeps `mmax0`, upper levels `mmax`), and the id maps (~2 × 24 B
    /// per entry). Upper levels are usually sparse, so this over-counts —
    /// it is an upper bound, documented as such.
    pub fn bytes_estimate(&self) -> u64 {
        let n = self.nodes.len() as u64;
        if n == 0 {
            return 0;
        }
        let vec_bytes = self.dim as u64 + 16;
        let links_bytes = 8 * (self.mmax0 as u64 + self.mmax as u64);
        let id_bytes = 48; // id_to_idx + idx_to_id entry, ~2 × 24 B
        n * (vec_bytes + links_bytes + id_bytes)
    }

    pub fn set_complete(&mut self, v: bool) {
        self.complete = v;
    }

    /// Params fingerprint for snapshot validation (#189): a snapshot
    /// graph must match the live def exactly, else import drops it to
    /// the rebuild path. Comparison resolves the def the same way
    /// [`HnswIndex::new`] does, so overrides and derived defaults agree.
    pub fn matches_def(&self, dim: u32, metric: Metric, params: HnswParams) -> bool {
        if self.dim != dim as usize || self.metric != metric {
            return false;
        }
        let expected = HnswIndex::new(dim as usize, metric, params);
        self.m == expected.m && self.ef_construction == expected.ef_construction
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

    /// #206 batch-simulated sequential build: the parallel bulk path.
    /// A batch of nodes first collects its candidate sets in parallel
    /// against the frozen graph (the read-only half of `upsert`), then
    /// the batch applies links sequentially in item order — the same
    /// structure a sequential insert produces, so recall matches the
    /// sequential oracle (only lag: batch-mates' back-links). Worker
    /// count changes wall-clock, never the graph. Levels roll in item
    /// order (the level RNG is sequential state). `complete` stays
    /// false until the caller flips it.
    pub fn build_batched(
        dim: usize,
        metric: Metric,
        params: HnswParams,
        items: &[(String, Vec<f32>)],
    ) -> HnswIndex {
        let mut g = HnswIndex::new(dim, metric, params);
        const BATCH: usize = 256;
        let workers = std::thread::available_parallelism()
            .map(|p| p.get())
            .unwrap_or(1);
        for batch in items.chunks(BATCH) {
            let levels: Vec<usize> = batch.iter().map(|_| g.random_level()).collect();
            // Phase A (read-only): candidate sets against the frozen graph.
            let next_task = std::sync::atomic::AtomicUsize::new(0);
            let prepared: std::sync::Mutex<Vec<Option<NodeCands>>> =
                std::sync::Mutex::new(vec![None; batch.len()]);
            {
                let reader = &g;
                std::thread::scope(|s| {
                    for _w in 0..workers {
                        s.spawn(|| loop {
                            let j = next_task.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                            if j >= batch.len() {
                                break;
                            }
                            let (_, vec) = &batch[j];
                            prepared.lock().unwrap()[j] = reader.insert_candidates(vec, levels[j]);
                        });
                    }
                });
            }
            // Phase B: apply links in item order.
            let prepared = prepared.into_inner().unwrap();
            for (j, prep) in prepared.into_iter().enumerate() {
                let (id, vec) = &batch[j];
                match prep {
                    Some(cands) => g.insert_apply(id, vec, levels[j], cands),
                    // Entry was absent at phase A (start of the build):
                    // plain sequential insert chains properly.
                    None => g.upsert(id, vec.clone()),
                }
            }
        }
        g
    }

    /// Read-only candidate collection for one node against a frozen
    /// graph — the search half of `upsert` (chain `cur = selected[0]`
    /// across levels; the only difference from a live insert is that
    /// the batch's own links don't exist yet). None = entry absent.
    fn insert_candidates(
        &self,
        vec: &[f32],
        level: usize,
    ) -> Option<NodeCands> {
        let mut cur = self.entry?;
        let qn = Self::query_norm_sq(vec);
        let top = self.nodes[cur].links.len() - 1;
        for l in ((level + 1)..=top).rev() {
            cur = self.greedy_closest(vec, qn, cur, l, &Stage1::Int8);
        }
        let mut cands: Vec<(usize, Vec<(f64, usize)>)> = Vec::new();
        for l in (0..=level.min(top)).rev() {
            let cand =
                self.search_layer(vec, qn, &[cur], self.ef_construction, l, None, &Stage1::Int8);
            if cand.is_empty() {
                continue;
            }
            cur = self.select_neighbors(cand.clone(), self.m)[0];
            cands.push((l, cand));
        }
        Some(cands)
    }

    /// Apply half of `upsert` for `build_batched`: push the node, wire
    /// links from the precomputed candidates, shrink, update entry.
    fn insert_apply(
        &mut self,
        id: &str,
        vec: &[f32],
        level: usize,
        cands: NodeCands,
    ) {
        if vec.len() != self.dim {
            return;
        }
        let idx = self.nodes.len();
        let (ivec, scale) = quantize(vec);
        let sum_sq = ivec.iter().map(|x| (*x as f64) * (*x as f64)).sum();
        self.nodes.push(HnswNode {
            ivec,
            scale,
            sum_sq,
            links: vec![Vec::new(); level + 1],
            bits: if self.stage1_binary { sign_bits(vec) } else { Vec::new() },
        });
        self.id_to_idx.insert(id.to_string(), idx);
        self.idx_to_id.push(id.to_string());
        let Some(entry) = self.entry else {
            self.entry = Some(idx);
            return;
        };
        let top = self.nodes[entry].links.len() - 1;
        for (l, cand) in cands {
            if cand.is_empty() {
                continue;
            }
            // Select M (link caps enforced separately by shrink).
            let neighbors = self.select_neighbors(cand, self.m);
            if neighbors.is_empty() {
                continue;
            }
            let mmax = if l == 0 { self.mmax0 } else { self.mmax };
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
        let dot = simd::dot_f32_i8(q, &n.ivec);
        match self.metric {
            Metric::Cosine => {
                let denom = qn.sqrt() * n.sum_sq.sqrt();
                if denom == 0.0 { 1.0 } else { 1.0 - dot / denom }
            }
            Metric::L2 => {
                let s = n.scale as f64;
                qn + s * s * n.sum_sq - 2.0 * s * dot
            }
            // IP on quantized bytes: the node vector is scale*ivec, so the
            // true dot - and its negated distance - needs the scale back.
            Metric::InnerProduct => -(n.scale as f64) * dot,
        }
    }

    /// Node vs node on quantized bytes (integer dot, exact): drives the
    /// diversity heuristic and link shrinking without any dequant
    /// temporaries.
    fn node_dist(&self, a: usize, b: usize) -> f64 {
        let (na, nb) = (&self.nodes[a], &self.nodes[b]);
        let dot = simd::dot_i8_i8(&na.ivec, &nb.ivec);
        match self.metric {
            Metric::Cosine => {
                let denom = (na.sum_sq * nb.sum_sq).sqrt();
                if denom == 0.0 { 1.0 } else { 1.0 - dot as f64 / denom }
            }
            Metric::L2 => {
                let (sa, sb) = (na.scale as f64, nb.scale as f64);
                sa * sa * na.sum_sq + sb * sb * nb.sum_sq - 2.0 * sa * sb * dot as f64
            }
            Metric::InnerProduct => -(na.scale as f64) * (nb.scale as f64) * (dot as f64),
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
            if self.stage1_binary {
                n.bits = sign_bits(&vec);
            }
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
            bits: if self.stage1_binary { sign_bits(&vec) } else { Vec::new() },
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
        // Greedy descent to the insertion levels (always int8: link
        // structure is toggle-independent, #190).
        for l in ((level + 1)..=top).rev() {
            cur = self.greedy_closest(&vec, qn, cur, l, &Stage1::Int8);
        }
        for l in (0..=level.min(top)).rev() {
            let cand =
                self.search_layer(&vec, qn, &[cur], self.ef_construction, l, None, &Stage1::Int8);
            if cand.is_empty() {
                continue;
            }
            // Select M (link caps enforced separately by shrink).
            let neighbors = self.select_neighbors(cand, self.m);
            if neighbors.is_empty() {
                continue;
            }
            cur = neighbors[0];
            let mmax = if l == 0 { self.mmax0 } else { self.mmax };
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

    /// Stage-1 distance for one node under the current traversal mode.
    fn stage_dist(&self, q: &[f32], qn: f64, idx: usize, stage1: &Stage1) -> f64 {
        match stage1 {
            Stage1::Int8 => self.q_dist(q, qn, idx),
            Stage1::Binary(qb) => hamming(&self.nodes[idx].bits, qb) as f64,
        }
    }

    /// Greedy single-best descent at one level (ef=1 beam).
    fn greedy_closest(
        &self,
        q: &[f32],
        qn: f64,
        entry: usize,
        level: usize,
        stage1: &Stage1,
    ) -> usize {
        let mut best = entry;
        let mut best_d = self.stage_dist(q, qn, entry, stage1);
        // ponytail: fixed-point loop, not a worklist — one pass per
        // improvement, terminates when no neighbor is closer.
        loop {
            let mut improved = false;
            for &nb in &self.nodes[best].links[level] {
                let d = self.stage_dist(q, qn, nb, stage1);
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
    /// layers without a HashSet alloc per call. `allow` (#192 filtered
    /// traversal) gates admission: only matching nodes enter the beam
    /// and results — non-matching ones are visited-marked and never
    /// expanded; the caller's widening + exact fallback covers filtered
    /// regions the walk can't reach. Entry seeds always expand but join
    /// results only when they match.
    #[allow(clippy::too_many_arguments)] // private; the args are the beam-search context (query, beam, layer, admission, stage-1 mode)
    fn search_layer(
        &self,
        q: &[f32],
        qn: f64,
        entries: &[usize],
        ef: usize,
        level: usize,
        allow: Option<&mut dyn FnMut(usize) -> bool>,
        stage1: &Stage1,
    ) -> Vec<(f64, usize)> {
        use std::collections::BinaryHeap;
        use std::cmp::Reverse;
        let ef = ef.max(1);
        let mut allow = allow;
        let mut visited = vec![false; self.nodes.len()];
        let mut cands: BinaryHeap<Reverse<Cand>> = BinaryHeap::new();
        let mut result: BinaryHeap<Cand> = BinaryHeap::new();
        for &e in entries {
            if e < self.nodes.len() && !visited[e] {
                visited[e] = true;
                let d = self.stage_dist(q, qn, e, stage1);
                cands.push(Reverse(Cand { dist: d, idx: e }));
                let admitted = match &mut allow {
                    Some(f) => f(e),
                    None => true,
                };
                if admitted {
                    result.push(Cand { dist: d, idx: e });
                    if result.len() > ef {
                        result.pop();
                    }
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
                    let d = self.stage_dist(q, qn, nb, stage1);
                    let admitted = match &mut allow {
                        Some(f) => f(nb),
                        None => true,
                    };
                    let mut push = admitted && result.len() < ef;
                    if admitted && !push {
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
        // #190: binary stage-1 when the def enabled it and the resident
        // graph carries bitsets (post-repair the invariant holds; during
        // repair `complete` is off, so mixed states never serve ANN).
        let words = self.dim.div_ceil(64);
        let stage1 = if self.stage1_binary
            && self.nodes[entry].bits.len() >= words
        {
            Stage1::Binary(&sign_bits(query))
        } else {
            Stage1::Int8
        };
        for l in (1..=top).rev() {
            cur = self.greedy_closest(query, qn, cur, l, &stage1);
        }
        self.search_layer(query, qn, &[cur], ef, 0, None, &stage1)
    }

    /// Filter-aware search (#192): same walk as [`Self::search`], but
    /// `allow` (idx → match) gates admission — only matching nodes enter
    /// the beam and results, so `ef` isn't wasted on doomed candidates.
    /// Upper-level descent stays unfiltered (routing, not results). The
    /// executor's widening + exact fallback stays the safety net for
    /// filtered regions the walk can't reach.
    pub fn search_filtered(
        &self,
        query: &[f32],
        ef: usize,
        allow: &mut dyn FnMut(usize) -> bool,
    ) -> Vec<(f64, usize)> {
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
        let words = self.dim.div_ceil(64);
        let stage1 = if self.stage1_binary && self.nodes[entry].bits.len() >= words {
            Stage1::Binary(&sign_bits(query))
        } else {
            Stage1::Int8
        };
        for l in (1..=top).rev() {
            cur = self.greedy_closest(query, qn, cur, l, &stage1);
        }
        self.search_layer(query, qn, &[cur], ef, 0, Some(allow), &stage1)
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

    /// V1 SIMD kernels agree with the sequential oracle: fp kernels within
    /// 1e-9 (AVX folds 4 lanes — addition order, not element math, differs),
    /// int8·int8 bit-exact (integer associativity).
    #[test]
    fn simd_kernels_match_sequential() {
        let f = |i: usize| ((i % 13) as f32 / 7.0) - 0.9 + (i % 5) as f32 * 0.01;
        let g = |i: usize| ((i * 11 + 3) % 17) as f32 / 9.0 - 1.0;
        for n in [1usize, 7, 8, 9, 32, 33, 96, 4096] {
            let a: Vec<f32> = (0..n).map(f).collect();
            let b: Vec<f32> = (0..n).map(g).collect();
            let (d, na, nb) = simd::cosine_parts(&a, &b);
            let s = scalars::cosine_parts(&a, &b);
            let tol = 1e-9 * (n as f64).max(1.0);
            assert!((d - s.0).abs() <= tol, "cosine dot n={n}: {} vs {}", d, s.0);
            assert!((na - s.1).abs() <= tol, "norm a n={n}");
            assert!((nb - s.2).abs() <= tol, "norm b n={n}");
            assert!((simd::l2_f32_f32(&a, &b) - scalars::l2_f32_f32(&a, &b)).abs() <= tol);
            assert!((simd::dot_f32_f32(&a, &b) - scalars::dot_f32_f32(&a, &b)).abs() <= tol);
            let q8: Vec<i8> = (0..n).map(|i| ((i * 7 + 1) % 251) as i64 as i8).collect();
            let v8: Vec<i8> = (0..n).map(|i| ((i * 5 + 2) % 255) as i64 as i8).collect();
            assert_eq!(
                simd::dot_i8_i8(&q8, &v8),
                scalars::dot_i8_i8(&q8, &v8),
                "int8 dot n={n}"
            );
            assert_eq!(simd::dot_i8_i8(&v8, &q8), scalars::dot_i8_i8(&v8, &q8));
            // f32·i8 with the same int8 fixtures
            let qf: Vec<f32> = v8.iter().map(|x| *x as f32 * 0.5).collect();
            assert!(
                (simd::dot_f32_i8(&qf, &q8) - scalars::dot_f32_i8(&qf, &q8)).abs()
                    <= tol * 2.0
            );
        }
    }

    #[test]
    fn roundtrip_bytes() {
        let v = vec![0.5, -1.25, 3.0];
        let b = encode_f32s(&v);
        assert_eq!(b.len(), 12);
        assert_eq!(decode_f32s(&b).expect("decode"), v);
        assert!(decode_f32s(&b[..5]).is_err());
    }

    /// #190: sign bitset encoding — one bit per dim, set on x >= 0; the
    /// Hamming stage-1 distance counts sign disagreements. Roundtrip and
    /// alignment rules extend the byte encoding's test discipline.
    #[test]
    fn sign_bitset_encoding_and_hamming() {
        // Signs +, -, +, + → 0b1101 (bit j set on v[j] >= 0).
        assert_eq!(sign_bits(&[1.0, -2.0, 0.0, 3.0]), vec![0b1101]);
        // Identical → 0; mirrored signs → every bit disagrees.
        let a = sign_bits(&[1.0, -1.0]);
        let b = sign_bits(&[-1.0, 1.0]);
        assert_eq!(hamming(&a, &a), 0);
        assert_eq!(hamming(&a, &b), 2);
        // 70 dims → two words; zero vector is all-zero bits.
        let v70: Vec<f32> =
            (0..70).map(|i| if i % 3 == 0 { -1.0 } else { 1.0 }).collect();
        assert_eq!(sign_bits(&v70).len(), 2);
        assert_eq!(hamming(&sign_bits(&v70), &sign_bits(&v70)), 0);
        assert_eq!(sign_bits(&[0.0, 0.0]), vec![0b11], "0.0 counts as +");
    }

    /// #190 at graph level: a binary-stage1 graph recalls the int8
    /// graph's oracle top-k on the same fixture (toy epsilon here; the
    /// duel harness owns the 0.99 gate at embedding dims). 64 dims so
    /// the sign bitset has enough resolution to rank (8 dims = 0..8
    /// hamming values, too coarse to rank anything).
    #[test]
    fn binary_stage1_graph_matches_int8_recall() {
        const DIM: usize = 64;
        let mk = |bin: bool| {
            HnswIndex::new(
                DIM,
                Metric::Cosine,
                HnswParams {
                    m: Some(16),
                    ef_construction: Some(128),
                    ef_search: None,
                    binary_stage1: bin,
                },
            )
        };
        let (mut g, mut int8) = (mk(true), mk(false));
        // Clustered fixture (real embeddings are anisotropic): 25 LCG
        // centers, each doc = center + tiny noise. Pure uniform randoms
        // concentrate at ~90° apart — sign bits can't rank noise, and
        // the oracle top-10 would be arbitrary.
        let lcg = |s: u64| -> f32 { (((s.wrapping_mul(6364136223846793005) >> 33) % 1001) as f32) / 250.0 - 2.0 };
        let noise = |s: u64| -> f32 { (((s.wrapping_mul(1442695040888963407) >> 33) % 21) as f32) / 1000.0 - 0.01 };
        let center = |c: usize, j: usize| lcg((c as u64).wrapping_mul(6364136223846793005).wrapping_add(j as u64));
        let vecs: Vec<Vec<f32>> = (0..200)
            .map(|i| {
                (0..DIM)
                    .map(|j| center(i % 25, j) + noise((i as u64).wrapping_mul(97).wrapping_add(j as u64)))
                    .collect()
            })
            .collect();
        for (i, v) in vecs.iter().enumerate() {
            g.upsert(&format!("n{i}"), v.clone());
            int8.upsert(&format!("n{i}"), v.clone());
        }
        g.set_complete(true);
        int8.set_complete(true);
        for qi in 0..10 {
            let q: Vec<f32> = (0..DIM)
                .map(|j| center(qi * 2 % 25, j) + noise(((qi + 31) as u64).wrapping_mul(97).wrapping_add(j as u64)))
                .collect();
            let mut brute: Vec<(f64, usize)> = vecs
                .iter()
                .enumerate()
                .map(|(i, v)| (cosine_distance(&q, v), i))
                .collect();
            brute.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
            let want: Vec<String> =
                brute.into_iter().take(10).map(|(_, i)| format!("n{i}")).collect();
            // Beam 8x the k: stage-1 is approximate; the exact rescore
            // downstream (executor) owns final ordering.
            // Emulate the executor's two-stage pipeline: the stage-1 beam
            // (80) prunes, the exact f32 rescore orders the survivors.
            // Measuring the raw hamming top-10 would test the prune, not
            // the served recall.
            let rescore = |cands: Vec<(f64, usize)>| -> Vec<String> {
                let mut scored: Vec<(f64, usize)> = cands
                    .into_iter()
                    .map(|(_, i)| (cosine_distance(&q, &vecs[i]), i))
                    .collect();
                scored.sort_by(|a, b| a.0.total_cmp(&b.0).then_with(|| a.1.cmp(&b.1)));
                scored.into_iter().take(10).map(|(_, i)| format!("n{i}")).collect()
            };
            let got = rescore(g.search(&q, 80));
            let hits = got.iter().filter(|id| want.contains(id)).count();
            assert!(hits >= 9, "binary stage-1 recall {hits}/10 at q{qi}");
            // The int8 path keeps the same epsilon on the same fixture.
            let got8 = rescore(int8.search(&q, 80));
            let hits8 = got8.iter().filter(|id| want.contains(id)).count();
            assert!(hits8 >= 9, "int8 recall {hits8}/10 at q{qi}");
        }
    }

    /// #192: the filtered walk admits only matching nodes into beam and
    /// results; the unfiltered `search` returns everything. Same query,
    /// same graph — allowed subset of the unfiltered order.
    #[test]
    fn search_filtered_gates_admission() {
        use crate::index::vector::HnswParams;
        let mut g = HnswIndex::new(2, Metric::L2, HnswParams::default());
        // Grid: evens are "matching" (x >= 0), odds far side.
        for i in 0..12 {
            let x = if i % 2 == 0 { i as f32 } else { -i as f32 };
            g.upsert(&format!("n{i}"), vec![x, 0.0]);
        }
        g.set_complete(true);
        let q = vec![5.0, 0.0];
        let all = g.search(&q, 6);
        let even: Vec<usize> = (0..12).filter(|i| i % 2 == 0).collect();
        let mut allow = |idx: usize| even.contains(&idx);
        let filtered = g.search_filtered(&q, 6, &mut allow);
        assert!(!filtered.is_empty());
        for (d, idx) in &filtered {
            assert!(even.contains(idx), "non-matching node admitted: {idx}");
            assert!(
                all.iter().any(|(ad, ai)| ai == idx && ad == d),
                "filtered row must ride the unfiltered order"
            );
        }
        // Sorted by distance (ties by idx), same contract as search.
        assert!(filtered.windows(2).all(|w| w[0].0 <= w[1].0));
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
    fn dim_scaled_params() {
        // P3 duel verdict: clamp(3√D,16,64); D≤16 keeps legacy values.
        assert_eq!(hnsw_m_for_dim(2), 16);
        assert_eq!(hnsw_m_for_dim(16), 16);
        assert_eq!(hnsw_m_for_dim(32), 17);
        assert_eq!(hnsw_m_for_dim(128), 34);
        assert_eq!(hnsw_m_for_dim(384), 59);
        assert_eq!(hnsw_m_for_dim(4096), 64);
        let lo = HnswIndex::new(8, Metric::Cosine, HnswParams::default());
        assert_eq!((lo.m, lo.mmax, lo.mmax0, lo.ef_construction), (16, 16, 32, 200));
        let hi = HnswIndex::new(384, Metric::Cosine, HnswParams::default());
        assert_eq!((hi.m, hi.mmax, hi.mmax0, hi.ef_construction), (59, 59, 118, 300));
    }

    #[test]
    fn hnsw_recall_high_dim() {
        // P3 gate: 300 pts dim 128 (the slope that warned at 0.94) must
        // clear 0.95 at the wider beam M=34/ef=256 pairing the executor
        // now uses for D≥128.
        let mut rng = FixtureRng(0x51);
        let pts: Vec<Vec<f32>> = (0..300).map(|_| (0..128).map(|_| rng.next_f32()).collect()).collect();
        let queries: Vec<Vec<f32>> = (0..10).map(|_| (0..128).map(|_| rng.next_f32()).collect()).collect();
        let r = hnsw_recall(&pts, &queries, 10, Metric::Cosine, 256);
        assert!(r >= 0.95, "high-dim recall {r}");
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
        let (z, s0) = quantize(&[0.0; 8]);
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
        let mut h = HnswIndex::new(dim, m, HnswParams::default());
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
        let mut h = HnswIndex::new(2, Metric::L2, HnswParams::default());
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
        let mut h1 = HnswIndex::new(2, Metric::Cosine, HnswParams::default());
        let mut h2 = HnswIndex::new(2, Metric::Cosine, HnswParams::default());
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
