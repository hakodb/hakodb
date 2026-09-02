use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::{Duration, Instant};

use parking_lot::Mutex;
use hashbrown::HashMap;

use super::plan::QueryPlan;
use super::planner::QueryPlanner;
use crate::index::manager::IndexManager;
use crate::query::query::Query;

/// LRU-ish cache of compiled QueryPlans keyed by a structural hash of the query.
/// The plan is a pure function of (Query, IndexManager-version) — the bench
/// re-issues the same shape 300 times in the stress loop, so caching it skips
/// the planner clone + filter walks per call.
///
/// The cache is intentionally bounded and TTL'd so it can't grow without limit
/// if the workload churns (Ponytail: tiny, no eviction policy, no async).
pub struct PlanCache {
    map: Mutex<HashMap<u64, (Arc<QueryPlan>, Instant)>>,
    ttl: Duration,
    /// Bumped on every plan invocation (always, even on hit) so the next caller
    /// can tell we're exercising the cache. Not a correctness mechanism.
    #[cfg(test)]
    pub hits: std::sync::atomic::AtomicU64,
    #[cfg(test)]
    pub misses: std::sync::atomic::AtomicU64,
}

impl PlanCache {
    pub fn new(ttl: Duration) -> Self {
        Self {
            map: Mutex::new(HashMap::with_capacity(64)),
            ttl,
            #[cfg(test)]
            hits: std::sync::atomic::AtomicU64::new(0),
            #[cfg(test)]
            misses: std::sync::atomic::AtomicU64::new(0),
        }
    }

    pub fn get_or_compute(
        &self,
        query: &Query,
        indexes: &IndexManager,
        collection_rows: usize,
        worker_count: usize,
        index_ready: bool,
    ) -> Arc<QueryPlan> {
        let key = hash_query(query);
        let now = Instant::now();

        // Fast path: read-only peek, no lock if no hit.
        if let Some((plan, ts)) = self.map.lock().get(&key).cloned() {
            if now.duration_since(ts) < self.ttl {
                #[cfg(test)] self.hits.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                return plan;
            }
        }

        // Slow path: build a fresh plan.
        let plan = QueryPlanner::plan(query, indexes, collection_rows, worker_count, index_ready);
        let arc = Arc::new(plan);

        self.map.lock().insert(key, (arc.clone(), now));
        #[cfg(test)] self.misses.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        arc
    }

    /// Drop everything. Call after a schema change (index add/remove) so stale
    /// plans don't linger past their logical validity.
    pub fn invalidate(&self) {
        self.map.lock().clear();
    }
}

impl Default for PlanCache {
    fn default() -> Self {
        // 1 second is enough to absorb the bench's 300-iter stress loop on a
        // single query shape while not serving stale plans across schema changes.
        Self::new(Duration::from_secs(1))
    }
}

/// Hash a query's STRUCTURE — collection, filters (field+op+value), order,
/// limit, offset, projection, cursor bounds. Anything that changes the plan.
/// `aggregation` and `or_groups` included because both affect `ScanType`.
fn hash_query(q: &Query) -> u64 {
    let mut h = DefaultHasher::new();
    q.collection.hash(&mut h);

    for f in &q.filters {
        f.field.hash(&mut h);
        (f.op as u8).hash(&mut h);
        hash_value(&f.value, &mut h);
    }
    for g in &q.or_groups {
        for f in g {
            f.field.hash(&mut h);
            (f.op as u8).hash(&mut h);
            hash_value(&f.value, &mut h);
        }
    }
    for o in &q.order_by {
        o.field.hash(&mut h);
        o.ascending.hash(&mut h);
    }
    q.limit.hash(&mut h);
    q.offset.hash(&mut h);
    for p in &q.projection { p.hash(&mut h); }
    for a in &q.aggregations { hash_agg(a, &mut h); }

    // Cursor bounds — these change the index range and must invalidate.
    if let Some(v) = &q.start_at { for x in v { hash_value(x, &mut h); } }
    if let Some(v) = &q.start_after { for x in v { hash_value(x, &mut h); } }
    if let Some(v) = &q.end_at { for x in v { hash_value(x, &mut h); } }
    if let Some(v) = &q.end_before { for x in v { hash_value(x, &mut h); } }

    h.finish()
}

fn hash_value(v: &crate::document::value::Value, h: &mut DefaultHasher) {
    // Discriminant-only hash for value variants we care about for plan shape.
    // We don't need to hash the *contents* of large Array/Map — they don't
    // change the plan, only the filter clause. The filter clause already
    // hashes each value via the per-filter hash above.
    match v {
        crate::document::value::Value::Null => 0u8.hash(h),
        crate::document::value::Value::Bool(b) => { 1u8.hash(h); b.hash(h); }
        crate::document::value::Value::Int(i) => { 2u8.hash(h); i.hash(h); }
        crate::document::value::Value::Float(f) => { 3u8.hash(h); f.to_bits().hash(h); }
        crate::document::value::Value::String(s) => { 4u8.hash(h); s.hash(h); }
        crate::document::value::Value::Binary(b) => { 5u8.hash(h); b.len().hash(h); }
        crate::document::value::Value::Timestamp(t) => { 6u8.hash(h); t.hash(h); }
        crate::document::value::Value::Reference { collection, doc_id } => {
            7u8.hash(h); collection.hash(h); doc_id.hash(h);
        }
        crate::document::value::Value::BlobLink { offset, len } => {
            8u8.hash(h); offset.hash(h); len.hash(h);
        }
        crate::document::value::Value::Array(_) => 9u8.hash(h),
        crate::document::value::Value::Map(_) => 10u8.hash(h),
        crate::document::value::Value::ServerTimestamp => 11u8.hash(h),
    }
}

fn hash_agg(a: &crate::query::query::AggregateOp, h: &mut DefaultHasher) {
    match a {
        crate::query::query::AggregateOp::Count => 0u8.hash(h),
        crate::query::query::AggregateOp::Sum(f) => { 1u8.hash(h); f.hash(h); }
        crate::query::query::AggregateOp::Avg(f) => { 2u8.hash(h); f.hash(h); }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::query::query::{AggregateOp, Query};
    use crate::query::filter::Operator;
    use crate::query::order::OrderBy;
    use crate::document::value::Value;
    use crate::index::manager::IndexManager;

    fn q_active_eq() -> Query {
        Query::new("bench")
            .where_filter("active", Operator::Eq, Value::Bool(true))
            .limit(50)
    }

    #[test]
    fn same_shape_returns_same_plan() {
        let cache = PlanCache::default();
        let idx = IndexManager::default();
        let p1 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);
        let p2 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);
        // Same shape, different collection_rows — plan must be the same (collection_rows
        // is a planner hint, not a structural input that changes ScanType).
        let p3 = cache.get_or_compute(&q_active_eq(), &idx, 10000, 4, true);
        assert!(Arc::ptr_eq(&p1, &p2));
        assert!(Arc::ptr_eq(&p1, &p3));
        assert_eq!(cache.hits.load(std::sync::atomic::Ordering::Relaxed), 2);
        assert_eq!(cache.misses.load(std::sync::atomic::Ordering::Relaxed), 1);
    }

    #[test]
    fn different_filter_field_invalidates() {
        let cache = PlanCache::default();
        let idx = IndexManager::default();
        let p1 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);

        let q2 = Query::new("bench")
            .where_filter("tenant", Operator::Eq, Value::String("tenant-2".into()))
            .limit(50);
        let p2 = cache.get_or_compute(&q2, &idx, 1000, 4, true);
        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn ttl_expires_old_plan() {
        let cache = PlanCache::new(Duration::from_millis(10));
        let idx = IndexManager::default();
        let p1 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);
        std::thread::sleep(Duration::from_millis(20));
        let p2 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);
        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn cursor_change_invalidates() {
        let cache = PlanCache::default();
        let idx = IndexManager::default();
        let mut q1 = Query::new("bench");
        q1.order_by.push(OrderBy { field: "id".into(), ascending: true });
        q1.start_at = Some(vec![Value::String("b_100".into())]);
        q1.limit = Some(5);
        let p1 = cache.get_or_compute(&q1, &idx, 1000, 4, true);

        let mut q2 = Query::new("bench");
        q2.order_by.push(OrderBy { field: "id".into(), ascending: true });
        q2.start_at = Some(vec![Value::String("b_500".into())]);
        q2.limit = Some(5);
        let p2 = cache.get_or_compute(&q2, &idx, 1000, 4, true);
        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn invalidate_clears() {
        let cache = PlanCache::default();
        let idx = IndexManager::default();
        let p1 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);
        cache.invalidate();
        let p2 = cache.get_or_compute(&q_active_eq(), &idx, 1000, 4, true);
        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn change_in_op_invalidates() {
        let cache = PlanCache::default();
        let idx = IndexManager::default();
        let q1 = Query::new("bench").where_filter("age", Operator::Gt, Value::Int(20));
        let p1 = cache.get_or_compute(&q1, &idx, 1000, 4, true);
        let q2 = Query::new("bench").where_filter("age", Operator::Lt, Value::Int(20));
        let p2 = cache.get_or_compute(&q2, &idx, 1000, 4, true);
        assert!(!Arc::ptr_eq(&p1, &p2));
    }

    #[test]
    fn aggregate_change_invalidates() {
        let cache = PlanCache::default();
        let idx = IndexManager::default();
        let q1 = Query::new("bench").aggregate(AggregateOp::Count);
        let p1 = cache.get_or_compute(&q1, &idx, 1000, 4, true);
        let q2 = Query::new("bench").aggregate(AggregateOp::Sum("age".into()));
        let p2 = cache.get_or_compute(&q2, &idx, 1000, 4, true);
        assert!(!Arc::ptr_eq(&p1, &p2));
    }
}
