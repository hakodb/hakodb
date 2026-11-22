//! Lightweight per-field cardinality estimates reported by EXPLAIN (#243).
//!
//! Stats are derived data: a bounded sample is taken once per collection on
//! first use (and again after restart). A `HashSet` of hashed
//! values gives an exact NDV for the sample; saturation just means "high
//! cardinality", which is all the planner needs. No per-query scan.
use hashbrown::{HashMap, HashSet};
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};

use crate::document::value::Value;

/// Cap keeps memory bounded on high-cardinality fields.
const MAX_DISTINCT: usize = 4096;

#[derive(Clone)]
pub struct Stat {
    values: HashSet<u64>,
    samples: usize,
    saturated: bool,
}

#[derive(Default, Clone)]
pub struct FieldStats {
    entries: HashMap<(String, String), Stat>,
    rebuilt: HashSet<String>,
}

impl FieldStats {
    /// Sample hash for a Value. Debug formatting is deterministic for the
    /// engine's Value enum and avoids needing a `Hash` impl on it.
    fn hash_value(value: &Value) -> u64 {
        let mut hasher = DefaultHasher::new();
        format!("{value:?}").hash(&mut hasher);
        hasher.finish()
    }

    pub fn observe(&mut self, collection: &str, field: &str, value: &Value) {
        let key = (collection.to_string(), field.to_string());
        let stat = self.entries.entry(key).or_insert_with(|| Stat {
            values: HashSet::new(),
            samples: 0,
            saturated: false,
        });
        stat.samples += 1;
        if !stat.saturated {
            stat.values.insert(Self::hash_value(value));
            if stat.values.len() >= MAX_DISTINCT {
                stat.saturated = true;
                stat.values.clear();
            }
        }
    }

    /// Number of distinct values in the sample, or `usize::MAX` when
    /// saturated (treat as uniquely selective, never sweep because of it).
    pub fn ndv(&self, collection: &str, field: &str) -> Option<usize> {
        self.entries
            .get(&(collection.to_string(), field.to_string()))
            .map(|s| if s.saturated { usize::MAX } else { s.values.len().max(1) })
    }

    pub fn samples(&self, collection: &str, field: &str) -> usize {
        self.entries
            .get(&(collection.to_string(), field.to_string()))
            .map_or(0, |s| s.samples)
    }

    /// Estimated fraction of rows matching an equality filter on `field`.
    pub fn equality_selectivity(&self, collection: &str, field: &str, rows: usize) -> Option<f64> {
        if rows == 0 {
            return Some(1.0);
        }
        let ndv = self.ndv(collection, field)?;
        if ndv == usize::MAX {
            return Some(1.0 / rows as f64);
        }
        Some((1.0 / ndv as f64).clamp(1.0 / rows as f64, 1.0))
    }

    pub fn has_field(&self, collection: &str, field: &str) -> bool {
        self.entries.contains_key(&(collection.to_string(), field.to_string()))
    }
    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }
    pub fn needs_rebuild(&self, collection: &str) -> bool {
        !self.rebuilt.contains(collection)
    }

    pub fn mark_rebuilt(&mut self, collection: &str) {
        self.rebuilt.insert(collection.to_string());
    }

    pub fn entries(&self) -> impl Iterator<Item = (&(String, String), &Stat)> {
        self.entries.iter()
    }

    pub fn import(&mut self, collection: &str, field: &str, stat: &Stat) {
        self.entries.insert(
            (collection.to_string(), field.to_string()),
            stat.clone(),
        );
    }}
