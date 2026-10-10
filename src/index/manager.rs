use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
// use std::collections::HashMap;
use hashbrown::HashMap;
use std::sync::Arc;

use super::composite::definition::CompositeIndexDefinition;
use super::composite::manager::CompositeIndexManager;
use super::inverted_index::InvertedIndex;
use super::vector::{decode_f32s, validate_dim, HnswIndex, Metric, VectorDef};
use crate::index::composite::composite_index::CompositeIndex;
use crate::index::secondary_index::SecondaryIndex;

use crate::error::HakoError;

#[derive(Default)]
pub struct IndexManager {
    pub composite: CompositeIndexManager,
    pub secondary: HashMap<String, HashMap<String, SecondaryIndex>>,
    pub fts: HashMap<String, HashMap<String, InvertedIndex>>,
    /// Vector definitions per (collection, field). P0: definitions only
    /// (the exact scan needs dim/metric, no RAM structure); P1's HNSW
    /// hangs off this same registry.
    pub vector: HashMap<String, HashMap<String, VectorDef>>,
    /// P1 HNSW graphs, keyed identically to `vector`. Derived state:
    /// never snapshotted (rebuilt from docs on open/create); `complete`
    /// tells the executor whether ANN may run or exact must.
    pub hnsw: HashMap<String, HashMap<String, HnswIndex>>,
}

/// WAL tails at snapshot time, per collection: (wal data_len, wal
/// file_len). Lets recovery tell a fresh snapshot from a stale one
/// (crash after the last snapshot) without trusting timestamps.
pub type WalTails = HashMap<String, (u64, u64)>;

impl IndexManager {
    pub fn create_fts_index(&mut self, collection: &str, field: &str) {
        self.fts
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), InvertedIndex::default());
    }

    /// Register a vector definition (P0: no RAM structure — the exact scan
    /// only needs dim + metric; dim mismatch at query time is fail-closed).
    /// Re-registering overwrites (same overwrite rule as fts/secondary).
    pub fn create_vector_index(
        &mut self,
        collection: &str,
        field: &str,
        dim: u32,
        metric: Metric,
    ) -> crate::error::Result<()> {
        validate_dim(dim)?;
        self.vector
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), VectorDef { dim, metric });
        Ok(())
    }

    /// Ensure an (empty, incomplete) HNSW graph for a registered def.
    /// Called at create/restore before any backfill fills it.
    pub fn ensure_vector_graph(&mut self, collection: &str, field: &str) {
        let def = match self.vector.get(collection).and_then(|m| m.get(field)) {
            Some(d) => (d.dim as usize, d.metric),
            None => return,
        };
        self.hnsw
            .entry(collection.to_string())
            .or_default()
            .entry(field.to_string())
            .or_insert_with(|| HnswIndex::new(def.0, def.1));
    }

    /// Upsert one doc's embedding into every HNSW graph of `collection`
    /// that indexes its field. Wrong-dim / non-binary values are ignored
    /// (fail-closed, same rule as the planner gate). Deletes need no
    /// hook: graph nodes are never removed, the executor filters
    /// candidates against storage liveness.
    pub fn index_vector(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
        if let Some(graphs) = self.hnsw.get_mut(collection) {
            for (field_name, graph) in graphs.iter_mut() {
                if let Some(Value::Binary(b)) = doc.get(field_name) {
                    if let Ok(v) = decode_f32s(b) {
                        graph.upsert(doc_id, v);
                    }
                }
            }
        }
    }

    /// Mark every graph of `collection` complete after a full pass
    /// (create backfill / recovery rescan) flowed through `index_vector`.
    /// Emptiness is data, not incompleteness: an empty pass still flips
    /// the flag, so later puts (hook-fed) serve ANN immediately instead
    /// of stranding the graph incomplete forever.
    pub fn complete_vector(&mut self, collection: &str) {
        if let Some(graphs) = self.hnsw.get_mut(collection) {
            for graph in graphs.values_mut() {
                graph.set_complete(true);
            }
        }
    }

    pub fn indexes_for_collection(
        &self,
        collection: &str,
    ) -> impl Iterator<Item = &CompositeIndex> {
        self.composite.indexes_for_collection(collection)
    }

    pub fn create_index(&mut self, definition: CompositeIndexDefinition) -> u32 {
        self.composite.create_index(definition)
    }

    pub fn has_index(&self, collection: &str, fields: &[String]) -> bool {
        if fields.is_empty() {
            return false;
        }

        self.composite
            .indexes_for_collection(collection)
            .any(|idx| {
                // An index can satisfy a query if the query fields 
                // are the leading prefix of the index fields.
                if idx.definition.fields.len() < fields.len() {
                    return false;
                }

                for i in 0..fields.len() {
                    if idx.definition.fields[i].field != fields[i] {
                        return false;
                    }
                }
                true
            })
    }

    pub fn exact_match_doc_ids(
        &self,
        collection: &str,
        fields: &[String],
        values: &[Value],
        // ) -> Option<Vec<String>> {
    ) -> Option<Vec<Arc<str>>> {
        self.composite
            .exact_match_doc_ids(collection, fields, values)
    }

    pub fn index_document(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
        // 1. Update Composite
        self.composite.index_document(collection, doc_id, doc);

        // 2. Update FTS
        if let Some(fields) = self.fts.get_mut(collection) {
            for (field_name, index) in fields.iter_mut() {
                if let Some(Value::String(text)) = doc.get(field_name) {
                    index.insert(text, doc_id.to_string());
                }
            }
        }

        // 3. ADD THIS: Update Secondary Indexes automatically
        // ponytail: one shared id Arc across every field of this doc (was a
        // String clone per field), keys encoded into a reused scratch buffer
        // with zero-alloc hits for hot values via `insert_borrowed`, and no
        // deep Value clone — `encode_scalar_into` borrows straight from the
        // doc (the old code cloned every indexed String value per write).
        if let Some(sec_map) = self.secondary.get_mut(collection) {
            let id_shared: Arc<str> = Arc::from(doc_id);
            crate::index::index_key::ENC_SCRATCH.with(|scratch| {
                let mut enc = scratch.borrow_mut();
                for (field_name, index) in sec_map.iter_mut() {
                    enc.clear();
                    if field_name.as_str() == "id" {
                        crate::index::index_key::encode_str_scalar_into(doc_id, &mut enc);
                    } else if field_name.as_str() == "_time" {
                        crate::index::index_key::encode_scalar_into(&Value::Int(doc._time), &mut enc);
                    } else if let Some(val) = doc.get(field_name) {
                        crate::index::index_key::encode_scalar_into(val, &mut enc);
                    } else {
                        continue;
                    }
                    index.insert_borrowed(&enc, id_shared.clone());
                }
            });
        }

        // 4. Vector HNSW (P1): upsert the embedding when the doc carries
        // an indexed binary field. Deletes intentionally need no hook
        // (liveness is external — see HnswIndex docs).
        self.index_vector(collection, doc_id, doc);
    }

    pub fn index_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        for (doc_id, doc) in docs {
            self.index_document(collection, doc_id, doc);
        }
    }

    pub fn remove_document(&mut self, collection: &str, doc_id: &str, doc: &HakoDoc) {
        self.composite.remove_document(collection, doc_id, doc);

        if let Some(fields) = self.fts.get_mut(collection) {
            for (field_name, index) in fields.iter_mut() {
                if let Some(Value::String(text)) = doc.get(field_name) {
                    index.remove(text, doc_id);
                }
            }
        }

        if let Some(sec_map) = self.secondary.get_mut(collection) {
            for (field_name, index) in sec_map.iter_mut() {
                if let Some(val) = doc.get(field_name) {
                    index.remove(&crate::index::index_key::encode_scalar(val), doc_id);
                }
            }
        }
    }

    pub fn remove_batch<'a, I>(&mut self, collection: &str, docs: I)
    where
        I: IntoIterator<Item = (&'a str, &'a HakoDoc)> + Clone,
    {
        for (doc_id, doc) in docs {
            self.remove_document(collection, doc_id, doc);
        }
    }

    /// Registers a new single-field index
    pub fn create_secondary_index(&mut self, collection: &str, field: &str) {
        self.secondary
            .entry(collection.to_string())
            .or_default()
            .insert(field.to_string(), SecondaryIndex::default());
    }

    /// Optimized lookup: Check secondary indexes if no composite exists
    pub fn lookup_secondary(
        &self,
        collection: &str,
        field: &str,
        value: &[u8],
    ) -> Option<Vec<Arc<str>>> {
        self.secondary
            .get(collection)?
            .get(field)?
            .range_scan(value, value) // Exact match scan
            .into()
    }

    pub fn export_state(&self) -> Result<Vec<u8>, HakoError> {
        // Composite rides along (hakodb#4): without it every restart
        // emptied composite indexes (probes missed -> full-sweep fallback
        // while secondary/fts survived). Old two-element files still load
        // via the legacy branch of import_state.
        bincode::serialize(&(&self.secondary, &self.fts, self.composite.export_snapshot()))
            .map_err(|e| HakoError::Corrupt(format!("Index export failed: {}", e)))
    }

    /// WAL tails at export, per collection: (wal data_len, wal file_len).
    /// Lets recovery tell a fresh snapshot from a stale one (crash after
    /// the last snapshot) without trusting timestamps.
    pub fn export_state_with_tails(&self, tails: &WalTails) -> Result<Vec<u8>, HakoError> {
        bincode::serialize(&(&self.secondary, &self.fts, self.composite.export_snapshot(), tails))
            .map_err(|e| HakoError::Corrupt(format!("Index export failed: {}", e)))
    }

    /// True when the manager holds any RAM entries for `collection`
    /// (secondary, fts, or composite trees). Vector counts as a
    /// registration (P0 keeps no RAM entries — rescan trivially holds).
    pub fn has_collection(&self, collection: &str) -> bool {
        self.secondary.contains_key(collection)
            || self.fts.contains_key(collection)
            || self.composite.has_collection(collection)
            || self.vector.contains_key(collection)
    }

    /// Drop every RAM entry for `collection` while keeping all index
    /// registrations (rescan refills them). Definitions are untouched.
    pub fn clear_collection(&mut self, collection: &str) {
        if let Some(sec_map) = self.secondary.get_mut(collection) {
            for idx in sec_map.values_mut() {
                idx.clear();
            }
        }
        if let Some(fts_map) = self.fts.get_mut(collection) {
            for idx in fts_map.values_mut() {
                idx.clear();
            }
        }
        // Vector defs stay registered; graphs reset to incomplete (the
        // recovery rescan refills them via index_document, then marks
        // complete — same order as every other family).
        if let Some(graphs) = self.hnsw.get_mut(collection) {
            for graph in graphs.values_mut() {
                graph.clear();
            }
        }
        self.composite.clear_collection(collection);
    }

    // pub fn import_state(&mut self, bytes: &[u8]) -> Result<(), HakoError> {
    //     // By importing hashbrown::HashMap at the top, 'HashMap' here 
    //     // now correctly refers to the hashbrown version.
    //     let (sec, fts): (
    //         HashMap<String, HashMap<String, crate::index::secondary_index::SecondaryIndex>>,
    //         HashMap<String, HashMap<String, crate::index::inverted_index::InvertedIndex>>
    //     ) = bincode::deserialize(bytes)
    //         .map_err(|e| HakoError::Corrupt(format!("Index import failed: {}", e)))?;
        
    //     self.secondary = sec;
    //     self.fts = fts;
    //     Ok(())
    // }
     pub fn import_state(&mut self, bytes: &[u8], skip: &std::collections::HashSet<String>) -> Result<(bool, Option<WalTails>), HakoError> {
        // Returns (composite trees came from the snapshot, WAL tails or
        // None when the file predates them). Four-element files are
        // current; three-element restore everything but report no tails
        // (caller rescans — one-time upgrade cost); legacy two-element
        // files restore secondary/fts and backfill composites from data.
        // Collections in `skip` were (re)built by a touch/load after
        // open — newer than any snapshot — so their entries are left
        // alone (both orders converge identically this way).
        type Trees = HashMap<u32, crate::index::composite::composite_index::CompositeIndex>;
        if let Ok((sec, fts, (trees, next_id), tails)) = bincode::deserialize::<(
            HashMap<String, HashMap<String, crate::index::secondary_index::SecondaryIndex>>,
            HashMap<String, HashMap<String, crate::index::inverted_index::InvertedIndex>>,
            (Trees, u32),
            WalTails,
        )>(bytes)
        {
            Self::merge_maps_skipping(&mut self.secondary, sec, skip);
            Self::merge_maps_skipping(&mut self.fts, fts, skip);
            self.composite.import_snapshot_filtered(trees, next_id, skip);
            return Ok((true, Some(tails)));
        }
        if let Ok((sec, fts, (trees, next_id))) = bincode::deserialize::<(
            HashMap<String, HashMap<String, crate::index::secondary_index::SecondaryIndex>>,
            HashMap<String, HashMap<String, crate::index::inverted_index::InvertedIndex>>,
            (Trees, u32),
        )>(bytes)
        {
            Self::merge_maps_skipping(&mut self.secondary, sec, skip);
            Self::merge_maps_skipping(&mut self.fts, fts, skip);
            self.composite.import_snapshot_filtered(trees, next_id, skip);
            return Ok((true, None));
        }
        let (sec, fts): (
            HashMap<String, HashMap<String, crate::index::secondary_index::SecondaryIndex>>,
            HashMap<String, HashMap<String, crate::index::inverted_index::InvertedIndex>>,
        ) = bincode::deserialize(bytes)
            .map_err(|e| HakoError::Corrupt(format!("Index import failed: {}", e)))?;
        Self::merge_maps_skipping(&mut self.secondary, sec, skip);
        Self::merge_maps_skipping(&mut self.fts, fts, skip);
        Ok((false, None))
    }

    fn merge_maps_skipping<K, V>(
        into: &mut HashMap<String, HashMap<K, V>>,
        from: HashMap<String, HashMap<K, V>>,
        skip: &std::collections::HashSet<String>,
    )
    where
        K: std::hash::Hash + Eq,
    {
        for (col, fields) in from {
            if skip.contains(&col) {
                continue;
            }
            let col_map = into.entry(col).or_default();
            for (field, index) in fields {
                if let Some(existing_index) = col_map.get_mut(&field) {
                    *existing_index = index;
                }
            }
        }
    }

    fn merge_maps<K, V>(into: &mut HashMap<String, HashMap<K, V>>, from: HashMap<String, HashMap<K, V>>)
    where
        K: std::hash::Hash + Eq,
    {
        for (col, fields) in from {
            let col_map = into.entry(col).or_default();
            for (field, index) in fields {
                if let Some(existing_index) = col_map.get_mut(&field) {
                    *existing_index = index;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::document::value::Value;
    use crate::index::composite::definition::SortDirection;

    fn doc(passw: &str, nim: &str) -> HakoDoc {
        let mut d = HakoDoc::default();
        d.insert("passw", Value::String(passw.into()));
        d.insert("nim", Value::String(nim.into()));
        d
    }

    fn manager_with_entries() -> IndexManager {
        let mut m = IndexManager::default();
        m.create_secondary_index("students", "nim");
        let id = m.create_index(
            CompositeIndexDefinition::new("students")
                .with_fields(vec![
                    ("passw".to_string(), SortDirection::Asc),
                    ("nim".to_string(), SortDirection::Asc),
                ]),
        );
        assert_eq!(id, 1);
        m.index_document("students", "d1", &doc("28021988", "F1"));
        m.index_document("students", "d2", &doc("28021988", "F2"));
        m
    }

    #[test]
    fn export_import_round_trip_keeps_composite() {
        let m = manager_with_entries();
        let bytes = m.export_state().expect("export");
        // Fresh manager with the SAME definitions (as restored from
        // definitions.json before import runs in open()).
        let mut m2 = IndexManager::default();
        m2.create_secondary_index("students", "nim");
        m2.create_index(
            CompositeIndexDefinition::new("students")
                .with_fields(vec![
                    ("passw".to_string(), SortDirection::Asc),
                    ("nim".to_string(), SortDirection::Asc),
                ]),
        );
        assert!(m2.import_state(&bytes, &std::collections::HashSet::new()).expect("import").0);
        // Trees restored verbatim: point probe finds both rows.
        let ids = m2
            .composite
            .exact_match_doc_ids(
                "students",
                &["passw".to_string(), "nim".to_string()],
                &[Value::String("28021988".into()), Value::String("F1".into())],
            )
            .expect("probe");
        assert_eq!(ids.len(), 1);
        assert_eq!(&*ids[0], "d1");
    }

    #[test]
    fn import_legacy_two_element_file_reports_false() {
        let m = manager_with_entries();
        // Legacy shape: secondary + fts only (pre-fix files).
        let legacy: Vec<u8> =
            bincode::serialize(&(&m.secondary, &m.fts)).expect("legacy export");
        let mut m2 = IndexManager::default();
        m2.create_secondary_index("students", "nim");
        m2.create_index(
            CompositeIndexDefinition::new("students")
                .with_fields(vec![
                    ("passw".to_string(), SortDirection::Asc),
                    ("nim".to_string(), SortDirection::Asc),
                ]),
        );
        assert!(!m2.import_state(&legacy, &std::collections::HashSet::new()).expect("legacy import").0);
        // Secondary restored, composite untouched (empty, awaiting backfill).
        assert!(m2.secondary.get("students").is_some_and(|mm| mm.contains_key("nim")));
        assert!(m2
            .composite
            .exact_match_doc_ids(
                "students",
                &["passw".to_string(), "nim".to_string()],
                &[Value::String("28021988".into()), Value::String("F1".into())],
            )
            .map_or(true, |v| v.is_empty()));
    }

    #[test]
    fn import_skips_definition_mismatch() {
        let m = manager_with_entries();
        let bytes = m.export_state().expect("export");
        // Same id, DIFFERENT fields: stale entry must not overwrite.
        let mut m2 = IndexManager::default();
        m2.create_secondary_index("students", "nim");
        m2.create_index(
            CompositeIndexDefinition::new("students")
                .with_fields(vec![("passw".to_string(), SortDirection::Asc)]),
        );
        assert!(m2.import_state(&bytes, &std::collections::HashSet::new()).expect("import").0);
        assert!(m2
            .composite
            .exact_match_doc_ids(
                "students",
                &["passw".to_string()],
                &[Value::String("28021988".into())],
            )
            .map_or(true, |v| v.is_empty()));
    }

    #[test]
    fn tails_roundtrip_and_clear_collection() {
        let m = manager_with_entries();
        let mut tails = WalTails::new();
        tails.insert("students".to_string(), (1234, 5678));
        let bytes = m.export_state_with_tails(&tails).expect("export");
        let mut m2 = IndexManager::default();
        let (restored, got) = m2.import_state(&bytes, &std::collections::HashSet::new()).expect("import");
        assert!(restored);
        assert_eq!(got.expect("tails").get("students"), Some(&(1234, 5678)));
        assert!(m2.has_collection("students"));
        m2.clear_collection("students");
        // Entries gone, registrations kept (rescan refills them).
        assert!(m2.has_collection("students"));
        assert!(m2
            .secondary
            .get("students")
            .is_some_and(|mm| mm.values().all(|idx| idx.get_map().is_empty())));
        // Legacy three-element files report no tails (caller rescans).
        let legacy3 = m.export_state().expect("export");
        let mut m3 = IndexManager::default();
        let (_, tails3) = m3.import_state(&legacy3, &std::collections::HashSet::new()).expect("import");
        assert!(tails3.is_none());
    }

    #[test]
    fn import_skips_touched_collections() {
        let m = manager_with_entries();
        let bytes = m.export_state().expect("export");
        let mut skip = std::collections::HashSet::new();
        skip.insert("students".to_string());
        let mut m2 = IndexManager::default();
        m2.create_secondary_index("students", "nim");
        let (restored, _) = m2.import_state(&bytes, &skip).expect("import");
        assert!(restored);
        // Untouched families still merge; skipped collection stays empty.
        assert!(m2
            .secondary
            .get("students")
            .is_some_and(|mm| mm.values().all(|idx| idx.get_map().is_empty())));
    }
}
