#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum SortDirection {
    Asc,
    Desc,
}

#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub struct CompositeField {
    pub field: String,
    pub direction: SortDirection,
}

#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct CompositeIndexDefinition {
    pub id: u32,
    pub collection: String,
    pub fields: Vec<CompositeField>,
    /// Enforce uniqueness across all documents in the collection
    /// (hakodb#71). Missing fields follow `sparse` (see below).
    #[serde(default)]
    pub unique: bool,
    /// When true, documents lacking any indexed field are not indexed
    /// at all. When false (dense), a missing field is treated as a
    /// concrete NULL tuple so the document still participates in
    /// uniqueness. No-unique indexes keep the old skip-missing
    /// behavior regardless of this flag.
    #[serde(default)]
    pub sparse: bool,
    /// Only documents matching this predicate are indexed (partial
    /// index). Evaluated with the standard query filter machinery.
    #[serde(default)]
    pub filter: Option<crate::query::filter::FilterGroup>,
}

impl CompositeIndexDefinition {
    pub fn new(collection: &str) -> Self {
        Self {
            id: 0,
            collection: collection.to_string(),
            fields: Vec::new(),
            unique: false,
            sparse: true,
            filter: None,
        }
    }

    pub fn with_fields(mut self, fields: Vec<(String, SortDirection)>) -> Self {
        self.fields = fields
            .into_iter()
            .map(|(field, direction)| CompositeField { field, direction })
            .collect();
        self
    }

    pub fn with_constraints(
        mut self,
        unique: bool,
        sparse: bool,
        filter: Option<crate::query::filter::FilterGroup>,
    ) -> Self {
        self.unique = unique;
        self.sparse = sparse;
        self.filter = filter;
        self
    }

    /// Tuple values for this index. Missing-field handling (hakodb#71):
    /// unique+dense (`sparse: false`) indexes materialize a missing field
    /// as `Value::Null` so the document still participates in uniqueness —
    /// every other case (and all legacy non-unique indexes) skips the doc
    /// entirely, matching the historical behavior. Non-scalar values
    /// (Map/Array/Reference/BlobLink) encode as bare type tags with no
    /// payload, so they can never distinguish two documents — a unique
    /// index skips claiming them (tree parity: the non-unique tree already
    /// stores tag-only keys).
    pub fn document_values_of(&self, doc_id: &str, doc: &crate::document::hako_doc::HakoDoc) -> Option<Vec<crate::document::value::Value>> {
        let mut values = Vec::with_capacity(self.fields.len());

        for f in &self.fields {
            // Virtual fields resolve from metadata, never from the body.
            let v = match f.field.as_str() {
                "id" => crate::document::value::Value::String(doc_id.to_string()),
                "_time" => crate::document::value::Value::Int(doc._time),
                // Dotted paths (hakodb#71 F2) ride the fetch hook; single
                // segments behave exactly like `get`.
                _ => match doc.get_path(&f.field) {
                    Some(v) => v.clone(),
                    None => {
                        if self.unique && !self.sparse {
                            crate::document::value::Value::Null
                        } else {
                            return None;
                        }
                    }
                },
            };
            if self.unique
                && matches!(
                    v,
                    crate::document::value::Value::Map(_)
                        | crate::document::value::Value::Array(_)
                        | crate::document::value::Value::Reference { .. }
                        | crate::document::value::Value::BlobLink { .. }
                )
            {
                return None;
            }
            values.push(v);
        }

        Some(values)
    }

    /// Filtered-index predicate (hakodb#71): only documents matching the
    /// definition's filter are indexed. Uses the standard operator
    /// machinery so index-time and query-time agree. Virtual fields
    /// (`id`/`_time`) resolve like document_values; a missing field never
    /// matches (fail-closed, same rule as the query executor).
    pub fn filter_passes(&self, doc_id: &str, doc: &crate::document::hako_doc::HakoDoc) -> bool {
        use crate::query::filter::{compare_values, FilterLogic};
        let Some(group) = &self.filter else { return true };
        let value_of = |field: &str| -> Option<crate::document::value::Value> {
            match field {
                "id" => Some(crate::document::value::Value::String(doc_id.to_string())),
                "_time" => Some(crate::document::value::Value::Int(doc._time)),
                _ => doc.get_path(field).cloned(),
            }
        };
        let matches = |f: &crate::query::filter::Filter| {
            value_of(&f.field)
                .is_some_and(|v| compare_values(&v, &f.op, &f.value))
        };
        match group.logic {
            FilterLogic::And => group.filters.iter().all(matches),
            FilterLogic::Or => group.filters.iter().any(matches),
        }
    }
}
