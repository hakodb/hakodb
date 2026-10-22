use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::engine::Hako;
use crate::query::query::{Query, AggregateOp};
use crate::query::filter::Operator;
use crate::error::Result;

/// Entry point for the fluent API: db.collection("users")
pub struct Collection<'a> {
    db: &'a Hako,
    name: String,
}

impl<'a> Collection<'a> {
    pub fn new(db: &'a Hako, name: &str) -> Self {
        Self { db, name: name.to_string() }
    }

    /// Start a query: db.collection("users").where_eq("active", true)
    pub fn where_eq(self, field: &str, value: impl Into<Value>) -> QueryBuilder<'a> {
        QueryBuilder::new(self.db, self.name).where_eq(field, value)
    }

    /// Get all documents in collection
    pub fn all(self) -> QueryBuilder<'a> {
        QueryBuilder::new(self.db, self.name)
    }

    /// Direct access to document by ID
    pub fn doc(self, id: &str) -> Result<Option<HakoDoc>> {
        self.db.get(&self.name, id)
    }
}

/// Marker filter constructor (shared by the builder method and direct
/// `Query::where_filter` users): anchor collection rides the filter
/// field, the walk rides a Map value (`id`, `dir` 0/1/2, `edge`?,
/// `depth`, Phase B `via`?/`weight`?).
pub fn traverse_filter(
    start_col: &str,
    start_id: &str,
    direction: crate::index::traverse::TraverseDir,
    edge_field: Option<&str>,
    max_depth: usize,
) -> crate::query::filter::Filter {
    traverse_marker(start_col, start_id, direction, edge_field, max_depth, None, None)
}

/// Weighted variant: `via_edge` names the edge collection, `weight`
/// names its numeric prop (Dijkstra ordering).
pub fn traverse_filter_weighted(
    start_col: &str,
    start_id: &str,
    direction: crate::index::traverse::TraverseDir,
    edge_field: Option<&str>,
    max_depth: usize,
    via_edge: &str,
    weight_field: Option<&str>,
) -> crate::query::filter::Filter {
    traverse_marker(
        start_col,
        start_id,
        direction,
        edge_field,
        max_depth,
        Some(via_edge),
        weight_field,
    )
}

fn traverse_marker(
    start_col: &str,
    start_id: &str,
    direction: crate::index::traverse::TraverseDir,
    edge_field: Option<&str>,
    max_depth: usize,
    via_edge: Option<&str>,
    weight_field: Option<&str>,
) -> crate::query::filter::Filter {
    let dir_n = match direction {
        crate::index::traverse::TraverseDir::Outgoing => 0,
        crate::index::traverse::TraverseDir::Incoming => 1,
        crate::index::traverse::TraverseDir::Both => 2,
    };
    let mut pairs: Vec<(std::sync::Arc<str>, Value)> = vec![
        (std::sync::Arc::from("id"), Value::String(start_id.into())),
        (std::sync::Arc::from("dir"), Value::Int(dir_n)),
        (
            std::sync::Arc::from("depth"),
            Value::Int(max_depth as i64),
        ),
    ];
    if let Some(f) = edge_field {
        pairs.push((std::sync::Arc::from("edge"), Value::String(f.into())));
    }
    if let Some(v) = via_edge {
        pairs.push((std::sync::Arc::from("via"), Value::String(v.into())));
    }
    if let Some(w) = weight_field {
        pairs.push((std::sync::Arc::from("weight"), Value::String(w.into())));
    }
    crate::query::filter::Filter {
        field: start_col.to_string(),
        op: Operator::Traverse,
        value: Value::Map(pairs),
    }
}

/// The stateful builder that carries the query and the database reference
pub struct QueryBuilder<'a> {
    db: &'a Hako,
    query: Query,
}

impl<'a> QueryBuilder<'a> {
    pub fn new(db: &'a Hako, collection: String) -> Self {
        Self {
            db,
            query: Query::new(&collection),
        }
    }

    // --- Fluent Filter Methods ---

    pub fn where_filter(mut self, field: &str, op: Operator, value: impl Into<Value>) -> Self {
        self.query = self.query.where_filter(field, op, value.into());
        self
    }

    pub fn where_eq(self, field: &str, value: impl Into<Value>) -> Self {
        self.where_filter(field, Operator::Eq, value)
    }

    pub fn where_gt(self, field: &str, value: impl Into<Value>) -> Self {
        self.where_filter(field, Operator::Gt, value)
    }

    /// KNN filter: `query` f32s ride the filter as LE Binary (same carrier
    /// as stored embeddings); k rides `.limit(k)`.
    pub fn where_near(self, field: &str, query: &[f32]) -> Self {
        self.where_filter(
            field,
            Operator::Near,
            Value::Binary(crate::index::vector::encode_f32s(query)),
        )
    }

    /// Graph-traversal marker (issue #53): BFS from (`start_col`,
    /// `start_id`) in `direction`, following `edge_field` (None = any ref
    /// field), up to `max_depth` (1..=32, hard error outside). Hits are
    /// filtered to the queried collection; cross-collection paths ride
    /// `find_paths`. Result cap rides `.limit(n)` (default 100 when
    /// unset). Incoming needs `create_traverse_index` on the anchor
    /// collection (fail-closed empty otherwise).
    pub fn where_traverse(
        self,
        start_col: &str,
        start_id: &str,
        direction: crate::index::traverse::TraverseDir,
        edge_field: Option<&str>,
        max_depth: usize,
    ) -> Self {
        let f = traverse_marker(start_col, start_id, direction, edge_field, max_depth, None, None);
        self.where_filter(&f.field.clone(), f.op, f.value)
    }

    /// Weighted variant (Phase B): `via_edge` names the edge collection,
    /// `weight` its numeric prop — Dijkstra ordering instead of BFS.
    #[allow(clippy::too_many_arguments)]
    pub fn where_traverse_weighted(
        self,
        start_col: &str,
        start_id: &str,
        direction: crate::index::traverse::TraverseDir,
        edge_field: Option<&str>,
        max_depth: usize,
        via_edge: &str,
        weight_field: Option<&str>,
    ) -> Self {
        let f = traverse_marker(
            start_col,
            start_id,
            direction,
            edge_field,
            max_depth,
            Some(via_edge),
            weight_field,
        );
        self.where_filter(&f.field.clone(), f.op, f.value)
    }

    pub fn order_by(mut self, field: &str, ascending: bool) -> Self {
        self.query = self.query.order_by(field, ascending);
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.query = self.query.limit(n);
        self
    }

    pub fn offset(mut self, n: usize) -> Self {
        self.query = self.query.offset(n);
        self
    }

    pub fn select(mut self, fields: &[&str]) -> Self {
        for f in fields { self.query = self.query.select(f); }
        self
    }

    // --- Terminal Execution Methods (Querying) ---

    /// Execute the query and return documents
    pub fn get(self) -> Result<Vec<(String, HakoDoc)>> {
        self.db.query(self.query)
    }

    /// Execute and return only the first document
    pub fn first(mut self) -> Result<Option<(String, HakoDoc)>> {
        self.query.limit = Some(1);
        let mut results = self.db.query(self.query)?;
        Ok(results.pop())
    }

    // --- Terminal Execution Methods (Aggregates) ---

    pub fn count(mut self) -> Result<f64> {
        self.query.aggregations = vec![AggregateOp::Count];
        let res = self.db.execute_aggregation(self.query)?;
        Ok(*res.get("count").unwrap_or(&0.0))
    }

    pub fn sum(mut self, field: &str) -> Result<f64> {
        self.query.aggregations = vec![AggregateOp::Sum(field.to_string())];
        let res = self.db.execute_aggregation(self.query)?;
        Ok(*res.get(&format!("sum_{}", field)).unwrap_or(&0.0))
    }

    pub fn avg(mut self, field: &str) -> Result<f64> {
        self.query.aggregations = vec![AggregateOp::Avg(field.to_string())];
        let res = self.db.execute_aggregation(self.query)?;
        Ok(*res.get(&format!("avg_{}", field)).unwrap_or(&0.0))
    }

    // --- Terminal Execution Methods (Chained Mutations) ---

    /// Delete all documents matching this query
    pub fn delete(self) -> Result<usize> {
        let results = self.db.query(self.query.clone())?;
        let count = results.len();
        if count == 0 { return Ok(0); }

        let mutations = results.into_iter()
            .map(|(id, _)| crate::engine::BatchMutation::Delete {
                collection: self.query.collection.clone(),
                doc_id: id,
            })
            .collect();

        self.db.write_batch(mutations)?;
        Ok(count)
    }

    /// Update all documents matching this query with the provided fields
    pub fn patch(self, updates: Vec<(String, Value)>) -> Result<usize> {
        let results = self.db.query(self.query.clone())?;
        let count = results.len();
        if count == 0 { return Ok(0); }

        let mutations = results.into_iter()
            .map(|(id, _)| crate::engine::BatchMutation::Patch {
                collection: self.query.collection.clone(),
                doc_id: id,
                updates: updates.clone(),
            })
            .collect();

        self.db.write_batch(mutations)?;
        Ok(count)
    }
}