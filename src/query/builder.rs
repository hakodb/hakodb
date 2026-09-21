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