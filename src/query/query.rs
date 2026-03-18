use crate::document::value::Value; // Fixed typo 'cuse'

use super::filter::{Filter, Operator};
use super::order::OrderBy;

#[derive(Debug, Clone)]
pub struct Query {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
    pub projection: Vec<String>,
    pub aggregations: Vec<AggregateOp>, // Added this field
}

#[derive(Debug, Clone)]
pub enum AggregateOp {
    Count,
    Sum(String), 
    Avg(String),
}

impl Query {
    pub fn new(collection: &str) -> Self {
        Self {
            collection: collection.to_string(),
            filters: Vec::new(),
            order_by: None,
            limit: None,
            projection: Vec::new(),
            aggregations: Vec::new(), // Initialize
        }
    }

    pub fn where_filter(mut self, field: &str, op: Operator, value: Value) -> Self {
        self.filters.push(Filter {
            field: field.to_string(),
            op,
            value,
        });
        self
    }

    pub fn where_eq(self, field: &str, value: Value) -> Self {
        self.where_filter(field, Operator::Eq, value)
    }

    pub fn order_by(mut self, field: &str, ascending: bool) -> Self {
        self.order_by = Some(OrderBy {
            field: field.to_string(),
            ascending,
        });
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn select_fields(mut self, fields: Vec<String>) -> Self {
        self.projection = fields;
        self
    }

    pub fn select(mut self, field: &str) -> Self {
        self.projection.push(field.to_string());
        self
    }

    pub fn composite_fields(&self) -> Vec<String> {
        self.filters.iter().map(|f| f.field.clone()).collect()
    }

    pub fn aggregate(mut self, op: AggregateOp) -> Self {
        self.aggregations.push(op);
        self
    }
}