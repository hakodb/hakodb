use crate::document::value::Value;

use super::filter::{Filter, Operator};
use super::order::OrderBy;

#[derive(Debug, Clone)]
pub struct Query {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub order_by: Option<OrderBy>,
    pub limit: Option<usize>,
}

impl Query {
    pub fn new(collection: &str) -> Self {
        Self {
            collection: collection.to_string(),
            filters: Vec::new(),
            order_by: None,
            limit: None,
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

    pub fn composite_fields(&self) -> Vec<String> {
        self.filters.iter().map(|f| f.field.clone()).collect()
    }
}
