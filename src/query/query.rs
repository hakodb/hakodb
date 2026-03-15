use crate::document::firelite_doc::Value;

#[derive(Debug, Clone)]
pub enum Operator {
    Eq,
    Gt,
    Gte,
    Lt,
    Lte,
}

#[derive(Debug, Clone)]
pub struct Filter {
    pub field: String,
    pub op: Operator,
    pub value: Value,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub limit: Option<usize>,
}

impl Query {
    pub fn new(collection: &str) -> Self {
        Self {
            collection: collection.into(),
            filters: Vec::new(),
            limit: None,
        }
    }

    pub fn where_filter(mut self, field: &str, op: Operator, value: Value) -> Self {
        self.filters.push(Filter {
            field: field.into(),
            op,
            value,
        });
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn index_fields(&self) -> Vec<String> {
        self.filters.iter().map(|f| f.field.clone()).collect()
    }
}
