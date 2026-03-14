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
pub struct OrderBy {
    pub field: String,
    pub ascending: bool,
}

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
            filters: vec![],
            order_by: None,
            limit: None,
        }
    }

    pub fn where_filter(
        mut self,
        field: &str,
        op: Operator,
        value: Value
    ) -> Self {

        self.filters.push(
            Filter {
                field: field.to_string(),
                op,
                value
            }
        );

        self
    }

    pub fn order_by(
        mut self,
        field: &str,
        ascending: bool
    ) -> Self {

        self.order_by = Some(
            OrderBy {
                field: field.to_string(),
                ascending
            }
        );

        self
    }

    pub fn limit(mut self, n: usize) -> Self {

        self.limit = Some(n);

        self
    }
}
