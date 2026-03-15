use std::cmp::Ordering;

use crate::document::value::Value;

#[derive(Debug, Clone)]
pub enum Operator {
    Eq,
    Ne,
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

pub fn compare_values(a: &Value, op: &Operator, b: &Value) -> bool {
    match (a, b) {
        (Value::Null, Value::Null) => matches!(op, Operator::Eq | Operator::Gte | Operator::Lte),
        (Value::Bool(a), Value::Bool(b)) => eval_ordering(a.cmp(b), op),
        (Value::Int(a), Value::Int(b)) => eval_ordering(a.cmp(b), op),
        (Value::Float(a), Value::Float(b)) => a
            .partial_cmp(b)
            .map(|o| eval_ordering(o, op))
            .unwrap_or(false),
        (Value::String(a), Value::String(b)) => eval_ordering(a.cmp(b), op),
        (Value::Binary(a), Value::Binary(b)) => eval_ordering(a.cmp(b), op),
        _ => false,
    }
}

fn eval_ordering(ord: Ordering, op: &Operator) -> bool {
    match op {
        Operator::Eq => ord == Ordering::Equal,
        Operator::Ne => ord != Ordering::Equal,
        Operator::Gt => ord == Ordering::Greater,
        Operator::Gte => ord == Ordering::Greater || ord == Ordering::Equal,
        Operator::Lt => ord == Ordering::Less,
        Operator::Lte => ord == Ordering::Less || ord == Ordering::Equal,
    }
}
