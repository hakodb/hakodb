use std::cmp::Ordering;
use std::collections::HashSet; // Added for cleaner FTS logic
use crate::document::value::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Match,      // Full word matching
    Contains,   // Substring matching
    StartsWith, // Prefix matching
    In,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub enum FilterLogic {
    And,
    Or,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FilterGroup {
    pub logic: FilterLogic,
    pub filters: Vec<Filter>,
}

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct Filter {
    pub field: String,
    pub op: Operator,
    pub value: Value,
}

pub fn compare_values(a: &Value, op: &Operator, b: &Value) -> bool {
    // Handle the 'In' operator first because it breaks the standard (a, b) pairing logic
    // (a is the field value, b is the array of allowed values)
    if matches!(op, Operator::In) {
        return if let Value::Array(allowed_values) = b {
            allowed_values.contains(a)
        } else {
            false
        };
    }
    match (a, b) {
        // String-specific logic for FTS and standard comparisons
        (Value::String(d), Value::String(f)) => match op {
            Operator::StartsWith => d.starts_with(f),
            Operator::Contains => d.contains(f),
            Operator::Match => {
                // We create owned lowercase strings to ensure references created 
                // by split_whitespace() remain valid while checking the HashSet.
                let d_lower = d.to_lowercase();
                let f_lower = f.to_lowercase();
                let doc_words: HashSet<&str> = d_lower.split_whitespace().collect();
                
                // Return true if every word in the filter exists in the document
                f_lower.split_whitespace().all(|w| doc_words.contains(w))
            }
            // Fallback for standard string sorting (Eq, Gt, etc.)
            _ => eval_ordering(d.cmp(f), op),
        },

        // Standard Order-based comparisons
        (Value::Int(a), Value::Int(b)) => eval_ordering(a.cmp(b), op),
        (Value::Timestamp(a), Value::Timestamp(b)) => eval_ordering(a.cmp(b), op),
        (Value::Binary(a), Value::Binary(b)) => eval_ordering(a.cmp(b), op),
        (Value::Bool(a), Value::Bool(b)) => eval_ordering(a.cmp(b), op),
        
        (Value::Float(a), Value::Float(b)) => {
            if let Some(ord) = a.partial_cmp(b) {
                eval_ordering(ord, op)
            } else {
                false
            }
        },

        (Value::Null, Value::Null) => eval_ordering(Ordering::Equal, op),
        (Value::Int(a_val), Value::Float(b_val)) => eval_ordering((*a_val as f64).total_cmp(b_val), op),
        (Value::Float(a_val), Value::Int(b_val)) => eval_ordering(a_val.total_cmp(&(*b_val as f64)), op),
        
        // Reference comparison
        (Value::Reference { collection: c1, doc_id: i1 }, Value::Reference { collection: c2, doc_id: i2 }) => {
            eval_ordering(c1.cmp(c2).then(i1.cmp(i2)), op)
        }

        // Cross-type comparisons or comparisons involving ServerTimestamp placeholders
        // In Firestore-style engines, comparing different types usually returns false.
        _ => false,
    }
}

/// Internal helper to map a Sort Ordering to an Operator boolean
fn eval_ordering(ord: Ordering, op: &Operator) -> bool {
    match op {
        Operator::Eq => ord == Ordering::Equal,
        Operator::Ne => ord != Ordering::Equal,
        Operator::Gt => ord == Ordering::Greater,
        Operator::Gte => ord == Ordering::Greater || ord == Ordering::Equal,
        Operator::Lt => ord == Ordering::Less,
        Operator::Lte => ord == Ordering::Less || ord == Ordering::Equal,
        // String-only operators return false if used on non-string types
        Operator::Match | Operator::Contains | Operator::StartsWith | Operator::In => false,
    }
}