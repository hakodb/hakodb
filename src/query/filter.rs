use crate::document::value::Value;
use std::cmp::Ordering;
use std::collections::HashSet; // Added for cleaner FTS logic

#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Operator {
    Eq,
    Ne,
    Gt,
    Gte,
    Lt,
    Lte,
    Match,      // Full word matching
    MatchPrefix, // Word-prefix matching (autocomplete; works on the FTS index)
    Contains,   // Substring matching
    StartsWith, // Prefix matching
    In,
    ArrayContains,
    ArrayContainsAny,
    NotIn,
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
    match op {
        Operator::In => {
            return if let Value::Array(allowed) = b {
                // allowed.contains(a)
                allowed.iter().any(|item| compare_values(a, &Operator::Eq, item))
            } else {
                false
            };
        }
        Operator::NotIn => {
            return if let Value::Array(allowed) = b {
                // !allowed.contains(a)
                !allowed.iter().any(|item| compare_values(a, &Operator::Eq, item))
            } else {
                true
            };
        }
        Operator::ArrayContains => {
            return if let Value::Array(items) = a {
                items.contains(b)
            } else {
                false
            };
        }
        Operator::ArrayContainsAny => {
            return if let (Value::Array(items), Value::Array(query_items)) = (a, b) {
                query_items.iter().any(|qi| items.contains(qi))
            } else {
                false
            };
        }
        _ => {} // Fall through to standard comparisons
    }


    // 2. Strict type equality (Fast Path)
    if matches!(op, Operator::Eq) && a == b { return true; }

    match (a, b) {
        // String-specific logic for FTS and standard comparisons
        // (Value::String(s), Value::Int(i)) => {
        //     if let Ok(parsed) = s.parse::<i64>() {
        //         return eval_ordering(parsed.cmp(i), op);
        //     }
        //     eval_ordering(a.cmp(b), op)
        // },
        // (Value::Int(i), Value::String(s)) => {
        //     if let Ok(parsed) = s.parse::<i64>() {
        //         return eval_ordering(i.cmp(&parsed), op);
        //     }
        //     eval_ordering(a.cmp(b), op)
        // },
        // (Value::String(s), Value::Float(f)) => {
        //     if let Ok(parsed) = s.parse::<f64>() {
        //         return eval_ordering(parsed.total_cmp(f), op);
        //     }
        //     eval_ordering(a.cmp(b), op)
        // },
        (Value::String(s), Value::Int(i)) | (Value::Int(i), Value::String(s)) => {
            if let Ok(parsed) = s.parse::<i64>() {
                return eval_ordering(parsed.cmp(i), op);
            }
            // If not a valid number string, return false for Eq, true for Ne
            if matches!(op, Operator::Eq) { return false; }
            if matches!(op, Operator::Ne) { return true; }
            eval_ordering(a.cmp(b), op)
        },
        (Value::String(s), Value::Float(f)) | (Value::Float(f), Value::String(s)) => {
            if let Ok(parsed) = s.parse::<f64>() {
                return eval_ordering(parsed.total_cmp(f), op);
            }
            if matches!(op, Operator::Eq) { return false; }
            if matches!(op, Operator::Ne) { return true; }
            eval_ordering(a.cmp(b), op)
        },
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
            Operator::MatchPrefix => {
                // Word-prefix matching: every query word must be a prefix of some
                // word in the document (mirrors the FTS-index prefix scan).
                let d_lower = d.to_lowercase();
                let f_lower = f.to_lowercase();
                let doc_words: HashSet<&str> = d_lower.split_whitespace().collect();

                f_lower
                    .split_whitespace()
                    .all(|w| doc_words.iter().any(|doc_word| doc_word.starts_with(w)))
            }
            // Fallback for standard string sorting (Eq, Gt, etc.)
            _ => eval_ordering(d.cmp(f), op),
        },

        // Standard Order-based comparisons
        (Value::Int(a), Value::Int(b)) => eval_ordering(a.cmp(b), op),
        (Value::Timestamp(a), Value::Timestamp(b)) => eval_ordering(a.cmp(b), op),
        (Value::Binary(a), Value::Binary(b)) => eval_ordering(a.cmp(b), op),
        (Value::Bool(a), Value::Bool(b)) => eval_ordering(a.cmp(b), op),
        // Compatibility bridge: some older gateway payloads encoded booleans as 0/1 integers.
        // Keep equality/ordering behavior stable across mixed bool/int datasets.
        (Value::Bool(a), Value::Int(b)) => eval_ordering((*a as i64).cmp(b), op),
        (Value::Int(a), Value::Bool(b)) => eval_ordering(a.cmp(&(*b as i64)), op),
        // Compatibility bridge: bool string literals ("true"/"false") from legacy payloads.
        (Value::Bool(a), Value::String(s)) => parse_bool_like(s)
            .map(|b| eval_ordering(a.cmp(&b), op))
            .unwrap_or(false),
        (Value::String(s), Value::Bool(b)) => parse_bool_like(s)
            .map(|a| eval_ordering(a.cmp(b), op))
            .unwrap_or(false),

        (Value::Float(a), Value::Float(b)) => {
            if let Some(ord) = a.partial_cmp(b) {
                eval_ordering(ord, op)
            } else {
                false
            }
        }

        (Value::Null, Value::Null) => eval_ordering(Ordering::Equal, op),
        (Value::Int(a_val), Value::Float(b_val)) => {
            eval_ordering((*a_val as f64).total_cmp(b_val), op)
        }
        (Value::Float(a_val), Value::Int(b_val)) => {
            eval_ordering(a_val.total_cmp(&(*b_val as f64)), op)
        }

        // Reference comparison
        (
            Value::Reference {
                collection: c1,
                doc_id: i1,
            },
            Value::Reference {
                collection: c2,
                doc_id: i2,
            },
        ) => eval_ordering(c1.cmp(c2).then(i1.cmp(i2)), op),

        // Cross-type comparisons or comparisons involving ServerTimestamp placeholders
        // In Firestore-style engines, comparing different types usually returns false.
        // _ => eval_ordering(a.cmp(b), op),
        _ => {
            if a.type_weight() != b.type_weight() {
                // Special case: Allow cross-comparison of Int and Float as "Numbers"
                let is_numeric = (matches!(a, Value::Int(_)) || matches!(a, Value::Float(_))) &&
                                 (matches!(b, Value::Int(_)) || matches!(b, Value::Float(_)));
                if !is_numeric { return eval_ordering(a.type_weight().cmp(&b.type_weight()), op); }
            }
            eval_ordering(a.cmp(b), op)
        }
    }
}

#[inline]
fn parse_bool_like(input: &str) -> Option<bool> {
    match input.trim().to_ascii_lowercase().as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
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
        Operator::Match
        | Operator::MatchPrefix
        | Operator::Contains
        | Operator::StartsWith
        | Operator::In
        | Operator::NotIn
        | Operator::ArrayContains
        | Operator::ArrayContainsAny => false,
    }
}
