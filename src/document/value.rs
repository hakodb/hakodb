use std::cmp::Ordering;

#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(untagged)]
pub enum Value {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    String(String),
    Binary(Vec<u8>),
    Timestamp(i64), 
    ServerTimestamp,
    Map(Vec<(String, Value)>),
}

impl Value {
    /// Returns a weight for the type to allow cross-type sorting.
    /// This ensures that Nulls always come first, followed by Bools, Numbers, etc.
    fn type_weight(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) => 2,   // Numbers (Int and Float) share the same weight
            Value::Float(_) => 2, // to allow inter-type numeric comparison.
            Value::Timestamp(_) => 3,
            Value::String(_) => 4,
            Value::Binary(_) => 5,
            Value::Map(_) => 6,
            Value::ServerTimestamp => 7,
        }
    }
}

// Manual implementation of PartialEq to handle numeric cross-comparison (Int == Float)
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        self.cmp(other) == Ordering::Equal
    }
}

impl Eq for Value {}

impl PartialOrd for Value {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for Value {
    fn cmp(&self, other: &Self) -> Ordering {
        let w1 = self.type_weight();
        let w2 = other.type_weight();

        if w1 != w2 {
            return w1.cmp(&w2);
        }

        // Weights are the same, compare internal data
        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            
            // Numeric Unification: Compare Ints and Floats together
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.total_cmp(&(*b as f64)),

            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
            
            (Value::Map(a), Value::Map(b)) => {
                // Compare by length first, then lexicographically by fields
                let res = a.len().cmp(&b.len());
                if res != Ordering::Equal {
                    return res;
                }
                a.cmp(b)
            }
            
            (Value::ServerTimestamp, Value::ServerTimestamp) => Ordering::Equal,
            
            // Fallback for theoretically unreachable cases due to weight check
            _ => Ordering::Equal,
        }
    }
}