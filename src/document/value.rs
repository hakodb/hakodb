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
    Reference { collection: String, doc_id: String },
    Timestamp(i64), 
    ServerTimestamp,
    Map(Vec<(String, Value)>),
    Array(Vec<Value>),
}

impl Value {
    /// Internal weight to allow sorting different types against each other.
    fn type_weight(&self) -> u8 {
        match self {
            Value::Null => 0,
            Value::Bool(_) => 1,
            Value::Int(_) | Value::Float(_) => 2, // Numbers sort together
            Value::Timestamp(_) => 3,
            Value::String(_) => 4,
            Value::Binary(_) => 5,
            Value::Reference { .. } => 6,
            Value::Array(_) => 7,
            Value::Map(_) => 8,
            Value::ServerTimestamp => 9,
        }
    }
}

// Ensure PartialEq matches the logic in Ord
impl PartialEq for Value {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (Value::Null, Value::Null) => true,
            (Value::Bool(a), Value::Bool(b)) => a == b,
            (Value::Int(a), Value::Int(b)) => a == b,
            (Value::Float(a), Value::Float(b)) => a.to_bits() == b.to_bits(),
            (Value::Int(a), Value::Float(b)) => (*a as f64) == *b,
            (Value::Float(a), Value::Int(b)) => *a == (*b as f64),
            (Value::String(a), Value::String(b)) => a == b,
            (Value::Binary(a), Value::Binary(b)) => a == b,
            (Value::Timestamp(a), Value::Timestamp(b)) => a == b,
            (Value::Map(a), Value::Map(b)) => a == b,
            (Value::ServerTimestamp, Value::ServerTimestamp) => true,
            _ => false,
        }
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

        match (self, other) {
            (Value::Null, Value::Null) => Ordering::Equal,
            (Value::Bool(a), Value::Bool(b)) => a.cmp(b),
            
            // Numeric comparison (cross-type support)
            (Value::Int(a), Value::Int(b)) => a.cmp(b),
            (Value::Float(a), Value::Float(b)) => a.total_cmp(b),
            (Value::Int(a), Value::Float(b)) => (*a as f64).total_cmp(b),
            (Value::Float(a), Value::Int(b)) => a.total_cmp(&(*b as f64)),

            (Value::String(a), Value::String(b)) => a.cmp(b),
            (Value::Binary(a), Value::Binary(b)) => a.cmp(b),
            (Value::Timestamp(a), Value::Timestamp(b)) => a.cmp(b),
            
            (Value::Reference { collection: c1, doc_id: i1 }, Value::Reference { collection: c2, doc_id: i2 }) => {
                c1.cmp(c2).then(i1.cmp(i2))
            }
            
            (Value::Map(a), Value::Map(b)) => {
                // Compare Map lengths, then lexicographically by fields
                let len_cmp = a.len().cmp(&b.len());
                if len_cmp != Ordering::Equal {
                    return len_cmp;
                }
                // Recursively compare key-value pairs
                for ((k1, v1), (k2, v2)) in a.iter().zip(b.iter()) {
                    let k_cmp = k1.cmp(k2);
                    if k_cmp != Ordering::Equal { return k_cmp; }
                    let v_cmp = v1.cmp(v2);
                    if v_cmp != Ordering::Equal { return v_cmp; }
                }
                Ordering::Equal
            }

            (Value::Array(a), Value::Array(b)) => {
                let res = a.len().cmp(&b.len());
                if res != Ordering::Equal { return res; }
                a.cmp(b) // Lexicographical comparison of elements
            }
            
            (Value::ServerTimestamp, Value::ServerTimestamp) => Ordering::Equal,
            _ => Ordering::Equal,
        }
    }
}