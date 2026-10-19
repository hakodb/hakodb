//! Optional per-collection schema validation (hakodb#71 Release D — F8).
//!
//! Schemaless stays the default: a collection with no schema accepts any
//! document, and even with a schema, undeclared fields are allowed — the
//! schema only constrains the fields it declares. Enforced in the same
//! synchronous admission gate as F1 unique constraints (`write_batch_internal`,
//! before WAL append), so batch atomicity holds: one invalid member rejects
//! the whole batch.
//!
//! Deliberate first slice: declared paths are top-level field names only.
//! Dotted paths are rejected at `set_schema` time (fail-closed) instead of
//! silently validating the wrong shape; nested rules ride the same
//! `HakoDoc::get_path` machinery later. No implicit type coercion: a
//! mismatched type is a `SchemaViolation`, never a silent cast. Defaults
//! fill *missing* fields only — they never overwrite a present value.

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::error::{HakoError, Result};

/// Declared type of a schema field.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum FieldKind {
    String,
    Int,
    Float,
    Bool,
    Null,
    Array,
    Map,
    Bytes,
    Timestamp,
    /// Any value, including Null. Useful for "required but untyped".
    Any,
}

impl FieldKind {
    pub fn name(&self) -> &'static str {
        match self {
            FieldKind::String => "string",
            FieldKind::Int => "int",
            FieldKind::Float => "float",
            FieldKind::Bool => "bool",
            FieldKind::Null => "null",
            FieldKind::Array => "array",
            FieldKind::Map => "map",
            FieldKind::Bytes => "bytes",
            FieldKind::Timestamp => "timestamp",
            FieldKind::Any => "any",
        }
    }

    pub fn matches(&self, v: &Value) -> bool {
        match self {
            FieldKind::Any => true,
            FieldKind::String => matches!(v, Value::String(_)),
            FieldKind::Int => matches!(v, Value::Int(_)),
            FieldKind::Float => matches!(v, Value::Float(_)),
            FieldKind::Bool => matches!(v, Value::Bool(_)),
            FieldKind::Null => matches!(v, Value::Null | Value::ServerTimestamp),
            FieldKind::Array => matches!(v, Value::Array(_)),
            FieldKind::Map => matches!(v, Value::Map(_)),
            FieldKind::Bytes => matches!(v, Value::Binary(_)),
            FieldKind::Timestamp => matches!(v, Value::Timestamp(_)),
        }
    }
}

/// One declared field rule.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct FieldRule {
    pub name: String,
    pub kind: FieldKind,
    pub required: bool,
    #[serde(default)]
    pub default: Option<Value>,
}

impl FieldRule {
    pub fn new(name: impl Into<String>, kind: FieldKind) -> Self {
        Self { name: name.into(), kind, required: false, default: None }
    }

    pub fn required(mut self) -> Self {
        self.required = true;
        self
    }

    pub fn with_default(mut self, value: Value) -> Self {
        self.default = Some(value);
        self
    }
}

/// Declaration for one collection. Empty `fields` = no constraints.
#[derive(Debug, Clone, Default, serde::Serialize, serde::Deserialize)]
pub struct CollectionSchema {
    pub fields: Vec<FieldRule>,
}

impl CollectionSchema {
    pub fn new(fields: Vec<FieldRule>) -> Self {
        Self { fields }
    }

    /// Validate the declaration itself (duplicates, dotted names, defaults
    /// that contradict their declared type). Runs before anything is stored.
    pub fn validate(&self) -> Result<()> {
        let mut seen = std::collections::HashSet::new();
        for rule in &self.fields {
            if rule.name.is_empty() {
                return Err(HakoError::InvalidInput("schema: empty field name".into()));
            }
            if rule.name.contains('.') {
                return Err(HakoError::InvalidInput(format!(
                    "schema: nested path `{}` is not supported yet (top-level rules only)",
                    rule.name
                )));
            }
            if !seen.insert(rule.name.clone()) {
                return Err(HakoError::InvalidInput(format!(
                    "schema: duplicate rule for `{}`",
                    rule.name
                )));
            }
            if let Some(v) = &rule.default {
                if !rule.kind.matches(v) {
                    return Err(HakoError::InvalidInput(format!(
                        "schema: default for `{}` is not a {}",
                        rule.name,
                        rule.kind.name()
                    )));
                }
            }
        }
        Ok(())
    }

    /// Apply defaults for missing fields and validate present ones.
    /// Mutates `doc` for defaults, so the persisted/stored document is the
    /// filled-in one (defaults are not a read-time illusion).
    pub fn apply(&self, collection: &str, doc: &mut HakoDoc) -> Result<()> {
        for rule in &self.fields {
            match doc.get(&rule.name) {
                Some(v) => {
                    if !rule.kind.matches(v) {
                        return Err(HakoError::SchemaViolation {
                            collection: collection.to_string(),
                            field: rule.name.clone(),
                            expected: rule.kind.name().to_string(),
                        });
                    }
                }
                None => {
                    if let Some(default) = &rule.default {
                        doc.insert(rule.name.clone(), default.clone());
                    } else if rule.required {
                        return Err(HakoError::SchemaViolation {
                            collection: collection.to_string(),
                            field: rule.name.clone(),
                            expected: format!("present {}", rule.kind.name()),
                        });
                    }
                }
            }
        }
        Ok(())
    }
}
