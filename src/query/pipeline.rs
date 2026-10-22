//! hakodb#71 Release C — F4 declarative aggregation pipeline (no SQL, no
//! JOINs).
//!
//! Stages run in order over decoded rows: `match`/`project`/`sort`/
//! `limit`/`offset` first, then `unwind`/`group` with `Accum::{sum,
//! count,min,max,avg}`. `match` reuses the full query machinery (planner,
//! dotted paths F2, elemMatch F3); later stages are streaming passes over
//! the materialised rows.
//!
//! Deliberate first-slice limits (fail-closed with `InvalidInput`, not
//! silent wrong answers): `unwind` takes a top-level field name only —
//! nested unwind rides the F2 rule later. `project` accepts dotted paths
//! (F6) and prunes subtrees with the same leaf-preserving semantics as
//! query projection. `group` keys and accum fields do accept dotted paths
//! (F2). `unwind` skips docs whose field is missing or not an array;
//! scalar elements are kept as the field value, so a scalar element cannot
//! be traversed further (F2 Map-only rule).

use crate::document::hako_doc::HakoDoc;
use crate::document::value::Value;
use crate::error::HakoError;
use crate::query::query::Query;
use std::collections::BTreeMap;

/// One accumulator in a `group` stage.
#[derive(Debug, Clone, PartialEq)]
pub enum Accum {
    Count,
    Sum(String),
    Min(String),
    Max(String),
    Avg(String),
}

/// Output name + accumulator.
#[derive(Debug, Clone, PartialEq)]
pub struct AccumSpec {
    pub out: String,
    pub op: Accum,
}

/// Pipeline stage. `Match` carries a full `Query` (its own limit/offset
/// apply as written; prefer the pipeline `Limit`/`Offset` stages).
#[allow(clippy::large_enum_variant)]
#[derive(Debug, Clone)]
pub enum Stage {
    Match(Query),
    Project(Vec<String>),
    Sort { field: String, ascending: bool },
    Limit(usize),
    Offset(usize),
    Unwind { field: String },
    Group { key: Option<String>, accums: Vec<AccumSpec> },
}

/// A pipeline over one collection. Non-`Match` stages seed from a full
/// scan of `collection` when they run before any `Match`.
#[derive(Debug, Clone)]
pub struct Pipeline {
    pub collection: String,
    pub stages: Vec<Stage>,
}

impl Pipeline {
    pub fn new(collection: &str) -> Self {
        Self {
            collection: collection.to_string(),
            stages: Vec::new(),
        }
    }

    pub fn stage(mut self, stage: Stage) -> Self {
        self.stages.push(stage);
        self
    }

    pub fn match_query(self, query: Query) -> Self {
        self.stage(Stage::Match(query))
    }

    pub fn project(self, fields: Vec<&str>) -> Self {
        self.stage(Stage::Project(fields.into_iter().map(|s| s.to_string()).collect()))
    }

    pub fn sort(self, field: &str, ascending: bool) -> Self {
        self.stage(Stage::Sort { field: field.to_string(), ascending })
    }

    pub fn limit(self, n: usize) -> Self {
        self.stage(Stage::Limit(n))
    }

    pub fn offset(self, n: usize) -> Self {
        self.stage(Stage::Offset(n))
    }

    pub fn unwind(self, field: &str) -> Self {
        self.stage(Stage::Unwind { field: field.to_string() })
    }

    pub fn group(self, key: Option<&str>, accums: Vec<(&str, Accum)>) -> Self {
        self.stage(Stage::Group {
            key: key.map(|k| k.to_string()),
            accums: accums
                .into_iter()
                .map(|(out, op)| AccumSpec { out: out.to_string(), op })
                .collect(),
        })
    }
}

/// Accumulator running state.
#[derive(Debug, Default, Clone)]
struct AccState {
    rows: u64,
    numeric: u64,
    sum_f: f64,
    sum_i: i64,
    all_int: bool,
    min: Option<Value>,
    max: Option<Value>,
}

impl AccState {
    fn new() -> Self {
        Self { all_int: true, ..Default::default() }
    }

    fn push(&mut self, op: &Accum, doc: &HakoDoc) {
        self.rows += 1;
        let field = match op {
            Accum::Count => return,
            Accum::Sum(f) | Accum::Min(f) | Accum::Max(f) | Accum::Avg(f) => f,
        };
        let Some(v) = doc.get_path(field) else { return };
        match op {
            Accum::Count => {}
            Accum::Sum(_) | Accum::Avg(_) => match v {
                Value::Int(i) => {
                    self.numeric += 1;
                    self.sum_i = self.sum_i.saturating_add(*i);
                    self.sum_f += *i as f64;
                }
                Value::Float(f) => {
                    self.numeric += 1;
                    self.all_int = false;
                    self.sum_f += f;
                }
                _ => {}
            },
            Accum::Min(_) => {
                if self.min.as_ref().is_none_or(|m| v < m) {
                    self.min = Some(v.clone());
                }
            }
            Accum::Max(_) => {
                if self.max.as_ref().is_none_or(|m| v > m) {
                    self.max = Some(v.clone());
                }
            }
        }
    }

    fn value(&self, op: &Accum) -> Value {
        match op {
            Accum::Count => Value::Int(self.rows as i64),
            Accum::Sum(_) => {
                if self.numeric == 0 {
                    Value::Int(0)
                } else if self.all_int {
                    Value::Int(self.sum_i)
                } else {
                    Value::Float(self.sum_f)
                }
            }
            Accum::Avg(_) => {
                if self.numeric == 0 {
                    Value::Null
                } else {
                    Value::Float(self.sum_f / self.numeric as f64)
                }
            }
            Accum::Min(_) => self.min.clone().unwrap_or(Value::Null),
            Accum::Max(_) => self.max.clone().unwrap_or(Value::Null),
        }
    }
}

/// Validate the first-slice limits up front (fail-closed, no silent
/// top-level-only behavior for dotted projection/unwind).
pub fn validate(p: &Pipeline) -> Result<(), HakoError> {
    for stage in &p.stages {
        match stage {
            Stage::Project(fields) => {
                for f in fields {
                    if f.is_empty() || f.split('.').any(|s| s.is_empty()) {
                        return Err(HakoError::InvalidInput(format!(
                            "pipeline project: invalid field path ({f})"
                        )));
                    }
                }
            }
            Stage::Unwind { field } => {
                if field.contains('.') {
                    return Err(HakoError::InvalidInput(format!(
                        "pipeline unwind: nested path ({field}) is not supported yet (F6)"
                    )));
                }
            }
            Stage::Group { key: Some(k), .. } if k.split('.').any(|s| s.is_empty()) => {
                return Err(HakoError::InvalidInput(format!(
                    "pipeline group: invalid key path ({k})"
                )));
            }
            _ => {}
        }
    }
    Ok(())
}

/// Run the remaining stages over materialised rows. `Match` is handled by
/// the engine (it needs the query executor); this helper gets rows from
/// the last match/full scan.
pub fn run_stages(
    mut rows: Vec<(String, HakoDoc)>,
    stages: &[Stage],
) -> Result<Vec<(String, HakoDoc)>, HakoError> {
    for stage in stages {
        match stage {
            Stage::Match(_) => {
                return Err(HakoError::InvalidInput(
                    "pipeline: Match must run through the engine (use Pipeline::match_query)".into(),
                ));
            }
            Stage::Project(fields) => {
                rows = rows
                    .into_iter()
                    .map(|(id, doc)| {
                        // F6: dotted paths prune subtrees, same semantics as
                        // query projection.
                        let next = HakoDoc {
                            _time: doc._time,
                            fields: doc.project_fields(fields),
                        };
                        (id, next)
                    })
                    .collect();
            }
            Stage::Sort { field, ascending } => {
                let asc = *ascending;
                rows.sort_by(|a, b| {
                    let ka = sort_key(a, field);
                    let kb = sort_key(b, field);
                    let ord = ka.cmp(&kb);
                    if asc { ord } else { ord.reverse() }
                });
            }
            Stage::Limit(n) => rows.truncate(*n),
            Stage::Offset(n) => {
                rows = rows.into_iter().skip(*n).collect();
            }
            Stage::Unwind { field } => {
                let mut out = Vec::with_capacity(rows.len());
                for (id, doc) in rows {
                    let items = match doc.get(field) {
                        Some(Value::Array(items)) => items.clone(),
                        _ => continue, // missing / non-array: skip the doc
                    };
                    for item in items {
                        let mut next = doc.clone();
                        next.insert(field.as_str(), item);
                        out.push((id.clone(), next));
                    }
                }
                rows = out;
            }
            Stage::Group { key, accums } => {
                let mut groups: BTreeMap<Option<Value>, Vec<AccState>> = BTreeMap::new();
                for (_, doc) in &rows {
                    let k = key.as_ref().and_then(|path| doc.get_path(path).cloned());
                    let states = groups.entry(k).or_insert_with(|| {
                        std::iter::repeat_with(AccState::new).take(accums.len()).collect()
                    });
                    for (state, spec) in states.iter_mut().zip(accums.iter()) {
                        state.push(&spec.op, doc);
                    }
                }
                let key_field = key.as_deref().map(|k| k.rsplit('.').next().unwrap_or(k));
                rows = groups
                    .into_iter()
                    .map(|(k, states)| {
                        let mut doc = HakoDoc::default();
                        let group_value = k.clone().unwrap_or_else(|| Value::String("$all".into()));
                        if let Some(name) = key_field {
                            doc.insert(name, k.unwrap_or(Value::Null));
                        }
                        for (state, spec) in states.iter().zip(accums.iter()) {
                            doc.insert(spec.out.as_str(), state.value(&spec.op));
                        }
                        (group_id(&group_value), doc)
                    })
                    .collect();
            }
        }
    }
    Ok(rows)
}

fn sort_key(row: &(String, HakoDoc), field: &str) -> Option<Value> {
    match field {
        "id" => Some(Value::String(row.0.clone())),
        "_time" => Some(Value::Int(row.1._time)),
        _ => row.1.get_path(field).cloned(),
    }
}

fn group_id(v: &Value) -> String {
    match v {
        Value::String(s) => s.clone(),
        other => format!("{other:?}"),
    }
}
