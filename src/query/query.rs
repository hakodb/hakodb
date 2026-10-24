use crate::document::value::Value; // Fixed typo 'cuse'

use super::filter::{Filter, Operator};
use super::order::OrderBy;

/// hakodb#71 F3 — one `elemMatch` clause: the stored field must be an
/// array containing an element where ALL the sub-filters hold. Sub-filter
/// fields are element-relative paths (`"sku"` or `"meta.notes"`; empty
/// `""` means the element value itself). Stored element must be a Map
/// whenever a sub-path is non-empty (fail-closed); absent/non-arrays
/// never match; empty sub-filter lists are refused at plan time (the
/// clause would be vacuously true and match every doc).
#[derive(Debug, Clone, PartialEq)]
pub struct ElemMatch {
    pub field: String,
    pub filters: Vec<Filter>,
}

#[derive(Debug, Clone)]
pub struct Query {
    pub collection: String,
    pub filters: Vec<Filter>,
    pub or_groups: Vec<Vec<crate::query::filter::Filter>>,
    /// hakodb#71 F3 — elemMatch clauses, always And-combined with the
    /// rest of the filters. Verified at decode time by the matchers
    /// (scan fallback, no index acceleration initially).
    pub elem_match: Vec<ElemMatch>,
    pub order_by: Vec<OrderBy>,
    pub limit: Option<usize>,
    pub offset: Option<usize>,
    pub projection: Vec<String>,
    pub aggregations: Vec<AggregateOp>, // Added this field

    pub start_at: Option<Vec<Value>>,
    pub start_after: Option<Vec<Value>>,
    pub end_at: Option<Vec<Value>>,
    pub end_before: Option<Vec<Value>>,
    /// ponytail: when true, blob-backed fields come back as `Value::BlobLink`
    /// (offset/len placeholders) instead of being inflated from the blob
    /// file. List views over docs with images skip MBs of reads per query;
    /// resolve on demand via `resolve_doc` / `hk_doc_resolve_blobs`.
    /// Default false — current eager behavior, zero risk to existing apps.
    pub defer_blobs: bool,
    /// ponytail: raw mode — `db.query_raw` returns storage-encoded bytes
    /// instead of decoded docs (opaque, version-scoped: decode with
    /// `HakoDoc::decode`, do not persist). Skips decode, filter
    /// re-verify and rayon dispatch; requires index-satisfied filters and
    /// ordering, else `query_raw` errors. Default false.
    pub raw: bool,
}

#[derive(Debug, Clone)]
pub enum AggregateOp {
    Count,
    Sum(String), 
    Avg(String),
    /// Smallest numeric value of `field`; absent key when no doc has one.
    Min(String),
    /// Largest numeric value of `field`; absent key when no doc has one.
    Max(String),
}

impl Query {
    pub fn new(collection: &str) -> Self {
        Self {
            collection: collection.to_string(),
            filters: Vec::new(),
            or_groups: Vec::new(),
            elem_match: Vec::new(),
            order_by: Vec::new(),
            limit: None,
            offset: None, 
            projection: Vec::new(),
            aggregations: Vec::new(), // Initialize
            
            start_at: None,
            start_after: None,
            end_at: None,
            end_before: None,
            defer_blobs: false,
            raw: false,
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
        self.order_by.push(OrderBy {
            field: field.to_string(),
            ascending,
        });
        self
    }

    pub fn limit(mut self, n: usize) -> Self {
        self.limit = Some(n);
        self
    }

    pub fn offset(mut self, offset: usize) -> Self { // <--- Fluent API
        self.offset = Some(offset);
        self
    }

    pub fn select_fields(mut self, fields: Vec<String>) -> Self {
        self.projection = fields;
        self
    }

    pub fn select(mut self, field: &str) -> Self {
        self.projection.push(field.to_string());
        self
    }

    pub fn composite_fields(&self) -> Vec<String> {
        self.filters.iter().map(|f| f.field.clone()).collect()
    }

    pub fn aggregate(mut self, op: AggregateOp) -> Self {
        self.aggregations.push(op);
        self
    }

    /// hakodb#71 F3 — element match: an element of the array at `field`
    /// satisfying ALL `filters` (element-relative paths). See `ElemMatch`.
    pub fn elem_match(mut self, field: &str, filters: Vec<Filter>) -> Self {
        self.elem_match.push(ElemMatch {
            field: field.to_string(),
            filters,
        });
        self
    }

    pub fn start_after(mut self, values: Vec<Value>) -> Self {
        self.start_after = Some(values);
        self
    }

    /// Return blob-backed fields as `Value::BlobLink` placeholders instead
    /// of inflating them. See field docs.
    pub fn defer_blobs(mut self, defer: bool) -> Self {
        self.defer_blobs = defer;
        self
    }

    /// Raw mode: return storage-encoded bytes via `db.query_raw` instead of
    /// decoded docs. See field docs for the contract.
    pub fn raw(mut self, raw: bool) -> Self {
        self.raw = raw;
        self
    }

    pub fn or_where(mut self, field: &str, op: Operator, value: Value) -> Self {
        // Simple logic: add to the last group or start a new one
        self.or_groups.push(vec![crate::query::filter::Filter {
            field: field.to_string(),
            op,
            value,
        }]);
        self
    }
}