use crate::index::manager::IndexManager;

use super::query::Query;

pub fn has_matching_composite_index(query: &Query, indexes: &IndexManager) -> bool {
    indexes.has_index(&query.collection, &query.composite_fields())
}
