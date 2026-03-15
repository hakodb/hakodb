use crate::index::composite::definition::CompositeIndexDefinition;

use super::query::Query;

pub fn matches_index(query: &Query, index: &CompositeIndexDefinition) -> bool {
    if query.filters.len() > index.fields.len() {
        return false;
    }

    query
        .filters
        .iter()
        .zip(index.fields.iter())
        .all(|(filter, field)| filter.field == field.field)
}
