use crate::query::query::Query;

use crate::index::composite::definition::CompositeIndexDefinition;

pub fn matches_index(
    query:&Query,
    index:&CompositeIndexDefinition
)->bool{

    if query.filters.len()>index.fields.len(){
        return false;
    }

    for (i,f) in query.filters.iter().enumerate(){

        if f.field != index.fields[i].field {
            return false;
        }

    }

    true

}
