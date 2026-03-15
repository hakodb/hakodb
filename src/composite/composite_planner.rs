use crate::query::query::Query;

use crate::index::composite::manager::CompositeIndexManager;

use super::composite_matcher::matches_index;

pub struct CompositePlanner;

impl CompositePlanner {

    pub fn find_index<'a>(
        query:&Query,
        manager:&'a CompositeIndexManager,
        collection:u32
    )->Option<&'a crate::index::composite::composite_index::CompositeIndex>{

        if let Some(list)=manager.get_indexes(collection){

            for idx in list {

                if matches_index(query,&idx.definition){

                    return Some(idx);

                }

            }

        }

        None

    }

}
