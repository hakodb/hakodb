use crate::query::query::Query;

#[derive(Clone)]
pub struct QueryTask {

    pub query: Query,

    pub collection_id: u32,

}

impl QueryTask {

    pub fn new(query: Query, collection_id: u32) -> Self {

        Self {
            query,
            collection_id,
        }

    }

}
