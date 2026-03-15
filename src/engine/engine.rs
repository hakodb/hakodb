use std::path::Path;
use std::sync::Mutex;

use crate::config::FireLiteConfig;
use crate::document::firelite_doc::FireLiteDoc;
use crate::error::Result;
use crate::index::composite::definition::{CompositeIndexDefinition, SortDirection};
use crate::index::manager::IndexManager;
use crate::query::executor::executor::ParallelQueryExecutor;
use crate::query::planner::QueryPlanner;
use crate::query::query::Query;
use crate::storage::engine::StorageEngine;

pub struct FireLite {
    storage: Mutex<StorageEngine>,
    indexes: Mutex<IndexManager>,
    executor: ParallelQueryExecutor,
}

impl FireLite {
    pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self> {
        Ok(Self {
            storage: Mutex::new(StorageEngine::open(path)?),
            indexes: Mutex::new(IndexManager::default()),
            executor: ParallelQueryExecutor::new(config.query_workers),
        })
    }

    pub fn create_composite_index(
        &self,
        collection: &str,
        fields: Vec<(String, SortDirection)>,
    ) -> u32 {
        self.indexes
            .lock()
            .expect("indexes lock poisoned")
            .create_index(CompositeIndexDefinition::new(collection).with_fields(fields))
    }

    pub fn put(&self, collection: &str, doc_id: &str, doc: &FireLiteDoc) -> Result<()> {
        let key = format!("{}:{}", collection, doc_id);
        self.storage
            .lock()
            .expect("storage lock poisoned")
            .put(key, &doc.encode())?;
        self.indexes
            .lock()
            .expect("indexes lock poisoned")
            .index_document(collection, doc_id, doc);
        Ok(())
    }

    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        let key = format!("{}:{}", collection, doc_id);
        Ok(self
            .storage
            .lock()
            .expect("storage lock poisoned")
            .get(&key)?
            .and_then(|v| FireLiteDoc::decode(&v)))
    }

    pub fn delete(&self, collection: &str, doc_id: &str) -> Result<()> {
        let key = format!("{}:{}", collection, doc_id);
        if let Some(doc) = self.get(collection, doc_id)? {
            self.indexes
                .lock()
                .expect("indexes lock poisoned")
                .remove_document(collection, doc_id, &doc);
        }
        self.storage
            .lock()
            .expect("storage lock poisoned")
            .delete(&key)
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>> {
        let plan = {
            let indexes = self.indexes.lock().expect("indexes lock poisoned");
            QueryPlanner::plan(&query, &indexes)
        };
        let mut storage = self.storage.lock().expect("storage lock poisoned");
        self.executor.execute(&mut storage, plan)
    }

    pub fn compact(&self) -> Result<()> {
        self.storage
            .lock()
            .expect("storage lock poisoned")
            .compact()
    }
}
