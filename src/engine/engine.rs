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
use crate::storage::engine::{StorageEngine, StorageMutation};

#[derive(Debug, Clone)]
pub enum BatchMutation {
    Put {
        collection: String,
        doc_id: String,
        doc: FireLiteDoc,
    },
    Delete {
        collection: String,
        doc_id: String,
    },
}

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

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        let mut storage_mutations = Vec::with_capacity(mutations.len());
        let mut removed_docs = Vec::new();

        {
            let mut storage = self.storage.lock().expect("storage lock poisoned");
            for mutation in &mutations {
                match mutation {
                    BatchMutation::Put {
                        collection,
                        doc_id,
                        doc,
                    } => {
                        storage_mutations.push(StorageMutation::Put {
                            key: format!("{}:{}", collection, doc_id),
                            value: doc.encode(),
                        });
                    }
                    BatchMutation::Delete { collection, doc_id } => {
                        let key = format!("{}:{}", collection, doc_id);
                        if let Some(bytes) = storage.get(&key)? {
                            if let Some(old_doc) = FireLiteDoc::decode(&bytes) {
                                removed_docs.push((collection.clone(), doc_id.clone(), old_doc));
                            }
                        }
                        storage_mutations.push(StorageMutation::Delete { key });
                    }
                }
            }

            storage.apply_batch(&storage_mutations)?;
        }

        let mut indexes = self.indexes.lock().expect("indexes lock poisoned");
        for mutation in mutations {
            match mutation {
                BatchMutation::Put {
                    collection,
                    doc_id,
                    doc,
                } => indexes.index_document(&collection, &doc_id, &doc),
                BatchMutation::Delete { .. } => {}
            }
        }

        for (collection, doc_id, old_doc) in removed_docs {
            indexes.remove_document(&collection, &doc_id, &old_doc);
        }

        Ok(())
    }

    pub fn put(&self, collection: &str, doc_id: &str, doc: &FireLiteDoc) -> Result<()> {
        self.write_batch(vec![BatchMutation::Put {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            doc: doc.clone(),
        }])
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
        self.write_batch(vec![BatchMutation::Delete {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        }])
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

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::document::value::Value;

    use super::{BatchMutation, FireLite};
    use crate::config::FireLiteConfig;
    use crate::document::firelite_doc::FireLiteDoc;

    fn temp_path(prefix: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "{}-{}",
            prefix,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .expect("clock should be after unix epoch")
                .as_nanos()
        ))
    }

    #[test]
    fn write_batch_commits_multiple_mutations() {
        let path = temp_path("firelite-batch");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");

        let mut user1 = FireLiteDoc::default();
        user1.insert("name", Value::String("alice".into()));

        let mut user2 = FireLiteDoc::default();
        user2.insert("name", Value::String("bob".into()));

        db.write_batch(vec![
            BatchMutation::Put {
                collection: "users".into(),
                doc_id: "1".into(),
                doc: user1,
            },
            BatchMutation::Put {
                collection: "users".into(),
                doc_id: "2".into(),
                doc: user2,
            },
        ])
        .expect("batch should succeed");

        assert!(db.get("users", "1").expect("get should succeed").is_some());
        assert!(db.get("users", "2").expect("get should succeed").is_some());

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }
}
