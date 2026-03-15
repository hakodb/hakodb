use std::collections::HashMap;
use std::path::Path;
use std::sync::mpsc::{channel, Receiver, Sender};
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

#[derive(Debug, Clone)]
pub enum ChangeKind {
    Put,
    Delete,
}

#[derive(Debug, Clone)]
pub struct ChangeEvent {
    pub path: String,
    pub kind: ChangeKind,
}

pub struct Transaction {
    mutations: Vec<BatchMutation>,
}

impl Transaction {
    pub fn put(&mut self, collection: &str, doc_id: &str, doc: FireLiteDoc) {
        self.mutations.push(BatchMutation::Put {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            doc,
        });
    }

    pub fn delete(&mut self, collection: &str, doc_id: &str) {
        self.mutations.push(BatchMutation::Delete {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        });
    }

    pub fn put_subdocument(
        &mut self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
        doc: FireLiteDoc,
    ) {
        self.put(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
            doc,
        );
    }

    pub fn commit(self, db: &FireLite) -> Result<()> {
        db.write_batch(self.mutations)
    }
}

pub struct FireLite {
    storage: Mutex<StorageEngine>,
    indexes: Mutex<IndexManager>,
    executor: ParallelQueryExecutor,
    tx_lock: Mutex<()>,
    listeners: Mutex<HashMap<String, Vec<Sender<ChangeEvent>>>>,
}

impl FireLite {
    pub fn open(path: impl AsRef<Path>, config: FireLiteConfig) -> Result<Self> {
        Ok(Self {
            storage: Mutex::new(StorageEngine::open(path, &config)?),
            indexes: Mutex::new(IndexManager::default()),
            executor: ParallelQueryExecutor::new(config.query_workers),
            tx_lock: Mutex::new(()),
            listeners: Mutex::new(HashMap::new()),
        })
    }

    pub fn begin_transaction(&self) -> Transaction {
        Transaction {
            mutations: Vec::new(),
        }
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

    pub fn watch_collection(&self, collection: &str) -> Receiver<ChangeEvent> {
        let (tx, rx) = channel();
        self.listeners
            .lock()
            .expect("listeners lock poisoned")
            .entry(collection.to_string())
            .or_default()
            .push(tx);
        rx
    }

    fn notify_watchers(&self, collection: &str, event: ChangeEvent) {
        if let Some(list) = self
            .listeners
            .lock()
            .expect("listeners lock poisoned")
            .get_mut(collection)
        {
            list.retain(|sender| sender.send(event.clone()).is_ok());
        }
    }

    pub fn write_batch(&self, mutations: Vec<BatchMutation>) -> Result<()> {
        let _tx_guard = self.tx_lock.lock().expect("transaction lock poisoned");

        let mut storage_mutations = Vec::with_capacity(mutations.len());
        let mut removed_docs = Vec::new();
        let mut change_events = Vec::new();

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
                            key: doc_key(collection, doc_id),
                            value: doc.encode(),
                        });
                        change_events.push((
                            collection.clone(),
                            ChangeEvent {
                                path: doc_key(collection, doc_id),
                                kind: ChangeKind::Put,
                            },
                        ));
                    }
                    BatchMutation::Delete { collection, doc_id } => {
                        let key = doc_key(collection, doc_id);
                        if let Some(bytes) = storage.get(&key)? {
                            if let Some(old_doc) = FireLiteDoc::decode(&bytes) {
                                removed_docs.push((collection.clone(), doc_id.clone(), old_doc));
                            }
                        }
                        storage_mutations.push(StorageMutation::Delete { key: key.clone() });
                        change_events.push((
                            collection.clone(),
                            ChangeEvent {
                                path: key,
                                kind: ChangeKind::Delete,
                            },
                        ));
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

        for (collection, event) in change_events {
            self.notify_watchers(&collection, event);
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

    pub fn put_subdocument(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
        doc: &FireLiteDoc,
    ) -> Result<()> {
        self.put(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
            doc,
        )
    }

    pub fn get(&self, collection: &str, doc_id: &str) -> Result<Option<FireLiteDoc>> {
        let key = doc_key(collection, doc_id);
        Ok(self
            .storage
            .lock()
            .expect("storage lock poisoned")
            .get(&key)?
            .and_then(|v| FireLiteDoc::decode(&v)))
    }

    pub fn get_subdocument(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
    ) -> Result<Option<FireLiteDoc>> {
        self.get(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
        )
    }

    pub fn delete(&self, collection: &str, doc_id: &str) -> Result<()> {
        self.write_batch(vec![BatchMutation::Delete {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
        }])
    }

    pub fn delete_subdocument(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
        subdoc_id: &str,
    ) -> Result<()> {
        self.delete(
            &subcollection_prefix(collection, doc_id, subcollection),
            subdoc_id,
        )
    }

    pub fn query(&self, query: Query) -> Result<Vec<(String, FireLiteDoc)>> {
        let plan = {
            let indexes = self.indexes.lock().expect("indexes lock poisoned");
            QueryPlanner::plan(&query, &indexes)
        };
        let mut storage = self.storage.lock().expect("storage lock poisoned");
        let indexes = self.indexes.lock().expect("indexes lock poisoned");
        self.executor.execute(&mut storage, &indexes, plan)
    }

    pub fn query_subcollection(
        &self,
        collection: &str,
        doc_id: &str,
        subcollection: &str,
    ) -> Result<Vec<(String, FireLiteDoc)>> {
        self.query(Query::new(&subcollection_prefix(
            collection,
            doc_id,
            subcollection,
        )))
    }

    pub fn compact(&self) -> Result<()> {
        self.storage
            .lock()
            .expect("storage lock poisoned")
            .compact()
    }

    pub fn flush(&self) -> Result<()> {
        self.storage
            .lock()
            .expect("storage lock poisoned")
            .flush_wal()
    }
}

fn doc_key(collection: &str, doc_id: &str) -> String {
    format!("{}:{}", collection, doc_id)
}

fn subcollection_prefix(collection: &str, doc_id: &str, subcollection: &str) -> String {
    format!("{}:{}/{}", collection, doc_id, subcollection)
}

#[cfg(test)]
mod tests {
    use std::fs;

    use crate::document::value::Value;

    use super::{BatchMutation, ChangeKind, FireLite};
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

    #[test]
    fn watch_stream_receives_changes() {
        let path = temp_path("firelite-watch");
        let db = FireLite::open(&path, FireLiteConfig::default()).expect("db open should succeed");
        let rx = db.watch_collection("users");

        let mut user = FireLiteDoc::default();
        user.insert("name", Value::String("eve".into()));
        db.put("users", "7", &user).expect("put should succeed");

        let event = rx.recv().expect("watch should receive event");
        assert!(event.path.contains("users:7"));
        assert!(matches!(event.kind, ChangeKind::Put));

        fs::remove_dir_all(path).expect("temp db dir should be removable");
    }
}
