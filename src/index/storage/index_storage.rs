use std::fs;

use crate::index::composite::manager::CompositeIndexManager;

use super::index_log::{IndexLog, DELETE, INSERT};
use super::index_recovery::replay_log;
use super::index_snapshot::write_snapshot;

pub struct IndexStorage {
    pub manager: CompositeIndexManager,
    log: IndexLog,
    snapshot_dir: String,
}

impl IndexStorage {
    pub fn open(log_path: &str, snapshot_dir: &str) -> std::io::Result<Self> {
        fs::create_dir_all(snapshot_dir)?;
        let mut manager = CompositeIndexManager::default();
        replay_log(log_path, &mut manager)?;
        Ok(Self {
            manager,
            log: IndexLog::open(log_path)?,
            snapshot_dir: snapshot_dir.to_string(),
        })
    }

    pub fn insert(&mut self, index_id: u32, key: Vec<u8>, doc: String) -> std::io::Result<()> {
        if let Some(index) = self.manager.get_mut(index_id) {
            // index.tree.insert(key.clone(), doc.clone());
            index.tree.insert(key.clone().into(), doc.clone().into());
        }
        self.log.append(INSERT, index_id, &key, &doc)
    }

    pub fn delete(&mut self, index_id: u32, key: Vec<u8>, doc: String) -> std::io::Result<()> {
        if let Some(index) = self.manager.get_mut(index_id) {
            // index.tree.remove(&key);
            index.tree.remove(&key[..]);
        }
        self.log.append(DELETE, index_id, &key, &doc)
    }

    pub fn snapshot(&self, index_id: u32) -> std::io::Result<()> {
        if let Some(index) = self.manager.get(index_id) {
            let path = format!("{}/{}.snap", self.snapshot_dir, index_id);
            write_snapshot(&path, index)?;
        }
        Ok(())
    }

    /// Clears the index log. Usually called after a successful snapshot.
    pub fn reset_log(&mut self) -> std::io::Result<()> {
        self.log.reset()
    }

}
