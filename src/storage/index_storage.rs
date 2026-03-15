use crate::index::composite::manager::CompositeIndexManager;

use super::index_log::IndexLog;
use super::index_snapshot::write_snapshot;
use super::index_recovery::replay_log;

pub struct IndexStorage {

    pub manager:CompositeIndexManager,

    log:IndexLog,

    snapshot_dir:String

}

impl IndexStorage {

    pub fn open(
        log_path:&str,
        snapshot_dir:&str
    )->std::io::Result<Self>{

        let mut manager = CompositeIndexManager::new();

        let log = IndexLog::open(log_path)?;

        replay_log(log_path,&mut manager).ok();

        Ok(
            Self{
                manager,
                log,
                snapshot_dir:snapshot_dir.to_string()
            }
        )

    }

    pub fn insert(
        &mut self,
        index_id:u32,
        key:Vec<u8>,
        doc:String
    ){

        self.manager.insert_raw(
            index_id,
            key.clone(),
            doc.clone()
        );

        let _ = self.log.append_insert(
            index_id,
            &key,
            &doc
        );

    }

    pub fn delete(
        &mut self,
        index_id:u32,
        key:Vec<u8>,
        doc:String
    ){

        self.manager.remove_raw(
            index_id,
            key.clone()
        );

        let _ = self.log.append_delete(
            index_id,
            &key,
            &doc
        );

    }

    pub fn snapshot(
        &self,
        index_id:u32
    ){

        if let Some(idx)=self.manager.get_index(index_id){

            let path = format!(
                "{}/{}.snap",
                self.snapshot_dir,
                index_id
            );

            let _ = write_snapshot(
                &path,
                index_id,
                idx
            );

        }

    }

}
