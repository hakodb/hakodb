use std::sync::{Arc,mpsc::Receiver};
use std::thread;

use crate::storage::engine::StorageEngine;
use crate::query::executor::task::QueryTask;

pub fn start_worker(

    id: usize,

    engine: Arc<StorageEngine>,

    rx: Receiver<QueryTask>,

){

    thread::spawn(move ||{

        while let Ok(task) = rx.recv() {

            execute_query(id,&engine,task);

        }

    });

}

fn execute_query(

    _id:usize,

    engine:&StorageEngine,

    task:QueryTask

){

    let docs = engine.scan_collection(task.collection_id);

    for doc in docs {

        if task.query.matches(&doc) {

            // result would be sent to stream
        }

    }

}
