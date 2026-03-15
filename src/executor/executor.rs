use std::sync::{Arc,mpsc::channel};

use crate::storage::engine::StorageEngine;

use super::scheduler::Scheduler;
use super::worker::start_worker;
use super::task::QueryTask;
use super::result_stream::ResultStream;

pub struct QueryExecutor {

    engine: Arc<StorageEngine>,

    scheduler: Scheduler,

}

impl QueryExecutor {

    pub fn new(

        engine: Arc<StorageEngine>,
        workers: usize,

    ) -> Self {

        let (scheduler,rx) = Scheduler::new();

        for id in 0..workers {

            start_worker(
                id,
                engine.clone(),
                rx.clone(),
            );

        }

        Self {
            engine,
            scheduler,
        }

    }

    pub fn execute(

        &self,
        query: crate::query::query::Query,
        collection_id:u32,

    ) -> ResultStream {

        let (tx,rx) = channel();

        let task = QueryTask::new(query,collection_id);

        self.scheduler.submit(task);

        ResultStream::new(rx)

    }

}
