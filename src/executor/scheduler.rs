use std::sync::mpsc::{Sender,Receiver,channel};

use super::task::QueryTask;

pub struct Scheduler {

    sender: Sender<QueryTask>,

}

impl Scheduler {

    pub fn new() -> (Self,Receiver<QueryTask>) {

        let (tx,rx) = channel();

        (Self{sender:tx},rx)

    }

    pub fn submit(&self, task: QueryTask) {

        let _ = self.sender.send(task);

    }

}
