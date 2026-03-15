use std::sync::mpsc::Receiver;

pub struct ResultStream {

    receiver: Receiver<String>

}

impl ResultStream {

    pub fn new(rx: Receiver<String>) -> Self {

        Self { receiver: rx }

    }

}

impl Iterator for ResultStream {

    type Item = String;

    fn next(&mut self) -> Option<Self::Item> {

        self.receiver.recv().ok()

    }

}
