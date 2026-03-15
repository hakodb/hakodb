use std::collections::{HashMap, VecDeque};

use super::page::Page;

#[derive(Debug)]
pub struct PageCache {
    capacity: usize,
    order: VecDeque<u64>,
    pages: HashMap<u64, Page>,
}

impl PageCache {
    pub fn new(capacity: usize) -> Self {
        Self {
            capacity,
            order: VecDeque::new(),
            pages: HashMap::new(),
        }
    }

    pub fn get(&mut self, id: u64) -> Option<Page> {
        if let Some(pos) = self.order.iter().position(|v| *v == id) {
            self.order.remove(pos);
            self.order.push_back(id);
        }
        self.pages.get(&id).cloned()
    }

    pub fn put(&mut self, page: Page) {
        if self.pages.contains_key(&page.id) {
            self.order.retain(|id| *id != page.id);
        }
        self.order.push_back(page.id);
        self.pages.insert(page.id, page);

        if self.pages.len() > self.capacity {
            if let Some(victim) = self.order.pop_front() {
                self.pages.remove(&victim);
            }
        }
    }
}
