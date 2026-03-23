use std::sync::atomic::{AtomicUsize, Ordering};

pub struct Allocator {
    offset: AtomicUsize,
}

impl Allocator {
    pub fn new() -> Self {
        Self {
            offset: AtomicUsize::new(0),
        }
    }

    pub fn allocate(&self, size: usize) -> usize {
        // self.offset.fetch_add(size, Ordering::SeqCst)
        self.offset.fetch_add(size, Ordering::Relaxed)
    }
}
