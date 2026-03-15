pub const DEFAULT_PAGE_SIZE: usize = 4096;

#[derive(Debug, Clone)]
pub struct Page {
    pub id: u64,
    pub data: Vec<u8>,
}

impl Page {
    pub fn new(id: u64, page_size: usize) -> Self {
        Self {
            id,
            data: vec![0; page_size],
        }
    }
}
