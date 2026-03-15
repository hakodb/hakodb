pub struct Arena {
    buffer: Vec<u8>,

    offset: usize,
}

impl Arena {
    pub fn new(size: usize) -> Self {
        Self {
            buffer: vec![0; size],
            offset: 0,
        }
    }

    pub fn alloc(&mut self, size: usize) -> Option<&mut [u8]> {
        if self.offset + size > self.buffer.len() {
            return None;
        }

        let start = self.offset;

        self.offset += size;

        Some(&mut self.buffer[start..start + size])
    }

    pub fn reset(&mut self) {
        self.offset = 0;
    }
}
