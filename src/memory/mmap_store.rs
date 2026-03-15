use std::{fs::OpenOptions, path::Path, sync::RwLock};

use memmap2::{MmapMut, MmapOptions};

use crate::error::Result;

pub struct MmapStore {
    mmap: RwLock<MmapMut>,
    pub size: usize,
}

impl MmapStore {
    pub fn open<P: AsRef<Path>>(path: P, size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;
        file.set_len(size as u64)?;
        let mmap = unsafe { MmapOptions::new().len(size).map_mut(&file)? };
        Ok(Self {
            mmap: RwLock::new(mmap),
            size,
        })
    }

    pub fn read_slice(&self, offset: usize, len: usize) -> Vec<u8> {
        let mmap = self.mmap.read().expect("mmap read lock poisoned");
        mmap[offset..offset + len].to_vec()
    }

    pub fn write_slice(&self, offset: usize, data: &[u8]) {
        let mut mmap = self.mmap.write().expect("mmap write lock poisoned");
        mmap[offset..offset + data.len()].copy_from_slice(data);
    }

    pub fn flush(&self) -> Result<()> {
        let mmap = self.mmap.read().expect("mmap read lock poisoned");
        mmap.flush()?;
        Ok(())
    }
}
