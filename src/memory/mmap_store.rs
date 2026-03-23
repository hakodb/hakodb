use std::{fs::OpenOptions, path::Path, sync::RwLock};

use memmap2::{MmapMut, MmapOptions};

use crate::error::Result;

pub struct MmapStore {
    mmap: Option<RwLock<MmapMut>>,
    pub size: usize,
}

impl MmapStore {
    pub fn open<P: AsRef<Path>>(path: P, _size: usize) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let metadata = file.metadata()?;
        let len = metadata.len() as usize;

        if len == 0 {
            return Ok(Self {
                mmap: None,
                size: 0,
            });
        }

        let mmap = unsafe { MmapOptions::new().len(len).map_mut(&file)? };
        
        Ok(Self {
            mmap: Some(RwLock::new(mmap)),
            size: len,
        })
    }

    pub fn read_slice(&self, offset: usize, len: usize) -> Vec<u8> {
        let Some(ref mmap_lock) = self.mmap else { return vec![0; len]; };
        let mmap = mmap_lock.read().expect("mmap lock poisoned");
        
        // Bounds check to prevent crashing on growing files
        if offset + len > mmap.len() {
            return vec![0; len];
        }
        mmap[offset..offset + len].to_vec()
    }

    pub fn write_slice(&self, offset: usize, data: &[u8]) {
        if let Some(ref mmap_lock) = self.mmap {
            let mut mmap = mmap_lock.write().expect("mmap write lock poisoned");
            if offset + data.len() <= mmap.len() {
                mmap[offset..offset + data.len()].copy_from_slice(data);
            }
        }
    }

    pub fn flush(&self) -> Result<()> {
        if let Some(ref mmap_lock) = self.mmap {
            let mmap = mmap_lock.read().expect("mmap read lock poisoned");
            mmap.flush()?;
        }
        Ok(())
    }
}
