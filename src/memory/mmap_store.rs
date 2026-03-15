use std::fs::{File,OpenOptions};
use std::io::{Seek,SeekFrom,Write};
use std::path::Path;
use std::sync::{Arc,RwLock};

use memmap2::{MmapMut,MmapOptions};

pub struct MmapStore {

    file: File,

    mmap: Arc<RwLock<MmapMut>>,

    pub size: usize,

}

impl MmapStore {

    pub fn open<P:AsRef<Path>>(path:P,size:usize)->std::io::Result<Self>{

        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;

        file.set_len(size as u64)?;

        let mmap = unsafe{
            MmapOptions::new()
                .len(size)
                .map_mut(&file)?
        };

        Ok(
            Self{
                file,
                mmap:Arc::new(RwLock::new(mmap)),
                size
            }
        )

    }

    pub fn read_slice(
        &self,
        offset:usize,
        len:usize
    )->Vec<u8>{

        let mmap = self.mmap.read().unwrap();

        mmap[offset..offset+len].to_vec()

    }

    pub fn write_slice(
        &self,
        offset:usize,
        data:&[u8]
    ){

        let mut mmap = self.mmap.write().unwrap();

        mmap[offset..offset+data.len()].copy_from_slice(data);

    }

    pub fn flush(&self){

        let mmap = self.mmap.read().unwrap();

        mmap.flush().ok();

    }

}
