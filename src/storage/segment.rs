use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::{FireLiteError, Result};

pub struct Segment {
    file: File,
}

impl Segment {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        Ok(Self {
            file: OpenOptions::new()
                .create(true)
                .read(true)
                .append(true)
                .open(path)?,
        })
    }

    pub fn append(&mut self, value: &[u8]) -> Result<u64> {
        let offset = self.file.seek(SeekFrom::End(0))?;
        self.file.write_all(&(value.len() as u32).to_le_bytes())?;
        self.file.write_all(value)?;
        self.file.sync_data()?;
        Ok(offset)
    }

    pub fn read_at(&mut self, offset: u64, expected_len: u32) -> Result<Vec<u8>> {
        self.file.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];
        self.file.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf);
        if len != expected_len {
            return Err(FireLiteError::Corrupt("segment length mismatch".into()));
        }
        let mut out = vec![0; len as usize];
        self.file.read_exact(&mut out)?;
        Ok(out)
    }

    pub fn truncate(&mut self) -> Result<()> {
        self.file.set_len(0)?;
        self.file.seek(SeekFrom::Start(0))?;
        Ok(())
    }
}
