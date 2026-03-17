use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
// use std::sync::Mutex;

use crate::error::{FireLiteError, Result};
use super::crypto::EncryptionContext;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub struct Segment {
    file: Option<File>,
    path: PathBuf,
    encryption: Option<EncryptionContext>,
    write_buffer: Vec<u8>,
}

impl Segment {
    pub fn open(path: impl AsRef<Path>, encryption: Option<EncryptionContext>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path_buf)?;
        Ok(Self {
            file: Some(file),
            path: path_buf,
            encryption,
            write_buffer: Vec::with_capacity(64 * 1024),
        })
    }

    pub fn append(&mut self, value: &[u8]) -> Result<(u64, u32)> {
        // Ok(self.append_batch(&[value])?[0])
        let mut results = self.append_batch(&[value])?;
        Ok(results.pop().unwrap())
    }

    pub fn append_batch(&mut self, values: &[&[u8]]) -> Result<Vec<(u64, u32)>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }

        let afile = self.file.as_mut().unwrap();
        let mut current_offset = afile.seek(SeekFrom::End(0))?;
        
        self.write_buffer.clear();
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            let payload = if let Some(enc) = &self.encryption {
                enc.encrypt(value)?
            } else {
                value.to_vec()
            };
            self.write_buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            self.write_buffer.extend_from_slice(&payload);
            results.push((current_offset, payload.len() as u32));
            current_offset += 4 + payload.len() as u64;
        }

        afile.write_all(&self.write_buffer)?;
        Ok(results)
    }

    pub fn flush(&self) -> Result<()> {
        self.file.as_ref().unwrap().sync_data()?;
        Ok(())
    }

    pub fn read_at(&self, offset: u64, stored_len: u32) -> Result<Vec<u8>> {
        let f = self.file.as_ref().unwrap();
        
        // 1. Read the length and payload in one go or use positional reads
        // We use a buffer to hold the length (4 bytes) + the data
        let total_to_read = 4 + stored_len as usize;
        let mut buffer = vec![0u8; total_to_read];

        // 2. ATOMIC POSITIONAL READ (No Locking!)
        #[cfg(windows)]
        f.seek_read(&mut buffer, offset)?;
        #[cfg(unix)]
        f.read_at(&mut buffer, offset)?;

        // 3. Verify length matches
        let len = u32::from_le_bytes(buffer[0..4].try_into().unwrap());
        if len != stored_len {
            return Err(FireLiteError::Corrupt("segment length mismatch".into()));
        }

        let out = buffer[4..].to_vec();

        if let Some(enc) = &self.encryption {
            enc.decrypt(&out)
        } else {
            Ok(out)
        }
    }

    pub fn truncate(&mut self) -> Result<()> {
        let afile = self.file.as_mut().unwrap();
        afile.set_len(0)?;
        Ok(())
    }

    pub fn size_bytes(&self) -> Result<u64> {
        Ok(self.file.as_ref().unwrap().metadata()?.len())
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn close(&mut self) {
        // Take the file out of Option, dropping it immediately
        let _ = self.file.take();
    }
}
