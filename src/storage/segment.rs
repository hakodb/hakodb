use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{FireLiteError, Result};

use super::crypto::EncryptionContext;

pub struct Segment {
    file: Option<File>,
    path: PathBuf,
    encryption: Option<EncryptionContext>,
}

impl Segment {
    pub fn open(path: impl AsRef<Path>, encryption: Option<EncryptionContext>) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        Ok(Self {
            file: Some(OpenOptions::new()
                .create(true)
                .read(true)
                .write(true)
                // .append(true)
                .open(&path_buf)?),
            path: path_buf,
            encryption,
        })
    }

    pub fn append(&mut self, value: &[u8]) -> Result<(u64, u32)> {
        // let payload = if let Some(enc) = &self.encryption {
        //     enc.encrypt(value)?
        // } else {
        //     value.to_vec()
        // };

        // let afile = self.file.as_mut().unwrap();

        // let offset = afile.seek(SeekFrom::End(0))?;
        // afile.write_all(&(payload.len() as u32).to_le_bytes())?;
        // afile.write_all(&payload)?;
        // afile.sync_data()?;
        // Ok((offset, payload.len() as u32))
        Ok(self.append_batch(&[value])?[0])
    }

    pub fn append_batch(&mut self, values: &[&[u8]]) -> Result<Vec<(u64, u32)>> {
        if values.is_empty() {
            return Ok(Vec::new());
        }

        let afile = self.file.as_mut().unwrap();
        let mut current_offset = afile.seek(SeekFrom::End(0))?;
        
        // Pre-calculate rough buffer size to minimize allocations
        let estimated_size = values.iter().map(|v| 4 + v.len()).sum();
        let mut buffer = Vec::with_capacity(estimated_size);
        let mut results = Vec::with_capacity(values.len());

        for value in values {
            if let Some(enc) = &self.encryption {
                let payload = enc.encrypt(value)?;
                buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
                buffer.extend_from_slice(&payload);
                results.push((current_offset, payload.len() as u32));
                current_offset += 4 + payload.len() as u64;
            } else {
                buffer.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buffer.extend_from_slice(value);
                results.push((current_offset, value.len() as u32));
                current_offset += 4 + value.len() as u64;
            }
        }

        afile.write_all(&buffer)?;
        
        // Single disk sync per batch
        afile.sync_data()?;

        Ok(results)
    }

    pub fn read_at(&mut self, offset: u64, stored_len: u32) -> Result<Vec<u8>> {
        let afile = self.file.as_mut().unwrap();

        afile.seek(SeekFrom::Start(offset))?;
        let mut len_buf = [0; 4];
        afile.read_exact(&mut len_buf)?;
        let len = u32::from_le_bytes(len_buf);
        if len != stored_len {
            return Err(FireLiteError::Corrupt("segment length mismatch".into()));
        }
        let mut out = vec![0; len as usize];
        afile.read_exact(&mut out)?;

        if let Some(enc) = &self.encryption {
            enc.decrypt(&out)
        } else {
            Ok(out)
        }
    }

    pub fn truncate(&mut self) -> Result<()> {

        let afile = self.file.as_mut().unwrap();

        afile.set_len(0)?;
        afile.seek(SeekFrom::Start(0))?;
        Ok(())
    }

    pub fn size_bytes(&mut self) -> Result<u64> {
        Ok(self.file.as_mut().unwrap().seek(SeekFrom::End(0))?)
    }

    pub fn path(&self) -> &Path {
        &self.path
    }

    pub fn close(&mut self) {
        // Take the file out of Option, dropping it immediately
        let _ = self.file.take();
    }
}
