use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

use crate::error::{FireLiteError, Result};
use super::crypto::EncryptionContext;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub struct Segment {
    file: Option<File>,
    path: PathBuf,
    pub(crate) encryption: Option<EncryptionContext>, // Made pub(crate) for compaction access
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

    /// Primary append used by standard writes (Uncompressed path)
    pub fn append(&mut self, value: &[u8]) -> Result<(u64, u32)> {
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
            // Standard append: bit 31 is 0 (uncompressed)
            self.write_buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            self.write_buffer.extend_from_slice(&payload);
            results.push((current_offset, payload.len() as u32));
            current_offset += 4 + payload.len() as u64;
        }

        afile.write_all(&self.write_buffer)?;
        Ok(results)
    }

    /// Internal helper used by Compaction to write data with specific bit-flags
    pub(crate) fn append_raw_with_len(&mut self, payload: &[u8], stored_len: u32) -> Result<u64> {
        let afile = self.file.as_mut().unwrap();
        let offset = afile.seek(SeekFrom::End(0))?;
        
        afile.write_all(&stored_len.to_le_bytes())?;
        afile.write_all(payload)?;
        
        Ok(offset)
    }

    pub fn flush(&self) -> Result<()> {
        self.file.as_ref().unwrap().sync_data()?;
        Ok(())
    }

    pub fn read_at(&self, offset: u64, stored_len: u32) -> Result<Vec<u8>> {
        let f = self.file.as_ref().unwrap();
        
        // 1. Extract the compression bit (bit 31) and the actual data length
        let is_compressed = (stored_len >> 31) == 1;
        let actual_payload_len = stored_len & 0x7FFFFFFF;

        let total_to_read = 4 + actual_payload_len as usize;
        let mut buffer = vec![0u8; total_to_read];

        // 2. Atomic Positional Read
        #[cfg(windows)]
        f.seek_read(&mut buffer, offset)?;
        #[cfg(unix)]
        f.read_at(&mut buffer, offset)?;

        // Byte 4 onwards is the payload (encrypted and/or compressed)
        let mut out = buffer[4..].to_vec();

        // 3. Decrypt first (standard security: Encrypt-then-Compress is avoided)
        if let Some(enc) = &self.encryption {
            out = enc.decrypt(&out)?;
        }

        // 4. Decompress if the flag was set
        if is_compressed {
            out = zstd::decode_all(&out[..])
                .map_err(|e| FireLiteError::StorageError(format!("Decompression failed: {}", e)))?;
        }

        Ok(out)
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
        let _ = self.file.take();
    }
}