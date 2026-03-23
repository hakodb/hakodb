use std::fs::{File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

use crate::memory::mmap_store::MmapStore;
use crate::memory::page_cache::{BlockKey, PageCache};
use crate::error::{FireLiteError, Result};
use super::crypto::EncryptionContext;

#[cfg(unix)]
use std::os::unix::fs::FileExt;
#[cfg(windows)]
use std::os::windows::fs::FileExt;

pub struct Segment {
    pub store: Arc<MmapStore>, 
    pub segment_id: u64,
    pub cache: Arc<Mutex<PageCache>>, 

    file: Option<File>,
    path: PathBuf,
    pub(crate) encryption: Option<EncryptionContext>,
    write_buffer: Vec<u8>,
}

impl Segment {
    pub fn open(
        path: impl AsRef<Path>, 
        segment_id: u64,
        encryption: Option<EncryptionContext>,
        cache: Arc<Mutex<PageCache>>,
        mmap_size: usize,
    ) -> Result<Self> {
        let path_buf = path.as_ref().to_path_buf();
        
        // 1. Open File handle for WRITES (Durability)
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(&path_buf)?;

        // 2. Open MmapStore for READS (Performance)
        let store = Arc::new(MmapStore::open(&path_buf, mmap_size)?);

        Ok(Self {
            store,
            segment_id,
            cache,
            file: Some(file),
            path: path_buf,
            encryption,
            write_buffer: Vec::with_capacity(64 * 1024),
        })
    }

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
            self.write_buffer.extend_from_slice(&(payload.len() as u32).to_le_bytes());
            self.write_buffer.extend_from_slice(&payload);
            results.push((current_offset, payload.len() as u32));
            current_offset += 4 + payload.len() as u64;
        }

        afile.write_all(&self.write_buffer)?;
        Ok(results)
    }

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

    pub fn read_at(&self, offset: u64, stored_len: u32, use_cache: bool) -> Result<Vec<u8>> {
        let cache_key = BlockKey { segment_id: self.segment_id, offset };

        // 1. Check cache only if requested
        if use_cache {
            let mut cache = self.cache.lock().unwrap();
            if let Some(data) = cache.get(&cache_key) {
                return Ok((*data).clone());
            }
        }

        // 2. Physical Read (Mmap or File)
        let is_compressed = (stored_len >> 31) == 1;
        let actual_payload_len = (stored_len & 0x7FFFFFFF) as usize;
        let total_to_read = 4 + actual_payload_len;

        let buffer = if offset + total_to_read as u64 <= self.store.size as u64 {
            self.store.read_slice(offset as usize, total_to_read)
        } else {
            let mut buf = vec![0u8; total_to_read];
            let f = self.file.as_ref().ok_or_else(|| FireLiteError::StorageError("File closed".into()))?;
            #[cfg(windows)] f.seek_read(&mut buf, offset)?;
            #[cfg(unix)] f.read_at(&mut buf, offset)?;
            buf
        };

        let mut out = buffer[4..].to_vec();
        if let Some(enc) = &self.encryption { out = enc.decrypt(&out)?; }
        if is_compressed {
            out = zstd::decode_all(&out[..])
                .map_err(|e| FireLiteError::StorageError(format!("Zstd fail: {}", e)))?;
        }

        // 3. Only populate cache if this isn't a one-time analytical scan
        if use_cache {
            let mut cache = self.cache.lock().unwrap();
            cache.put(cache_key, out.clone());
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