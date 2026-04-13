use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::error::{FireLiteError, Result};

pub struct BlobManager {
    file: Arc<File>,
    pub blob_size: AtomicU64,
}

impl BlobManager {
    pub fn new(file: Arc<File>, initial_size: u64) -> Self {
        Self {
            file,
            blob_size: AtomicU64::new(initial_size),
        }
    }

    pub fn file(&self) -> Arc<File> {
        Arc::clone(&self.file)
    }

    pub fn reserve_raw(&self, len: u32) -> u64 {
        self.blob_size.fetch_add(len as u64, Ordering::SeqCst)
    }

    pub fn reserve_len(&self, len: usize) -> u64 {
        self.blob_size.fetch_add(len as u64, Ordering::SeqCst)
    }

    pub fn set_size(&self, size: u64) {
        self.blob_size.store(size, Ordering::Release);
    }

    pub fn read_at(&self, offset: u64, len: u32) -> Result<Vec<u8>> {
        // CRASH SAFETY: If the offset is MAX, the blob was lazily written 
        // and lost in a power failure. Gracefully return an empty payload.
        if offset == u64::MAX {
            return Ok(Vec::new());
        }

        let mut buf = vec![0u8; len as usize];
        
        // Swallowing the OS error with `let _ =` ensures that if the file 
        // hasn't synced the new length yet, it doesn't break the database.
        #[cfg(unix)] {
            use std::os::unix::fs::FileExt;
            let _ = self.file.read_exact_at(&mut buf, offset);
        }
        #[cfg(windows)] {
            use std::os::windows::fs::FileExt;
            let _ = self.file.seek_read(&mut buf, offset);
        }
        
        Ok(buf)
    }

    pub fn write_at(&self, buf: &[u8], offset: u64) -> Result<()> {
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file
                .write_all_at(buf, offset)
                .map_err(|e| FireLiteError::StorageError(format!("Blob IO fail: {e}")))?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.file
                .seek_write(buf, offset)
                .map_err(|e| FireLiteError::StorageError(format!("Blob IO fail: {e}")))?;
        }
        Ok(())
    }

    pub fn extract_blobs(
        &self,
        collection: &str,
        key: &str,
        doc: &mut FireLiteDoc,
        threshold: usize,
    ) -> Vec<BlobWork> {
        let mut work_items = Vec::new();
        let mut has_blobs = false;

        for (_, value) in &mut doc.fields {
            let len = value.len_bytes();
            if len > threshold {
                let raw = match value {
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Binary(b) => b.clone(),
                    _ => continue,
                };

                let offset = self.reserve_raw(len as u32);
                
                work_items.push(BlobWork::PutRaw {
                    collection: collection.to_string(),
                    key: key.to_string(),
                    skeleton: Vec::new(), 
                    offset,
                    data: Arc::new(raw),
                    timestamp: doc._time,
                    len: len as u32     
                });

                *value = Value::BlobLink { offset, len: len as u32 };
                has_blobs = true;
            }
        }

        if has_blobs {
            if let Some(BlobWork::PutRaw { skeleton, .. }) = work_items.last_mut() {
                *skeleton = doc.encode();
            }
        }
        work_items
    }

    pub fn extract_blobs_raw(
        &self,
        collection: &str,
        key: &str,
        doc: &mut FireLiteDoc,
        threshold: usize,
    ) -> Vec<BlobWork> {
        // Functionally identical to extract_blobs but distinct for raw integrations
        self.extract_blobs(collection, key, doc, threshold)
    }
    
    pub fn extract_patch_blobs(
        &self,
        collection: &str,
        doc: &mut FireLiteDoc,
        updates: Vec<(String, Value)>,
        threshold: usize,
    ) -> Vec<BlobWork> {
        let mut work_items = Vec::new();
        for (k, mut v) in updates {
            let len = v.len_bytes();
            if len > threshold {
                let raw = match &v {
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Binary(b) => b.clone(),
                    _ => { doc.insert(k, v); continue; }
                };
                
                let payload_len = len as u32;
                let offset = self.reserve_raw(payload_len);
                
                work_items.push(BlobWork::PutRaw {
                    collection: collection.to_string(),
                    key: String::new(), 
                    skeleton: Vec::new(), 
                    offset,
                    data: Arc::new(raw),
                    timestamp: doc._time,
                    len: payload_len
                });
                v = Value::BlobLink { offset, len: payload_len };
            }
            doc.insert(k, v);
        }
        work_items
    }

    pub fn extract_blobs_placeholder(&self, _col: &str, doc: &mut FireLiteDoc, threshold: usize) -> Vec<BlobWork> {
        for (_, value) in &mut doc.fields {
            if value.len_bytes() > threshold {
                *value = Value::BlobLink { offset: u64::MAX, len: value.len_bytes() as u32 }; 
            }
        }
        vec![]
    }
}

#[derive(Debug, Clone)]
pub enum BlobWork {
    PutRaw {
        collection: String,
        key: String,
        skeleton: Vec<u8>,
        offset: u64,
        data: Arc<Vec<u8>>,
        timestamp: i64,
        len: u32,
    },
    Put {
        collection: String,
        key: String,
        data: Arc<Vec<u8>>,
    },
}