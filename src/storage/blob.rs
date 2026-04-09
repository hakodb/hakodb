use std::fs::File;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use crate::document::firelite_doc::FireLiteDoc;
use crate::document::value::Value;
use crate::error::{FireLiteError, Result};

use super::crypto::EncryptionContext;

pub struct BlobManager {
    file: Arc<File>,
    encryption: Option<EncryptionContext>,
    compression_enabled: bool,
    compression_level: i32,
    pub blob_size: AtomicU64,
}

impl BlobManager {
    pub fn new(
        file: Arc<File>,
        encryption: Option<EncryptionContext>,
        compression_enabled: bool,
        compression_level: i32,
        initial_size: u64,
    ) -> Self {
        Self {
            file,
            encryption,
            compression_enabled,
            compression_level,
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
        let mut buf = vec![0u8; len as usize];
        #[cfg(unix)]
        {
            use std::os::unix::fs::FileExt;
            self.file.read_exact_at(&mut buf, offset)?;
        }
        #[cfg(windows)]
        {
            use std::os::windows::fs::FileExt;
            self.file.seek_read(&mut buf, offset)?;
        }

        if let Some(enc) = &self.encryption {
            enc.decrypt(&buf)
        } else {
            Ok(buf)
        }
    }

    pub fn prepare_payload(&self, data: &[u8]) -> Vec<u8> {
        let mut payload = data.to_vec();
        if self.compression_enabled && payload.len() > 2048 {
            if let Ok(c) = zstd::encode_all(&payload[..], self.compression_level) {
                if c.len() < payload.len() {
                    payload = c;
                }
            }
        }
        if let Some(enc) = &self.encryption {
            if let Ok(encrypted) = enc.encrypt(&payload) {
                payload = encrypted;
            }
        }
        payload
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
        doc: &mut FireLiteDoc,
        threshold: usize,
        now: i64,
    ) -> Vec<BlobWork> {
        let mut work_items = Vec::new();
        for (_, value) in &mut doc.fields {
            if matches!(value, Value::ServerTimestamp) {
                *value = Value::Timestamp(now);
            }
            let len = value.len_bytes();
            if len > threshold {
                let raw = match value {
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Binary(b) => b.clone(),
                    _ => continue,
                };
                let payload = self.prepare_payload(&raw);
                let payload_len = payload.len() as u32;
                let data_arc = Arc::new(payload);
                let offset = self.reserve_raw(payload_len);
                work_items.push(BlobWork::PutRaw {
                    collection: collection.to_string(),
                    offset,
                    data: data_arc,
                });
                *value = Value::BlobLink {
                    offset,
                    len: payload_len,
                };
            }
        }
        work_items
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
            if v.len_bytes() > threshold {
                let raw = match &v {
                    Value::String(s) => s.as_bytes().to_vec(),
                    Value::Binary(b) => b.clone(),
                    _ => {
                        doc.insert(k, v);
                        continue;
                    }
                };
                let payload = self.prepare_payload(&raw);
                let payload_len = payload.len() as u32;
                let data_arc = Arc::new(payload);
                let offset = self.reserve_raw(payload_len);
                work_items.push(BlobWork::PutRaw {
                    collection: collection.to_string(),
                    offset,
                    data: data_arc,
                });
                v = Value::BlobLink {
                    offset,
                    len: payload_len,
                };
            }
            doc.insert(k, v);
        }
        work_items
    }
}

#[derive(Debug, Clone)]
pub enum BlobWork {
    PutRaw {
        collection: String,
        offset: u64,
        data: Arc<Vec<u8>>,
    },
    Put {
        collection: String,
        key: String,
        data: Arc<Vec<u8>>,
    },
}
