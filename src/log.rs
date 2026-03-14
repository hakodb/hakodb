```rust
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::Path;

use crate::error::FireLiteError;

pub type Result<T> = std::result::Result<T, FireLiteError>;

/// Log record layout
///
/// [record_len: u32]
/// [key_len: u32]
/// [key bytes]
/// [doc_len: u32]
/// [doc bytes]
///
#[derive(Debug, Clone)]
pub struct Record {
    pub key: String,
    pub doc: Vec<u8>,
}

pub struct Log {
    file: File,
}

impl Log {
    /// Open or create a log file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        Ok(Self { file })
    }

    /// Append a record to the end of the log
    /// Returns the offset where the record was written
    pub fn append(&mut self, record: &Record) -> Result<u64> {
        let encoded = encode_record(record);

        let offset = self.file.seek(SeekFrom::End(0))?;

        self.file.write_all(&encoded)?;
        self.file.flush()?;

        Ok(offset)
    }

    /// Read record at specific offset
    pub fn read_at(&mut self, offset: u64) -> Result<Record> {
        self.file.seek(SeekFrom::Start(offset))?;

        // read record length
        let mut len_buf = [0u8; 4];
        self.file.read_exact(&mut len_buf)?;

        let record_len = u32::from_le_bytes(len_buf) as usize;

        // read remaining record bytes
        let mut buf = vec![0u8; record_len];
        self.file.read_exact(&mut buf)?;

        decode_record(&buf)
    }

    /// Scan the log and rebuild the primary index
    ///
    /// Returns Vec<(key, offset)>
    pub fn scan(&mut self) -> Result<Vec<(String, u64)>> {
        let mut index = Vec::new();

        self.file.seek(SeekFrom::Start(0))?;

        let mut offset = 0u64;

        loop {
            let mut len_buf = [0u8; 4];

            match self.file.read_exact(&mut len_buf) {
                Ok(_) => {}
                Err(_) => break,
            }

            let record_len = u32::from_le_bytes(len_buf) as usize;

            let mut buf = vec![0u8; record_len];

            if self.file.read_exact(&mut buf).is_err() {
                break;
            }

            if let Ok(record) = decode_record(&buf) {
                index.push((record.key, offset));
            }

            offset += 4 + record_len as u64;
        }

        Ok(index)
    }
}

/// Encode record into binary format
fn encode_record(record: &Record) -> Vec<u8> {
    let key_bytes = record.key.as_bytes();
    let doc_bytes = &record.doc;

    let key_len = key_bytes.len() as u32;
    let doc_len = doc_bytes.len() as u32;

    let record_len = 4 + key_len + 4 + doc_len;

    let mut buf = Vec::with_capacity((record_len + 4) as usize);

    buf.extend(&(record_len as u32).to_le_bytes());
    buf.extend(&key_len.to_le_bytes());
    buf.extend(key_bytes);
    buf.extend(&doc_len.to_le_bytes());
    buf.extend(doc_bytes);

    buf
}

/// Decode record from binary format
fn decode_record(buf: &[u8]) -> Result<Record> {
    let mut pos = 0;

    // key_len
    let key_len = u32::from_le_bytes(
        buf[pos..pos + 4]
            .try_into()
            .map_err(|_| FireLiteError::InvalidRecord)?,
    ) as usize;

    pos += 4;

    let key = String::from_utf8(
        buf[pos..pos + key_len].to_vec(),
    )
    .map_err(|_| FireLiteError::InvalidRecord)?;

    pos += key_len;

    // doc_len
    let doc_len = u32::from_le_bytes(
        buf[pos..pos + 4]
            .try_into()
            .map_err(|_| FireLiteError::InvalidRecord)?,
    ) as usize;

    pos += 4;

    let doc = buf[pos..pos + doc_len].to_vec();

    Ok(Record { key, doc })
}
```
