use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Seek, SeekFrom, Write};
use std::path::Path;

use crc32fast::Hasher;
use memmap2::Mmap;

use crate::error::FireLiteError;

pub type Result<T> = std::result::Result<T, FireLiteError>;

/// Record structure returned by reads
#[derive(Debug, Clone)]
pub struct Record {
    pub key: String,
    pub doc: Vec<u8>,
}

/// Binary layout
///
/// [record_len u32]
/// [crc32 u32]
/// [key_len u32]
/// [key bytes]
/// [doc_len u32]
/// [doc bytes]
///
pub struct Log {
    file: File,
    writer: BufWriter<File>,
    mmap: Option<Mmap>,
}

impl Log {
    /// Open log file
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .open(path)?;

        let writer = BufWriter::new(file.try_clone()?);

        Ok(Self {
            file,
            writer,
            mmap: None,
        })
    }

    /// Append record to log
    pub fn append(&mut self, record: &Record) -> Result<u64> {
        let encoded = encode_record(record);

        let offset = self.file.seek(SeekFrom::End(0))?;

        self.writer.write_all(&encoded)?;

        Ok(offset)
    }

    /// Flush buffered writes
    pub fn flush(&mut self) -> Result<()> {
        self.writer.flush()?;
        self.file.sync_data()?;

        // refresh mmap after writes
        self.mmap = None;

        Ok(())
    }

    /// Ensure mmap exists
    fn ensure_mmap(&mut self) -> Result<()> {
        if self.mmap.is_none() {
            let mmap = unsafe { Mmap::map(&self.file)? };
            self.mmap = Some(mmap);
        }

        Ok(())
    }

    /// Read record at offset
    pub fn read_at(&mut self, offset: u64) -> Result<Record> {
        self.ensure_mmap()?;

        let mmap = self.mmap.as_ref().unwrap();

        let mut pos = offset as usize;

        if pos + 4 > mmap.len() {
            return Err(FireLiteError::InvalidRecord);
        }

        let record_len =
            u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap()) as usize;

        pos += 4;

        let crc =
            u32::from_le_bytes(mmap[pos..pos + 4].try_into().unwrap());

        pos += 4;

        let record_bytes = &mmap[pos..pos + record_len];

        verify_crc(record_bytes, crc)?;

        decode_record(record_bytes)
    }

    /// Scan entire log and rebuild index
    pub fn scan(&mut self) -> Result<Vec<(String, u64)>> {
        self.ensure_mmap()?;

        let mmap = self.mmap.as_ref().unwrap();

        let mut offset = 0usize;
        let mut result = Vec::new();

        while offset + 8 < mmap.len() {
            let record_len =
                u32::from_le_bytes(mmap[offset..offset + 4].try_into().unwrap())
                    as usize;

            let crc =
                u32::from_le_bytes(mmap[offset + 4..offset + 8].try_into().unwrap());

            let start = offset + 8;
            let end = start + record_len;

            if end > mmap.len() {
                break;
            }

            let record_bytes = &mmap[start..end];

            if verify_crc(record_bytes, crc).is_err() {
                break;
            }

            if let Ok(record) = decode_record(record_bytes) {
                result.push((record.key, offset as u64));
            }

            offset = end;
        }

        Ok(result)
    }
}

/// Encode record
fn encode_record(record: &Record) -> Vec<u8> {
    let key_bytes = record.key.as_bytes();
    let doc_bytes = &record.doc;

    let key_len = key_bytes.len() as u32;
    let doc_len = doc_bytes.len() as u32;

    let record_len = 4 + key_len + 4 + doc_len;

    let mut record_buf = Vec::with_capacity(record_len as usize);

    record_buf.extend(&key_len.to_le_bytes());
    record_buf.extend(key_bytes);
    record_buf.extend(&doc_len.to_le_bytes());
    record_buf.extend(doc_bytes);

    let mut hasher = Hasher::new();
    hasher.update(&record_buf);
    let crc = hasher.finalize();

    let mut buf = Vec::with_capacity((record_len + 8) as usize);

    buf.extend(&(record_len as u32).to_le_bytes());
    buf.extend(&crc.to_le_bytes());
    buf.extend(record_buf);

    buf
}

/// Decode record
fn decode_record(buf: &[u8]) -> Result<Record> {
    let mut pos = 0;

    let key_len =
        u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;

    pos += 4;

    let key = String::from_utf8(buf[pos..pos + key_len].to_vec())
        .map_err(|_| FireLiteError::InvalidRecord)?;

    pos += key_len;

    let doc_len =
        u32::from_le_bytes(buf[pos..pos + 4].try_into().unwrap()) as usize;

    pos += 4;

    let doc = buf[pos..pos + doc_len].to_vec();

    Ok(Record { key, doc })
}

/// Verify CRC integrity
fn verify_crc(data: &[u8], expected: u32) -> Result<()> {
    let mut hasher = Hasher::new();
    hasher.update(data);
    let crc = hasher.finalize();

    if crc != expected {
        return Err(FireLiteError::InvalidRecord);
    }

    Ok(())
}
