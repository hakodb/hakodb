use std::fs::{File, OpenOptions};
use std::io::{BufWriter, Write, Seek, SeekFrom};

pub const INSERT: u8 = 1;
pub const DELETE: u8 = 2;

pub struct IndexLog {
    writer: BufWriter<File>,
}

impl IndexLog {
    pub fn open(path: &str) -> std::io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn append(&mut self, op: u8, index_id: u32, key: &[u8], doc: &str) -> std::io::Result<()> {
        self.writer.write_all(&[op])?;
        self.writer.write_all(&index_id.to_be_bytes())?;
        self.writer.write_all(&(key.len() as u16).to_be_bytes())?;
        self.writer.write_all(key)?;
        self.writer.write_all(&(doc.len() as u16).to_be_bytes())?;
        self.writer.write_all(doc.as_bytes())?;
        // self.writer.flush()?;
        Ok(())
    }

    pub fn flush(&mut self) -> std::io::Result<()> {
        self.writer.flush()
    }

        /// Truncates the log file to 0 bytes.
    pub fn reset(&mut self) -> std::io::Result<()> {
        let file = self.writer.get_mut();
        file.set_len(0)?;
        file.seek(SeekFrom::Start(0))?;
        Ok(())
    }

}
