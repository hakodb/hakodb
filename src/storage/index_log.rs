use std::fs::{File,OpenOptions};
use std::io::{Write,BufWriter};

pub const INSERT:u8 = 1;
pub const DELETE:u8 = 2;

pub struct IndexLog {

    writer: BufWriter<File>

}

impl IndexLog {

    pub fn open(path:&str)->std::io::Result<Self>{

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)?;

        Ok(
            Self{
                writer:BufWriter::new(file)
            }
        )

    }

    pub fn append_insert(
        &mut self,
        index_id:u32,
        key:&[u8],
        doc:&str
    )->std::io::Result<()>{

        self.writer.write_all(&[INSERT])?;

        self.writer.write_all(&index_id.to_be_bytes())?;

        self.writer.write_all(&(key.len() as u16).to_be_bytes())?;
        self.writer.write_all(key)?;

        self.writer.write_all(&(doc.len() as u16).to_be_bytes())?;
        self.writer.write_all(doc.as_bytes())?;

        Ok(())

    }

    pub fn append_delete(
        &mut self,
        index_id:u32,
        key:&[u8],
        doc:&str
    )->std::io::Result<()>{

        self.writer.write_all(&[DELETE])?;

        self.writer.write_all(&index_id.to_be_bytes())?;

        self.writer.write_all(&(key.len() as u16).to_be_bytes())?;
        self.writer.write_all(key)?;

        self.writer.write_all(&(doc.len() as u16).to_be_bytes())?;
        self.writer.write_all(doc.as_bytes())?;

        Ok(())

    }

    pub fn flush(&mut self)->std::io::Result<()>{

        self.writer.flush()

    }

}
