use std::fs::File;
use std::io::{Write,BufWriter};

use crate::index::composite::composite_index::CompositeIndex;

pub fn write_snapshot(
    path:&str,
    index_id:u32,
    index:&CompositeIndex
)->std::io::Result<()>{

    let file = File::create(path)?;

    let mut writer = BufWriter::new(file);

    writer.write_all(&index_id.to_be_bytes())?;

    let size = index.tree.len() as u64;

    writer.write_all(&size.to_be_bytes())?;

    for (key,doc) in &index.tree {

        writer.write_all(&(key.len() as u16).to_be_bytes())?;
        writer.write_all(key)?;

        writer.write_all(&(doc.len() as u16).to_be_bytes())?;
        writer.write_all(doc.as_bytes())?;

    }

    writer.flush()?;

    Ok(())

}
