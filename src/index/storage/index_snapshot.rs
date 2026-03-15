use std::fs::File;
use std::io::{BufWriter, Write};

use crate::index::composite::composite_index::CompositeIndex;

pub fn write_snapshot(path: &str, index: &CompositeIndex) -> std::io::Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    writer.write_all(&index.definition.id.to_be_bytes())?;
    writer.write_all(&(index.tree.len() as u64).to_be_bytes())?;
    for (key, doc) in &index.tree {
        writer.write_all(&(key.len() as u16).to_be_bytes())?;
        writer.write_all(key)?;
        writer.write_all(&(doc.len() as u16).to_be_bytes())?;
        writer.write_all(doc.as_bytes())?;
    }
    writer.flush()
}
