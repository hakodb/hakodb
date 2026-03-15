use std::fs::File;
use std::io::{BufReader, Read};

use crate::index::composite::manager::CompositeIndexManager;

use super::index_log::{DELETE, INSERT};

pub fn replay_log(path: &str, manager: &mut CompositeIndexManager) -> std::io::Result<()> {
    let file = match File::open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    let mut reader = BufReader::new(file);
    loop {
        let mut op = [0u8; 1];
        if reader.read(&mut op)? == 0 {
            break;
        }

        let mut id = [0u8; 4];
        reader.read_exact(&mut id)?;
        let index_id = u32::from_be_bytes(id);

        let mut key_len = [0u8; 2];
        reader.read_exact(&mut key_len)?;
        let key_len = u16::from_be_bytes(key_len) as usize;

        let mut key = vec![0; key_len];
        reader.read_exact(&mut key)?;

        let mut doc_len = [0u8; 2];
        reader.read_exact(&mut doc_len)?;
        let doc_len = u16::from_be_bytes(doc_len) as usize;

        let mut doc = vec![0; doc_len];
        reader.read_exact(&mut doc)?;
        let doc_id = String::from_utf8_lossy(&doc).to_string();

        if let Some(index) = manager.get_mut(index_id) {
            match op[0] {
                INSERT => {
                    index.tree.insert(key, doc_id);
                }
                DELETE => {
                    index.tree.remove(&key);
                }
                _ => {}
            }
        }
    }
    Ok(())
}
