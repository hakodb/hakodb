use std::fs::OpenOptions;
use std::io::{BufReader, Read};
use std::sync::Arc;

use crate::index::composite::manager::CompositeIndexManager;
use super::index_log::{DELETE, INSERT};

pub fn replay_log(path: &str, manager: &mut CompositeIndexManager) -> std::io::Result<()> {
    // Removed 'mut' as set_len() and metadata() only require a shared reference (&self)
    let file = match OpenOptions::new().read(true).write(true).open(path) {
        Ok(file) => file,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(e) => return Err(e),
    };

    let mut last_valid_pos: u64 = 0;
    let file_len = file.metadata()?.len();
    let mut reader = BufReader::new(&file);

    loop {
        let mut op = [0u8; 1];
        if reader.read(&mut op)? == 0 {
            break;
        }

        let mut try_read = |buf: &mut [u8]| -> std::io::Result<bool> {
            match reader.read_exact(buf) {
                Ok(_) => Ok(true),
                Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => Ok(false),
                Err(e) => Err(e),
            }
        };

        let mut id = [0u8; 4];
        if !try_read(&mut id)? { break; }
        let index_id = u32::from_be_bytes(id);

        let mut k_len_buf = [0u8; 2];
        if !try_read(&mut k_len_buf)? { break; }
        let key_len = u16::from_be_bytes(k_len_buf) as usize;

        let mut key = vec![0; key_len];
        if !try_read(&mut key)? { break; }

        let mut d_len_buf = [0u8; 2];
        if !try_read(&mut d_len_buf)? { break; }
        let doc_len = u16::from_be_bytes(d_len_buf) as usize;

        let mut doc = vec![0; doc_len];
        if !try_read(&mut doc)? { break; }
        let doc_id = String::from_utf8_lossy(&doc).to_string();

        if let Some(index) = manager.get_mut(index_id) {
            match op[0] {
                INSERT => {
                    index.tree.insert(key.into(), Arc::from(doc_id));
                }
                DELETE => {
                    index.tree.remove(&key[..]);
                }
                _ => {}
            }
        }
        
        last_valid_pos += 1 + 4 + 2 + key_len as u64 + 2 + doc_len as u64;
    }

    if last_valid_pos < file_len {
        file.set_len(last_valid_pos)?;
    }

    Ok(())
}