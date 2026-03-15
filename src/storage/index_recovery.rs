use std::fs::File;
use std::io::{Read,BufReader};

use crate::index::composite::manager::CompositeIndexManager;

use super::index_log::{INSERT,DELETE};

pub fn replay_log(
    path:&str,
    manager:&mut CompositeIndexManager
)->std::io::Result<()>{

    let file = File::open(path)?;

    let mut reader = BufReader::new(file);

    loop {

        let mut op=[0u8;1];

        if reader.read(&mut op)? == 0 {
            break;
        }

        let mut id_buf=[0u8;4];
        reader.read_exact(&mut id_buf)?;

        let index_id = u32::from_be_bytes(id_buf);

        let mut klen=[0u8;2];
        reader.read_exact(&mut klen)?;

        let key_len = u16::from_be_bytes(klen);

        let mut key=vec![0u8;key_len as usize];
        reader.read_exact(&mut key)?;

        let mut dlen=[0u8;2];
        reader.read_exact(&mut dlen)?;

        let doc_len = u16::from_be_bytes(dlen);

        let mut doc=vec![0u8;doc_len as usize];
        reader.read_exact(&mut doc)?;

        let doc_id = String::from_utf8(doc).unwrap();

        match op[0] {

            INSERT=>{
                manager.insert_raw(index_id,key,doc_id);
            }

            DELETE=>{
                manager.remove_raw(index_id,key);
            }

            _=>{}

        }

    }

    Ok(())

}
