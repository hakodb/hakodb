// src/util/catalog.rs
use hashbrown::HashMap;
use std::sync::RwLock;
use std::path::{Path, PathBuf};
use std::io::{Read, Write};

pub struct Catalog {
    path: PathBuf,
    // Unified map: "c:name" -> col_id, "k:name" -> key_id
    data: RwLock<HashMap<String, u32>>,
    next_col_id: std::sync::atomic::AtomicU32,
    next_key_id: std::sync::atomic::AtomicU16,
}

impl Catalog {
    pub fn load(root: &Path) -> Self {
        let path = root.join("catalog.dat");
        let mut map = HashMap::new();
        let mut max_c = 0;
        let mut max_k = 0;

        if path.exists() {
            if let Ok(mut file) = std::fs::File::open(&path) {
                let mut buf = Vec::new();
                if file.read_to_end(&mut buf).is_ok() {
                    let mut pos = 0;
                    while pos + 4 <= buf.len() {
                        let name_len = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
                        pos += 4;
                        if let Ok(name) = std::str::from_utf8(&buf[pos..pos+name_len]) {
                            pos += name_len;
                            let id = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap());
                            pos += 4;
                            
                            if name.starts_with("c:") {
                                if id >= max_c { max_c = id + 1; }
                            } else if name.starts_with("k:") {
                                if id >= max_k as u32 { max_k = id as u16 + 1; }
                            }
                            map.insert(name.to_string(), id);
                        }
                    }
                }
            }
        }

        Self {
            path,
            data: RwLock::new(map),
            next_col_id: std::sync::atomic::AtomicU32::new(max_c),
            next_key_id: std::sync::atomic::AtomicU16::new(max_k),
        }
    }

    pub fn get_folder_name(&self, collection: &str) -> String {
        let key = format!("c:{}", collection);
        {
            let read = self.data.read().unwrap();
            if let Some(id) = read.get(&key) {
                return format!("c{}", id);
            }
        }
        
        let mut write = self.data.write().unwrap();
        let id = self.next_col_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        write.insert(key, id);
        format!("c{}", id)
    }

    pub fn get_key_id(&self, field_name: &str) -> u16 {
        let key = format!("k:{}", field_name);
        {
            let read = self.data.read().unwrap();
            if let Some(id) = read.get(&key) {
                return *id as u16;
            }
        }
        
        let mut write = self.data.write().unwrap();
        let id = self.next_key_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        write.insert(key, id as u32);
        id
    }

    pub fn resolve_key(&self, id: u16) -> Option<String> {
        let read = self.data.read().unwrap();
        let target_val = id as u32;
        for (name, val) in read.iter() {
            if name.starts_with("k:") && *val == target_val {
                return Some(name[2..].to_string());
            }
        }
        None
    }

    pub fn get_all_collections(&self) -> Vec<String> {
        self.data.read().unwrap().keys()
            .filter(|k| k.starts_with("c:"))
            .map(|k| k[2..].to_string())
            .collect()
    }

    pub fn save(&self) {
        if let Ok(mut file) = std::fs::File::create(&self.path) {
            let map = self.data.read().unwrap();
            for (name, id) in map.iter() {
                let name_bytes = name.as_bytes();
                let _ = file.write_all(&(name_bytes.len() as u32).to_le_bytes());
                let _ = file.write_all(name_bytes);
                let _ = file.write_all(&id.to_le_bytes());
            }
            let _ = file.sync_all();
        }
    }
}