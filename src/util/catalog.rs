use hashbrown::HashMap;
use std::sync::{RwLock, atomic::{AtomicU32, AtomicU16, Ordering}};
use std::path::{Path, PathBuf};
use std::io::{Read, Write};
use std::fs;
use std::sync::Arc;

pub struct Catalog {
    path: PathBuf,
    pub(crate) root_path: PathBuf,
    // Unified map: "c:name" -> col_id, "k:name" -> key_id
    data: RwLock<HashMap<String, u32>>,
    rev_keys: RwLock<HashMap<u16, Arc<str>>>,
    next_col_id: AtomicU32,
    next_key_id: AtomicU16,
}

impl Catalog {
    pub fn load(root: &Path) -> Self {
        let path = root.join("catalog.dat");
        let mut map = HashMap::new();
        let mut max_c = 0;
        let mut max_k = 0;

        // 1. Try to load existing catalog
        if path.exists() {
            if let Ok(mut file) = fs::File::open(&path) {
                let mut buf = Vec::new();
                if file.read_to_end(&mut buf).is_ok() {
                    Self::parse_catalog_bytes(&buf, &mut map, &mut max_c, &mut max_k);
                }
            }
        }

        let mut rev_keys = HashMap::new();
        for (name, id) in &map {
            if name.starts_with("k:") {
                rev_keys.insert(*id as u16, Arc::from(&name[2..]));
            }
        }

        Self {
            path,
            root_path: root.to_path_buf(),
            data: RwLock::new(map),
            rev_keys: RwLock::new(rev_keys),
            next_col_id: AtomicU32::new(max_c),
            next_key_id: AtomicU16::new(max_k),
        }
    }

    pub fn get_collection_path(&self, collection: &str) -> PathBuf {
        let folder_name = self.get_folder_name(collection);
        self.root_path.join(folder_name)
    }

    pub fn recover_from_disk(&self) -> Vec<(String, u32)> {
        let mut recovered = Vec::new();
        if let Ok(entries) = fs::read_dir(&self.root_path) {
            for entry in entries.flatten() {
                let path = entry.path();
                if path.is_dir() {
                    let folder_name = entry.file_name().to_string_lossy().to_string();
                    if folder_name.starts_with('c') {
                        let id_str = &folder_name[1..];
                        if let Ok(id) = id_str.parse::<u32>() {
                            let ident_path = path.join("identity.bin");
                            if let Ok(real_name) = fs::read_to_string(&ident_path) {
                                let key = format!("c:{}", real_name);
                                
                                let mut write = self.data.write().unwrap();
                                write.entry(key).or_insert(id);
                                
                                // Update atomic counter if we found a higher ID
                                let current_max = self.next_col_id.load(Ordering::SeqCst);
                                if id >= current_max {
                                    self.next_col_id.store(id + 1, Ordering::SeqCst);
                                }
                                
                                recovered.push((real_name, id));
                            } 
                            // else {
                            //     // GHOST: No identity file. Delete it in background.
                            //     let _ = fs::remove_dir_all(path);
                            // }
                        }
                    }
                }
            }
        }
        recovered
    }

    fn parse_catalog_bytes(buf: &[u8], map: &mut HashMap<String, u32>, max_c: &mut u32, max_k: &mut u16) {
        let mut pos = 0;
        while pos + 4 <= buf.len() {
            let name_len = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
            pos += 4;
            if pos + name_len + 4 > buf.len() { break; }
            if let Ok(name) = std::str::from_utf8(&buf[pos..pos+name_len]) {
                pos += name_len;
                let id = u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap());
                pos += 4;
                
                if name.starts_with("c:") {
                    if id >= *max_c { *max_c = id + 1; }
                } else if name.starts_with("k:") {
                    if id >= *max_k as u32 { *max_k = id as u16 + 1; }
                }
                map.insert(name.to_string(), id);
            }
        }
    }

    pub fn get_folder_name(&self, collection: &str) -> String {
        let key = format!("c:{}", collection);
        
        // Check cache first
        {
            let read = self.data.read().unwrap();
            if let Some(id) = read.get(&key) {
                return format!("c{}", id);
            }
        }
        
        // Not in catalog: Create it safely
        let mut write = self.data.write().unwrap();
        
        // Double-check inside write lock
        if let Some(id) = write.get(&key) {
            return format!("c{}", id);
        }

        let id = self.next_col_id.fetch_add(1, Ordering::SeqCst);
        let folder_name = format!("c{}", id);
        let folder_path = self.root_path.join(&folder_name);

        // ATOMICITY STEP: 
        // 1. Create Folder
        let _ = fs::create_dir_all(&folder_path);
        // 2. Write identity.bin IMMEDIATELY
        let ident_path = folder_path.join("identity.bin");
        let tmp_ident = folder_path.join("~identity.tmp"); 
        // let _ = fs::write(ident_path, collection);
        let _ = fs::write(&tmp_ident, collection);
        let _ = fs::rename(tmp_ident, ident_path); 

        write.insert(key, id);
        
        // Persistent save immediately for new collections
        drop(write);
        self.save();

        folder_name
    }

    pub fn get_key_id(&self, field_name: &str) -> u16 {
        let key = format!("k:{}", field_name);
        {
            let read = self.data.read().unwrap();
            if let Some(id) = read.get(&key) { return *id as u16; }
        }
        let mut write = self.data.write().unwrap();
        let id = self.next_key_id.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        write.insert(key, id as u32);
        self.rev_keys.write().unwrap().insert(id, Arc::from(field_name));
        id
    }

    // NEW: Fast resolution that returns a shared pointer
    pub fn resolve_key_shared(&self, id: u16) -> Arc<str> {
        if let Some(existing) = self.rev_keys.read().unwrap().get(&id) {
            return Arc::clone(existing);
        }
        // Fallback for unknown IDs
        Arc::from(format!("$id:{}", id))
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
        let tmp_path = self.path.with_extension("tmp");
        if let Ok(mut file) = fs::File::create(&tmp_path) {
            let map = self.data.read().unwrap();
            for (name, id) in map.iter() {
                let name_bytes = name.as_bytes();
                let _ = file.write_all(&(name_bytes.len() as u32).to_le_bytes());
                let _ = file.write_all(name_bytes);
                let _ = file.write_all(&id.to_le_bytes());
            }
            let _ = file.sync_all();
            drop(file);
            
            // Atomic Rename: overwrites catalog.dat only if tmp was written successfully
            let _ = fs::rename(tmp_path, &self.path);
        }
    }
}