use std::collections::HashMap;
use std::path::Path;

use crate::error::FireLiteError;
use crate::storage::log::Log;

pub type Result<T> = std::result::Result<T, FireLiteError>;

const OP_INSERT: u8 = 1;
const OP_DELETE: u8 = 2;

#[derive(Clone)]
struct DocMeta {
    offset: u64,
}

pub struct FireLiteEngine {
    log: Log,

    /// collection_name -> collection_id
    collections: HashMap<String, u32>,

    /// collection_id -> name
    reverse_collections: HashMap<u32, String>,

    /// next collection id
    next_collection_id: u32,

    /// (collection_id, doc_id) -> record offset
    index: HashMap<(u32, String), DocMeta>,
}

impl FireLiteEngine {

    pub fn open<P: AsRef<Path>>(path: P) -> Result<Self> {

        let mut log = Log::open(path)?;

        let mut engine = Self {
            log,
            collections: HashMap::new(),
            reverse_collections: HashMap::new(),
            next_collection_id: 1,
            index: HashMap::new(),
        };

        engine.rebuild_index()?;

        Ok(engine)
    }

    fn get_or_create_collection(&mut self, name: &str) -> u32 {

        if let Some(id) = self.collections.get(name) {
            return *id;
        }

        let id = self.next_collection_id;

        self.collections.insert(name.to_string(), id);
        self.reverse_collections.insert(id, name.to_string());

        self.next_collection_id += 1;

        id
    }

    pub fn insert(
        &mut self,
        collection: &str,
        doc_id: &str,
        document: Vec<u8>,
    ) -> Result<()> {

        let cid = self.get_or_create_collection(collection);

        let record = encode_record(
            cid,
            doc_id,
            OP_INSERT,
            &document
        );

        let offset = self.log.append_raw(&record)?;

        self.index.insert(
            (cid, doc_id.to_string()),
            DocMeta { offset },
        );

        Ok(())
    }

    pub fn get(
        &mut self,
        collection: &str,
        doc_id: &str,
    ) -> Result<Option<Vec<u8>>> {

        let cid = match self.collections.get(collection) {
            Some(v) => *v,
            None => return Ok(None),
        };

        let meta = match self.index.get(&(cid, doc_id.to_string())) {
            Some(v) => v,
            None => return Ok(None),
        };

        let record = self.log.read_raw(meta.offset)?;

        let decoded = decode_record(&record)?;

        if decoded.op == OP_DELETE {
            return Ok(None);
        }

        Ok(Some(decoded.document))
    }

    pub fn delete(
        &mut self,
        collection: &str,
        doc_id: &str,
    ) -> Result<()> {

        let cid = match self.collections.get(collection) {
            Some(v) => *v,
            None => return Ok(()),
        };

        let record = encode_record(
            cid,
            doc_id,
            OP_DELETE,
            &[]
        );

        self.log.append_raw(&record)?;

        self.index.remove(&(cid, doc_id.to_string()));

        Ok(())
    }

    fn rebuild_index(&mut self) -> Result<()> {

        let entries = self.log.scan_raw()?;

        for (offset, record_bytes) in entries {

            let record = decode_record(&record_bytes)?;

            let cid = record.collection_id;

            if !self.reverse_collections.contains_key(&cid) {
                let name = format!("collection_{}", cid);

                self.reverse_collections.insert(cid, name.clone());
                self.collections.insert(name, cid);
            }

            if record.op == OP_INSERT {

                self.index.insert(
                    (cid, record.doc_id.clone()),
                    DocMeta { offset },
                );

            } else {

                self.index.remove(&(cid, record.doc_id.clone()));
            }
        }

        Ok(())
    }
}

struct DecodedRecord {
    collection_id: u32,
    doc_id: String,
    op: u8,
    document: Vec<u8>,
}

fn encode_record(
    collection_id: u32,
    doc_id: &str,
    op: u8,
    doc: &[u8],
) -> Vec<u8> {

    use crc32fast::Hasher;

    let doc_id_bytes = doc_id.as_bytes();

    let mut payload = Vec::new();

    payload.extend(&collection_id.to_le_bytes());

    payload.extend(&(doc_id_bytes.len() as u16).to_le_bytes());
    payload.extend(doc_id_bytes);

    payload.push(op);

    payload.extend(&(doc.len() as u32).to_le_bytes());
    payload.extend(doc);

    let mut hasher = Hasher::new();
    hasher.update(&payload);

    let crc = hasher.finalize();

    let mut record = Vec::new();

    record.extend(&(payload.len() as u32).to_le_bytes());
    record.extend(&crc.to_le_bytes());
    record.extend(payload);

    record
}

fn decode_record(buf: &[u8]) -> Result<DecodedRecord> {

    let mut pos = 0;

    let collection_id =
        u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap());
    pos += 4;

    let doc_id_len =
        u16::from_le_bytes(buf[pos..pos+2].try_into().unwrap()) as usize;
    pos += 2;

    let doc_id =
        String::from_utf8(buf[pos..pos+doc_id_len].to_vec())
        .map_err(|_| FireLiteError::InvalidRecord)?;
    pos += doc_id_len;

    let op = buf[pos];
    pos += 1;

    let doc_len =
        u32::from_le_bytes(buf[pos..pos+4].try_into().unwrap()) as usize;
    pos += 4;

    let document = buf[pos..pos+doc_len].to_vec();

    Ok(DecodedRecord {
        collection_id,
        doc_id,
        op,
        document,
    })
}
