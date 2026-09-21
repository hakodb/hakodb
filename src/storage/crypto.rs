use std::sync::atomic::{AtomicU64, Ordering};
use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use sha2::{Digest, Sha256};
use rand::RngCore; 
use std::sync::Arc;
use crate::error::{HakoError, Result};

const NONCE_LEN: usize = 12;

#[derive(Clone)]
pub struct EncryptionContext {
    cipher: ChaCha20Poly1305,
    prefix: u32,
    counter: Arc<AtomicU64>,
}

impl EncryptionContext {
    pub fn from_secret(secret: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        let key_bytes = hasher.finalize();
        let key = Key::from_slice(&key_bytes);

        // One-time RNG call to establish a unique session prefix
        let mut rng = rand::thread_rng();
        let prefix = rng.next_u32();

        Self {
            cipher: ChaCha20Poly1305::new(key),
            prefix,
            counter: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce_bytes = [0u8; NONCE_LEN];
        
        // Atomically increment the counter (Lock-free and extremely fast)
        let count = self.counter.fetch_add(1, Ordering::Relaxed);

        // Nonce = [4-byte random prefix][8-byte incrementing counter]
        nonce_bytes[0..4].copy_from_slice(&self.prefix.to_be_bytes());
        nonce_bytes[4..12].copy_from_slice(&count.to_be_bytes());

        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce_bytes), plaintext)
            .map_err(|_| HakoError::StorageError("encryption failed".into()))?;

        // Pre-allocate to avoid multiple small re-allocations
        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend_from_slice(&nonce_bytes);
        out.extend_from_slice(&ciphertext);
        Ok(out)
    }

    pub fn decrypt(&self, input: &[u8]) -> Result<Vec<u8>> {
        if input.len() < NONCE_LEN {
            return Err(HakoError::Corrupt(
                "ciphertext shorter than nonce".into(),
            ));
        }
        let (nonce, ciphertext) = input.split_at(NONCE_LEN);
        self.cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| HakoError::Corrupt("decryption failed".into()))
    }
}
