use chacha20poly1305::aead::{Aead, KeyInit};
use chacha20poly1305::{ChaCha20Poly1305, Key, Nonce};
use rand::RngCore;
use sha2::{Digest, Sha256};

use crate::error::{FireLiteError, Result};

const NONCE_LEN: usize = 12;

#[derive(Clone)]
pub struct EncryptionContext {
    cipher: ChaCha20Poly1305,
}

impl EncryptionContext {
    pub fn from_secret(secret: &str) -> Self {
        let mut hasher = Sha256::new();
        hasher.update(secret.as_bytes());
        let key_bytes = hasher.finalize();
        let key = Key::from_slice(&key_bytes);
        Self {
            cipher: ChaCha20Poly1305::new(key),
        }
    }

    pub fn encrypt(&self, plaintext: &[u8]) -> Result<Vec<u8>> {
        let mut nonce = [0u8; NONCE_LEN];
        // rand::rngs::OsRng.fill_bytes(&mut nonce);
        rand::thread_rng().fill_bytes(&mut nonce);
        let ciphertext = self
            .cipher
            .encrypt(Nonce::from_slice(&nonce), plaintext)
            .map_err(|_| FireLiteError::StorageError("encryption failed".into()))?;

        let mut out = Vec::with_capacity(NONCE_LEN + ciphertext.len());
        out.extend(nonce);
        out.extend(ciphertext);
        Ok(out)
    }

    pub fn decrypt(&self, input: &[u8]) -> Result<Vec<u8>> {
        if input.len() < NONCE_LEN {
            return Err(FireLiteError::Corrupt(
                "ciphertext shorter than nonce".into(),
            ));
        }
        let (nonce, ciphertext) = input.split_at(NONCE_LEN);
        self.cipher
            .decrypt(Nonce::from_slice(nonce), ciphertext)
            .map_err(|_| FireLiteError::Corrupt("decryption failed".into()))
    }
}
