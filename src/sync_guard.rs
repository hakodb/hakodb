//! Fail-closed handshake guards for encrypted collections (sync Layer 0).
//!
//! # Posture (read this before changing anything here)
//!
//! Encryption at rest (WAL/segments) and sync-time plaintext are INDEPENDENT
//! properties in FireLite. Before this module, a node holding the at-rest
//! key would happily broadcast an encrypted collection's documents IN
//! PLAINTEXT to any peer — including peers that could never read that
//! collection locally — with zero signal. The cloud server additionally
//! persisted whatever it received, key or not.
//!
//! This module does NOT make the wire confidential. It converts *silent*
//! leaks into two explicit outcomes:
//! - peers proving the same at-rest key (matching fingerprint) keep syncing
//!   exactly as before (wire still plaintext between verified key-holders);
//! - everyone else is refused the encrypted collections, loudly.
//!
//! ## Rules (enforced identically on mesh and cloud, both directions)
//!
//! Let `E(C)` = "collection C is encrypted on THIS node"
//! (`FireLite::is_collection_encrypted`), `F` = this node's key fingerprint,
//! `P` = the peer's announced capabilities (`None` = never announced,
//! e.g. an older peer).
//!
//! - `caps_allow(E, F, P)` is true iff `!E`, or `P` presents `F` (non-zero).
//! - Sender side: skip collection C for peer P when `!caps_allow(...)`.
//! - Receiver side: drop collection C ops from sender P when
//!   `!caps_allow(...)`.
//! - A node with no key configured fingerprints as all-zeros, which never
//!   matches a real key. Plaintext collections are unaffected everywhere.
//!
//! ## Residual risks (deliberately NOT fixed here — see Layer 1 E2E)
//!
//! - A passive observer still sees plaintext between matched peers (no wire
//!   encryption; that needs TLS on mesh / E2E rooms).
//! - The cloud server operator still sees whatever clients send (they chose
//!   to trust that infra; the server only enforces the same endpoint rules
//!   when it itself holds keys).
//! - A peer on OLD code sends plaintext unconditionally; new receivers drop
//!   it for locally-encrypted collections, but for locally-PLAINTEXT
//!   collections there is nothing to distinguish — upgrade all peers.

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::{Duration, Instant};

/// Capability advertisement exchanged after authentication/handshake.
/// `key_fp` is SHA-256 of the node's at-rest encryption secret, or all
/// zeros when the node holds no key. `encrypted_cols` lists the
/// collections this node encrypts at rest, in the sender's own naming
/// scope (mesh: plain names; cloud: plain on clients, storage names are
/// resolved by the receiver — decisions below key off the fingerprint,
/// so scope skew cannot open a hole, only close one conservatively...
/// see `caps_allow`).
#[derive(Clone, Default, Debug, serde::Serialize, serde::Deserialize)]
pub struct PeerCaps {
    pub key_fp: [u8; 32],
    pub encrypted_cols: Vec<String>,
}

/// SHA-256 fingerprint of an at-rest encryption secret. Stable for a given
/// secret; reveals nothing usable (preimage-resistant). Verified holders
/// match; keyless nodes hash to nothing (they never hash at all — see
/// `local_fingerprint`).
pub fn key_fingerprint(secret: &str) -> [u8; 32] {
    use sha2::{Digest, Sha256};
    let mut hasher = Sha256::new();
    hasher.update(secret.as_bytes());
    let out = hasher.finalize();
    let mut fp = [0u8; 32];
    fp.copy_from_slice(&out);
    fp
}

/// This node's fingerprint: hash of the configured secret, or zeros when
/// no encryption key is configured. Zeros never equal a real fingerprint.
pub fn local_fingerprint(encryption_key: Option<&str>) -> [u8; 32] {
    match encryption_key {
        Some(secret) if !secret.is_empty() => key_fingerprint(secret),
        _ => [0u8; 32],
    }
}

/// The single decision function for both directions. `col_encrypted_locally`
/// comes from `FireLite::is_collection_encrypted` (at-rest config of THIS
/// node for THIS collection); `local_fp` from `local_fingerprint`;
/// `peer` is the counterparty's announced caps (`None` = unknown/old peer).
///
/// Fail-closed: any doubt drops the collection. Plaintext collections
/// always pass (zero behavior change for unencrypted deployments).
pub fn caps_allow(
    col_encrypted_locally: bool,
    local_fp: [u8; 32],
    peer: Option<&PeerCaps>,
) -> bool {
    if !col_encrypted_locally {
        return true;
    }
    if local_fp == [0u8; 32] {
        // No local key to verify against: refuse rather than guess.
        // (Unreachable in practice — a collection can only be locally
        // encrypted when a key is configured.)
        return false;
    }
    match peer {
        Some(p) => p.key_fp == local_fp,
        None => false,
    }
}

/// Short hex prefix for logs: diagnosable, not sensitive (it's a hash).
pub fn fp_short(fp: &[u8; 32]) -> String {
    fp.iter().take(4).map(|b| format!("{b:02x}")).collect()
}

/// Per-peer capability store + throttled loud warnings. One instance is
/// shared by every task of a syncer; entries die with the instance
/// (restart/reconnect re-announces — no stale caps, no leak).
#[derive(Default)]
pub struct CapsMap {
    caps: Mutex<HashMap<String, PeerCaps>>,
    warned: Mutex<HashMap<(String, String), Instant>>,
}

/// Minimum gap between repeat warnings for the same (peer, collection).
pub const WARN_THROTTLE: Duration = Duration::from_secs(300);

impl CapsMap {
    /// Record a peer's announcement. Clears that peer's warn history so a
    /// fixed configuration stops warning and a still-broken one re-warns.
    pub fn set(&self, peer: &str, caps: PeerCaps) {
        if let Ok(mut guard) = self.caps.lock() {
            guard.insert(peer.to_string(), caps);
        }
        if let Ok(mut warned) = self.warned.lock() {
            warned.retain(|(p, _), _| p != peer);
        }
    }

    pub fn get(&self, peer: &str) -> Option<PeerCaps> {
        self.caps.lock().ok()?.get(peer).cloned()
    }

    pub fn remove(&self, peer: &str) {
        if let Ok(mut guard) = self.caps.lock() {
            guard.remove(peer);
        }
        if let Ok(mut warned) = self.warned.lock() {
            warned.retain(|(p, _), _| p != peer);
        }
    }

    /// Loud, throttled warning. Returns true when actually emitted (lets
    /// tests assert throttle behavior without scraping stderr).
    pub fn warn(&self, peer: &str, col: &str, msg: &str) -> bool {
        let now = Instant::now();
        let key = (peer.to_string(), col.to_string());
        let due = match self.warned.lock() {
            Ok(mut guard) => match guard.get(&key) {
                Some(&last) if now.duration_since(last) < WARN_THROTTLE => false,
                _ => {
                    guard.insert(key, now);
                    true
                }
            },
            Err(_) => true,
        };
        if due {
            eprintln!("[sync-guard] {msg}");
        }
        due
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const KEY_A: &str = "correct horse battery staple";
    const KEY_B: &str = "something entirely different";

    fn caps_for(key: &str) -> PeerCaps {
        PeerCaps {
            key_fp: key_fingerprint(key),
            encrypted_cols: vec!["notes".to_string()],
        }
    }

    #[test]
    fn fingerprint_is_stable_and_distinct() {
        assert_eq!(key_fingerprint(KEY_A), key_fingerprint(KEY_A));
        assert_ne!(key_fingerprint(KEY_A), key_fingerprint(KEY_B));
        assert_eq!(key_fingerprint(KEY_A).len(), 32);
        assert_eq!(local_fingerprint(None), [0u8; 32]);
        assert_eq!(local_fingerprint(Some("")), [0u8; 32]);
        assert_eq!(local_fingerprint(Some(KEY_A)), key_fingerprint(KEY_A));
    }

    #[test]
    fn plaintext_collections_always_allowed() {
        let a = caps_for(KEY_A);
        // Plaintext locally: allowed regardless of peer state — even a
        // mismatched or unknown peer. This is what keeps unencrypted
        // deployments byte-identical to before.
        assert!(caps_allow(false, key_fingerprint(KEY_A), Some(&a)));
        assert!(caps_allow(false, key_fingerprint(KEY_A), None));
        assert!(caps_allow(false, [0u8; 32], None));
    }

    #[test]
    fn encrypted_collections_require_fingerprint_match() {
        let fp_a = key_fingerprint(KEY_A);
        let a = caps_for(KEY_A);
        let b = caps_for(KEY_B);
        // Match (verified key-holder): allowed, wire stays plaintext.
        assert!(caps_allow(true, fp_a, Some(&a)));
        // Mismatch, unknown (old peer), or explicit keyless: refused.
        assert!(!caps_allow(true, fp_a, Some(&b)));
        assert!(!caps_allow(true, fp_a, None));
        assert!(!caps_allow(
            true,
            fp_a,
            Some(&PeerCaps {
                key_fp: [0u8; 32],
                encrypted_cols: vec![],
            })
        ));
        // No local key to verify against: refuse (defensive; unreachable
        // when is_collection_encrypted is the source of truth).
        assert!(!caps_allow(true, [0u8; 32], Some(&a)));
    }

    #[test]
    fn warn_throttle_emits_once_per_window() {
        let map = CapsMap::default();
        assert!(map.warn("peer", "col", "first"));
        assert!(!map.warn("peer", "col", "suppressed"));
        assert!(map.warn("other", "col", "different peer still warns"));
        // Fresh announcement resets the throttle for that peer.
        map.set("peer", caps_for(KEY_A));
        assert!(map.warn("peer", "col", "re-warns after re-announce"));
        map.remove("peer");
        assert!(map.get("peer").is_none());
    }

    #[test]
    fn caps_roundtrip_through_bincode() {
        // Mesh wire format: the new variant must survive a round trip...
        let caps = caps_for(KEY_A);
        let bytes = bincode::serialize(&caps).unwrap();
        let back: PeerCaps = bincode::deserialize(&bytes).unwrap();
        assert_eq!(back.key_fp, caps.key_fp);
        assert_eq!(back.encrypted_cols, caps.encrypted_cols);
    }
}
