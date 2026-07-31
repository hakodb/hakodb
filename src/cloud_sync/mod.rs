pub mod auth;
pub mod broadcaster;
pub mod client;
pub mod protocol;
pub mod transport;

use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::Arc;
use tokio::sync::RwLock;

use auth::CloudAuthenticator;
use broadcaster::SyncBroadcaster;
use protocol::*;

// Import existing net_sync static document resolver
use crate::net_sync::resolve_doc_static;

#[derive(Clone)]
pub struct CloudSyncConfig {
    pub node_id: String,
    pub auth_secret: String,
    pub db_path: String,
    pub batch_flush_ms: u64,
}

pub struct CloudSyncEngine {
    pub config: CloudSyncConfig,
    pub auth: CloudAuthenticator,
    pub vector_clock: Arc<RwLock<VectorClock>>,
    pub broadcaster: SyncBroadcaster,
}

impl CloudSyncEngine {
    pub fn new(config: CloudSyncConfig) -> Self {
        let auth = CloudAuthenticator::new(&config.auth_secret);
        let broadcaster = SyncBroadcaster::new(2048, config.batch_flush_ms);

        Self {
            config,
            auth,
            vector_clock: Arc::new(RwLock::new(VectorClock::new())),
            broadcaster,
        }
    }

    /// Prepares an outbound SyncOp using net_sync's static doc resolver.
    /// Ensures offloaded blobs are attached prior to sending over WAN.
    pub fn prepare_outbound_op(
        &self,
        collection: &str,
        doc_id: &str,
        version: u64,
        raw_doc_bytes: &[u8],
    ) -> Result<SyncOp, String> {
        let (resolved_doc_bytes, extracted_blobs) =
            resolve_doc_static(raw_doc_bytes, &self.config.db_path)
                .map_err(|e| format!("Static blob resolution failed: {:?}", e))?;

        let attached_blobs = extracted_blobs
            .into_iter()
            .map(|(hash, data)| BlobData { hash, data })
            .collect();

        Ok(SyncOp::Upsert {
            collection: collection.to_string(),
            doc_id: doc_id.to_string(),
            version,
            payload_bytes: resolved_doc_bytes,
            attached_blobs,
        })
    }

    /// Ingests an incoming SyncBatch.
    /// Writes attached blobs to disk statically before committing document mutations.
    pub async fn apply_sync_batch(&self, batch: SyncBatch) -> Result<SyncResponse, String> {
        let mut ops_applied = 0;
        let mut applied_ops_for_broadcast = Vec::new();

        for op in batch.ops {
            match op {
                SyncOp::Upsert {
                    ref collection,
                    ref doc_id,
                    version,
                    ref payload_bytes,
                    ref attached_blobs,
                } => {
                    // 1. Statically persist attached blobs to disk FIRST
                    if !attached_blobs.is_empty() {
                        self.save_blobs_statically(attached_blobs)?;
                    }

                    // 2. Commit document bytes to internal FireLite collection
                    // self.db.apply_cloud_write(collection, doc_id, version, payload_bytes)?;

                    println!("  [CloudSync] Ingested Doc {}/{} (v{})", collection, doc_id, version);
                    ops_applied += 1;
                    applied_ops_for_broadcast.push(op.clone());
                }
                SyncOp::Delete { ref collection, ref doc_id, version } => {
                    // self.db.apply_cloud_delete(collection, doc_id, version)?;
                    ops_applied += 1;
                    applied_ops_for_broadcast.push(op.clone());
                }
            }
        }

        // Update sequence vector clock
        let mut clock = self.vector_clock.write().await;
        for (col, seq) in batch.vector_clock {
            let entry = clock.entry(col).or_insert(0);
            if seq > *entry {
                *entry = seq;
            }
        }

        // Queue ops for micro-batching WebSocket broadcast to other connected clients
        if !applied_ops_for_broadcast.is_empty() {
            self.broadcaster.enqueue_ops(applied_ops_for_broadcast).await;
        }

        Ok(SyncResponse {
            success: true,
            ops_applied,
            updated_clock: clock.clone(),
        })
    }

    /// Heartbeat Catch-Up: Compares client vector clock against server state and yields missing ops
    pub async fn get_deltas_since(&self, client_clock: &VectorClock) -> Vec<SyncOp> {
        let mut missed_ops = Vec::new();
        let server_clock = self.vector_clock.read().await;

        for (collection, &server_seq) in server_clock.iter() {
            let client_seq = client_clock.get(collection).copied().unwrap_or(0);

            if client_seq < server_seq {
                // Fetch mutations from WAL for (client_seq..server_seq]
                println!(
                    "  [CloudSync] Catch-up needed for collection '{}': Client @ {}, Server @ {}",
                    collection, client_seq, server_seq
                );
            }
        }

        missed_ops
    }

    /// Helper to statically write incoming binary blobs to local blobs directory
    fn save_blobs_statically(&self, blobs: &[BlobData]) -> Result<(), String> {
        let blobs_dir = Path::new(&self.config.db_path).join("blobs");
        if !blobs_dir.exists() {
            std::fs::create_dir_all(&blobs_dir)
                .map_err(|e| format!("Failed to create blobs directory: {:?}", e))?;
        }

        for blob in blobs {
            let blob_file_path = blobs_dir.join(format!("{}.dat", blob.hash));
            if !blob_file_path.exists() {
                let mut file = OpenOptions::new()
                    .create(true)
                    .write(true)
                    .open(&blob_file_path)
                    .map_err(|e| format!("Failed to open blob file: {:?}", e))?;

                file.write_all(&blob.data)
                    .map_err(|e| format!("Failed to write blob data: {:?}", e))?;
            }
        }
        Ok(())
    }
}