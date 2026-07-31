use tokio::net::TcpListener;
use tokio_util::codec::{Framed, LengthDelimitedCodec};
use futures::StreamExt;
use std::sync::Arc;
use crate::cloud_sync::{CloudSyncEngine, protocol::SyncBatch};

pub async fn start_tcp_listener(
    engine: Arc<CloudSyncEngine>,
    bind_addr: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind(bind_addr).await?;
    println!("⚡ CloudSync Direct TCP Listening on {}", bind_addr);

    loop {
        let (socket, _) = listener.accept().await?;
        let engine_ref = engine.clone();

        tokio::spawn(async move {
            let mut framed = Framed::new(socket, LengthDelimitedCodec::new());
            while let Some(Ok(bytes)) = framed.next().await {
                if let Ok(batch) = bincode::deserialize::<SyncBatch>(&bytes) {
                    let _ = engine_ref.apply_sync_batch(batch).await;
                }
            }
        });
    }
}