//! sync_frame: async halves of the shared length-prefix framing.
//!
//! Pure byte layout lives in `sync_core` (`frame_bytes`, `MAX_FRAME`) so it
//! unit-tests without I/O; this module only moves those bytes across tokio
//! streams. Used by the TCP mesh (`net-sync`) and unix sockets
//! (`socket-sync`); the cloud plane frames with msgpack over WS instead.
//! Gated on either plane so minimal (no-sync) builds stay tokio-free.

use tokio::io::{AsyncReadExt, AsyncWriteExt};

use crate::sync_core::{frame_bytes, MAX_FRAME};

/// Send one framed payload (length-prefix + bytes).
pub async fn send_frame<W: AsyncWriteExt + Unpin>(
    w: &mut W,
    data: &[u8],
) -> tokio::io::Result<()> {
    let frame = frame_bytes(data)?;
    w.write_all(&frame).await
}

/// Receive one framed payload, cap-checked BEFORE allocating.
pub async fn recv_frame<R: AsyncReadExt + Unpin>(r: &mut R) -> tokio::io::Result<Vec<u8>> {
    let mut len_buf = [0u8; 4];
    r.read_exact(&mut len_buf).await?;
    let len = u32::from_le_bytes(len_buf) as usize;
    if len == 0 || len > MAX_FRAME {
        return Err(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "bad frame length",
        ));
    }
    let mut buf = vec![0u8; len];
    r.read_exact(&mut buf).await?;
    Ok(buf)
}
