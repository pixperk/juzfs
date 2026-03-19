use std::path::Path;

use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::broadcast;


/// send a length-prefixed oplog entry over TCP
async fn send_entry(stream: &mut TcpStream, data: &[u8]) -> std::io::Result<()> {
    let len = (data.len() as u32).to_le_bytes();
    stream.write_all(&len).await?;
    stream.write_all(data).await?;
    stream.flush().await?;
    Ok(())
}

/// replay oplog entries from disk starting at `skip` offset, send each to the shadow.
/// returns the number of entries sent.
fn read_oplog_entries(path: &Path) -> Vec<Vec<u8>> {
    // read raw bytes (not deserialized) from the oplog file
    use std::io::Read;
    let mut file = match std::fs::File::open(path) {
        Ok(f) => f,
        Err(_) => return Vec::new(),
    };

    let mut entries = Vec::new();
    let mut len_buf = [0u8; 4];
    loop {
        match file.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(_) => break,
        }
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        if file.read_exact(&mut buf).is_err() {
            break;
        }
        entries.push(buf);
    }
    entries
}

/// handle a single shadow master connection:
/// 1. read the shadow's current offset
/// 2. send backlog from oplog files (old + current)
/// 3. switch to live broadcast
async fn handle_shadow(
    mut stream: TcpStream,
    mut rx: broadcast::Receiver<Vec<u8>>,
    oplog_path: String,
) {
    // step 1: read shadow's offset
    let mut offset_buf = [0u8; 8];
    if let Err(e) = stream.read_exact(&mut offset_buf).await {
        tracing::warn!(error = %e, "failed to read shadow offset");
        return;
    }
    let shadow_offset = u64::from_le_bytes(offset_buf);
    tracing::info!(shadow_offset, "shadow requesting catch-up");

    // step 2: send backlog from oplog files
    let oplog = Path::new(&oplog_path);
    let old_oplog = oplog.with_extension("old.bin");

    // collect all entries from old oplog (if exists) + current oplog
    let mut all_entries: Vec<Vec<u8>> = Vec::new();
    all_entries.extend(read_oplog_entries(&old_oplog));
    all_entries.extend(read_oplog_entries(oplog));

    // skip entries the shadow already has
    let backlog = &all_entries[shadow_offset as usize..];
    if !backlog.is_empty() {
        tracing::info!(backlog = backlog.len(), "sending backlog to shadow");
        for entry in backlog {
            if let Err(e) = send_entry(&mut stream, entry).await {
                tracing::warn!(error = %e, "failed to send backlog entry");
                return;
            }
        }
    }

    tracing::info!("backlog sent, switching to live stream");

    // step 3: live broadcast
    loop {
        match rx.recv().await {
            Ok(entry_bytes) => {
                if let Err(e) = send_entry(&mut stream, &entry_bytes).await {
                    tracing::warn!(error = %e, "shadow send failed, disconnecting");
                    break;
                }
            }
            Err(broadcast::error::RecvError::Lagged(n)) => {
                tracing::warn!(missed = n, "shadow fell behind, entries dropped");
            }
            Err(broadcast::error::RecvError::Closed) => {
                tracing::info!("broadcast channel closed, shadow shutting down");
                break;
            }
        }
    }
}

/// start the shadow replication listener.
/// accepts multiple shadow master connections, each gets its own broadcast subscriber.
pub async fn start_shadow_listener(
    addr: &str,
    tx: broadcast::Sender<Vec<u8>>,
    oplog_path: String,
) {
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(error = %e, addr = addr, "failed to bind shadow listener");
            return;
        }
    };
    tracing::info!(addr = addr, "shadow replication listener started");

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                tracing::info!(shadow = %peer, "shadow master connected");
                let rx = tx.subscribe();
                let path = oplog_path.clone();
                tokio::spawn(async move {
                    handle_shadow(stream, rx, path).await;
                    tracing::info!(shadow = %peer, "shadow master disconnected");
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "shadow accept failed");
            }
        }
    }
}
