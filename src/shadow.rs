use tokio::io::AsyncWriteExt;
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

/// handle a single shadow master connection:
/// subscribe to the broadcast channel and stream entries as they arrive
async fn handle_shadow(mut stream: TcpStream, mut rx: broadcast::Receiver<Vec<u8>>) {
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
                // shadow is behind but still connected, continue with next entry
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
pub async fn start_shadow_listener(addr: &str, tx: broadcast::Sender<Vec<u8>>) {
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
                tokio::spawn(async move {
                    handle_shadow(stream, rx).await;
                    tracing::info!(shadow = %peer, "shadow master disconnected");
                });
            }
            Err(e) => {
                tracing::error!(error = %e, "shadow accept failed");
            }
        }
    }
}
