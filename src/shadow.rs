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
pub async fn start_shadow_listener(addr: &str, tx: broadcast::Sender<Vec<u8>>, oplog_path: String) {
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

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write;
    use tempfile::tempdir;

    fn write_oplog_entry_to_file(file: &mut std::fs::File, data: &[u8]) {
        use std::io::Write;
        let len = (data.len() as u32).to_le_bytes();
        file.write_all(&len).unwrap();
        file.write_all(data).unwrap();
    }

    #[test]
    fn test_read_oplog_entries_empty_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oplog.bin");
        std::fs::File::create(&path).unwrap();
        let entries = read_oplog_entries(&path);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_read_oplog_entries_missing_file() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("does_not_exist.bin");
        let entries = read_oplog_entries(&path);
        assert!(entries.is_empty());
    }

    #[test]
    fn test_read_oplog_entries_round_trip() {
        let dir = tempdir().unwrap();
        let path = dir.path().join("oplog.bin");
        let mut file = std::fs::File::create(&path).unwrap();

        let data1 = b"hello world";
        let data2 = b"second entry";
        let data3 = b"third";
        write_oplog_entry_to_file(&mut file, data1);
        write_oplog_entry_to_file(&mut file, data2);
        write_oplog_entry_to_file(&mut file, data3);
        file.flush().unwrap();
        drop(file);

        let entries = read_oplog_entries(&path);
        assert_eq!(entries.len(), 3);
        assert_eq!(entries[0], data1);
        assert_eq!(entries[1], data2);
        assert_eq!(entries[2], data3);
    }

    #[test]
    fn test_read_oplog_entries_truncated() {
        // partial entry (length says 100 but only 5 bytes of data)
        let dir = tempdir().unwrap();
        let path = dir.path().join("oplog.bin");
        let mut file = std::fs::File::create(&path).unwrap();

        // write one good entry
        write_oplog_entry_to_file(&mut file, b"good");
        // write a bad entry: length prefix says 100 but only 3 bytes follow
        file.write_all(&100u32.to_le_bytes()).unwrap();
        file.write_all(b"bad").unwrap();
        file.flush().unwrap();
        drop(file);

        let entries = read_oplog_entries(&path);
        // should recover the good entry and stop at the truncated one
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0], b"good");
    }

    #[tokio::test]
    async fn test_send_entry_length_prefixed() {
        // create a TCP pair and verify send_entry writes length + data
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let sender = tokio::spawn(async move {
            let mut stream = TcpStream::connect(addr).await.unwrap();
            send_entry(&mut stream, b"test payload").await.unwrap();
            send_entry(&mut stream, b"second").await.unwrap();
        });

        let (mut stream, _) = listener.accept().await.unwrap();
        // read first entry
        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        assert_eq!(len, 12); // "test payload"
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"test payload");

        // read second entry
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        assert_eq!(len, 6); // "second"
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"second");

        sender.await.unwrap();
    }

    #[tokio::test]
    async fn test_handle_shadow_backlog_from_zero() {
        // write 3 entries to an oplog file, connect a shadow at offset 0,
        // verify it receives all 3
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        let mut file = std::fs::File::create(&oplog_path).unwrap();
        write_oplog_entry_to_file(&mut file, b"entry0");
        write_oplog_entry_to_file(&mut file, b"entry1");
        write_oplog_entry_to_file(&mut file, b"entry2");
        file.flush().unwrap();
        drop(file);

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let rx = tx.subscribe();
        drop(tx); // channel closes so handler exits live loop after backlog
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&0u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        let mut len_buf = [0u8; 4];
        for expected in [b"entry0".as_slice(), b"entry1", b"entry2"] {
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, expected);
        }

        drop(stream);
        let _ = handler.await;
    }

    #[tokio::test]
    async fn test_handle_shadow_backlog_with_offset() {
        // write 5 entries, connect shadow at offset 3, should only get entries 3 and 4
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        let mut file = std::fs::File::create(&oplog_path).unwrap();
        for i in 0..5 {
            write_oplog_entry_to_file(&mut file, format!("entry{i}").as_bytes());
        }
        file.flush().unwrap();
        drop(file);

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let rx = tx.subscribe();
        drop(tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&3u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        let mut len_buf = [0u8; 4];
        for expected in [b"entry3".as_slice(), b"entry4"] {
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, expected);
        }

        drop(stream);
        let _ = handler.await;
    }

    #[tokio::test]
    async fn test_handle_shadow_live_broadcast() {
        // no backlog entries, but broadcast live entries after connect
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        std::fs::File::create(&oplog_path).unwrap(); // empty oplog

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let rx = tx.subscribe();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&0u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        tokio::time::sleep(std::time::Duration::from_millis(50)).await;

        tx.send(b"live1".to_vec()).unwrap();
        tx.send(b"live2".to_vec()).unwrap();

        let mut len_buf = [0u8; 4];
        for expected in [b"live1".as_slice(), b"live2"] {
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, expected);
        }

        drop(tx);
        drop(stream);
        let _ = handler.await;
    }

    #[tokio::test]
    async fn test_handle_shadow_backlog_then_live() {
        // 2 entries on disk, then 1 live broadcast - verify ordering
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        let mut file = std::fs::File::create(&oplog_path).unwrap();
        write_oplog_entry_to_file(&mut file, b"disk0");
        write_oplog_entry_to_file(&mut file, b"disk1");
        file.flush().unwrap();
        drop(file);

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let rx = tx.subscribe();
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&0u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        // read 2 backlog entries
        let mut len_buf = [0u8; 4];
        for expected in [b"disk0".as_slice(), b"disk1"] {
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, expected);
        }

        // now send a live entry
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        tx.send(b"live0".to_vec()).unwrap();

        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"live0");

        drop(tx);
        drop(stream);
        let _ = handler.await;
    }

    #[tokio::test]
    async fn test_broadcast_channel_closed_stops_shadow() {
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        std::fs::File::create(&oplog_path).unwrap();

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let rx = tx.subscribe();
            // drop tx so channel closes after shadow enters live loop
            drop(tx);
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&0u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        // handler should exit cleanly because channel is closed
        let result = handler.await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_old_and_current_oplog_combined() {
        // entries split across oplog.old.bin and oplog.bin
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        let old_path = dir.path().join("oplog.old.bin");

        // 2 entries in old oplog
        let mut old_file = std::fs::File::create(&old_path).unwrap();
        write_oplog_entry_to_file(&mut old_file, b"old0");
        write_oplog_entry_to_file(&mut old_file, b"old1");
        old_file.flush().unwrap();
        drop(old_file);

        // 2 entries in current oplog
        let mut cur_file = std::fs::File::create(&oplog_path).unwrap();
        write_oplog_entry_to_file(&mut cur_file, b"cur0");
        write_oplog_entry_to_file(&mut cur_file, b"cur1");
        cur_file.flush().unwrap();
        drop(cur_file);

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let rx = tx.subscribe();
        drop(tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&0u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        let mut len_buf = [0u8; 4];
        for expected in [b"old0".as_slice(), b"old1", b"cur0", b"cur1"] {
            stream.read_exact(&mut len_buf).await.unwrap();
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            stream.read_exact(&mut buf).await.unwrap();
            assert_eq!(buf, expected);
        }

        drop(stream);
        let _ = handler.await;
    }

    #[tokio::test]
    async fn test_old_and_current_oplog_with_offset() {
        // 2 in old + 2 in current, shadow at offset 3 should only get cur1
        let dir = tempdir().unwrap();
        let oplog_path = dir.path().join("oplog.bin");
        let old_path = dir.path().join("oplog.old.bin");

        let mut old_file = std::fs::File::create(&old_path).unwrap();
        write_oplog_entry_to_file(&mut old_file, b"old0");
        write_oplog_entry_to_file(&mut old_file, b"old1");
        old_file.flush().unwrap();
        drop(old_file);

        let mut cur_file = std::fs::File::create(&oplog_path).unwrap();
        write_oplog_entry_to_file(&mut cur_file, b"cur0");
        write_oplog_entry_to_file(&mut cur_file, b"cur1");
        cur_file.flush().unwrap();
        drop(cur_file);

        let (tx, _) = broadcast::channel::<Vec<u8>>(16);
        let rx = tx.subscribe();
        drop(tx);
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let path_str = oplog_path.to_str().unwrap().to_string();

        let handler = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            handle_shadow(stream, rx, path_str).await;
        });

        let mut stream = TcpStream::connect(addr).await.unwrap();
        stream.write_all(&3u64.to_le_bytes()).await.unwrap();
        stream.flush().await.unwrap();

        let mut len_buf = [0u8; 4];
        stream.read_exact(&mut len_buf).await.unwrap();
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await.unwrap();
        assert_eq!(buf, b"cur1");

        drop(stream);
        let _ = handler.await;
    }
}
