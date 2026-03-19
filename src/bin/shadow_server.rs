use std::sync::Arc;

use juzfs::{
    master::{Master, OpLogEntry},
    messages::{ClientToMaster, MasterToClient},
    protocol::{MessageType, decode_payload, read_raw_frame, send_frame},
};
use tokio::io::AsyncReadExt;
use tokio::net::{TcpListener, TcpStream};

/// connect to primary and replay oplog entries as they arrive
async fn replication_loop(primary_addr: &str, master: Arc<Master>) {
    loop {
        tracing::info!(primary = primary_addr, "connecting to primary for replication");
        match TcpStream::connect(primary_addr).await {
            Ok(mut stream) => {
                tracing::info!("connected to primary, receiving oplog stream");
                if let Err(e) = receive_oplog_stream(&mut stream, &master).await {
                    tracing::error!(error = %e, "replication stream error");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to connect to primary");
            }
        }
        // retry after a delay
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// read length-prefixed oplog entries from the primary and replay them
async fn receive_oplog_stream(stream: &mut TcpStream, master: &Master) -> std::io::Result<()> {
    let mut len_buf = [0u8; 4];
    let mut max_handle = 0u64;
    let mut entries_applied = 0u64;

    loop {
        stream.read_exact(&mut len_buf).await?;
        let len = u32::from_le_bytes(len_buf) as usize;
        let mut buf = vec![0u8; len];
        stream.read_exact(&mut buf).await?;

        let entry: OpLogEntry = bincode::deserialize(&buf)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let mut files_recovered = 0u64;
        let mut chunks_recovered = 0u64;
        Master::apply_entry(
            master,
            entry,
            &mut max_handle,
            &mut files_recovered,
            &mut chunks_recovered,
        )
        .await;

        entries_applied += 1;
        if entries_applied % 100 == 0 {
            tracing::info!(entries = entries_applied, "shadow replay progress");
        }
    }
}

/// handle read-only client requests
async fn handle_client(mut stream: TcpStream, master: Arc<Master>) {
    loop {
        let (msg_type, payload) = match read_raw_frame(&mut stream).await {
            Ok(v) => v,
            Err(_) => break,
        };

        if msg_type != 1 {
            tracing::warn!(msg_type, "shadow only handles client messages");
            break;
        }

        let msg: ClientToMaster = match decode_payload(&payload) {
            Ok(m) => m,
            Err(e) => {
                tracing::error!(error = %e, "decode failed");
                break;
            }
        };

        let response = match msg {
            ClientToMaster::GetFileChunks { filename } => {
                match master.get_file_chunks(&filename).await {
                    Some(chunks) => MasterToClient::FileChunks(chunks),
                    None => MasterToClient::Error("file not found".into()),
                }
            }
            ClientToMaster::GetChunkLocations { handle } => {
                match master.get_chunk_locations(handle).await {
                    Some(locations) => MasterToClient::ChunkLocations { handle, locations },
                    None => MasterToClient::Error("chunk not found".into()),
                }
            }
            _ => MasterToClient::Error("shadow master is read-only".into()),
        };

        let _ = send_frame(&mut stream, MessageType::MasterToClient, &response).await;
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_target(false)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info".into()),
        )
        .init();

    let args: Vec<String> = std::env::args().collect();
    let primary_addr = args.get(1).map(|s| s.as_str()).unwrap_or("127.0.0.1:5001");
    let listen_port = args.get(2).map(|s| s.as_str()).unwrap_or("5002");

    // shadow doesn't use oplog on disk, it replays from primary
    let master = Arc::new(Master::new(60, "/dev/null").expect("failed to create shadow master"));

    // start replication from primary
    let repl_master = Arc::clone(&master);
    let primary = primary_addr.to_string();
    tokio::spawn(async move {
        replication_loop(&primary, repl_master).await;
    });

    // listen for read-only client requests
    let bind_addr = format!("0.0.0.0:{}", listen_port);
    let listener = TcpListener::bind(&bind_addr).await.expect("failed to bind shadow listener");
    tracing::info!(addr = %bind_addr, primary = primary_addr, "shadow master started (read-only)");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let m = Arc::clone(&master);
                tokio::spawn(handle_client(stream, m));
            }
            Err(e) => {
                tracing::error!(error = %e, "accept failed");
            }
        }
    }
}
