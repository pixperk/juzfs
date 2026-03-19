use std::sync::Arc;

use juzfs::{
    master::{Master, OpLogEntry},
    messages::{ChunkServerToMaster, ClientToMaster, MasterToChunkServer, MasterToClient},
    protocol::{MessageType, decode_payload, read_raw_frame, send_frame},
};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::RwLock;

/// connect to primary and replay oplog entries as they arrive.
/// tracks total entries applied so we can resume from the right offset on reconnect.
async fn replication_loop(primary_addr: &str, master: Arc<Master>, offset: Arc<RwLock<u64>>) {
    loop {
        let current_offset = *offset.read().await;
        tracing::info!(primary = primary_addr, offset = current_offset, "connecting to primary for replication");
        match TcpStream::connect(primary_addr).await {
            Ok(mut stream) => {
                // send our current offset so primary knows where to start
                let offset_bytes = current_offset.to_le_bytes();
                if let Err(e) = stream.write_all(&offset_bytes).await {
                    tracing::error!(error = %e, "failed to send offset");
                    continue;
                }
                let _ = stream.flush().await;

                tracing::info!(offset = current_offset, "connected to primary, receiving oplog stream");
                if let Err(e) = receive_oplog_stream(&mut stream, &master, &offset).await {
                    tracing::error!(error = %e, "replication stream error");
                }
            }
            Err(e) => {
                tracing::warn!(error = %e, "failed to connect to primary");
            }
        }
        tokio::time::sleep(std::time::Duration::from_secs(5)).await;
    }
}

/// read length-prefixed oplog entries from the primary and replay them
async fn receive_oplog_stream(
    stream: &mut TcpStream,
    master: &Master,
    offset: &RwLock<u64>,
) -> std::io::Result<()> {
    let mut len_buf = [0u8; 4];
    let mut max_handle = 0u64;

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

        let mut off = offset.write().await;
        *off += 1;
        let current = *off;
        drop(off);

        if current % 100 == 0 {
            tracing::info!(entries = current, "shadow replay progress");
        }
    }
}

/// handle incoming connections: client reads (msg_type 1) or chunkserver heartbeats (msg_type 3)
async fn handle_connection(mut stream: TcpStream, master: Arc<Master>) {
    loop {
        let (msg_type, payload) = match read_raw_frame(&mut stream).await {
            Ok(v) => v,
            Err(_) => break,
        };

        match msg_type {
            1 => handle_client_msg(&mut stream, &master, &payload).await,
            3 => handle_chunkserver_msg(&mut stream, &master, &payload).await,
            _ => {
                tracing::warn!(msg_type, "shadow: unknown message type");
                break;
            }
        }
    }
}

async fn handle_client_msg(stream: &mut TcpStream, master: &Master, payload: &[u8]) {
    let msg: ClientToMaster = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "decode failed");
            return;
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

    let _ = send_frame(stream, MessageType::MasterToClient, &response).await;
}

async fn handle_chunkserver_msg(stream: &mut TcpStream, master: &Master, payload: &[u8]) {
    let msg: ChunkServerToMaster = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "decode chunkserver msg failed");
            return;
        }
    };

    match msg {
        ChunkServerToMaster::Register { addr, available_space } => {
            tracing::info!(chunkserver = %addr, "shadow: chunkserver registered");
            master.register_chunkserver(addr, available_space).await;
        }
        ChunkServerToMaster::Heartbeat { addr, chunks, available_space } => {
            master.heartbeat(&addr, chunks, available_space).await;
        }
    };

    let _ = send_frame(stream, MessageType::MasterToChunkServer, &MasterToChunkServer::Ok).await;
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
    let offset = Arc::new(RwLock::new(0u64));
    let repl_offset = Arc::clone(&offset);
    tokio::spawn(async move {
        replication_loop(&primary, repl_master, repl_offset).await;
    });

    // listen for read-only client requests
    let bind_addr = format!("0.0.0.0:{}", listen_port);
    let listener = TcpListener::bind(&bind_addr).await.expect("failed to bind shadow listener");
    tracing::info!(addr = %bind_addr, primary = primary_addr, "shadow master started (read-only)");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let m = Arc::clone(&master);
                tokio::spawn(handle_connection(stream, m));
            }
            Err(e) => {
                tracing::error!(error = %e, "accept failed");
            }
        }
    }
}
