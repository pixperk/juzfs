use std::sync::Arc;

use juzfs::{
    master::Master,
    messages::{ChunkServerToMaster, ClientToMaster, MasterToChunkServer, MasterToClient},
    protocol::{MessageType, decode_payload, read_raw_frame, send_frame},
};
use tokio::net::{TcpListener, TcpStream};

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
                tracing::warn!(msg_type, "unknown message type, dropping connection");
                break;
            }
        }
    }
}

async fn handle_client_msg(stream: &mut TcpStream, master: &Master, payload: &[u8]) {
    let msg: ClientToMaster = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode client message");
            return;
        }
    };

    let response = match msg {
        ClientToMaster::CreateFile { filename } => {
            tracing::info!(file = %filename, "create file");
            match master.create_file(filename).await {
                Ok(()) => MasterToClient::Ok,
                Err(e) => {
                    tracing::error!(error = %e, "oplog write failed on create_file");
                    MasterToClient::Error("internal error".into())
                }
            }
        }
        ClientToMaster::GetFileChunks { filename } => {
            match master.get_file_chunks(&filename).await {
                Some(chunks) => {
                    tracing::debug!(file = %filename, chunks = chunks.len(), "get file chunks");
                    MasterToClient::FileChunks(chunks)
                }
                None => {
                    tracing::warn!(file = %filename, "file not found");
                    MasterToClient::Error("file not found".into())
                }
            }
        }
        ClientToMaster::GetChunkLocations { handle } => {
            match master.get_chunk_locations(handle).await {
                Some(locations) => {
                    tracing::debug!(chunk = handle, replicas = locations.len(), "get chunk locations");
                    MasterToClient::ChunkLocations { handle, locations }
                }
                None => {
                    tracing::warn!(chunk = handle, "chunk not found");
                    MasterToClient::Error("chunk not found".into())
                }
            }
        }
        ClientToMaster::AllocateChunk { filename } => {
            let locations = master.choose_locations(3).await;
            if locations.is_empty() {
                tracing::error!(file = %filename, "no chunkservers available for allocation");
                MasterToClient::Error("no chunkservers available".into())
            } else {
                match master.add_chunk(&filename, locations).await {
                    Ok(Some(handle)) => {
                        let locs = master.get_chunk_locations(handle).await.unwrap();
                        tracing::info!(file = %filename, chunk = handle, replicas = ?locs, "allocated chunk");
                        MasterToClient::ChunkLocations {
                            handle,
                            locations: locs,
                        }
                    }
                    Ok(None) => {
                        tracing::warn!(file = %filename, "allocate chunk failed, file not found");
                        MasterToClient::Error("file not found".into())
                    }
                    Err(e) => {
                        tracing::error!(error = %e, "oplog write failed on add_chunk");
                        MasterToClient::Error("internal error".into())
                    }
                }
            }
        }
        ClientToMaster::GetPrimary { handle } => match master.grant_lease(handle).await {
            Ok(Some((primary, secondaries, version, version_bumped))) => {
                if version_bumped {
                    tracing::info!(
                        chunk = handle,
                        primary = %primary,
                        version = version,
                        secondaries = ?secondaries,
                        "granted new lease, notifying replicas"
                    );
                    let all_replicas: Vec<String> = std::iter::once(primary.clone())
                        .chain(secondaries.iter().cloned())
                        .collect();
                    for addr in &all_replicas {
                        if let Ok(mut conn) = TcpStream::connect(addr).await {
                            let msg = MasterToChunkServer::UpdateVersion { handle, version };
                            let _ =
                                send_frame(&mut conn, MessageType::MasterToChunkServer, &msg).await;
                        } else {
                            tracing::warn!(chunk = handle, replica = %addr, "failed to notify replica of version update");
                        }
                    }
                } else {
                    tracing::debug!(
                        chunk = handle,
                        primary = %primary,
                        version = version,
                        "reusing existing lease"
                    );
                }
                MasterToClient::PrimaryInfo {
                    primary,
                    secondaries,
                }
            }
            Ok(None) => {
                tracing::warn!(chunk = handle, "grant lease failed, chunk not found");
                MasterToClient::Error("chunk not found".into())
            }
            Err(e) => {
                tracing::error!(error = %e, "oplog write failed on grant_lease");
                MasterToClient::Error("internal error".into())
            }
        },
        ClientToMaster::DeleteFile { filename } => {
            tracing::info!(file = %filename, "delete file");
            match master.delete_file(filename).await {
                Ok(true) => MasterToClient::Ok,
                Ok(false) => {
                    tracing::warn!("delete failed, file not found");
                    MasterToClient::Error("file not found".into())
                }
                Err(e) => {
                    tracing::error!(error = %e, "oplog write failed on delete_file");
                    MasterToClient::Error("internal error".into())
                }
            }
        }
    };

    let _ = send_frame(stream, MessageType::MasterToClient, &response).await;
}

async fn handle_chunkserver_msg(stream: &mut TcpStream, master: &Master, payload: &[u8]) {
    let msg: ChunkServerToMaster = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode chunkserver message");
            return;
        }
    };

    let response = match msg {
        ChunkServerToMaster::Register {
            addr,
            available_space,
        } => {
            tracing::info!(
                chunkserver = %addr,
                available_mb = available_space / (1024 * 1024),
                "chunkserver registered"
            );
            master.register_chunkserver(addr, available_space).await;
            MasterToChunkServer::Ok
        }
        ChunkServerToMaster::Heartbeat {
            addr,
            chunks,
            available_space,
        } => {
            tracing::debug!(
                chunkserver = %addr,
                chunks = ?chunks,
                available_mb = available_space / (1024 * 1024),
                "heartbeat"
            );
            master.heartbeat(&addr, chunks, available_space).await;
            MasterToChunkServer::Ok
        }
    };

    let _ = send_frame(stream, MessageType::MasterToChunkServer, &response).await;
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

    let master = Arc::new(Master::recover(60, "oplog.bin").await.expect("failed to recover master from oplog"));
    tracing::info!("master recovered from oplog");
    let listener = match TcpListener::bind("0.0.0.0:5000").await {
        Ok(listener) => listener,
        Err(e) => {
            tracing::error!(error = %e, "failed to bind");
            return;
        }
    };

    tracing::info!("master listening on 0.0.0.0:5000");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let master_clone = Arc::clone(&master);
                tokio::spawn(handle_connection(stream, master_clone));
            }
            Err(e) => {
                tracing::error!(error = %e, "accept failed");
            }
        }
    }
}
