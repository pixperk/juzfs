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
            Err(_) => break, // client disconnected
        };

        match msg_type {
            //1 is for client messages, 3 is for chunkserver messages
            1 => handle_client_msg(&mut stream, &master, &payload).await,
            3 => handle_chunkserver_msg(&mut stream, &master, &payload).await,
            _ => {
                tracing::warn!("unknown msg_type: {}", msg_type);
                break;
            }
        }
    }
}

async fn handle_client_msg(stream: &mut TcpStream, master: &Master, payload: &[u8]) {
    let msg: ClientToMaster = match decode_payload(payload) {
        Ok(m) => m,
        Err(_) => return,
    };

    let response = match msg {
        ClientToMaster::CreateFile { filename } => {
            master.create_file(filename).await;
            MasterToClient::Ok
        }
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
        ClientToMaster::AllocateChunk { filename } => {
            let locations = master.choose_locations(3).await;
            if locations.is_empty() {
                MasterToClient::Error("no chunkservers available".into())
            } else {
                match master.add_chunk(&filename, locations).await {
                    Some(handle) => {
                        let locs = master.get_chunk_locations(handle).await.unwrap();
                        MasterToClient::ChunkLocations {
                            handle,
                            locations: locs,
                        }
                    }
                    None => MasterToClient::Error("file not found".into()),
                }
            }
        }
        ClientToMaster::GetPrimary { handle } => match master.grant_lease(handle).await {
            Some((primary, secondaries)) => MasterToClient::PrimaryInfo {
                primary,
                secondaries,
            },
            None => MasterToClient::Error("chunk not found".into()),
        },
    };

    let _ = send_frame(stream, MessageType::MasterToClient, &response).await;
}

async fn handle_chunkserver_msg(stream: &mut TcpStream, master: &Master, payload: &[u8]) {
    let msg: ChunkServerToMaster = match decode_payload(payload) {
        Ok(m) => m,
        Err(_) => return,
    };

    let response = match msg {
        ChunkServerToMaster::Register {
            addr,
            available_space,
        } => {
            master.register_chunkserver(addr, available_space).await;
            MasterToChunkServer::Ok
        }
        ChunkServerToMaster::Heartbeat {
            addr,
            chunks,
            available_space,
        } => {
            master.heartbeat(&addr, chunks, available_space).await;
            // later: return DeleteChunks/ReplicateChunk instructions here
            MasterToChunkServer::Ok
        }
    };

    let _ = send_frame(stream, MessageType::MasterToChunkServer, &response).await;
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let master = Arc::new(Master::new(60)); //gfs uses 60 seconds as the default heartbeat timeout
    let listener = match TcpListener::bind("0.0.0.0:5000").await {
        Ok(listener) => listener,
        Err(e) => {
            eprintln!("Failed to bind to address: {}", e);
            return;
        }
    };

    tracing::info!("Master server is running on 0.0.0.0:5000");

    loop {
        match listener.accept().await {
            Ok((stream, addr)) => {
                tracing::info!("New connection from {}", addr);
                let master_clone = Arc::clone(&master);
                tokio::spawn(handle_connection(stream, master_clone));
            }
            Err(e) => {
                eprintln!("Failed to accept connection: {}", e);
            }
        }
    }
}
