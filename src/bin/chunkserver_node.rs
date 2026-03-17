use std::sync::Arc;

use juzfs::{
    chunkserver::ChunkServer,
    messages::*,
    protocol::{decode_payload, read_raw_frame, send_frame, MessageType},
};
use tokio::net::{TcpListener, TcpStream};

async fn handle_connection(mut stream: TcpStream, cs: Arc<ChunkServer>) {
    loop {
        let (msg_type, payload) = match read_raw_frame(&mut stream).await {
            Ok(v) => v,
            Err(_) => break,
        };

        match msg_type {
            // 5 = ClientToChunkServer
            5 => handle_client_msg(&mut stream, &cs, &payload).await,
            // 7 = ChunkServerToChunkServer (data pipeline forwarding)
            7 => handle_cs_msg(&mut stream, &cs, &payload).await,
            _ => {
                tracing::warn!("unknown msg_type: {}", msg_type);
                break;
            }
        }
    }
}

async fn handle_client_msg(stream: &mut TcpStream, cs: &ChunkServer, payload: &[u8]) {
    let msg: ClientToChunkServer = match decode_payload(payload) {
        Ok(m) => m,
        Err(_) => return,
    };

    let response = match msg {
        ClientToChunkServer::Read {
            handle,
            offset,
            length,
        } => match cs.read_chunk(handle, offset as usize, length as usize).await {
            Ok(data) => ChunkServerToClient::Data(data),
            Err(e) => ChunkServerToClient::Error(e.to_string()),
        },
        ClientToChunkServer::PushData { handle, data } => {
            cs.buffer_push(handle, data).await;
            ChunkServerToClient::Ok
        }
        ClientToChunkServer::Write { handle, secondaries } => {
            // primary coordination: the single point of write ordering
            // 1. assign a monotonic serial — this is the global order all replicas obey
            let serial = cs.next_serial();

            // 2. flush own buffer first (primary commits before asking secondaries)
            if let Err(e) = cs.flush_push(handle).await {
                let _ = send_frame(stream, MessageType::ChunkServerToClient,
                    &ChunkServerToClient::Error(e.to_string())).await;
                return;
            }

            // 3. tell each secondary to commit with the same serial number
            //    done sequentially so secondaries apply in serial order
            for addr in &secondaries {
                let commit = ChunkServerToChunkServer::CommitWrite { handle, serial };
                match TcpStream::connect(addr).await {
                    Ok(mut conn) => {
                        if let Err(e) = send_frame(
                            &mut conn,
                            MessageType::ChunkServerToChunkServer,
                            &commit,
                        ).await {
                            let _ = send_frame(stream, MessageType::ChunkServerToClient,
                                &ChunkServerToClient::Error(format!("secondary {} send failed: {}", addr, e))).await;
                            return;
                        }
                        // wait for secondary ack
                        match juzfs::protocol::read_frame::<ChunkServerAck>(&mut conn).await {
                            Ok((_, ChunkServerAck::Ok)) => {}
                            Ok((_, ChunkServerAck::Error(e))) => {
                                let _ = send_frame(stream, MessageType::ChunkServerToClient,
                                    &ChunkServerToClient::Error(format!("secondary {} failed: {}", addr, e))).await;
                                return;
                            }
                            Err(e) => {
                                let _ = send_frame(stream, MessageType::ChunkServerToClient,
                                    &ChunkServerToClient::Error(format!("secondary {} ack failed: {}", addr, e))).await;
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        let _ = send_frame(stream, MessageType::ChunkServerToClient,
                            &ChunkServerToClient::Error(format!("secondary {} unreachable: {}", addr, e))).await;
                        return;
                    }
                }
            }

            // 4. all replicas committed in serial order, ack client
            ChunkServerToClient::Ok
        }
    };

    let _ = send_frame(stream, MessageType::ChunkServerToClient, &response).await;
}

async fn handle_cs_msg(stream: &mut TcpStream, cs: &ChunkServer, payload: &[u8]) {
    let msg: ChunkServerToChunkServer = match decode_payload(payload) {
        Ok(m) => m,
        Err(_) => return,
    };

    let response = match msg {
        ChunkServerToChunkServer::ForwardData {
            handle,
            data,
            remaining,
        } => {
            // buffer the data locally
            cs.buffer_push(handle, data.clone()).await;

            // forward to next in chain if any
            if let Some((next, rest)) = remaining.split_first() {
                let fwd = ChunkServerToChunkServer::ForwardData {
                    handle,
                    data,
                    remaining: rest.to_vec(),
                };
                if let Ok(mut next_conn) = TcpStream::connect(next).await {
                    let _ = send_frame(
                        &mut next_conn,
                        MessageType::ChunkServerToChunkServer,
                        &fwd,
                    )
                    .await;
                    // wait for ack from next
                    let _: Result<(u8, ChunkServerAck), _> =
                        juzfs::protocol::read_frame(&mut next_conn).await;
                }
            }

            ChunkServerAck::Ok
        }
        ChunkServerToChunkServer::CommitWrite { handle, .. } => {
            match cs.flush_push(handle).await {
                Ok(_) => ChunkServerAck::Ok,
                Err(e) => ChunkServerAck::Error(e.to_string()),
            }
        }
    };

    let _ = send_frame(stream, MessageType::ChunkServerAck, &response).await;
}

/// heartbeat loop: register with master, then send periodic heartbeats
async fn heartbeat_loop(cs: Arc<ChunkServer>, master_addr: String) {
    // register first
    if let Ok(mut conn) = TcpStream::connect(&master_addr).await {
        let register = ChunkServerToMaster::Register {
            addr: cs.addr().to_string(),
            available_space: 1_000_000_000, // 1GB placeholder
        };
        let _ = send_frame(&mut conn, MessageType::ChunkServerToMaster, &register).await;
        let _: Result<(u8, MasterToChunkServer), _> =
            juzfs::protocol::read_frame(&mut conn).await;
    }

    // periodic heartbeat
    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        if let Ok(mut conn) = TcpStream::connect(&master_addr).await {
            let chunks = cs.list_chunks().await;
            let hb = ChunkServerToMaster::Heartbeat {
                addr: cs.addr().to_string(),
                chunks,
                available_space: 1_000_000_000,
            };
            let _ = send_frame(&mut conn, MessageType::ChunkServerToMaster, &hb).await;
            let _: Result<(u8, MasterToChunkServer), _> =
                juzfs::protocol::read_frame(&mut conn).await;
        }
    }
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt::init();

    let args: Vec<String> = std::env::args().collect();
    if args.len() < 4 {
        eprintln!("usage: chunkserver-node <addr> <data_dir> <master_addr>");
        eprintln!("  e.g: chunkserver-node 127.0.0.1:6000 /tmp/cs1 127.0.0.1:5000");
        return;
    }

    let addr = &args[1];
    let data_dir = &args[2];
    let master_addr = args[3].clone();

    let cs = Arc::new(ChunkServer::new(data_dir.into(), addr.clone()));
    if let Err(e) = cs.init().await {
        eprintln!("failed to init chunkserver: {}", e);
        return;
    }

    tracing::info!("chunkserver listening on {}", addr);

    // spawn heartbeat loop
    let cs_hb = Arc::clone(&cs);
    tokio::spawn(heartbeat_loop(cs_hb, master_addr));

    // accept data connections
    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            eprintln!("failed to bind: {}", e);
            return;
        }
    };

    loop {
        match listener.accept().await {
            Ok((stream, peer)) => {
                tracing::info!("connection from {}", peer);
                let cs = Arc::clone(&cs);
                tokio::spawn(handle_connection(stream, cs));
            }
            Err(e) => eprintln!("accept error: {}", e),
        }
    }
}
