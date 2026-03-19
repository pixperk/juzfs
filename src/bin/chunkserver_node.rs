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
            4 => handle_master_msg(&cs, &payload).await,
            5 => handle_client_msg(&mut stream, &cs, &payload).await,
            7 => handle_cs_msg(&mut stream, &cs, &payload).await,
            _ => {
                tracing::warn!(msg_type, "unknown message type, dropping connection");
                break;
            }
        }
    }
}

async fn handle_master_msg(cs: &ChunkServer, payload: &[u8]) {
    let msg: MasterToChunkServer = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode master message");
            return;
        }
    };

    match msg {
        MasterToChunkServer::UpdateVersion { handle, version } => {
            tracing::info!(chunk = handle, version, "version update from master");
            if let Err(e) = cs.update_version(handle, version).await {
                tracing::error!(chunk = handle, error = %e, "failed to update version");
            }
        }
        MasterToChunkServer::DeleteChunks(handles) => {
            tracing::info!(chunks = ?handles, "gc: deleting chunks");
            for h in handles {
                if let Err(e) = cs.delete_chunk(h).await {
                    tracing::error!(chunk = h, error = %e, "failed to delete chunk");
                }
            }
        }
        _ => {}
    }
}

async fn handle_client_msg(stream: &mut TcpStream, cs: &ChunkServer, payload: &[u8]) {
    let msg: ClientToChunkServer = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode client message");
            return;
        }
    };

    let response = match msg {
        ClientToChunkServer::Read {
            handle,
            offset,
            length,
        } => match cs.read_chunk(handle, offset as usize, length as usize).await {
            Ok(data) => {
                tracing::info!(chunk = handle, offset, bytes = data.len(), "read");
                ChunkServerToClient::Data(data)
            }
            Err(e) => {
                tracing::error!(chunk = handle, offset, error = %e, "read failed");
                ChunkServerToClient::Error(e.to_string())
            }
        },
        ClientToChunkServer::PushData { handle, data } => {
            tracing::info!(chunk = handle, bytes = data.len(), "push data buffered");
            cs.buffer_push(handle, data).await;
            ChunkServerToClient::Ok
        }
        ClientToChunkServer::Write { handle, secondaries } => {
            let serial = cs.next_serial();
            tracing::info!(chunk = handle, serial, secondaries = ?secondaries, "write (primary)");

            if let Err(e) = cs.flush_push(handle).await {
                tracing::error!(chunk = handle, error = %e, "flush failed");
                let _ = send_frame(stream, MessageType::ChunkServerToClient,
                    &ChunkServerToClient::Error(e.to_string())).await;
                return;
            }

            for addr in &secondaries {
                let commit = ChunkServerToChunkServer::CommitWrite { handle, serial };
                match TcpStream::connect(addr).await {
                    Ok(mut conn) => {
                        if let Err(e) = send_frame(
                            &mut conn,
                            MessageType::ChunkServerToChunkServer,
                            &commit,
                        ).await {
                            tracing::error!(chunk = handle, secondary = %addr, error = %e, "secondary send failed");
                            let _ = send_frame(stream, MessageType::ChunkServerToClient,
                                &ChunkServerToClient::Error(format!("secondary {} send failed: {}", addr, e))).await;
                            return;
                        }
                        match juzfs::protocol::read_frame::<ChunkServerAck>(&mut conn).await {
                            Ok((_, ChunkServerAck::Ok)) => {
                                tracing::debug!(chunk = handle, secondary = %addr, "secondary committed");
                            }
                            Ok((_, ChunkServerAck::Error(e))) => {
                                tracing::error!(chunk = handle, secondary = %addr, error = %e, "secondary commit failed");
                                let _ = send_frame(stream, MessageType::ChunkServerToClient,
                                    &ChunkServerToClient::Error(format!("secondary {} failed: {}", addr, e))).await;
                                return;
                            }
                            Err(e) => {
                                tracing::error!(chunk = handle, secondary = %addr, error = %e, "secondary ack failed");
                                let _ = send_frame(stream, MessageType::ChunkServerToClient,
                                    &ChunkServerToClient::Error(format!("secondary {} ack failed: {}", addr, e))).await;
                                return;
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(chunk = handle, secondary = %addr, error = %e, "secondary unreachable");
                        let _ = send_frame(stream, MessageType::ChunkServerToClient,
                            &ChunkServerToClient::Error(format!("secondary {} unreachable: {}", addr, e))).await;
                        return;
                    }
                }
            }

            tracing::info!(chunk = handle, serial, "write committed on all replicas");
            ChunkServerToClient::Ok
        }
        ClientToChunkServer::Append { handle, data, secondaries } => {
            let max_chunk = cs.chunk_size();
            let current_size = cs.chunk_size_on_disk(handle).unwrap_or(0);

            if current_size + data.len() as u64 > max_chunk {
                tracing::info!(
                    chunk = handle,
                    current = current_size,
                    append = data.len(),
                    limit = max_chunk,
                    "chunk full, padding and asking client to retry"
                );
                let _ = cs.pad_chunk(handle, max_chunk).await;
                for addr in &secondaries {
                    if let Ok(mut c) = TcpStream::connect(addr).await {
                        let pad = ChunkServerToChunkServer::CommitAppend {
                            handle, data: vec![], offset: max_chunk, serial: cs.next_serial(),
                        };
                        let _ = send_frame(&mut c, MessageType::ChunkServerToChunkServer, &pad).await;
                        let _: Result<(u8, ChunkServerAck), _> = juzfs::protocol::read_frame(&mut c).await;
                    }
                }
                ChunkServerToClient::RetryNewChunk
            } else {
                let offset = current_size;
                let serial = cs.next_serial();
                tracing::info!(chunk = handle, offset, bytes = data.len(), serial, "append (primary)");

                if let Err(e) = cs.append_to_chunk(handle, &data, offset).await {
                    tracing::error!(chunk = handle, error = %e, "append failed");
                    ChunkServerToClient::Error(e.to_string())
                } else {
                    let mut all_ok = true;
                    for addr in &secondaries {
                        let commit = ChunkServerToChunkServer::CommitAppend {
                            handle, data: data.clone(), offset, serial,
                        };
                        match TcpStream::connect(addr).await {
                            Ok(mut c) => {
                                let _ = send_frame(&mut c, MessageType::ChunkServerToChunkServer, &commit).await;
                                match juzfs::protocol::read_frame::<ChunkServerAck>(&mut c).await {
                                    Ok((_, ChunkServerAck::Ok)) => {
                                        tracing::debug!(chunk = handle, secondary = %addr, "secondary append ok");
                                    }
                                    _ => {
                                        tracing::error!(chunk = handle, secondary = %addr, "secondary append failed");
                                        all_ok = false; break;
                                    }
                                }
                            }
                            Err(e) => {
                                tracing::error!(chunk = handle, secondary = %addr, error = %e, "secondary unreachable");
                                all_ok = false; break;
                            }
                        }
                    }
                    if all_ok {
                        tracing::info!(chunk = handle, offset, "append committed on all replicas");
                        ChunkServerToClient::AppendOk { offset }
                    } else {
                        ChunkServerToClient::Error("secondary append failed".into())
                    }
                }
            }
        }
    };

    let _ = send_frame(stream, MessageType::ChunkServerToClient, &response).await;
}

async fn handle_cs_msg(stream: &mut TcpStream, cs: &ChunkServer, payload: &[u8]) {
    let msg: ChunkServerToChunkServer = match decode_payload(payload) {
        Ok(m) => m,
        Err(e) => {
            tracing::error!(error = %e, "failed to decode cs-to-cs message");
            return;
        }
    };

    let response = match msg {
        ChunkServerToChunkServer::ForwardData {
            handle,
            data,
            remaining,
        } => {
            tracing::info!(chunk = handle, bytes = data.len(), remaining = remaining.len(), "forward data buffered");
            cs.buffer_push(handle, data.clone()).await;

            let mut chain_err: Option<String> = None;
            if let Some((next, rest)) = remaining.split_first() {
                tracing::debug!(chunk = handle, next = %next, "forwarding to next in chain");
                let fwd = ChunkServerToChunkServer::ForwardData {
                    handle,
                    data,
                    remaining: rest.to_vec(),
                };
                match TcpStream::connect(next).await {
                    Ok(mut next_conn) => {
                        if let Err(e) = send_frame(
                            &mut next_conn,
                            MessageType::ChunkServerToChunkServer,
                            &fwd,
                        ).await {
                            tracing::error!(chunk = handle, next = %next, error = %e, "forward send failed");
                            chain_err = Some(format!("forward to {} failed: {}", next, e));
                        } else {
                            match juzfs::protocol::read_frame::<ChunkServerAck>(&mut next_conn).await {
                                Ok((_, ChunkServerAck::Ok)) => {}
                                Ok((_, ChunkServerAck::Error(e))) => {
                                    tracing::error!(chunk = handle, next = %next, error = %e, "forward chain failed");
                                    chain_err = Some(format!("chain forward {} failed: {}", next, e));
                                }
                                Err(e) => {
                                    tracing::error!(chunk = handle, next = %next, error = %e, "forward ack failed");
                                    chain_err = Some(format!("forward ack from {} failed: {}", next, e));
                                }
                            }
                        }
                    }
                    Err(e) => {
                        tracing::error!(chunk = handle, next = %next, error = %e, "failed to forward data");
                        chain_err = Some(format!("forward to {} unreachable: {}", next, e));
                    }
                }
            }

            match chain_err {
                Some(e) => ChunkServerAck::Error(e),
                None => ChunkServerAck::Ok,
            }
        }
        ChunkServerToChunkServer::CommitWrite { handle, serial } => {
            tracing::info!(chunk = handle, serial, "commit write (secondary)");
            match cs.flush_push(handle).await {
                Ok(_) => ChunkServerAck::Ok,
                Err(e) => {
                    tracing::error!(chunk = handle, error = %e, "commit write failed");
                    ChunkServerAck::Error(e.to_string())
                }
            }
        }
        ChunkServerToChunkServer::CommitAppend { handle, data, offset, serial } => {
            if data.is_empty() {
                tracing::info!(chunk = handle, "pad chunk (secondary)");
                match cs.pad_chunk(handle, cs.chunk_size()).await {
                    Ok(_) => ChunkServerAck::Ok,
                    Err(e) => {
                        tracing::error!(chunk = handle, error = %e, "pad failed");
                        ChunkServerAck::Error(e.to_string())
                    }
                }
            } else {
                tracing::info!(chunk = handle, offset, bytes = data.len(), serial, "commit append (secondary)");
                match cs.append_to_chunk(handle, &data, offset).await {
                    Ok(_) => ChunkServerAck::Ok,
                    Err(e) => {
                        tracing::error!(chunk = handle, error = %e, "append failed");
                        ChunkServerAck::Error(e.to_string())
                    }
                }
            }
        }
    };

    let _ = send_frame(stream, MessageType::ChunkServerAck, &response).await;
}

/// heartbeat loop: register with master, then send periodic heartbeats
async fn heartbeat_loop(cs: Arc<ChunkServer>, master_addr: String) {
    if let Ok(mut conn) = TcpStream::connect(&master_addr).await {
        let register = ChunkServerToMaster::Register {
            addr: cs.addr().to_string(),
            available_space: cs.available_space(),
        };
        let _ = send_frame(&mut conn, MessageType::ChunkServerToMaster, &register).await;
        let _: Result<(u8, MasterToChunkServer), _> =
            juzfs::protocol::read_frame(&mut conn).await;
        tracing::info!(master = %master_addr, "registered with master");
    } else {
        tracing::error!(master = %master_addr, "failed to register with master");
    }

    loop {
        tokio::time::sleep(tokio::time::Duration::from_secs(5)).await;

        if let Ok(mut conn) = TcpStream::connect(&master_addr).await {
            let chunks = cs.list_chunks().await;
            tracing::debug!(
                chunks = chunks.len(),
                available_mb = cs.available_space() / (1024 * 1024),
                "sending heartbeat"
            );
            let hb = ChunkServerToMaster::Heartbeat {
                addr: cs.addr().to_string(),
                chunks,
                available_space: cs.available_space(),
            };
            let _ = send_frame(&mut conn, MessageType::ChunkServerToMaster, &hb).await;
            match juzfs::protocol::read_raw_frame(&mut conn).await {
                Ok((_msg_type, payload)) => {
                    handle_master_msg(&cs, &payload).await;
                }
                Err(e) => {
                    tracing::warn!(error = %e, "failed to read heartbeat response");
                }
            }
        } else {
            tracing::warn!(master = %master_addr, "heartbeat failed, master unreachable");
        }
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
    if args.len() < 4 {
        eprintln!("usage: chunkserver-node <addr> <data_dir> <master_addr> [capacity_bytes] [chunk_size_bytes]");
        eprintln!("  e.g: chunkserver-node 127.0.0.1:6000 /tmp/cs1 127.0.0.1:5000");
        return;
    }

    let addr = &args[1];
    let data_dir = &args[2];
    let master_addr = args[3].clone();

    let capacity: u64 = args.get(4).and_then(|s| s.parse().ok()).unwrap_or(1_000_000_000);
    let chunk_size: u64 = args.get(5).and_then(|s| s.parse().ok()).unwrap_or(juzfs::CHUNK_SIZE);
    let cs = Arc::new(ChunkServer::new(data_dir.into(), addr.clone(), capacity, chunk_size));
    if let Err(e) = cs.init().await {
        tracing::error!(error = %e, "failed to init chunkserver");
        return;
    }

    tracing::info!(
        addr = %addr,
        data_dir = %data_dir,
        capacity_mb = capacity / (1024 * 1024),
        "chunkserver starting"
    );

    let cs_hb = Arc::clone(&cs);
    tokio::spawn(heartbeat_loop(cs_hb, master_addr));

    let listener = match TcpListener::bind(addr).await {
        Ok(l) => l,
        Err(e) => {
            tracing::error!(addr = %addr, error = %e, "failed to bind");
            return;
        }
    };

    tracing::info!(addr = %addr, "listening for connections");

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let cs = Arc::clone(&cs);
                tokio::spawn(handle_connection(stream, cs));
            }
            Err(e) => tracing::error!(error = %e, "accept failed"),
        }
    }
}
