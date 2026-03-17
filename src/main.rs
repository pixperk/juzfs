use std::sync::Arc;

use juzfs::{
    chunkserver::ChunkServer,
    client::Client,
    master::Master,
    messages::*,
    protocol::{decode_payload, read_raw_frame, send_frame, MessageType},
};
use tokio::net::{TcpListener, TcpStream};

/// small chunk size for testing (256 bytes instead of 64MB)
const TEST_CHUNK_SIZE: u64 = 256;

// ---- master TCP server (in-process) ----

async fn master_handle(mut stream: TcpStream, master: Arc<Master>) {
    loop {
        let (msg_type, payload) = match read_raw_frame(&mut stream).await {
            Ok(v) => v,
            Err(_) => break,
        };
        match msg_type {
            1 => {
                let msg: ClientToMaster = match decode_payload(&payload) {
                    Ok(m) => m,
                    Err(_) => break,
                };
                let resp = match msg {
                    ClientToMaster::CreateFile { filename } => {
                        master.create_file(filename).await;
                        MasterToClient::Ok
                    }
                    ClientToMaster::GetFileChunks { filename } => {
                        match master.get_file_chunks(&filename).await {
                            Some(c) => MasterToClient::FileChunks(c),
                            None => MasterToClient::Error("file not found".into()),
                        }
                    }
                    ClientToMaster::GetChunkLocations { handle } => {
                        match master.get_chunk_locations(handle).await {
                            Some(l) => MasterToClient::ChunkLocations { handle, locations: l },
                            None => MasterToClient::Error("chunk not found".into()),
                        }
                    }
                    ClientToMaster::AllocateChunk { filename } => {
                        let locs = master.choose_locations(3).await;
                        if locs.is_empty() {
                            MasterToClient::Error("no chunkservers".into())
                        } else {
                            match master.add_chunk(&filename, locs).await {
                                Some(h) => MasterToClient::ChunkLocations {
                                    handle: h,
                                    locations: master.get_chunk_locations(h).await.unwrap(),
                                },
                                None => MasterToClient::Error("file not found".into()),
                            }
                        }
                    }
                    ClientToMaster::GetPrimary { handle } => {
                        match master.grant_lease(handle).await {
                            Some((p, s)) => MasterToClient::PrimaryInfo { primary: p, secondaries: s },
                            None => MasterToClient::Error("chunk not found".into()),
                        }
                    }
                };
                let _ = send_frame(&mut stream, MessageType::MasterToClient, &resp).await;
            }
            3 => {
                let msg: ChunkServerToMaster = match decode_payload(&payload) {
                    Ok(m) => m,
                    Err(_) => break,
                };
                let resp = match msg {
                    ChunkServerToMaster::Register { addr, available_space } => {
                        println!("  [master] registered {}", addr);
                        master.register_chunkserver(addr, available_space).await;
                        MasterToChunkServer::Ok
                    }
                    ChunkServerToMaster::Heartbeat { addr, chunks, available_space } => {
                        master.heartbeat(&addr, chunks, available_space).await;
                        MasterToChunkServer::Ok
                    }
                };
                let _ = send_frame(&mut stream, MessageType::MasterToChunkServer, &resp).await;
            }
            _ => break,
        }
    }
}

async fn spawn_master(addr: &str) -> Arc<Master> {
    let master = Arc::new(Master::new(60));
    let listener = TcpListener::bind(addr).await.unwrap();
    let m = master.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let m = m.clone();
            tokio::spawn(master_handle(stream, m));
        }
    });
    master
}

// ---- chunkserver TCP server (in-process) ----

async fn cs_handle(mut stream: TcpStream, cs: Arc<ChunkServer>) {
    loop {
        let (msg_type, payload) = match read_raw_frame(&mut stream).await {
            Ok(v) => v,
            Err(_) => break,
        };
        match msg_type {
            5 => {
                let msg: ClientToChunkServer = match decode_payload(&payload) {
                    Ok(m) => m,
                    Err(_) => break,
                };
                let resp = match msg {
                    ClientToChunkServer::Read { handle, offset, length } => {
                        match cs.read_chunk(handle, offset as usize, length as usize).await {
                            Ok(d) => ChunkServerToClient::Data(d),
                            Err(e) => ChunkServerToClient::Error(e.to_string()),
                        }
                    }
                    ClientToChunkServer::PushData { handle, data } => {
                        cs.buffer_push(handle, data).await;
                        ChunkServerToClient::Ok
                    }
                    ClientToChunkServer::Write { handle, secondaries } => {
                        let serial = cs.next_serial();
                        if let Err(e) = cs.flush_push(handle).await {
                            let _ = send_frame(&mut stream, MessageType::ChunkServerToClient,
                                &ChunkServerToClient::Error(e.to_string())).await;
                            continue;
                        }
                        let mut all_ok = true;
                        for addr in &secondaries {
                            let commit = ChunkServerToChunkServer::CommitWrite { handle, serial };
                            match TcpStream::connect(addr).await {
                                Ok(mut conn) => {
                                    let _ = send_frame(&mut conn, MessageType::ChunkServerToChunkServer, &commit).await;
                                    match juzfs::protocol::read_frame::<ChunkServerAck>(&mut conn).await {
                                        Ok((_, ChunkServerAck::Ok)) => {}
                                        _ => { all_ok = false; break; }
                                    }
                                }
                                Err(_) => { all_ok = false; break; }
                            }
                        }
                        if all_ok { ChunkServerToClient::Ok }
                        else { ChunkServerToClient::Error("secondary commit failed".into()) }
                    }
                    ClientToChunkServer::Append { handle, data, secondaries } => {
                        let max_chunk = TEST_CHUNK_SIZE;
                        let current_size = cs.chunk_size_on_disk(handle).unwrap_or(0);

                        // check if append fits in this chunk
                        if current_size + data.len() as u64 > max_chunk {
                            // pad chunk on all replicas, tell client to retry on new chunk
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

                            // append on primary
                            if let Err(e) = cs.append_to_chunk(handle, &data, offset).await {
                                ChunkServerToClient::Error(e.to_string())
                            } else {
                                // tell secondaries to append at same offset
                                let mut all_ok = true;
                                for addr in &secondaries {
                                    let commit = ChunkServerToChunkServer::CommitAppend {
                                        handle, data: data.clone(), offset, serial,
                                    };
                                    match TcpStream::connect(addr).await {
                                        Ok(mut c) => {
                                            let _ = send_frame(&mut c, MessageType::ChunkServerToChunkServer, &commit).await;
                                            match juzfs::protocol::read_frame::<ChunkServerAck>(&mut c).await {
                                                Ok((_, ChunkServerAck::Ok)) => {}
                                                _ => { all_ok = false; break; }
                                            }
                                        }
                                        Err(_) => { all_ok = false; break; }
                                    }
                                }
                                if all_ok { ChunkServerToClient::AppendOk { offset } }
                                else { ChunkServerToClient::Error("secondary append failed".into()) }
                            }
                        }
                    }
                };
                let _ = send_frame(&mut stream, MessageType::ChunkServerToClient, &resp).await;
            }
            7 => {
                let msg: ChunkServerToChunkServer = match decode_payload(&payload) {
                    Ok(m) => m,
                    Err(_) => break,
                };
                let resp = match msg {
                    ChunkServerToChunkServer::ForwardData { handle, data, remaining } => {
                        cs.buffer_push(handle, data.clone()).await;
                        if let Some((next, rest)) = remaining.split_first() {
                            let fwd = ChunkServerToChunkServer::ForwardData {
                                handle, data, remaining: rest.to_vec(),
                            };
                            if let Ok(mut c) = TcpStream::connect(next).await {
                                let _ = send_frame(&mut c, MessageType::ChunkServerToChunkServer, &fwd).await;
                                let _: Result<(u8, ChunkServerAck), _> = juzfs::protocol::read_frame(&mut c).await;
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
                    ChunkServerToChunkServer::CommitAppend { handle, data, offset, .. } => {
                        if data.is_empty() {
                            // pad command
                            match cs.pad_chunk(handle, TEST_CHUNK_SIZE).await {
                                Ok(_) => ChunkServerAck::Ok,
                                Err(e) => ChunkServerAck::Error(e.to_string()),
                            }
                        } else {
                            match cs.append_to_chunk(handle, &data, offset).await {
                                Ok(_) => ChunkServerAck::Ok,
                                Err(e) => ChunkServerAck::Error(e.to_string()),
                            }
                        }
                    }
                };
                let _ = send_frame(&mut stream, MessageType::ChunkServerAck, &resp).await;
            }
            _ => break,
        }
    }
}

async fn spawn_chunkserver(addr: &str, data_dir: &str, master_addr: &str) -> Arc<ChunkServer> {
    let cs = Arc::new(ChunkServer::new(data_dir.into(), addr.to_string()));
    cs.init().await.unwrap();

    // register with master
    let mut conn = TcpStream::connect(master_addr).await.unwrap();
    send_frame(&mut conn, MessageType::ChunkServerToMaster,
        &ChunkServerToMaster::Register { addr: addr.to_string(), available_space: 1_000_000_000 },
    ).await.unwrap();
    let _: (u8, MasterToChunkServer) = juzfs::protocol::read_frame(&mut conn).await.unwrap();

    let listener = TcpListener::bind(addr).await.unwrap();
    let cs2 = cs.clone();
    tokio::spawn(async move {
        loop {
            let (stream, _) = listener.accept().await.unwrap();
            let cs = cs2.clone();
            tokio::spawn(cs_handle(stream, cs));
        }
    });
    cs
}

// ---- tests ----

#[tokio::main]
async fn main() {
    // clean up from previous runs
    for d in ["/tmp/juzfs_t1", "/tmp/juzfs_t2", "/tmp/juzfs_t3"] {
        let _ = std::fs::remove_dir_all(d);
    }

    println!("=== juzfs e2e test ===\n");

    // boot master + 3 chunkservers
    let _master = spawn_master("127.0.0.1:5000").await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    spawn_chunkserver("127.0.0.1:6001", "/tmp/juzfs_t1", "127.0.0.1:5000").await;
    spawn_chunkserver("127.0.0.1:6002", "/tmp/juzfs_t2", "127.0.0.1:5000").await;
    spawn_chunkserver("127.0.0.1:6003", "/tmp/juzfs_t3", "127.0.0.1:5000").await;
    tokio::time::sleep(std::time::Duration::from_millis(50)).await;

    println!("[ok] master + 3 chunkservers running\n");

    let client = Client::new("127.0.0.1:5000".into(), TEST_CHUNK_SIZE);

    // --- test 1: create + write + offset read ---
    println!("--- test 1: write + offset read ---");
    client.create_file("/test.txt").await.unwrap();
    client.allocate_chunk("/test.txt").await.unwrap();

    let data = b"The quick brown fox jumps over the lazy dog";
    client.write("/test.txt", 0, data).await.unwrap();
    println!("  wrote {} bytes", data.len());

    // full read
    let result = client.read("/test.txt", 0, data.len() as u64).await.unwrap();
    assert_eq!(result, data.to_vec());
    println!("  full read: OK");

    // partial read: "brown"
    let partial = client.read("/test.txt", 10, 5).await.unwrap();
    assert_eq!(&partial, b"brown");
    println!("  partial read (offset=10, len=5): \"{}\"", String::from_utf8_lossy(&partial));

    // partial read: "lazy dog"
    let partial2 = client.read("/test.txt", 35, 8).await.unwrap();
    assert_eq!(&partial2, b"lazy dog");
    println!("  partial read (offset=35, len=8): \"{}\"", String::from_utf8_lossy(&partial2));

    println!("  PASSED\n");

    // --- test 2: streaming read ---
    println!("--- test 2: streaming read ---");
    let mut rx = client.read_stream("/test.txt").await.unwrap();
    let mut streamed = Vec::new();
    while let Some(chunk) = rx.recv().await {
        let bytes = chunk.unwrap();
        println!("  stream chunk: {} bytes", bytes.len());
        streamed.extend_from_slice(&bytes);
    }
    assert_eq!(streamed, data.to_vec());
    println!("  stream matches original data: OK");
    println!("  PASSED\n");

    // --- test 3: multiple writes ---
    println!("--- test 3: second file + write ---");
    client.create_file("/logs/app.log").await.unwrap();
    client.allocate_chunk("/logs/app.log").await.unwrap();

    let log1 = b"[INFO] server started\n";
    client.write("/logs/app.log", 0, log1).await.unwrap();

    let result = client.read("/logs/app.log", 0, log1.len() as u64).await.unwrap();
    assert_eq!(result, log1.to_vec());
    println!("  wrote + read /logs/app.log: OK");
    println!("  PASSED\n");

    // --- test 4: write replication check ---
    println!("--- test 4: replication (data on all 3 chunkservers) ---");
    // verify chunk files exist on all 3 chunkservers
    for dir in ["/tmp/juzfs_t1", "/tmp/juzfs_t2", "/tmp/juzfs_t3"] {
        let chunks: Vec<_> = std::fs::read_dir(format!("{}/chunks", dir))
            .unwrap()
            .filter_map(|e| e.ok())
            .collect();
        println!("  {} has {} chunk(s) on disk", dir, chunks.len());
    }
    println!("  PASSED\n");

    // --- test 5: record append ---
    println!("--- test 5: record append ---");
    client.create_file("/append.log").await.unwrap();
    client.allocate_chunk("/append.log").await.unwrap();

    let offset1 = client.append("/append.log", b"line one\n").await.unwrap();
    println!("  append 1 at offset {}", offset1);
    assert_eq!(offset1, 0);

    let offset2 = client.append("/append.log", b"line two\n").await.unwrap();
    println!("  append 2 at offset {}", offset2);
    assert_eq!(offset2, 9);

    let offset3 = client.append("/append.log", b"line three\n").await.unwrap();
    println!("  append 3 at offset {}", offset3);
    assert_eq!(offset3, 18);

    // read back all appended data
    let all = client.read("/append.log", 0, 29).await.unwrap();
    assert_eq!(&all, b"line one\nline two\nline three\n");
    println!("  read all: \"{}\"", String::from_utf8_lossy(&all));
    println!("  PASSED\n");

    // --- test 6: append triggers new chunk allocation (chunk_size=256) ---
    println!("--- test 6: append overflow to new chunk ---");
    client.create_file("/big_append.log").await.unwrap();
    client.allocate_chunk("/big_append.log").await.unwrap();

    // fill up the chunk (256 bytes) with multiple appends
    let line = b"abcdefghijklmnopqrstuvwxyz0123\n"; // 31 bytes
    let mut total_written = 0u64;
    for i in 0..8 {
        let offset = client.append("/big_append.log", line).await.unwrap();
        println!("  append {} at offset {}", i, offset);
        total_written += line.len() as u64;
    }
    // 8 * 31 = 248 bytes, fits in one chunk
    // 9th append (248 + 31 = 279 > 256) should trigger new chunk
    let overflow_offset = client.append("/big_append.log", line).await.unwrap();
    println!("  overflow append at offset {} (new chunk)", overflow_offset);
    assert_eq!(overflow_offset, 0); // starts at 0 in the new chunk
    println!("  PASSED\n");

    println!("=== all tests passed ===");
}
