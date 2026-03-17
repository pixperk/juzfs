use tokio::net::TcpListener;

use juzfs::messages::*;
use juzfs::protocol::*;

/// spins up a listener on a random port, returns the addr
async fn start_listener() -> (TcpListener, String) {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap().to_string();
    (listener, addr)
}

#[tokio::test]
async fn test_roundtrip_client_to_master() {
    let (listener, addr) = start_listener().await;

    let send_handle = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let msg = ClientToMaster::CreateFile {
            filename: "/data/test.log".into(),
        };
        send_frame(&mut stream, MessageType::ClientToMaster, &msg)
            .await
            .unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let (msg_type, msg): (u8, ClientToMaster) = read_frame(&mut server_stream).await.unwrap();

    assert_eq!(msg_type, MessageType::ClientToMaster as u8);
    match msg {
        ClientToMaster::CreateFile { filename } => assert_eq!(filename, "/data/test.log"),
        _ => panic!("wrong variant"),
    }

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_roundtrip_master_to_client() {
    let (listener, addr) = start_listener().await;

    let send_handle = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let msg = MasterToClient::ChunkLocations {
            handle: 42,
            locations: vec!["10.0.0.1:9000".into(), "10.0.0.2:9000".into()],
        };
        send_frame(&mut stream, MessageType::MasterToClient, &msg)
            .await
            .unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let (msg_type, msg): (u8, MasterToClient) = read_frame(&mut server_stream).await.unwrap();

    assert_eq!(msg_type, MessageType::MasterToClient as u8);
    match msg {
        MasterToClient::ChunkLocations { handle, locations } => {
            assert_eq!(handle, 42);
            assert_eq!(locations, vec!["10.0.0.1:9000", "10.0.0.2:9000"]);
        }
        _ => panic!("wrong variant"),
    }

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_roundtrip_chunk_data() {
    let (listener, addr) = start_listener().await;
    let payload_data: Vec<u8> = (0..1024).map(|i| (i % 256) as u8).collect();
    let payload_clone = payload_data.clone();

    let send_handle = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let msg = ClientToChunkServer::PushData {
            handle: 7,
            data: payload_clone,
        };
        send_frame(&mut stream, MessageType::ClientToChunkServer, &msg)
            .await
            .unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let (_, msg): (u8, ClientToChunkServer) = read_frame(&mut server_stream).await.unwrap();

    match msg {
        ClientToChunkServer::PushData { handle, data } => {
            assert_eq!(handle, 7);
            assert_eq!(data, payload_data);
        }
        _ => panic!("wrong variant"),
    }

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_multiple_frames_on_same_stream() {
    let (listener, addr) = start_listener().await;

    let send_handle = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();

        let msg1 = ClientToMaster::CreateFile {
            filename: "/a".into(),
        };
        let msg2 = ClientToMaster::GetFileChunks {
            filename: "/a".into(),
        };
        let msg3 = ClientToMaster::GetChunkLocations { handle: 1 };

        send_frame(&mut stream, MessageType::ClientToMaster, &msg1)
            .await
            .unwrap();
        send_frame(&mut stream, MessageType::ClientToMaster, &msg2)
            .await
            .unwrap();
        send_frame(&mut stream, MessageType::ClientToMaster, &msg3)
            .await
            .unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();

    let (_, msg1): (u8, ClientToMaster) = read_frame(&mut server_stream).await.unwrap();
    let (_, msg2): (u8, ClientToMaster) = read_frame(&mut server_stream).await.unwrap();
    let (_, msg3): (u8, ClientToMaster) = read_frame(&mut server_stream).await.unwrap();

    assert!(matches!(msg1, ClientToMaster::CreateFile { .. }));
    assert!(matches!(msg2, ClientToMaster::GetFileChunks { .. }));
    assert!(matches!(msg3, ClientToMaster::GetChunkLocations { .. }));

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_bad_magic_rejected() {
    let (listener, addr) = start_listener().await;

    let send_handle = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        // send garbage header with wrong magic bytes
        use tokio::io::AsyncWriteExt;
        let bad_header: [u8; 8] = [0xFF, 0xFF, 1, 1, 0, 0, 0, 0];
        stream.write_all(&bad_header).await.unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let result: std::io::Result<(u8, ClientToMaster)> = read_frame(&mut server_stream).await;

    assert!(result.is_err());
    assert_eq!(result.unwrap_err().kind(), std::io::ErrorKind::InvalidData);

    send_handle.await.unwrap();
}

#[tokio::test]
async fn test_heartbeat_roundtrip() {
    let (listener, addr) = start_listener().await;

    let send_handle = tokio::spawn(async move {
        let mut stream = tokio::net::TcpStream::connect(&addr).await.unwrap();
        let msg = ChunkServerToMaster::Heartbeat {
            addr: "10.0.0.5:9000".into(),
            chunks: vec![1, 2, 3, 4, 5],
            available_space: 999_000,
        };
        send_frame(&mut stream, MessageType::ChunkServerToMaster, &msg)
            .await
            .unwrap();
    });

    let (mut server_stream, _) = listener.accept().await.unwrap();
    let (msg_type, msg): (u8, ChunkServerToMaster) =
        read_frame(&mut server_stream).await.unwrap();

    assert_eq!(msg_type, MessageType::ChunkServerToMaster as u8);
    match msg {
        ChunkServerToMaster::Heartbeat {
            addr,
            chunks,
            available_space,
        } => {
            assert_eq!(addr, "10.0.0.5:9000");
            assert_eq!(chunks, vec![1, 2, 3, 4, 5]);
            assert_eq!(available_space, 999_000);
        }
        _ => panic!("wrong variant"),
    }

    send_handle.await.unwrap();
}
