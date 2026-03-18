use juzfs::chunkserver::ChunkServer;
use tempfile::tempdir;

async fn make_chunkserver(dir: &std::path::Path) -> ChunkServer {
    let cs = ChunkServer::new(dir.to_path_buf(), "127.0.0.1:9000".into(), 1_000_000_000);
    cs.init().await.unwrap();
    cs
}

#[tokio::test]
async fn test_store_and_read_chunk() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    let data = b"hello juzfs chunk data".to_vec();
    cs.store_chunk(1, data.clone()).await.unwrap();

    assert!(cs.has_chunk(1).await);

    let result = cs.read_chunk(1, 0, data.len()).await.unwrap();
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_read_chunk_partial() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    let data = b"abcdefghijklmnop".to_vec();
    cs.store_chunk(1, data).await.unwrap();

    let result = cs.read_chunk(1, 4, 4).await.unwrap();
    assert_eq!(result, b"efgh".to_vec());
}

#[tokio::test]
async fn test_read_out_of_bounds() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    cs.store_chunk(1, b"short".to_vec()).await.unwrap();

    // length past end gets clamped, returns available data
    let result = cs.read_chunk(1, 0, 100).await.unwrap();
    assert_eq!(result, b"short".to_vec());

    // offset past end is an error
    let err = cs.read_chunk(1, 100, 5).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::InvalidInput);
}

#[tokio::test]
async fn test_checksum_detects_corruption() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    cs.store_chunk(1, b"original data".to_vec()).await.unwrap();

    // corrupt the chunk file directly
    let chunk_path = dir.path().join("chunks").join("00000001.chunk");
    std::fs::write(&chunk_path, b"corrupted!!!").unwrap();

    let err = cs.read_chunk(1, 0, 12).await;
    assert!(err.is_err());
    assert_eq!(err.unwrap_err().kind(), std::io::ErrorKind::InvalidData);
}

#[tokio::test]
async fn test_two_phase_write_buffer_then_flush() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    let data = b"buffered write data".to_vec();
    cs.buffer_push(42, data.clone()).await;

    // not on disk yet
    assert!(!cs.has_chunk(42).await);

    // flush commits to disk
    cs.flush_push(42).await.unwrap();

    assert!(cs.has_chunk(42).await);
    let result = cs.read_chunk(42, 0, data.len()).await.unwrap();
    assert_eq!(result, data);
}

#[tokio::test]
async fn test_flush_nonexistent_buffer_is_noop() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    // flushing a handle that was never buffered should not error
    cs.flush_push(999).await.unwrap();
    assert!(!cs.has_chunk(999).await);
}

#[tokio::test]
async fn test_delete_chunk() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    cs.store_chunk(1, b"data".to_vec()).await.unwrap();
    assert!(cs.has_chunk(1).await);

    cs.delete_chunk(1).await.unwrap();
    assert!(!cs.has_chunk(1).await);

    // files should be gone
    assert!(!dir.path().join("chunks/00000001.chunk").exists());
    assert!(!dir.path().join("checksums/00000001.csum").exists());
}

#[tokio::test]
async fn test_list_chunks() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    cs.store_chunk(10, b"a".to_vec()).await.unwrap();
    cs.store_chunk(20, b"b".to_vec()).await.unwrap();
    cs.store_chunk(30, b"c".to_vec()).await.unwrap();

    let mut chunks = cs.list_chunks().await;
    chunks.sort();
    assert_eq!(chunks, vec![(10, 0), (20, 0), (30, 0)]);
}

#[tokio::test]
async fn test_init_recovers_existing_chunks() {
    let dir = tempdir().unwrap();

    // first instance stores some chunks
    {
        let cs = make_chunkserver(dir.path()).await;
        cs.store_chunk(1, b"data1".to_vec()).await.unwrap();
        cs.store_chunk(2, b"data2".to_vec()).await.unwrap();
    }

    // new instance on same dir should recover them
    let cs2 = make_chunkserver(dir.path()).await;
    assert!(cs2.has_chunk(1).await);
    assert!(cs2.has_chunk(2).await);

    let mut chunks = cs2.list_chunks().await;
    chunks.sort();
    assert_eq!(chunks, vec![(1, 0), (2, 0)]);
}

#[tokio::test]
async fn test_update_version() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;

    cs.store_chunk(1, b"data".to_vec()).await.unwrap();
    assert_eq!(cs.get_version(1).await, Some(0));

    cs.update_version(1, 5).await.unwrap();
    assert_eq!(cs.get_version(1).await, Some(5));

    // version persists across restarts
    let cs2 = make_chunkserver(dir.path()).await;
    assert_eq!(cs2.get_version(1).await, Some(5));
}

#[tokio::test]
async fn test_addr() {
    let dir = tempdir().unwrap();
    let cs = make_chunkserver(dir.path()).await;
    assert_eq!(cs.addr(), "127.0.0.1:9000");
}
