use std::time::Instant;

use juzfs::master::Master;

fn make_master() -> Master {
    Master::new(60)
}

#[tokio::test]
async fn test_create_file_and_get_chunks() {
    let m = make_master();
    m.create_file("/data/log.txt".into()).await;

    let chunks = m.get_file_chunks("/data/log.txt").await;
    assert_eq!(chunks, Some(vec![]));

    assert_eq!(m.get_file_chunks("/nope").await, None);
}

#[tokio::test]
async fn test_allocate_chunk_handle_increments() {
    let m = make_master();
    assert_eq!(m.allocate_chunk_handle().await, 1);
    assert_eq!(m.allocate_chunk_handle().await, 2);
    assert_eq!(m.allocate_chunk_handle().await, 3);
}

#[tokio::test]
async fn test_add_chunk_to_file() {
    let m = make_master();
    m.create_file("/data/log.txt".into()).await;

    let locs = vec!["10.0.0.1:9000".into(), "10.0.0.2:9000".into()];
    let handle = m.add_chunk("/data/log.txt", locs.clone()).await;
    assert_eq!(handle, Some(1));

    let chunks = m.get_file_chunks("/data/log.txt").await;
    assert_eq!(chunks, Some(vec![1]));

    let locations = m.get_chunk_locations(1).await;
    assert_eq!(locations, Some(locs));
}

#[tokio::test]
async fn test_add_chunk_to_nonexistent_file() {
    let m = make_master();
    assert_eq!(m.add_chunk("/nope", vec!["addr".into()]).await, None);
}

#[tokio::test]
async fn test_register_and_heartbeat() {
    let m = make_master();
    m.register_chunkserver("10.0.0.1:9000".into(), 500).await;

    {
        let servers = m.chunkservers.read().await;
        assert!(servers.contains_key("10.0.0.1:9000"));
        assert_eq!(servers["10.0.0.1:9000"].available_space, 500);
        assert!(servers["10.0.0.1:9000"].chunks.is_empty());
    }

    m.heartbeat("10.0.0.1:9000", vec![(1, 0), (2, 0), (3, 0)], 400).await;

    let servers = m.chunkservers.read().await;
    assert_eq!(servers["10.0.0.1:9000"].chunks, vec![(1, 0), (2, 0), (3, 0)]);
    assert_eq!(servers["10.0.0.1:9000"].available_space, 400);
}

#[tokio::test]
async fn test_choose_locations_picks_most_space() {
    let m = make_master();
    m.register_chunkserver("small:9000".into(), 100).await;
    m.register_chunkserver("big:9000".into(), 1000).await;
    m.register_chunkserver("medium:9000".into(), 500).await;

    let locs = m.choose_locations(2).await;
    assert_eq!(locs.len(), 2);
    assert_eq!(locs[0], "big:9000");
    assert_eq!(locs[1], "medium:9000");
}

#[tokio::test]
async fn test_choose_locations_fewer_servers_than_requested() {
    let m = make_master();
    m.register_chunkserver("only:9000".into(), 100).await;
    assert_eq!(m.choose_locations(3).await.len(), 1);
}

#[tokio::test]
async fn test_multiple_chunks_per_file() {
    let m = make_master();
    m.create_file("/data/big.bin".into()).await;

    m.add_chunk("/data/big.bin", vec!["a:9000".into()]).await;
    m.add_chunk("/data/big.bin", vec!["b:9000".into()]).await;
    m.add_chunk("/data/big.bin", vec!["c:9000".into()]).await;

    let chunks = m.get_file_chunks("/data/big.bin").await.unwrap();
    assert_eq!(chunks, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_lease_expiry_is_set() {
    let m = make_master();
    m.create_file("/f".into()).await;
    m.add_chunk("/f", vec!["a:9000".into()]).await;

    let chunks = m.chunks.read().await;
    let info = &chunks[&1];
    assert!(info.lease_expiry.is_some());
    assert_eq!(info.version, 1);
    let remaining = info.lease_expiry.unwrap().duration_since(Instant::now());
    assert!(remaining.as_secs() > 50);
}

#[tokio::test]
async fn test_grant_lease_new() {
    let m = make_master();
    m.create_file("/f".into()).await;
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into(), "c:9000".into()]).await;

    let (primary, secondaries, version, bumped) = m.grant_lease(1).await.unwrap();

    assert_eq!(primary, "a:9000");
    assert_eq!(secondaries.len(), 2);
    assert!(secondaries.contains(&"b:9000".to_string()));
    assert!(secondaries.contains(&"c:9000".to_string()));
    assert_eq!(version, 2);
    assert!(bumped);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 2);
    assert_eq!(chunks[&1].primary, Some("a:9000".into()));
}

#[tokio::test]
async fn test_grant_lease_reuses_existing() {
    let m = make_master();
    m.create_file("/f".into()).await;
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()]).await;

    let (p1, _, v1, bumped1) = m.grant_lease(1).await.unwrap();
    assert!(bumped1);

    // second grant while lease is still active, no version bump
    let (p2, _, v2, bumped2) = m.grant_lease(1).await.unwrap();
    assert_eq!(p1, p2);
    assert_eq!(v1, v2);
    assert!(!bumped2);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 2);
}

#[tokio::test]
async fn test_grant_lease_expired_regrants() {
    let m = Master::new(0);
    m.create_file("/f".into()).await;
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()]).await;

    // first grant: version 1 -> 2
    let (_, _, v1, _) = m.grant_lease(1).await.unwrap();
    assert_eq!(v1, 2);

    // lease expired immediately, next grant bumps 2 -> 3
    let (primary, _, v2, bumped) = m.grant_lease(1).await.unwrap();
    assert_eq!(primary, "a:9000");
    assert_eq!(v2, 3);
    assert!(bumped);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 3);
}

#[tokio::test]
async fn test_grant_lease_nonexistent_chunk() {
    let m = make_master();
    assert!(m.grant_lease(999).await.is_none());
}

#[tokio::test]
async fn test_heartbeat_detects_stale_replica() {
    let m = make_master();
    m.create_file("/f".into()).await;
    m.register_chunkserver("a:9000".into(), 1000).await;
    m.register_chunkserver("b:9000".into(), 1000).await;
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()]).await;

    // grant lease bumps version to 2
    let (_, _, version, _) = m.grant_lease(1).await.unwrap();
    assert_eq!(version, 2);

    // "a" reports current version, "b" reports stale version
    m.heartbeat("a:9000", vec![(1, 2)], 1000).await;
    m.heartbeat("b:9000", vec![(1, 1)], 1000).await;

    let chunks = m.chunks.read().await;
    let locs = &chunks[&1].locations;
    assert!(locs.contains(&"a:9000".to_string()));
    assert!(!locs.contains(&"b:9000".to_string()));
}
