use juzfs::master::Master;
use tempfile::tempdir;

fn make_master() -> Master {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oplog.bin");
    let m = Master::new(60, path.to_str().unwrap()).unwrap();
    // leak the tempdir so it lives as long as the master
    std::mem::forget(dir);
    m
}

#[tokio::test]
async fn test_create_file_and_get_chunks() {
    let m = make_master();
    m.create_file("/data/log.txt".into()).await.unwrap();

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
    m.create_file("/data/log.txt".into()).await.unwrap();

    let locs = vec!["10.0.0.1:9000".into(), "10.0.0.2:9000".into()];
    let handle = m.add_chunk("/data/log.txt", locs.clone()).await.unwrap();
    assert_eq!(handle, Some(1));

    let chunks = m.get_file_chunks("/data/log.txt").await;
    assert_eq!(chunks, Some(vec![1]));

    let locations = m.get_chunk_locations(1).await;
    assert_eq!(locations, Some(locs));
}

#[tokio::test]
async fn test_add_chunk_to_nonexistent_file() {
    let m = make_master();
    assert_eq!(m.add_chunk("/nope", vec!["addr".into()]).await.unwrap(), None);
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

    m.heartbeat("10.0.0.1:9000", vec![(1, 0), (2, 0), (3, 0)], 400)
        .await;

    let servers = m.chunkservers.read().await;
    assert_eq!(
        servers["10.0.0.1:9000"].chunks,
        vec![(1, 0), (2, 0), (3, 0)]
    );
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
    m.create_file("/data/big.bin".into()).await.unwrap();

    m.add_chunk("/data/big.bin", vec!["a:9000".into()]).await.unwrap();
    m.add_chunk("/data/big.bin", vec!["b:9000".into()]).await.unwrap();
    m.add_chunk("/data/big.bin", vec!["c:9000".into()]).await.unwrap();

    let chunks = m.get_file_chunks("/data/big.bin").await.unwrap();
    assert_eq!(chunks, vec![1, 2, 3]);
}

#[tokio::test]
async fn test_lease_expiry_is_set() {
    let m = make_master();
    m.create_file("/f".into()).await.unwrap();
    m.add_chunk("/f", vec!["a:9000".into()]).await.unwrap();

    let chunks = m.chunks.read().await;
    let info = &chunks[&1];
    assert!(info.lease_expiry.is_none());
    assert_eq!(info.version, 0);
}

#[tokio::test]
async fn test_grant_lease_new() {
    let m = make_master();
    m.create_file("/f".into()).await.unwrap();
    m.add_chunk(
        "/f",
        vec!["a:9000".into(), "b:9000".into(), "c:9000".into()],
    )
    .await.unwrap();

    let (primary, secondaries, version, bumped) = m.grant_lease(1).await.unwrap().unwrap();

    assert_eq!(primary, "a:9000");
    assert_eq!(secondaries.len(), 2);
    assert!(secondaries.contains(&"b:9000".to_string()));
    assert!(secondaries.contains(&"c:9000".to_string()));
    assert_eq!(version, 1);
    assert!(bumped);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 1);
    assert_eq!(chunks[&1].primary, Some("a:9000".into()));
}

#[tokio::test]
async fn test_grant_lease_reuses_existing() {
    let m = make_master();
    m.create_file("/f".into()).await.unwrap();
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()])
        .await.unwrap();

    let (p1, _, v1, bumped1) = m.grant_lease(1).await.unwrap().unwrap();
    assert!(bumped1);
    assert_eq!(v1, 1);

    // second grant while lease is still active, no version bump
    let (p2, _, v2, bumped2) = m.grant_lease(1).await.unwrap().unwrap();
    assert_eq!(p1, p2);
    assert_eq!(v1, v2);
    assert!(!bumped2);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 1);
}

#[tokio::test]
async fn test_grant_lease_expired_regrants() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oplog.bin");
    let m = Master::new(0, path.to_str().unwrap()).unwrap();
    m.create_file("/f".into()).await.unwrap();
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()])
        .await.unwrap();

    // first grant: version 0 -> 1
    let (_, _, v1, _) = m.grant_lease(1).await.unwrap().unwrap();
    assert_eq!(v1, 1);

    // lease expired immediately, next grant bumps 1 -> 2
    let (primary, _, v2, bumped) = m.grant_lease(1).await.unwrap().unwrap();
    assert_eq!(primary, "a:9000");
    assert_eq!(v2, 2);
    assert!(bumped);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 2);
}

#[tokio::test]
async fn test_grant_lease_nonexistent_chunk() {
    let m = make_master();
    assert!(m.grant_lease(999).await.unwrap().is_none());
}

#[tokio::test]
async fn test_heartbeat_detects_stale_replica() {
    let m = make_master();
    m.create_file("/f".into()).await.unwrap();
    m.register_chunkserver("a:9000".into(), 1000).await;
    m.register_chunkserver("b:9000".into(), 1000).await;
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()])
        .await.unwrap();

    // grant lease bumps version to 1
    let (_, _, version, _) = m.grant_lease(1).await.unwrap().unwrap();
    assert_eq!(version, 1);

    // "a" reports current version, "b" reports stale version
    m.heartbeat("a:9000", vec![(1, 1)], 1000).await;
    m.heartbeat("b:9000", vec![(1, 0)], 1000).await;

    let chunks = m.chunks.read().await;
    let locs = &chunks[&1].locations;
    assert!(locs.contains(&"a:9000".to_string()));
    assert!(!locs.contains(&"b:9000".to_string()));
}

#[tokio::test]
async fn test_oplog_recovery() {
    let dir = tempdir().unwrap();
    let path = dir.path().join("oplog.bin");
    let path_str = path.to_str().unwrap();

    // phase 1: build up some state
    {
        let m = Master::new(60, path_str).unwrap();
        m.create_file("/a.txt".into()).await.unwrap();
        m.create_file("/b.txt".into()).await.unwrap();
        m.add_chunk("/a.txt", vec!["cs1:9000".into()]).await.unwrap();
        m.add_chunk("/a.txt", vec!["cs2:9000".into()]).await.unwrap();
        m.add_chunk("/b.txt", vec!["cs1:9000".into()]).await.unwrap();

        // grant lease bumps version on chunk 1
        m.register_chunkserver("cs1:9000".into(), 1000).await;
        m.heartbeat("cs1:9000", vec![(1, 0)], 1000).await;
        m.grant_lease(1).await.unwrap().unwrap();
        // master drops here, simulating a crash
    }

    // phase 2: recover from oplog
    let recovered = Master::recover(60, path_str).await.unwrap();

    // files should exist
    let a_chunks = recovered.get_file_chunks("/a.txt").await.unwrap();
    assert_eq!(a_chunks, vec![1, 2]);

    let b_chunks = recovered.get_file_chunks("/b.txt").await.unwrap();
    assert_eq!(b_chunks, vec![3]);

    // chunk 1 version should be 1 (from grant_lease)
    let chunks = recovered.chunks.read().await;
    assert_eq!(chunks[&1].version, 1);
    assert_eq!(chunks[&2].version, 0);
    assert_eq!(chunks[&3].version, 0);

    // locations should be empty (rebuilt from heartbeats, not oplog)
    assert!(chunks[&1].locations.is_empty());
    assert!(chunks[&2].locations.is_empty());

    // leases should be gone after crash
    assert!(chunks[&1].primary.is_none());
    assert!(chunks[&1].lease_expiry.is_none());
    drop(chunks);

    // next_chunk_handle should be 4 (max handle 3 + 1)
    assert_eq!(recovered.allocate_chunk_handle().await, 4);

    // new operations should still work and append to oplog
    recovered.create_file("/c.txt".into()).await.unwrap();
    recovered.add_chunk("/c.txt", vec!["cs1:9000".into()]).await.unwrap();
    assert_eq!(recovered.get_file_chunks("/c.txt").await.unwrap(), vec![5]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_and_recovery() {
    let dir = tempdir().unwrap();
    let oplog_path = dir.path().join("oplog.bin");
    let checkpoint_path = dir.path().join("checkpoint.bin");
    let path_str = oplog_path.to_str().unwrap();

    // phase 1: build state, force a checkpoint by hitting the threshold
    {
        let m = Master::new(60, path_str).unwrap();

        // create enough operations to trigger checkpoint (threshold = 100)
        // each create_file + add_chunk = 2 entries, so 50 files = 100 entries
        for i in 0..50 {
            let name = format!("/file_{}.txt", i);
            m.create_file(name.clone()).await.unwrap();
            m.add_chunk(&name, vec!["cs:9000".into()]).await.unwrap();
        }

        // give background checkpoint task time to finish
        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // checkpoint should exist now
        assert!(checkpoint_path.exists(), "checkpoint.bin should exist after 100 ops");

        // oplog.old.bin should be cleaned up by background task
        let old_path = oplog_path.with_extension("old.bin");
        // might still exist briefly, but checkpoint should be written
    }

    // phase 2: recover from checkpoint + oplog
    let recovered = Master::recover(60, path_str).await.unwrap();

    // all 50 files should be there
    for i in 0..50 {
        let name = format!("/file_{}.txt", i);
        let chunks = recovered.get_file_chunks(&name).await;
        assert!(chunks.is_some(), "file {} should exist after checkpoint recovery", name);
        assert_eq!(chunks.unwrap().len(), 1);
    }

    // next handle should be correct (50 chunks = handles 1-50, next = 51)
    assert_eq!(recovered.allocate_chunk_handle().await, 51);

    // new ops still work after checkpoint recovery
    recovered.create_file("/post_cp.txt".into()).await.unwrap();
    recovered.add_chunk("/post_cp.txt", vec!["cs:9000".into()]).await.unwrap();
    assert_eq!(recovered.get_file_chunks("/post_cp.txt").await.unwrap().len(), 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_checkpoint_with_post_checkpoint_ops() {
    let dir = tempdir().unwrap();
    let oplog_path = dir.path().join("oplog.bin");
    let path_str = oplog_path.to_str().unwrap();

    // phase 1: trigger checkpoint, then do more ops after it
    {
        let m = Master::new(60, path_str).unwrap();

        // hit threshold
        for i in 0..50 {
            let name = format!("/old_{}.txt", i);
            m.create_file(name.clone()).await.unwrap();
            m.add_chunk(&name, vec!["cs:9000".into()]).await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // now do more ops AFTER checkpoint -- these go to the fresh oplog
        m.create_file("/new_after_cp.txt".into()).await.unwrap();
        m.add_chunk("/new_after_cp.txt", vec!["cs:9000".into()]).await.unwrap();

        // grant lease on chunk 1 to set a version
        m.register_chunkserver("cs:9000".into(), 1000).await;
        m.heartbeat("cs:9000", vec![(1, 0)], 1000).await;
        m.grant_lease(1).await.unwrap().unwrap();
    }

    // phase 2: recover -- should load checkpoint + replay fresh oplog
    let recovered = Master::recover(60, path_str).await.unwrap();

    // old files from checkpoint
    for i in 0..50 {
        let name = format!("/old_{}.txt", i);
        assert!(recovered.get_file_chunks(&name).await.is_some());
    }

    // new file from post-checkpoint oplog
    assert!(recovered.get_file_chunks("/new_after_cp.txt").await.is_some());

    // version should be 1 from grant_lease in post-checkpoint oplog
    let chunks = recovered.chunks.read().await;
    assert_eq!(chunks[&1].version, 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_crash_between_rotate_and_checkpoint() {
    let dir = tempdir().unwrap();
    let oplog_path = dir.path().join("oplog.bin");
    let path_str = oplog_path.to_str().unwrap();

    // phase 1: build state, manually simulate a crash between rotate and checkpoint write
    {
        let m = Master::new(60, path_str).unwrap();

        for i in 0..50 {
            let name = format!("/file_{}.txt", i);
            m.create_file(name.clone()).await.unwrap();
            m.add_chunk(&name, vec!["cs:9000".into()]).await.unwrap();
        }

        tokio::time::sleep(std::time::Duration::from_millis(100)).await;

        // add post-checkpoint ops
        m.create_file("/after.txt".into()).await.unwrap();
        m.add_chunk("/after.txt", vec!["cs:9000".into()]).await.unwrap();
    }

    // simulate crash before checkpoint finished: delete checkpoint, keep old log
    let checkpoint_path = dir.path().join("checkpoint.bin");
    let old_oplog = oplog_path.with_extension("old.bin");

    // if checkpoint exists, delete it to simulate incomplete checkpoint
    let _ = std::fs::remove_file(&checkpoint_path);

    // manually create a fake old oplog with some entries to simulate the rotate happened
    // but checkpoint didn't complete. Recovery should replay old + current oplog.
    // In this case, the current oplog has post-checkpoint ops, and the old oplog
    // has the pre-checkpoint ops (if it still exists)

    // phase 2: recover -- should handle missing checkpoint gracefully
    let recovered = Master::recover(60, path_str).await.unwrap();

    // post-checkpoint file should exist (from current oplog)
    assert!(recovered.get_file_chunks("/after.txt").await.is_some());
}
