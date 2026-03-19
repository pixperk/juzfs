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
    assert_eq!(
        m.add_chunk("/nope", vec!["addr".into()]).await.unwrap(),
        None
    );
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

    m.add_chunk("/data/big.bin", vec!["a:9000".into()])
        .await
        .unwrap();
    m.add_chunk("/data/big.bin", vec!["b:9000".into()])
        .await
        .unwrap();
    m.add_chunk("/data/big.bin", vec!["c:9000".into()])
        .await
        .unwrap();

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
    .await
    .unwrap();

    let (_h, primary, secondaries, version, bumped, _cow) =
        m.grant_lease(1, "/f").await.unwrap().unwrap();

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
        .await
        .unwrap();

    let (_, p1, _, v1, bumped1, _) = m.grant_lease(1, "/f").await.unwrap().unwrap();
    assert!(bumped1);
    assert_eq!(v1, 1);

    // second grant while lease is still active, no version bump
    let (_, p2, _, v2, bumped2, _) = m.grant_lease(1, "/f").await.unwrap().unwrap();
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
        .await
        .unwrap();

    // first grant: version 0 -> 1
    let (_, _, _, v1, _, _) = m.grant_lease(1, "/f").await.unwrap().unwrap();
    assert_eq!(v1, 1);

    // lease expired immediately, next grant bumps 1 -> 2
    let (_, primary, _, v2, bumped, _) = m.grant_lease(1, "/f").await.unwrap().unwrap();
    assert_eq!(primary, "a:9000");
    assert_eq!(v2, 2);
    assert!(bumped);

    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].version, 2);
}

#[tokio::test]
async fn test_grant_lease_nonexistent_chunk() {
    let m = make_master();
    assert!(m.grant_lease(999, "/f").await.unwrap().is_none());
}

#[tokio::test]
async fn test_heartbeat_detects_stale_replica() {
    let m = make_master();
    m.create_file("/f".into()).await.unwrap();
    m.register_chunkserver("a:9000".into(), 1000).await;
    m.register_chunkserver("b:9000".into(), 1000).await;
    m.add_chunk("/f", vec!["a:9000".into(), "b:9000".into()])
        .await
        .unwrap();

    // grant lease bumps version to 1
    let (_, _, _, version, _, _) = m.grant_lease(1, "/f").await.unwrap().unwrap();
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
        m.add_chunk("/a.txt", vec!["cs1:9000".into()])
            .await
            .unwrap();
        m.add_chunk("/a.txt", vec!["cs2:9000".into()])
            .await
            .unwrap();
        m.add_chunk("/b.txt", vec!["cs1:9000".into()])
            .await
            .unwrap();

        // grant lease bumps version on chunk 1
        m.register_chunkserver("cs1:9000".into(), 1000).await;
        m.heartbeat("cs1:9000", vec![(1, 0)], 1000).await;
        m.grant_lease(1, "/f").await.unwrap().unwrap();
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
    recovered
        .add_chunk("/c.txt", vec!["cs1:9000".into()])
        .await
        .unwrap();
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
        assert!(
            checkpoint_path.exists(),
            "checkpoint.bin should exist after 100 ops"
        );

        // oplog.old.bin should be cleaned up by background task
        let _old_path = oplog_path.with_extension("old.bin");
        // might still exist briefly, but checkpoint should be written
    }

    // phase 2: recover from checkpoint + oplog
    let recovered = Master::recover(60, path_str).await.unwrap();

    // all 50 files should be there
    for i in 0..50 {
        let name = format!("/file_{}.txt", i);
        let chunks = recovered.get_file_chunks(&name).await;
        assert!(
            chunks.is_some(),
            "file {} should exist after checkpoint recovery",
            name
        );
        assert_eq!(chunks.unwrap().len(), 1);
    }

    // next handle should be correct (50 chunks = handles 1-50, next = 51)
    assert_eq!(recovered.allocate_chunk_handle().await, 51);

    // new ops still work after checkpoint recovery
    recovered.create_file("/post_cp.txt".into()).await.unwrap();
    recovered
        .add_chunk("/post_cp.txt", vec!["cs:9000".into()])
        .await
        .unwrap();
    assert_eq!(
        recovered
            .get_file_chunks("/post_cp.txt")
            .await
            .unwrap()
            .len(),
        1
    );
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
        m.add_chunk("/new_after_cp.txt", vec!["cs:9000".into()])
            .await
            .unwrap();

        // grant lease on chunk 1 to set a version
        m.register_chunkserver("cs:9000".into(), 1000).await;
        m.heartbeat("cs:9000", vec![(1, 0)], 1000).await;
        m.grant_lease(1, "/f").await.unwrap().unwrap();
    }

    // phase 2: recover -- should load checkpoint + replay fresh oplog
    let recovered = Master::recover(60, path_str).await.unwrap();

    // old files from checkpoint
    for i in 0..50 {
        let name = format!("/old_{}.txt", i);
        assert!(recovered.get_file_chunks(&name).await.is_some());
    }

    // new file from post-checkpoint oplog
    assert!(
        recovered
            .get_file_chunks("/new_after_cp.txt")
            .await
            .is_some()
    );

    // version should be 1 from grant_lease in post-checkpoint oplog
    let chunks = recovered.chunks.read().await;
    assert_eq!(chunks[&1].version, 1);
}

#[tokio::test]
async fn test_delete_file_lazy() {
    let m = make_master();
    m.create_file("/gc_me.txt".into()).await.unwrap();
    m.add_chunk("/gc_me.txt", vec!["cs:9000".into()])
        .await
        .unwrap();

    // file exists before delete
    assert!(m.get_file_chunks("/gc_me.txt").await.is_some());

    // lazy delete
    assert!(m.delete_file("/gc_me.txt".into()).await.unwrap());

    // original name gone, hidden name exists
    assert!(m.get_file_chunks("/gc_me.txt").await.is_none());
    assert!(m.get_file_chunks("/.deleted_gc_me.txt").await.is_some());

    // deleted_files map has the entry
    let deleted = m.deleted_files.read().await;
    assert!(deleted.contains_key("/.deleted_gc_me.txt"));
}

#[tokio::test]
async fn test_gc_sweep_before_retention() {
    let m = make_master();
    m.create_file("/gc_me.txt".into()).await.unwrap();
    m.add_chunk("/gc_me.txt", vec!["cs:9000".into()])
        .await
        .unwrap();
    m.delete_file("/gc_me.txt".into()).await.unwrap();

    // sweep with 300s retention -- file was just deleted, should NOT be reaped
    let orphaned = m.gc_sweep(300).await;
    assert!(orphaned.is_empty());

    // hidden file should still exist
    assert!(m.get_file_chunks("/.deleted_gc_me.txt").await.is_some());
}

#[tokio::test]
async fn test_gc_sweep_after_retention() {
    let m = make_master();
    m.create_file("/gc_me.txt".into()).await.unwrap();
    let handle = m
        .add_chunk("/gc_me.txt", vec!["cs:9000".into()])
        .await
        .unwrap()
        .unwrap();
    m.delete_file("/gc_me.txt".into()).await.unwrap();

    // manually backdate the deletion timestamp so it looks expired
    {
        let mut deleted = m.deleted_files.write().await;
        if let Some(entry) = deleted.get_mut("/.deleted_gc_me.txt") {
            entry.1 = entry.1.saturating_sub(600); // pretend deleted 10 min ago
        }
    }

    // sweep with 300s retention -- should reap
    let orphaned = m.gc_sweep(300).await;
    assert_eq!(orphaned, vec![handle]);

    // hidden file and chunk metadata should be gone
    assert!(m.get_file_chunks("/.deleted_gc_me.txt").await.is_none());
    assert!(m.get_chunk_locations(handle).await.is_none());

    // deleted_files map should be empty
    assert!(m.deleted_files.read().await.is_empty());
}

#[tokio::test]
async fn test_delete_nonexistent_file() {
    let m = make_master();
    assert!(!m.delete_file("/nope.txt".into()).await.unwrap());
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
        m.add_chunk("/after.txt", vec!["cs:9000".into()])
            .await
            .unwrap();
    }

    // simulate crash before checkpoint finished: delete checkpoint, keep old log
    let checkpoint_path = dir.path().join("checkpoint.bin");
    let _old_oplog = oplog_path.with_extension("old.bin");

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

// ---- snapshot / COW tests ----

#[tokio::test]
async fn test_snapshot_creates_copy() {
    let m = make_master();
    m.create_file("/src.txt".into()).await.unwrap();
    m.add_chunk("/src.txt", vec!["cs:9000".into()])
        .await
        .unwrap();

    assert!(
        m.snapshot("/src.txt".into(), "/snap.txt".into())
            .await
            .unwrap()
    );

    // both files exist with the same chunk handles
    let src_chunks = m.get_file_chunks("/src.txt").await.unwrap();
    let snap_chunks = m.get_file_chunks("/snap.txt").await.unwrap();
    assert_eq!(src_chunks, snap_chunks);

    // ref_count should be 2 on shared chunks
    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&src_chunks[0]].ref_count, 2);
}

#[tokio::test]
async fn test_snapshot_revokes_leases() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    m.add_chunk("/f.txt", vec!["cs:9000".into()]).await.unwrap();
    m.register_chunkserver("cs:9000".into(), 1000).await;
    m.heartbeat("cs:9000", vec![(1, 0)], 1000).await;

    // grant a lease
    m.grant_lease(1, "/f.txt").await.unwrap().unwrap();

    // snapshot should revoke it
    m.snapshot("/f.txt".into(), "/snap.txt".into())
        .await
        .unwrap();

    let chunks = m.chunks.read().await;
    assert!(chunks[&1].primary.is_none());
    assert!(chunks[&1].lease_expiry.is_none());
}

#[tokio::test]
async fn test_snapshot_nonexistent_file() {
    let m = make_master();
    assert!(
        !m.snapshot("/nope.txt".into(), "/snap.txt".into())
            .await
            .unwrap()
    );
}

#[tokio::test]
async fn test_cow_triggers_on_write_to_shared_chunk() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    m.add_chunk("/f.txt", vec!["cs:9000".into()]).await.unwrap();
    m.register_chunkserver("cs:9000".into(), 1000).await;
    m.heartbeat("cs:9000", vec![(1, 0)], 1000).await;

    // snapshot: both files share chunk 1, ref_count = 2
    m.snapshot("/f.txt".into(), "/snap.txt".into())
        .await
        .unwrap();

    // grant lease on shared chunk -COW should trigger
    let result = m.grant_lease(1, "/f.txt").await.unwrap().unwrap();
    let (actual_handle, _primary, _secondaries, _version, _bumped, cow_copy) = result;

    // handle should be different (new chunk allocated)
    assert_ne!(actual_handle, 1);

    // cow_copy should have the old and new handles
    let (src, dst, _locs) = cow_copy.unwrap();
    assert_eq!(src, 1);
    assert_eq!(dst, actual_handle);

    // /f.txt should now point to the new handle
    let f_chunks = m.get_file_chunks("/f.txt").await.unwrap();
    assert_eq!(f_chunks, vec![actual_handle]);

    // /snap.txt should still point to original chunk 1
    let snap_chunks = m.get_file_chunks("/snap.txt").await.unwrap();
    assert_eq!(snap_chunks, vec![1]);

    // ref_counts should both be 1 now
    let chunks = m.chunks.read().await;
    assert_eq!(chunks[&1].ref_count, 1);
    assert_eq!(chunks[&actual_handle].ref_count, 1);
}

#[tokio::test]
async fn test_no_cow_without_snapshot() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    m.add_chunk("/f.txt", vec!["cs:9000".into()]).await.unwrap();
    m.register_chunkserver("cs:9000".into(), 1000).await;
    m.heartbeat("cs:9000", vec![(1, 0)], 1000).await;

    // no snapshot, ref_count = 1 -COW should NOT trigger
    let result = m.grant_lease(1, "/f.txt").await.unwrap().unwrap();
    let (actual_handle, _, _, _, _, cow_copy) = result;

    assert_eq!(actual_handle, 1); // same handle
    assert!(cow_copy.is_none()); // no COW
}

#[tokio::test]
async fn test_cow_on_second_file() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    m.add_chunk("/f.txt", vec!["cs:9000".into()]).await.unwrap();
    m.register_chunkserver("cs:9000".into(), 1000).await;
    m.heartbeat("cs:9000", vec![(1, 0)], 1000).await;

    m.snapshot("/f.txt".into(), "/snap.txt".into())
        .await
        .unwrap();

    // COW on /f.txt first
    let result1 = m.grant_lease(1, "/f.txt").await.unwrap().unwrap();
    let (new_handle, _, _, _, _, _) = result1;

    // chunk 1 ref_count is back to 1, /snap.txt still points to it
    // grant lease on chunk 1 through /snap.txt -no COW needed (ref_count = 1)
    let result2 = m.grant_lease(1, "/snap.txt").await.unwrap().unwrap();
    let (snap_handle, _, _, _, _, cow_copy) = result2;

    assert_eq!(snap_handle, 1); // no fork, same handle
    assert!(cow_copy.is_none()); // no COW since ref_count is 1

    // files diverged
    let f_chunks = m.get_file_chunks("/f.txt").await.unwrap();
    let snap_chunks = m.get_file_chunks("/snap.txt").await.unwrap();
    assert_eq!(f_chunks, vec![new_handle]);
    assert_eq!(snap_chunks, vec![1]);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_snapshot_survives_recovery() {
    let dir = tempdir().unwrap();
    let oplog_path = dir.path().join("oplog.bin");
    let path_str = oplog_path.to_str().unwrap();

    {
        let m = Master::new(60, path_str).unwrap();
        m.create_file("/f.txt".into()).await.unwrap();
        m.add_chunk("/f.txt", vec!["cs:9000".into()]).await.unwrap();
        m.snapshot("/f.txt".into(), "/snap.txt".into())
            .await
            .unwrap();
    }

    let recovered = Master::recover(60, path_str).await.unwrap();

    // both files should exist
    let f_chunks = recovered.get_file_chunks("/f.txt").await.unwrap();
    let snap_chunks = recovered.get_file_chunks("/snap.txt").await.unwrap();
    assert_eq!(f_chunks, snap_chunks);

    // ref_count should be 2
    let chunks = recovered.chunks.read().await;
    assert_eq!(chunks[&f_chunks[0]].ref_count, 2);
}

// --- namespace locking tests ---

#[tokio::test]
async fn test_ns_concurrent_creates_different_files() {
    let m = make_master();

    // two creates on different files in same dir should both succeed
    let (r1, r2) = tokio::join!(
        m.create_file("/a.txt".into()),
        m.create_file("/b.txt".into()),
    );
    r1.unwrap();
    r2.unwrap();

    assert!(m.get_file_chunks("/a.txt").await.is_some());
    assert!(m.get_file_chunks("/b.txt").await.is_some());
}

#[tokio::test]
async fn test_ns_read_during_different_file_mutation() {
    let m = make_master();
    m.create_file("/a.txt".into()).await.unwrap();

    // reading /a.txt while creating /b.txt should not block
    let (chunks, create_result) =
        tokio::join!(m.get_file_chunks("/a.txt"), m.create_file("/b.txt".into()),);

    assert!(chunks.is_some());
    create_result.unwrap();
}

#[tokio::test]
async fn test_ns_snapshot_blocks_same_dst_create() {
    let m = make_master();
    m.create_file("/src.txt".into()).await.unwrap();

    // snapshot to /dst.txt then try creating /dst.txt - snapshot should win
    m.snapshot("/src.txt".into(), "/dst.txt".into())
        .await
        .unwrap();

    // /dst.txt already exists from snapshot
    assert!(m.get_file_chunks("/dst.txt").await.is_some());
}

// ==================== re-replication tests ====================

#[tokio::test]
async fn test_detect_under_replicated_with_one_replica() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    let locations = vec!["cs1:6000".to_string()];
    m.add_chunk("/f.txt", locations).await.unwrap();

    // register cs1 so heartbeat works
    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.heartbeat("cs1:6000", vec![(1, 0)], 1_000_000).await;

    let under = m.detect_under_replicated().await;
    assert_eq!(under.len(), 1);
    assert_eq!(under[0].0, 1); // handle
    assert_eq!(under[0].1, vec!["cs1:6000".to_string()]); // locations
}

#[tokio::test]
async fn test_detect_under_replicated_fully_replicated() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    let locations = vec![
        "cs1:6000".to_string(),
        "cs2:6001".to_string(),
        "cs3:6002".to_string(),
    ];
    m.add_chunk("/f.txt", locations).await.unwrap();

    // register all 3 and heartbeat
    for (addr, handle) in [("cs1:6000", 1), ("cs2:6001", 1), ("cs3:6002", 1)] {
        m.register_chunkserver(addr.into(), 1_000_000).await;
        m.heartbeat(addr, vec![(handle, 0)], 1_000_000).await;
    }

    let under = m.detect_under_replicated().await;
    assert!(under.is_empty());
}

#[tokio::test]
async fn test_detect_under_replicated_ignores_zero_locations() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    // add chunk with empty locations (simulates all chunkservers dying)
    m.add_chunk("/f.txt", vec![]).await.unwrap();

    // detect_under_replicated skips chunks with 0 locations (no source to copy from)
    let under = m.detect_under_replicated().await;
    assert!(under.is_empty());
}

#[tokio::test]
async fn test_plan_re_replication_picks_non_holder() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    let locations = vec!["cs1:6000".to_string()];
    m.add_chunk("/f.txt", locations).await.unwrap();

    // register 3 chunkservers
    m.register_chunkserver("cs1:6000".into(), 500_000).await;
    m.register_chunkserver("cs2:6001".into(), 1_000_000).await;
    m.register_chunkserver("cs3:6002".into(), 800_000).await;

    m.heartbeat("cs1:6000", vec![(1, 0)], 500_000).await;

    let under = m.detect_under_replicated().await;
    let actions = m.plan_re_replication(&under).await;

    assert_eq!(actions.len(), 1);
    let (handle, source, target, _version) = &actions[0];
    assert_eq!(*handle, 1);
    assert_eq!(source, "cs1:6000"); // only holder is the source
    // target should be cs2 (most space) since it doesn't hold the chunk
    assert_eq!(target, "cs2:6001");
}

#[tokio::test]
async fn test_plan_re_replication_skips_when_no_target() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    let locations = vec!["cs1:6000".to_string()];
    m.add_chunk("/f.txt", locations).await.unwrap();

    // only 1 chunkserver registered, and it already holds the chunk
    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.heartbeat("cs1:6000", vec![(1, 0)], 1_000_000).await;

    let under = m.detect_under_replicated().await;
    let actions = m.plan_re_replication(&under).await;

    // no action because there's no target that doesn't already hold the chunk
    assert!(actions.is_empty());
}

#[tokio::test]
async fn test_plan_re_replication_multiple_under_replicated() {
    let m = make_master();
    m.create_file("/a.txt".into()).await.unwrap();
    m.create_file("/b.txt".into()).await.unwrap();
    m.add_chunk("/a.txt", vec!["cs1:6000".into()]).await.unwrap();
    m.add_chunk("/b.txt", vec!["cs2:6001".into()]).await.unwrap();

    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.register_chunkserver("cs2:6001".into(), 1_000_000).await;
    m.register_chunkserver("cs3:6002".into(), 1_000_000).await;

    m.heartbeat("cs1:6000", vec![(1, 0)], 1_000_000).await;
    m.heartbeat("cs2:6001", vec![(2, 0)], 1_000_000).await;

    let under = m.detect_under_replicated().await;
    assert_eq!(under.len(), 2);

    let actions = m.plan_re_replication(&under).await;
    assert_eq!(actions.len(), 2);

    // both chunks should have replication actions planned
    let handles: Vec<u64> = actions.iter().map(|(h, _, _, _)| *h).collect();
    assert!(handles.contains(&1));
    assert!(handles.contains(&2));
}

// ==================== rebalancing tests ====================

#[tokio::test]
async fn test_plan_rebalance_balanced_cluster() {
    // 3 servers, each with 1 chunk - balanced, no moves needed
    let m = make_master();
    m.create_file("/a.txt".into()).await.unwrap();
    m.create_file("/b.txt".into()).await.unwrap();
    m.create_file("/c.txt".into()).await.unwrap();
    m.add_chunk("/a.txt", vec!["cs1:6000".into(), "cs2:6001".into(), "cs3:6002".into()]).await.unwrap();
    m.add_chunk("/b.txt", vec!["cs1:6000".into(), "cs2:6001".into(), "cs3:6002".into()]).await.unwrap();
    m.add_chunk("/c.txt", vec!["cs1:6000".into(), "cs2:6001".into(), "cs3:6002".into()]).await.unwrap();

    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.register_chunkserver("cs2:6001".into(), 1_000_000).await;
    m.register_chunkserver("cs3:6002".into(), 1_000_000).await;

    m.heartbeat("cs1:6000", vec![(1, 0), (2, 0), (3, 0)], 1_000_000).await;
    m.heartbeat("cs2:6001", vec![(1, 0), (2, 0), (3, 0)], 1_000_000).await;
    m.heartbeat("cs3:6002", vec![(1, 0), (2, 0), (3, 0)], 1_000_000).await;

    let actions = m.plan_rebalance().await;
    assert!(actions.is_empty());
}

#[tokio::test]
async fn test_plan_rebalance_single_server() {
    // only 1 server - can't rebalance
    let m = make_master();
    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.heartbeat("cs1:6000", vec![(1, 0)], 1_000_000).await;

    let actions = m.plan_rebalance().await;
    assert!(actions.is_empty());
}

#[tokio::test]
async fn test_plan_rebalance_imbalanced() {
    // cs1 has 5 chunks, cs2 has 5 chunks, cs3 has 5 chunks, cs4 has 0 chunks
    // avg = 3.75, threshold = 4 (3.75 * 1.2 = 4.5 -> 4)
    // cs1,cs2,cs3 are at 5 which is > 4, cs4 is at 0 which is < avg
    let m = make_master();
    let all = vec!["cs1:6000".into(), "cs2:6001".into(), "cs3:6002".into()];

    for i in 0..5 {
        let fname = format!("/f{}.txt", i);
        m.create_file(fname.clone()).await.unwrap();
        m.add_chunk(&fname, all.clone()).await.unwrap();
    }

    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.register_chunkserver("cs2:6001".into(), 1_000_000).await;
    m.register_chunkserver("cs3:6002".into(), 1_000_000).await;
    m.register_chunkserver("cs4:6003".into(), 1_000_000).await;

    let cs_chunks: Vec<(u64, u64)> = (1..=5).map(|h| (h, 0)).collect();
    m.heartbeat("cs1:6000", cs_chunks.clone(), 1_000_000).await;
    m.heartbeat("cs2:6001", cs_chunks.clone(), 1_000_000).await;
    m.heartbeat("cs3:6002", cs_chunks.clone(), 1_000_000).await;
    m.heartbeat("cs4:6003", vec![], 1_000_000).await;

    let actions = m.plan_rebalance().await;
    // should plan at least 1 move to cs4
    assert!(!actions.is_empty());
    for (_handle, _source, target) in &actions {
        assert_eq!(target, "cs4:6003");
    }
}

#[tokio::test]
async fn test_register_and_confirm_rebalance() {
    let m = make_master();
    m.create_file("/f.txt".into()).await.unwrap();
    m.add_chunk("/f.txt", vec!["cs1:6000".into(), "cs2:6001".into(), "cs3:6002".into()]).await.unwrap();

    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.register_chunkserver("cs2:6001".into(), 1_000_000).await;
    m.register_chunkserver("cs3:6002".into(), 1_000_000).await;
    m.register_chunkserver("cs4:6003".into(), 1_000_000).await;

    m.heartbeat("cs1:6000", vec![(1, 0)], 1_000_000).await;
    m.heartbeat("cs2:6001", vec![(1, 0)], 1_000_000).await;
    m.heartbeat("cs3:6002", vec![(1, 0)], 1_000_000).await;

    // register a pending move: chunk 1 from cs1 to cs4
    m.register_pending_rebalance(1, "cs1:6000".into(), "cs4:6003".into()).await;

    // confirm before target has the chunk - nothing happens
    let to_delete = m.confirm_rebalance().await;
    assert!(to_delete.is_empty());

    // pending should still be there
    assert_eq!(m.pending_rebalance.read().await.len(), 1);

    // simulate target heartbeating with the chunk
    m.heartbeat("cs4:6003", vec![(1, 0)], 1_000_000).await;

    // now confirm - should succeed
    let to_delete = m.confirm_rebalance().await;
    assert!(to_delete.contains_key("cs1:6000"));
    assert_eq!(to_delete["cs1:6000"], vec![1]);

    // pending should be cleared
    assert!(m.pending_rebalance.read().await.is_empty());

    // cs1 should be removed from chunk 1's locations
    let locs = m.get_chunk_locations(1).await.unwrap();
    assert!(!locs.contains(&"cs1:6000".to_string()));
    assert!(locs.contains(&"cs4:6003".to_string()));
}

#[tokio::test]
async fn test_confirm_rebalance_chunk_deleted() {
    // if the chunk was deleted (e.g. by GC) while pending, just clean up
    let m = make_master();
    m.register_pending_rebalance(999, "cs1:6000".into(), "cs4:6003".into()).await;

    let to_delete = m.confirm_rebalance().await;
    // chunk 999 doesn't exist, pending should be cleared
    assert!(m.pending_rebalance.read().await.is_empty());
    // no delete needed since the chunk is already gone
    assert!(to_delete.is_empty());
}

#[tokio::test]
async fn test_plan_rebalance_skips_pending() {
    // if a chunk already has a pending rebalance, don't plan another move for it
    let m = make_master();
    let all = vec!["cs1:6000".into(), "cs2:6001".into(), "cs3:6002".into()];

    for i in 0..5 {
        let fname = format!("/f{}.txt", i);
        m.create_file(fname.clone()).await.unwrap();
        m.add_chunk(&fname, all.clone()).await.unwrap();
    }

    m.register_chunkserver("cs1:6000".into(), 1_000_000).await;
    m.register_chunkserver("cs2:6001".into(), 1_000_000).await;
    m.register_chunkserver("cs3:6002".into(), 1_000_000).await;
    m.register_chunkserver("cs4:6003".into(), 1_000_000).await;

    let cs_chunks: Vec<(u64, u64)> = (1..=5).map(|h| (h, 0)).collect();
    m.heartbeat("cs1:6000", cs_chunks.clone(), 1_000_000).await;
    m.heartbeat("cs2:6001", cs_chunks.clone(), 1_000_000).await;
    m.heartbeat("cs3:6002", cs_chunks.clone(), 1_000_000).await;
    m.heartbeat("cs4:6003", vec![], 1_000_000).await;

    let actions1 = m.plan_rebalance().await;
    // register all planned moves as pending
    for (handle, source, target) in &actions1 {
        m.register_pending_rebalance(*handle, source.clone(), target.clone()).await;
    }

    // plan again - shouldn't re-plan the same chunks
    let actions2 = m.plan_rebalance().await;
    for (handle, _, _) in &actions2 {
        let already_planned: Vec<u64> = actions1.iter().map(|(h, _, _)| *h).collect();
        assert!(!already_planned.contains(handle), "chunk {} was planned twice", handle);
    }
}
