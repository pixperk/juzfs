use core::time;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, io, path::Path};
use tokio::sync::{Mutex, RwLock, broadcast};

use crate::namespace::NamespaceLock;
use crate::oplog::{Checkpoint, OpLog, load_checkpoint, write_checkpoint};

pub type ChunkHandle = u64;

pub struct ChunkInfo {
    pub handle: ChunkHandle,
    pub version: u64,
    pub primary: Option<String>, // chunkserver addr holding the write lease
    pub lease_expiry: Option<Instant>, // GFS uses ~60s leases
    pub locations: Vec<String>,  // chunkserver addrs holding replicas
    pub ref_count: u64,          // COW snapshots: >1 means shared, copy on write
}

pub struct ChunkServerState {
    pub addr: String,
    pub last_heartbeat: Instant,
    pub chunks: Vec<(ChunkHandle, u64)>, // (handle, version) pairs reported by this server
    pub available_space: u64,
}

const CHECKPOINT_THRESHOLD: u64 = 100;
const REPLICATION_FACTOR: usize = 3;

pub struct Master {
    pub files: Arc<RwLock<HashMap<String, Vec<ChunkHandle>>>>,
    pub chunks: Arc<RwLock<HashMap<ChunkHandle, ChunkInfo>>>,
    pub chunkservers: Arc<RwLock<HashMap<String, ChunkServerState>>>,
    pub next_chunk_handle: Arc<RwLock<ChunkHandle>>,
    /// deleted files awaiting garbage collection: hidden_name -> (original_name, deletion_timestamp)
    pub deleted_files: Arc<RwLock<HashMap<String, (String, u64)>>>,
    pub expiry: time::Duration,
    pub oplog: Mutex<OpLog>,
    oplog_path: String,
    checkpoint_path: String,
    pub ns_lock: NamespaceLock,
    /// broadcast channel for streaming oplog entries to shadow masters
    pub oplog_tx: broadcast::Sender<Vec<u8>>,
    /// pending rebalance moves: handle -> (source_to_delete, target_to_confirm)
    pub pending_rebalance: Arc<RwLock<HashMap<ChunkHandle, (String, String)>>>,
}

#[derive(serde::Serialize, serde::Deserialize)]
pub enum OpLogEntry {
    CreateFile {
        filename: String,
    },
    AddChunk {
        filename: String,
        handle: ChunkHandle,
    },
    GrantLease {
        handle: ChunkHandle,
        primary: String,
        version: u64,
    },
    DeleteFile {
        filename: String,
        hidden_name: String,
        timestamp: u64,
    },
    Snapshot {
        src: String,
        dst: String,
    },
}

impl Master {
    /// recover master state from an existing oplog, then reopen it for appending.
    /// locations and leases are not restored -- chunkservers repopulate via heartbeat.
    pub async fn recover(expiry_in_seconds: u64, path_str: &str) -> io::Result<Self> {
        let master = Master::new(expiry_in_seconds, path_str)?;

        let mut max_handle: ChunkHandle = 0;
        let mut files_recovered = 0u64;
        let mut chunks_recovered = 0u64;

        // step 1: load checkpoint if it exists
        let checkpoint_path = Path::new(&master.checkpoint_path);
        match load_checkpoint(checkpoint_path) {
            Ok(cp) => {
                for (filename, handles) in &cp.files {
                    master
                        .files
                        .write()
                        .await
                        .insert(filename.clone(), handles.clone());
                    files_recovered += 1;
                    for &h in handles {
                        max_handle = max_handle.max(h);
                        chunks_recovered += 1;
                    }
                }
                for (&handle, &(version, ref_count)) in &cp.chunks {
                    master.chunks.write().await.insert(
                        handle,
                        ChunkInfo {
                            handle,
                            version,
                            primary: None,
                            lease_expiry: None,
                            locations: Vec::new(),
                            ref_count,
                        },
                    );
                }
                *master.next_chunk_handle.write().await = cp.next_chunk_handle;
                max_handle = max_handle.max(cp.next_chunk_handle.saturating_sub(1));
                *master.deleted_files.write().await = cp.deleted_files;
                tracing::info!(
                    files = files_recovered,
                    chunks = chunks_recovered,
                    next_handle = cp.next_chunk_handle,
                    "loaded checkpoint"
                );
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {
                tracing::info!("no checkpoint found");
            }
            Err(e) => {
                tracing::error!(error = %e, "checkpoint load failed, skipping");
            }
        }

        // step 2: replay old oplog first (exists if crash happened between rotate and checkpoint)
        let old_oplog_path = Path::new(path_str).with_extension("old.bin");
        let old_entries = Self::load_oplog(&old_oplog_path);
        let old_count = old_entries.len();
        for entry in old_entries {
            Self::apply_entry(
                &master,
                entry,
                &mut max_handle,
                &mut files_recovered,
                &mut chunks_recovered,
            )
            .await;
        }
        if old_count > 0 {
            tracing::info!(entries = old_count, "replayed old oplog");
        }

        // step 3: replay current oplog on top
        let entries = Self::load_oplog(Path::new(path_str));
        let entry_count = entries.len();
        for entry in entries {
            Self::apply_entry(
                &master,
                entry,
                &mut max_handle,
                &mut files_recovered,
                &mut chunks_recovered,
            )
            .await;
        }

        if max_handle > 0 {
            *master.next_chunk_handle.write().await = max_handle + 1;
        }

        tracing::info!(
            oplog_entries = entry_count,
            files = files_recovered,
            chunks = chunks_recovered,
            next_handle = max_handle + 1,
            "recovery complete"
        );

        Ok(master)
    }

    pub fn new(expiry_in_seconds: u64, path_str: &str) -> io::Result<Self> {
        let checkpoint_path = Path::new(path_str)
            .with_file_name("checkpoint.bin")
            .to_str()
            .unwrap()
            .to_string();
        Ok(Master {
            files: Arc::new(RwLock::new(HashMap::new())),
            chunks: Arc::new(RwLock::new(HashMap::new())),
            chunkservers: Arc::new(RwLock::new(HashMap::new())),
            next_chunk_handle: Arc::new(RwLock::new(1)),
            deleted_files: Arc::new(RwLock::new(HashMap::new())),
            expiry: time::Duration::from_secs(expiry_in_seconds),
            oplog: Mutex::new(OpLog::new(Path::new(path_str))?),
            oplog_path: path_str.to_string(),
            checkpoint_path,
            ns_lock: NamespaceLock::new(),
            oplog_tx: broadcast::channel(1024).0,
            pending_rebalance: Arc::new(RwLock::new(HashMap::new())),
        })
    }

    /// log an entry to oplog and broadcast to shadow masters
    async fn log_and_broadcast(&self, entry: &OpLogEntry) -> io::Result<()> {
        let bytes = bincode::serialize(entry)
            .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("serialize: {}", e)))?;

        let mut oplog = self.oplog.lock().await;
        oplog.log(entry)?;
        drop(oplog);

        // best-effort broadcast to shadows (don't fail the operation if no shadows connected)
        let _ = self.oplog_tx.send(bytes);
        Ok(())
    }

    fn load_oplog(path: &Path) -> Vec<OpLogEntry> {
        match OpLog::replay(path) {
            Ok(entry) => entry,
            Err(e) if e.kind() == io::ErrorKind::NotFound => Vec::new(),
            Err(e) => {
                tracing::error!(path = %path.display(), error = %e, "oplog replay failed");
                Vec::new()
            }
        }
    }

    pub async fn apply_entry(
        master: &Master,
        entry: OpLogEntry,
        max_handle: &mut ChunkHandle,
        files_recovered: &mut u64,
        chunks_recovered: &mut u64,
    ) {
        match entry {
            OpLogEntry::CreateFile { filename } => {
                master.files.write().await.insert(filename, Vec::new());
                *files_recovered += 1;
            }
            OpLogEntry::AddChunk { filename, handle } => {
                if let Some(chunks_list) = master.files.write().await.get_mut(&filename) {
                    chunks_list.push(handle);
                }
                master.chunks.write().await.insert(
                    handle,
                    ChunkInfo {
                        handle,
                        version: 0,
                        primary: None,
                        lease_expiry: None,
                        locations: Vec::new(),
                        ref_count: 1,
                    },
                );
                *max_handle = (*max_handle).max(handle);
                *chunks_recovered += 1;
            }
            OpLogEntry::GrantLease {
                handle, version, ..
            } => {
                if let Some(info) = master.chunks.write().await.get_mut(&handle) {
                    info.version = version;
                }
            }
            OpLogEntry::DeleteFile {
                filename,
                hidden_name,
                timestamp,
            } => {
                let mut files = master.files.write().await;
                if let Some(chunks) = files.remove(&filename) {
                    files.insert(hidden_name.clone(), chunks);
                }
                drop(files);
                master
                    .deleted_files
                    .write()
                    .await
                    .insert(hidden_name, (filename, timestamp));
            }
            OpLogEntry::Snapshot { src, dst } => {
                let mut files = master.files.write().await;
                if let Some(handles) = files.get(&src).cloned() {
                    let mut chunks = master.chunks.write().await;
                    for &h in &handles {
                        if let Some(info) = chunks.get_mut(&h) {
                            info.ref_count += 1;
                        }
                    }
                    files.insert(dst, handles);
                }
            }
        }
    }

    ///allocates a new unique chunkhandle by incrementing a counter. this is used whenever a new chunk is created for a file.
    pub async fn allocate_chunk_handle(&self) -> ChunkHandle {
        let mut handle = self.next_chunk_handle.write().await;
        let h = *handle;
        *handle += 1;
        h
    }

    ///creates a file entry in master metadata
    pub async fn create_file(&self, filename: String) -> io::Result<()> {
        let _ns = self.ns_lock.lock_mutate(&filename).await;
        self.log_and_broadcast(&OpLogEntry::CreateFile {
            filename: filename.clone(),
        })
        .await?;

        let mut files = self.files.write().await;
        files.insert(filename, Vec::new());
        drop(files);
        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(())
    }

    /// lazy delete: rename file to a hidden name with a deletion timestamp.
    /// the file's chunks are not removed yet -- a background GC scan will clean them up
    /// after the retention window expires.
    pub async fn delete_file(&self, filename: String) -> io::Result<bool> {
        let _ns = self.ns_lock.lock_mutate(&filename).await;
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let hidden_name = format!("/.deleted_{}", filename.trim_start_matches('/'));

        self.log_and_broadcast(&OpLogEntry::DeleteFile {
            filename: filename.clone(),
            hidden_name: hidden_name.clone(),
            timestamp: now,
        })
        .await?;

        let mut files = self.files.write().await;
        let chunks = match files.remove(&filename) {
            Some(c) => c,
            None => return Ok(false),
        };
        // move the file entry under the hidden name so it can be undeleted
        files.insert(hidden_name.clone(), chunks);
        drop(files);

        let mut deleted = self.deleted_files.write().await;
        deleted.insert(hidden_name, (filename, now));
        drop(deleted);

        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(true)
    }

    /// create a snapshot: clone the file entry, bump ref_counts, revoke leases.
    /// no data is copied -- COW handles that on first write.
    pub async fn snapshot(&self, src: String, dst: String) -> io::Result<bool> {
        let _ns_src = self.ns_lock.lock_read(&src).await;
        let _ns_dst = self.ns_lock.lock_mutate(&dst).await;
        self.log_and_broadcast(&OpLogEntry::Snapshot {
            src: src.clone(),
            dst: dst.clone(),
        })
        .await?;

        let mut files = self.files.write().await;
        let handles = match files.get(&src).cloned() {
            Some(h) => h,
            None => return Ok(false),
        };
        files.insert(dst, handles.clone());
        drop(files);

        // bump ref_counts and revoke leases on shared chunks
        let mut chunks = self.chunks.write().await;
        for &h in &handles {
            if let Some(info) = chunks.get_mut(&h) {
                info.ref_count += 1;
                info.primary = None;
                info.lease_expiry = None;
            }
        }
        drop(chunks);

        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(true)
    }

    ///allocates a new chunkhandle for a file and updates the master's metadata to reflect the new chunk and its locations. used when a client writes to a file and needs to create a new chunk.
    pub async fn add_chunk(
        &self,
        filename: &str,
        locations: Vec<String>,
    ) -> io::Result<Option<ChunkHandle>> {
        let _ns = self.ns_lock.lock_mutate(filename).await;
        let handle = self.allocate_chunk_handle().await;

        self.log_and_broadcast(&OpLogEntry::AddChunk {
            filename: filename.to_string(),
            handle,
        })
        .await?;

        let mut files = self.files.write().await;
        let chunks_list = match files.get_mut(filename) {
            Some(c) => c,
            None => return Ok(None),
        };
        chunks_list.push(handle);

        let info = ChunkInfo {
            handle,
            version: 0,
            primary: None,
            lease_expiry: None,
            locations: locations.clone(),
            ref_count: 1,
        };

        let mut chunks = self.chunks.write().await;
        chunks.insert(handle, info);
        drop(chunks);
        drop(files);
        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(Some(handle))
    }

    ///given a chunkhandle, return the list of chunkservers that hold replicas of that chunk. used by clients to know where to read/write data.
    pub async fn get_chunk_locations(&self, handle: ChunkHandle) -> Option<Vec<String>> {
        let chunks = self.chunks.read().await;
        chunks.get(&handle).map(|info| info.locations.clone())
    }

    ///get chunnkhandles associated with a given filename, used by clients to know which chunks to read/write for a file.
    pub async fn get_file_chunks(&self, filename: &str) -> Option<Vec<ChunkHandle>> {
        let _ns = self.ns_lock.lock_read(filename).await;
        let files = self.files.read().await;
        files.get(filename).cloned()
    }

    ///registers a chunkserver with the master, storing its address and available space for future placement decisions.
    pub async fn register_chunkserver(&self, addr: String, available_space: u64) {
        let mut servers = self.chunkservers.write().await;
        servers.insert(
            addr.clone(),
            ChunkServerState {
                addr,
                last_heartbeat: Instant::now(),
                chunks: Vec::new(),
                available_space,
            },
        );
    }

    /// processes heartbeat from a chunkserver, rebuilds chunk locations,
    /// and detects stale replicas by comparing reported version against master's version.
    /// stale replicas are not added to locations, so clients never get sent to them.
    /// returns list of orphaned chunk handles the chunkserver should delete.
    pub async fn heartbeat(
        &self,
        addr: &str,
        chunks: Vec<(ChunkHandle, u64)>,
        available_space: u64,
    ) -> Vec<ChunkHandle> {
        let mut servers = self.chunkservers.write().await;
        if let Some(server) = servers.get_mut(addr) {
            server.last_heartbeat = Instant::now();
            server.chunks = chunks.clone();
            server.available_space = available_space;
        }
        drop(servers);

        let mut chunk_map = self.chunks.write().await;
        let mut orphaned = Vec::new();
        for (handle, reported_version) in &chunks {
            if let Some(info) = chunk_map.get_mut(handle) {
                if *reported_version >= info.version {
                    // version matches or is ahead, add to locations
                    if !info.locations.contains(&addr.to_string()) {
                        info.locations.push(addr.to_string());
                    }
                } else {
                    // stale replica: reported version is behind master's version
                    // remove from locations so clients won't read from it
                    info.locations.retain(|l| l != addr);
                }
            } else {
                // master doesn't know this chunk, it's orphaned
                orphaned.push(*handle);
            }
        }
        orphaned
    }

    ///picks a certain number (replication_factor) of chunkservers with most available space for new chunk placement
    pub async fn choose_locations(&self, replication_factor: usize) -> Vec<String> {
        let servers = self.chunkservers.read().await;
        let mut sorted: Vec<_> = servers.values().collect();
        sorted.sort_by(|a, b| b.available_space.cmp(&a.available_space));
        sorted
            .iter()
            .take(replication_factor)
            .map(|s| s.addr.clone())
            .collect()
    }

    /// grants lease to a chunkserver and returns (primary, secondaries, version, version_bumped).
    /// if chunk is shared (ref_count > 1), do a COW copy: allocate new handle,
    /// clone chunk info, swap handle in the file's chunk list.
    /// returns the handle to actually grant the lease on.
    /// returns (new_handle, Option<(old_handle, locations)>)
    /// if COW triggered, the Option has the old handle + locations so caller can send CopyChunk
    async fn cow_copy_if_needed(
        &self,
        handle: ChunkHandle,
        filename: &str,
    ) -> io::Result<(ChunkHandle, Option<(ChunkHandle, Vec<String>)>)> {
        let mut chunks = self.chunks.write().await;
        let info = match chunks.get(&handle) {
            Some(i) => i,
            None => return Ok((handle, None)),
        };

        if info.ref_count <= 1 {
            return Ok((handle, None));
        }

        // allocate new handle
        let mut next = self.next_chunk_handle.write().await;
        let new_handle = *next;
        *next += 1;
        drop(next);

        let locations = info.locations.clone();

        // create new ChunkInfo cloned from old, with ref_count 1
        let new_info = ChunkInfo {
            handle: new_handle,
            version: info.version,
            primary: None,
            lease_expiry: None,
            locations: locations.clone(),
            ref_count: 1,
        };

        // decrement old chunk's ref_count
        chunks.get_mut(&handle).unwrap().ref_count -= 1;

        chunks.insert(new_handle, new_info);
        drop(chunks);

        // swap handle in the file's chunk list
        let mut files = self.files.write().await;
        if let Some(handles) = files.get_mut(filename) {
            if let Some(pos) = handles.iter().position(|&h| h == handle) {
                handles[pos] = new_handle;
            }
        }

        tracing::info!(
            old = handle,
            new = new_handle,
            file = filename,
            "cow: copied chunk metadata"
        );

        Ok((new_handle, Some((handle, locations))))
    }

    /// version_bumped is true when a new lease was granted (version incremented),
    /// meaning the caller should notify all replicas to update their version.
    /// when reusing an existing lease, version_bumped is false.
    pub async fn grant_lease(
        &self,
        handle: ChunkHandle,
        filename: &str,
    ) -> io::Result<
        Option<(
            ChunkHandle,
            String,
            Vec<String>,
            u64,
            bool,
            Option<(ChunkHandle, ChunkHandle, Vec<String>)>,
        )>,
    > {
        let _ns = self.ns_lock.lock_mutate(filename).await;
        let (handle, cow_info) = self.cow_copy_if_needed(handle, filename).await?;
        let mut chunks = self.chunks.write().await;
        let info = match chunks.get_mut(&handle) {
            Some(i) => i,
            None => return Ok(None),
        };

        // check if existing lease is still valid
        if let (Some(primary), Some(expiry)) = (&info.primary, info.lease_expiry) {
            if expiry > Instant::now() {
                let secondaries = info
                    .locations
                    .iter()
                    .filter(|l| *l != primary)
                    .cloned()
                    .collect();
                return Ok(Some((
                    handle,
                    primary.clone(),
                    secondaries,
                    info.version,
                    false,
                    None,
                )));
            }
        }

        // grant new lease: pick first location as primary, bump version
        let primary = match info.locations.first() {
            Some(p) => p.clone(),
            None => return Ok(None),
        };
        info.primary = Some(primary.clone());
        info.version += 1;
        info.lease_expiry = Some(Instant::now() + self.expiry);

        let version = info.version;
        let secondaries = info
            .locations
            .iter()
            .filter(|l| *l != &primary)
            .cloned()
            .collect();

        drop(chunks);

        self.log_and_broadcast(&OpLogEntry::GrantLease {
            handle,
            primary: primary.clone(),
            version,
        })
        .await?;

        // build cow copy info: (src_handle, dst_handle, locations)
        let cow_copy = cow_info.map(|(old_handle, locs)| (old_handle, handle, locs));

        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(Some((
            handle,
            primary,
            secondaries,
            version,
            true,
            cow_copy,
        )))
    }

    /// gfs-style checkpoint: rotate the oplog (fast, under lock), then write
    /// checkpoint in background. new mutations continue to the fresh log immediately.
    pub async fn maybe_checkpoint(&self) -> io::Result<()> {
        let mut oplog = self.oplog.lock().await;
        if oplog.count() < CHECKPOINT_THRESHOLD {
            return Ok(());
        }

        // step 1: rotate log (fast -- just rename + open new file)
        oplog.rotate(Path::new(&self.oplog_path))?;
        tracing::info!("oplog rotated, starting background checkpoint");
        drop(oplog); // release lock -- new mutations can proceed

        // step 2: snapshot current state (reads don't block mutations)
        let files = self.files.read().await.clone();
        let chunks_map: HashMap<ChunkHandle, (u64, u64)> = self
            .chunks
            .read()
            .await
            .iter()
            .map(|(&h, info)| (h, (info.version, info.ref_count)))
            .collect();
        let next_handle = *self.next_chunk_handle.read().await;

        let deleted_files = self.deleted_files.read().await.clone();

        let cp = Checkpoint {
            files,
            chunks: chunks_map,
            next_chunk_handle: next_handle,
            deleted_files,
        };

        // step 3: write checkpoint + delete old log in background (blocking I/O)
        let checkpoint_path = self.checkpoint_path.clone();
        let oplog_path = self.oplog_path.clone();
        tokio::task::spawn_blocking(move || {
            if let Err(e) = write_checkpoint(Path::new(&checkpoint_path), &cp) {
                tracing::error!(error = %e, "background checkpoint write failed");
                return;
            }
            let old_path = Path::new(&oplog_path).with_extension("old.bin");
            let _ = std::fs::remove_file(&old_path);
            tracing::info!("checkpoint written, old oplog deleted");
        });

        Ok(())
    }

    /// sweep deleted files past the retention window.
    /// removes their chunks from metadata and the hidden file entry.
    /// returns orphaned chunk handles (for step 3: telling chunkservers to delete them).
    pub async fn gc_sweep(&self, retention_secs: u64) -> Vec<ChunkHandle> {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();

        let mut deleted = self.deleted_files.write().await;
        let expired: Vec<_> = deleted
            .iter()
            .filter(|(_, (_, ts))| now - ts >= retention_secs)
            .map(|(hidden, (orig, _))| (hidden.clone(), orig.clone()))
            .collect();

        if expired.is_empty() {
            return Vec::new();
        }

        let mut files = self.files.write().await;
        let mut chunks = self.chunks.write().await;
        let mut orphaned_chunks = Vec::new();

        for (hidden_name, _original) in &expired {
            if let Some(handles) = files.remove(hidden_name) {
                for h in handles {
                    chunks.remove(&h);
                    orphaned_chunks.push(h);
                }
            }
            deleted.remove(hidden_name);
        }

        tracing::info!(
            expired_files = expired.len(),
            orphaned_chunks = orphaned_chunks.len(),
            "gc sweep complete"
        );

        orphaned_chunks
    }

    /// find chunks with fewer replicas than REPLICATION_FACTOR.
    /// returns (handle, current_locations, version) for each under-replicated chunk.
    pub async fn detect_under_replicated(&self) -> Vec<(ChunkHandle, Vec<String>, u64)> {
        let chunks = self.chunks.read().await;
        let mut under = Vec::new();
        for (handle, info) in chunks.iter() {
            if info.locations.len() < REPLICATION_FACTOR && !info.locations.is_empty() {
                under.push((*handle, info.locations.clone(), info.version));
            }
        }
        // priority: fewer replicas first (1 replica before 2)
        under.sort_by_key(|(_, locations, _)| locations.len());
        under
    }

    /// for each under-replicated chunk, pick a target chunkserver that doesn't already
    /// hold it and has the most available space.
    /// returns list of (handle, source_addr, target_addr, version) to act on.
    pub async fn plan_re_replication(
        &self,
        under: &[(ChunkHandle, Vec<String>, u64)],
    ) -> Vec<(ChunkHandle, String, String, u64)> {
        let servers = self.chunkservers.read().await;
        let mut sorted: Vec<_> = servers.values().collect();
        sorted.sort_by(|a, b| b.available_space.cmp(&a.available_space));

        let mut actions = Vec::new();
        for (handle, locations, version) in under {
            // pick a target: most space, doesn't already hold this chunk
            let target = sorted.iter().find(|s| !locations.contains(&s.addr));
            let target_addr = match target {
                Some(t) => t.addr.clone(),
                None => continue, // no available target
            };
            // pick source: first location (any live replica works)
            let source = locations[0].clone();
            actions.push((*handle, source, target_addr, *version));
        }
        actions
    }

    /// compute the average chunk count across all chunkservers.
    /// returns (avg, most_loaded_addr, most_loaded_count, least_loaded_addr, least_loaded_count).
    async fn server_load_stats(&self) -> Option<(f64, String, usize, String, usize)> {
        let servers = self.chunkservers.read().await;
        if servers.len() < 2 {
            return None;
        }
        let total_chunks: usize = servers.values().map(|s| s.chunks.len()).sum();
        let avg = total_chunks as f64 / servers.len() as f64;

        let most = servers.values().max_by_key(|s| s.chunks.len())?;
        let least = servers.values().min_by_key(|s| s.chunks.len())?;

        Some((avg, most.addr.clone(), most.chunks.len(), least.addr.clone(), least.chunks.len()))
    }

    /// phase 1: find chunks to move from overloaded servers to underloaded ones.
    /// a server is "overloaded" if it has more than avg * 1.2 chunks.
    /// returns list of (handle, source, target) moves to initiate.
    pub async fn plan_rebalance(&self) -> Vec<(ChunkHandle, String, String)> {
        let stats = match self.server_load_stats().await {
            Some(s) => s,
            None => return Vec::new(),
        };
        let (avg, _, _, _, _) = stats;
        let threshold = (avg * 1.2) as usize;
        if threshold == 0 {
            return Vec::new();
        }

        let servers = self.chunkservers.read().await;
        let chunks = self.chunks.read().await;
        let pending = self.pending_rebalance.read().await;

        // find overloaded servers
        let mut overloaded: Vec<_> = servers.values()
            .filter(|s| s.chunks.len() > threshold)
            .collect();
        overloaded.sort_by(|a, b| b.chunks.len().cmp(&a.chunks.len()));

        // find underloaded servers (below average)
        let mut underloaded: Vec<_> = servers.values()
            .filter(|s| (s.chunks.len() as f64) < avg)
            .collect();
        underloaded.sort_by(|a, b| a.chunks.len().cmp(&b.chunks.len()));

        let mut actions = Vec::new();
        let mut target_idx = 0;

        for source in &overloaded {
            let excess = source.chunks.len() - threshold;
            let mut moved = 0;

            for (handle, _version) in &source.chunks {
                if moved >= excess { break; }
                if target_idx >= underloaded.len() { break; }
                if pending.contains_key(handle) { continue; }

                // only move chunks that have enough replicas (don't break replication)
                let info = match chunks.get(handle) {
                    Some(i) => i,
                    None => continue,
                };
                if info.locations.len() < REPLICATION_FACTOR { continue; }

                // pick a target that doesn't already hold this chunk
                let target = loop {
                    if target_idx >= underloaded.len() { break None; }
                    let t = &underloaded[target_idx];
                    if !info.locations.contains(&t.addr) {
                        break Some(t.addr.clone());
                    }
                    target_idx += 1;
                };

                let target_addr = match target {
                    Some(t) => t,
                    None => break,
                };

                actions.push((*handle, source.addr.clone(), target_addr));
                moved += 1;
            }
        }

        actions
    }

    /// record that a rebalance move has been initiated (phase 1 complete).
    /// the master will confirm on the next cycle after the target heartbeats the chunk.
    pub async fn register_pending_rebalance(&self, handle: ChunkHandle, source: String, target: String) {
        self.pending_rebalance.write().await.insert(handle, (source, target));
    }

    /// phase 2: check pending rebalance moves. if the target now holds the chunk,
    /// remove the source from the chunk's location list and tell it to delete.
    /// returns list of (source_addr, handles_to_delete).
    pub async fn confirm_rebalance(&self) -> HashMap<String, Vec<ChunkHandle>> {
        let mut pending = self.pending_rebalance.write().await;
        let chunks = self.chunks.read().await;
        let mut to_delete: HashMap<String, Vec<ChunkHandle>> = HashMap::new();
        let mut confirmed = Vec::new();

        for (handle, (source, target)) in pending.iter() {
            let info = match chunks.get(handle) {
                Some(i) => i,
                None => {
                    confirmed.push(*handle);
                    continue;
                }
            };

            // check if target now has the chunk (reported via heartbeat)
            if info.locations.contains(target) {
                // target confirmed, schedule source for deletion
                to_delete.entry(source.clone()).or_default().push(*handle);
                confirmed.push(*handle);
            }
        }

        for h in &confirmed {
            pending.remove(h);
        }
        drop(pending);
        drop(chunks);

        // remove source from location lists
        let mut chunks = self.chunks.write().await;
        for (source, handles) in &to_delete {
            for handle in handles {
                if let Some(info) = chunks.get_mut(handle) {
                    info.locations.retain(|l| l != source);
                }
            }
        }

        to_delete
    }
}
