use core::time;
use std::sync::Arc;
use std::time::Instant;
use std::{collections::HashMap, io, path::Path};
use tokio::sync::{Mutex, RwLock};

use crate::oplog::{Checkpoint, OpLog, load_checkpoint, write_checkpoint};

pub type ChunkHandle = u64;

pub struct ChunkInfo {
    pub handle: ChunkHandle,
    pub version: u64,
    pub primary: Option<String>, // chunkserver addr holding the write lease
    pub lease_expiry: Option<Instant>, // GFS uses ~60s leases
    pub locations: Vec<String>,  // chunkserver addrs holding replicas
}

pub struct ChunkServerState {
    pub addr: String,
    pub last_heartbeat: Instant,
    pub chunks: Vec<(ChunkHandle, u64)>, // (handle, version) pairs reported by this server
    pub available_space: u64,
}

const CHECKPOINT_THRESHOLD: u64 = 100;

pub struct Master {
    pub files: Arc<RwLock<HashMap<String, Vec<ChunkHandle>>>>,
    pub chunks: Arc<RwLock<HashMap<ChunkHandle, ChunkInfo>>>,
    pub chunkservers: Arc<RwLock<HashMap<String, ChunkServerState>>>,
    pub next_chunk_handle: Arc<RwLock<ChunkHandle>>,
    pub expiry: time::Duration,
    pub oplog: Mutex<OpLog>,
    oplog_path: String,
    checkpoint_path: String,
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
                for (&handle, &version) in &cp.chunks {
                    master.chunks.write().await.insert(
                        handle,
                        ChunkInfo {
                            handle,
                            version,
                            primary: None,
                            lease_expiry: None,
                            locations: Vec::new(),
                        },
                    );
                }
                *master.next_chunk_handle.write().await = cp.next_chunk_handle;
                max_handle = max_handle.max(cp.next_chunk_handle.saturating_sub(1));
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
            expiry: time::Duration::from_secs(expiry_in_seconds),
            oplog: Mutex::new(OpLog::new(Path::new(path_str))?),
            oplog_path: path_str.to_string(),
            checkpoint_path,
        })
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

    async fn apply_entry(
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
        let mut oplog = self.oplog.lock().await;
        oplog.log(&OpLogEntry::CreateFile {
            filename: filename.clone(),
        })?;
        drop(oplog);

        let mut files = self.files.write().await;
        files.insert(filename, Vec::new());
        drop(files);
        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(())
    }

    ///allocates a new chunkhandle for a file and updates the master's metadata to reflect the new chunk and its locations. used when a client writes to a file and needs to create a new chunk.
    pub async fn add_chunk(
        &self,
        filename: &str,
        locations: Vec<String>,
    ) -> io::Result<Option<ChunkHandle>> {
        let handle = self.allocate_chunk_handle().await;

        let mut oplog = self.oplog.lock().await;
        oplog.log(&OpLogEntry::AddChunk {
            filename: filename.to_string(),
            handle,
        })?;
        drop(oplog);

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
    pub async fn heartbeat(
        &self,
        addr: &str,
        chunks: Vec<(ChunkHandle, u64)>,
        available_space: u64,
    ) {
        let mut servers = self.chunkservers.write().await;
        if let Some(server) = servers.get_mut(addr) {
            server.last_heartbeat = Instant::now();
            server.chunks = chunks.clone();
            server.available_space = available_space;
        }
        drop(servers);

        let mut chunk_map = self.chunks.write().await;
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
            }
        }
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
    /// version_bumped is true when a new lease was granted (version incremented),
    /// meaning the caller should notify all replicas to update their version.
    /// when reusing an existing lease, version_bumped is false.
    pub async fn grant_lease(
        &self,
        handle: ChunkHandle,
    ) -> io::Result<Option<(String, Vec<String>, u64, bool)>> {
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
                return Ok(Some((primary.clone(), secondaries, info.version, false)));
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

        let mut oplog = self.oplog.lock().await;
        oplog.log(&OpLogEntry::GrantLease {
            handle,
            primary: primary.clone(),
            version,
        })?;
        drop(oplog);

        if let Err(e) = self.maybe_checkpoint().await {
            tracing::error!(error = %e, "checkpoint failed");
        }
        Ok(Some((primary, secondaries, version, true)))
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
        let chunks_map: HashMap<ChunkHandle, u64> = self
            .chunks
            .read()
            .await
            .iter()
            .map(|(&h, info)| (h, info.version))
            .collect();
        let next_handle = *self.next_chunk_handle.read().await;

        let cp = Checkpoint {
            files,
            chunks: chunks_map,
            next_chunk_handle: next_handle,
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
}
