use core::time;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Instant;
use tokio::sync::RwLock;

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

pub struct Master {
    pub files: Arc<RwLock<HashMap<String, Vec<ChunkHandle>>>>,
    pub chunks: Arc<RwLock<HashMap<ChunkHandle, ChunkInfo>>>,
    pub chunkservers: Arc<RwLock<HashMap<String, ChunkServerState>>>,
    pub next_chunk_handle: Arc<RwLock<ChunkHandle>>,
    pub expiry: time::Duration, // lease expiry duration
}

impl Master {
    pub fn new(expiry_in_seconds: u64) -> Self {
        Master {
            files: Arc::new(RwLock::new(HashMap::new())),
            chunks: Arc::new(RwLock::new(HashMap::new())),
            chunkservers: Arc::new(RwLock::new(HashMap::new())),
            next_chunk_handle: Arc::new(RwLock::new(1)),
            expiry: time::Duration::from_secs(expiry_in_seconds),
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
    pub async fn create_file(&self, filename: String) {
        let mut files = self.files.write().await;
        files.insert(filename, Vec::new());
    }

    ///allocates a new chunkhandle for a file and updates the master's metadata to reflect the new chunk and its locations. used when a client writes to a file and needs to create a new chunk.
    pub async fn add_chunk(&self, filename: &str, locations: Vec<String>) -> Option<ChunkHandle> {
        let handle = self.allocate_chunk_handle().await;

        let mut files = self.files.write().await;
        let chunks_list = files.get_mut(filename)?;
        chunks_list.push(handle);

        let info = ChunkInfo {
            handle,
            version: 1,
            primary: None,
            lease_expiry: Some(Instant::now() + self.expiry),
            locations: locations.clone(),
        };

        let mut chunks = self.chunks.write().await;
        chunks.insert(handle, info);

        Some(handle)
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
    pub async fn heartbeat(&self, addr: &str, chunks: Vec<(ChunkHandle, u64)>, available_space: u64) {
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
    pub async fn grant_lease(&self, handle: ChunkHandle) -> Option<(String, Vec<String>, u64, bool)> {
        let mut chunks = self.chunks.write().await;
        let info = chunks.get_mut(&handle)?;

        // check if existing lease is still valid
        if let (Some(primary), Some(expiry)) = (&info.primary, info.lease_expiry) {
            if expiry > Instant::now() {
                let secondaries = info
                    .locations
                    .iter()
                    .filter(|l| *l != primary)
                    .cloned()
                    .collect();
                return Some((primary.clone(), secondaries, info.version, false));
            }
        }

        // grant new lease: pick first location as primary, bump version
        let primary = info.locations.first()?.clone();
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

        Some((primary, secondaries, version, true))
    }
}
