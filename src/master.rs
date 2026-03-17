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
    pub chunks: Vec<ChunkHandle>, // chunks this server reports holding
    pub available_space: u64,     // bytes, for placement decisions
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

    pub async fn allocate_chunk_handle(&self) -> ChunkHandle {
        let mut handle = self.next_chunk_handle.write().await;
        let h = *handle;
        *handle += 1;
        h
    }

    pub async fn create_file(&self, filename: String) {
        let mut files = self.files.write().await;
        files.insert(filename, Vec::new());
    }

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

    pub async fn get_chunk_locations(&self, handle: ChunkHandle) -> Option<Vec<String>> {
        let chunks = self.chunks.read().await;
        chunks.get(&handle).map(|info| info.locations.clone())
    }

    pub async fn get_file_chunks(&self, filename: &str) -> Option<Vec<ChunkHandle>> {
        let files = self.files.read().await;
        files.get(filename).cloned()
    }

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

    pub async fn heartbeat(&self, addr: &str, chunks: Vec<ChunkHandle>) {
        let mut servers = self.chunkservers.write().await;
        if let Some(server) = servers.get_mut(addr) {
            server.last_heartbeat = Instant::now();
            server.chunks = chunks;
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
}
