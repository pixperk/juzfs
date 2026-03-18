use std::{
    collections::HashMap,
    fs, io,
    num::NonZeroUsize,
    path::PathBuf,
    sync::atomic::{AtomicU64, Ordering},
};

use crc32fast::Hasher;
use lru::LruCache;
use tokio::sync::{Mutex, RwLock};

use crate::master::ChunkHandle;

const BLOCK_SIZE: usize = 64 * 1024; // 64KB blocks, same as GFS

/// compute a crc32 checksum for each 64KB block of data
fn compute_checksums(data: &[u8]) -> Vec<u32> {
    data.chunks(BLOCK_SIZE)
        .map(|block| {
            let mut hasher = Hasher::new();
            hasher.update(block);
            hasher.finalize()
        })
        .collect()
}

/// serialize checksums to bytes (4 bytes per checksum, big-endian)
fn checksums_to_bytes(checksums: &[u32]) -> Vec<u8> {
    checksums.iter().flat_map(|c| c.to_be_bytes()).collect()
}

/// deserialize bytes back to checksums
fn bytes_to_checksums(bytes: &[u8]) -> io::Result<Vec<u32>> {
    if bytes.len() % 4 != 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "corrupt checksum file",
        ));
    }
    Ok(bytes
        .chunks_exact(4)
        .map(|c| u32::from_be_bytes([c[0], c[1], c[2], c[3]]))
        .collect())
}

pub struct ChunkServer {
    data_dir: PathBuf,
    addr: String,
    capacity: u64,
    stored_chunks: RwLock<HashMap<ChunkHandle, u64>>,
    push_buffer: Mutex<LruCache<ChunkHandle, Vec<u8>>>,
    /// monotonically increasing serial number, only used when this CS acts as primary
    /// ensures all replicas apply mutations in the same order
    next_serial: AtomicU64,
}

impl ChunkServer {
    pub fn new(data_dir: PathBuf, addr: String, capacity: u64) -> Self {
        Self {
            data_dir,
            addr,
            capacity,
            stored_chunks: RwLock::new(HashMap::new()),
            push_buffer: Mutex::new(LruCache::new(NonZeroUsize::new(32).unwrap())),
            next_serial: AtomicU64::new(0),
        }
    }

    fn chunk_path(&self, handle: ChunkHandle) -> PathBuf {
        self.data_dir
            .join("chunks")
            .join(format!("{:08x}.chunk", handle))
    }

    fn checksum_path(&self, handle: ChunkHandle) -> PathBuf {
        self.data_dir
            .join("checksums")
            .join(format!("{:08x}.csum", handle))
    }

    fn version_path(&self, handle: ChunkHandle) -> PathBuf {
        self.data_dir
            .join("versions")
            .join(format!("{:08x}.ver", handle))
    }

    fn read_version_from_disk(&self, handle: ChunkHandle) -> u64 {
        fs::read_to_string(self.version_path(handle))
            .ok()
            .and_then(|s| s.trim().parse().ok())
            .unwrap_or(0)
    }

    fn write_version_to_disk(&self, handle: ChunkHandle, version: u64) -> io::Result<()> {
        fs::write(self.version_path(handle), version.to_string())
    }

    pub async fn init(&self) -> io::Result<()> {
        fs::create_dir_all(self.data_dir.join("chunks"))?;
        fs::create_dir_all(self.data_dir.join("checksums"))?;
        fs::create_dir_all(self.data_dir.join("versions"))?;

        // recover: scan existing chunk files, load versions from disk
        let mut stored = self.stored_chunks.write().await;
        for entry in fs::read_dir(self.data_dir.join("chunks"))? {
            let entry = entry?;
            if let Some(name) = entry.path().file_stem().and_then(|s| s.to_str()) {
                if let Ok(handle) = u64::from_str_radix(name, 16) {
                    let version = self.read_version_from_disk(handle);
                    stored.insert(handle, version);
                }
            }
        }
        Ok(())
    }

    pub async fn has_chunk(&self, handle: ChunkHandle) -> bool {
        let stored = self.stored_chunks.read().await;
        stored.contains_key(&handle)
    }

    pub async fn store_chunk(&self, handle: ChunkHandle, data: Vec<u8>) -> io::Result<()> {
        let chunk_path = self.chunk_path(handle);
        let checksum_path = self.checksum_path(handle);

        // write chunk data
        fs::write(&chunk_path, &data)?;

        // compute crc32 per 64KB block and write
        let checksums = compute_checksums(&data);
        fs::write(&checksum_path, checksums_to_bytes(&checksums))?;

        // preserve existing version, default to 0 for new chunks
        let mut stored = self.stored_chunks.write().await;
        if !stored.contains_key(&handle) {
            self.write_version_to_disk(handle, 0)?;
            stored.insert(handle, 0);
        }

        Ok(())
    }

    pub async fn read_chunk(
        &self,
        handle: ChunkHandle,
        offset: usize,
        length: usize,
    ) -> io::Result<Vec<u8>> {
        let data = fs::read(self.chunk_path(handle))?;

        if offset >= data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read offset past end of chunk",
            ));
        }

        // clamp length to available data (last chunk may be smaller than chunk_size)
        let length = length.min(data.len() - offset);

        // verify per-block crc32 checksums
        let stored_bytes = fs::read(self.checksum_path(handle))?;
        let expected = bytes_to_checksums(&stored_bytes)?;
        let actual = compute_checksums(&data);

        if expected.len() != actual.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum block count mismatch",
            ));
        }

        for (i, (exp, act)) in expected.iter().zip(actual.iter()).enumerate() {
            if exp != act {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("checksum mismatch at block {}", i),
                ));
            }
        }

        Ok(data[offset..offset + length].to_vec())
    }

    pub async fn buffer_push(&self, handle: ChunkHandle, data: Vec<u8>) {
        let mut buffer = self.push_buffer.lock().await;
        buffer.put(handle, data);
    }

    pub async fn flush_push(&self, handle: ChunkHandle) -> io::Result<()> {
        let mut buffer = self.push_buffer.lock().await;
        if let Some(data) = buffer.pop(&handle) {
            drop(buffer);
            self.store_chunk(handle, data).await?;
        }
        Ok(())
    }

    /// get current size of a chunk on disk (used by primary to check if append fits)
    pub fn chunk_size_on_disk(&self, handle: ChunkHandle) -> io::Result<u64> {
        let meta = fs::metadata(self.chunk_path(handle))?;
        Ok(meta.len())
    }

    /// append data to an existing chunk at a specific offset
    /// rewrites the full file (read + append + rewrite checksums)
    /// all replicas must append at the same offset for consistency
    pub async fn append_to_chunk(
        &self,
        handle: ChunkHandle,
        data: &[u8],
        offset: u64,
    ) -> io::Result<()> {
        let chunk_path = self.chunk_path(handle);
        let checksum_path = self.checksum_path(handle);

        // read existing data, extend at offset
        let mut existing = if chunk_path.exists() {
            fs::read(&chunk_path)?
        } else {
            Vec::new()
        };

        // pad if offset is past current length (shouldn't happen normally)
        if (offset as usize) > existing.len() {
            existing.resize(offset as usize, 0);
        }

        // truncate to offset and append new data
        existing.truncate(offset as usize);
        existing.extend_from_slice(data);

        // rewrite chunk + checksums
        fs::write(&chunk_path, &existing)?;
        let checksums = compute_checksums(&existing);
        fs::write(&checksum_path, checksums_to_bytes(&checksums))?;

        let mut stored = self.stored_chunks.write().await;
        if !stored.contains_key(&handle) {
            self.write_version_to_disk(handle, 0)?;
            stored.insert(handle, 0);
        }

        Ok(())
    }

    /// pad a chunk to exactly max_chunk_size (used when append doesn't fit, before moving to next chunk)
    pub async fn pad_chunk(&self, handle: ChunkHandle, max_chunk_size: u64) -> io::Result<()> {
        let chunk_path = self.chunk_path(handle);
        let checksum_path = self.checksum_path(handle);

        let mut data = fs::read(&chunk_path)?;
        data.resize(max_chunk_size as usize, 0);

        fs::write(&chunk_path, &data)?;
        let checksums = compute_checksums(&data);
        fs::write(&checksum_path, checksums_to_bytes(&checksums))?;

        Ok(())
    }

    pub async fn delete_chunk(&self, handle: ChunkHandle) -> io::Result<()> {
        fs::remove_file(self.chunk_path(handle))?;
        fs::remove_file(self.checksum_path(handle))?;
        let _ = fs::remove_file(self.version_path(handle));

        let mut stored = self.stored_chunks.write().await;
        stored.remove(&handle);

        Ok(())
    }

    /// returns (handle, version) for each stored chunk
    pub async fn list_chunks(&self) -> Vec<(ChunkHandle, u64)> {
        self.stored_chunks
            .read()
            .await
            .iter()
            .map(|(&h, &v)| (h, v))
            .collect()
    }

    /// master calls this when granting a new lease to bump the version
    /// persists to disk so version survives restarts
    pub async fn update_version(&self, handle: ChunkHandle, version: u64) -> io::Result<()> {
        self.write_version_to_disk(handle, version)?;
        let mut stored = self.stored_chunks.write().await;
        if let Some(v) = stored.get_mut(&handle) {
            *v = version;
        }
        Ok(())
    }

    /// get the local version for a chunk
    pub async fn get_version(&self, handle: ChunkHandle) -> Option<u64> {
        self.stored_chunks.read().await.get(&handle).copied()
    }

    /// assign next serial number (only meaningful when acting as primary)
    /// guarantees all replicas apply writes in the same global order
    pub fn next_serial(&self) -> u64 {
        self.next_serial.fetch_add(1, Ordering::SeqCst)
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }

    pub fn available_space(&self) -> u64 {
        self.capacity.saturating_sub(self.used_space())
    }

    /// total bytes used by chunk files on disk
    pub fn used_space(&self) -> u64 {
        let chunks_dir = self.data_dir.join("chunks");
        fs::read_dir(chunks_dir)
            .map(|entries| {
                entries
                    .filter_map(|e| e.ok())
                    .filter_map(|e| e.metadata().ok())
                    .map(|m| m.len())
                    .sum()
            })
            .unwrap_or(0)
    }
}
