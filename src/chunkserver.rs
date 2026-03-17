use std::{
    collections::HashSet,
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
    stored_chunks: RwLock<HashSet<ChunkHandle>>,
    push_buffer: Mutex<LruCache<ChunkHandle, Vec<u8>>>,
    /// monotonically increasing serial number, only used when this CS acts as primary
    /// ensures all replicas apply mutations in the same order
    next_serial: AtomicU64,
}

impl ChunkServer {
    pub fn new(data_dir: PathBuf, addr: String) -> Self {
        Self {
            data_dir,
            addr,
            stored_chunks: RwLock::new(HashSet::new()),
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

    pub async fn init(&self) -> io::Result<()> {
        fs::create_dir_all(self.data_dir.join("chunks"))?;
        fs::create_dir_all(self.data_dir.join("checksums"))?;

        // recover: scan existing chunk files into stored_chunks
        let mut stored = self.stored_chunks.write().await;
        for entry in fs::read_dir(self.data_dir.join("chunks"))? {
            let entry = entry?;
            if let Some(name) = entry.path().file_stem().and_then(|s| s.to_str()) {
                if let Ok(handle) = u64::from_str_radix(name, 16) {
                    stored.insert(handle);
                }
            }
        }
        Ok(())
    }

    pub async fn has_chunk(&self, handle: ChunkHandle) -> bool {
        let stored = self.stored_chunks.read().await;
        stored.contains(&handle)
    }

    pub async fn store_chunk(&self, handle: ChunkHandle, data: Vec<u8>) -> io::Result<()> {
        let chunk_path = self.chunk_path(handle);
        let checksum_path = self.checksum_path(handle);

        // write chunk data
        fs::write(&chunk_path, &data)?;

        // compute crc32 per 64KB block and write
        let checksums = compute_checksums(&data);
        fs::write(&checksum_path, checksums_to_bytes(&checksums))?;

        // update in-memory state
        let mut stored = self.stored_chunks.write().await;
        stored.insert(handle);

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

    pub async fn delete_chunk(&self, handle: ChunkHandle) -> io::Result<()> {
        let chunk_path = self.chunk_path(handle);
        let checksum_path = self.checksum_path(handle);

        fs::remove_file(&chunk_path)?;
        fs::remove_file(&checksum_path)?;

        let mut stored = self.stored_chunks.write().await;
        stored.remove(&handle);

        Ok(())
    }

    pub async fn list_chunks(&self) -> Vec<ChunkHandle> {
        self.stored_chunks.read().await.iter().copied().collect()
    }

    /// assign next serial number (only meaningful when acting as primary)
    /// guarantees all replicas apply writes in the same global order
    pub fn next_serial(&self) -> u64 {
        self.next_serial.fetch_add(1, Ordering::SeqCst)
    }

    pub fn addr(&self) -> &str {
        &self.addr
    }
}
