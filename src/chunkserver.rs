use std::{
    collections::{HashMap, HashSet},
    fs, io,
    path::PathBuf,
};

use tokio::sync::RwLock;

use crate::master::ChunkHandle;

pub struct ChunkServer {
    data_dir: PathBuf,
    addr: String,
    stored_chunks: RwLock<HashSet<ChunkHandle>>,
    push_buffer: RwLock<HashMap<ChunkHandle, Vec<u8>>>,
}

impl ChunkServer {
    pub fn new(data_dir: PathBuf, addr: String) -> Self {
        Self {
            data_dir,
            addr,
            stored_chunks: RwLock::new(HashSet::new()),
            push_buffer: RwLock::new(HashMap::new()),
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

    pub fn init(&self) -> io::Result<()> {
        fs::create_dir_all(self.data_dir.join("chunks"))?;
        fs::create_dir_all(self.data_dir.join("checksums"))?;

        // recover: scan existing chunk files into stored_chunks
        let mut stored = self.stored_chunks.blocking_write();
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

        // compute and write checksum (simple sum of bytes for demo)
        let checksum: u32 = data.iter().map(|b| *b as u32).sum();
        fs::write(&checksum_path, checksum.to_be_bytes())?;

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

        if offset + length > data.len() {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "read out of bounds",
            ));
        }

        // verify checksum
        let stored_csum = fs::read(self.checksum_path(handle))?;
        let expected =
            u32::from_be_bytes(stored_csum.try_into().map_err(|_| {
                io::Error::new(io::ErrorKind::InvalidData, "corrupt checksum file")
            })?);
        let actual: u32 = data.iter().map(|b| *b as u32).sum();
        if actual != expected {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "checksum mismatch",
            ));
        }

        Ok(data[offset..offset + length].to_vec())
    }

    pub async fn buffer_push(&self, handle: ChunkHandle, data: Vec<u8>) {
        let mut buffer = self.push_buffer.write().await;
        buffer.insert(handle, data);
    }

    pub async fn flush_push(&self, handle: ChunkHandle) -> io::Result<()> {
        let mut buffer = self.push_buffer.write().await;
        if let Some(data) = buffer.remove(&handle) {
            drop(buffer); // release lock before I/O
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

    pub fn addr(&self) -> &str {
        &self.addr
    }
}
