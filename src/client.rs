use std::{collections::HashMap, io, time::Instant};

use tokio::{net::TcpStream, sync::RwLock};

use crate::{
    master::ChunkHandle,
    messages::{
        ChunkServerAck, ChunkServerToChunkServer, ChunkServerToClient, ClientToChunkServer,
        ClientToMaster, MasterToClient,
    },
    protocol::{MessageType, read_frame, send_frame},
};

pub struct Client {
    master_addr: String,
    chunk_size: u64,
    metadata_cache: RwLock<HashMap<String, Vec<CachedChunkInfo>>>, // filename -> chunk info
}

struct CachedChunkInfo {
    handle: ChunkHandle,
    locations: Vec<String>,
    fetched_at: Instant,
}

impl Client {
    pub fn new(master_addr: String, chunk_size: u64) -> Self {
        Self {
            master_addr,
            chunk_size,
            metadata_cache: RwLock::new(HashMap::new()),
        }
    }

    /// create a file on the master
    pub async fn create_file(&self, filename: &str) -> io::Result<()> {
        let mut conn = TcpStream::connect(&self.master_addr).await?;
        send_frame(
            &mut conn,
            MessageType::ClientToMaster,
            &ClientToMaster::CreateFile {
                filename: filename.to_string(),
            },
        )
        .await?;
        let (_, resp): (u8, MasterToClient) = read_frame(&mut conn).await?;
        match resp {
            MasterToClient::Ok => Ok(()),
            MasterToClient::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response",
            )),
        }
    }

    /// delete a file (lazy -- master renames to hidden name, GC cleans up later)
    pub async fn delete_file(&self, filename: &str) -> io::Result<()> {
        let mut conn = TcpStream::connect(&self.master_addr).await?;
        send_frame(
            &mut conn,
            MessageType::ClientToMaster,
            &ClientToMaster::DeleteFile {
                filename: filename.to_string(),
            },
        )
        .await?;
        let (_, resp): (u8, MasterToClient) = read_frame(&mut conn).await?;
        match resp {
            MasterToClient::Ok => Ok(()),
            MasterToClient::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response",
            )),
        }
    }

    /// ask master to allocate a new chunk for a file, returns (handle, locations)
    pub async fn allocate_chunk(&self, filename: &str) -> io::Result<(ChunkHandle, Vec<String>)> {
        let mut conn = TcpStream::connect(&self.master_addr).await?;
        send_frame(
            &mut conn,
            MessageType::ClientToMaster,
            &ClientToMaster::AllocateChunk {
                filename: filename.to_string(),
            },
        )
        .await?;
        let (_, resp): (u8, MasterToClient) = read_frame(&mut conn).await?;
        match resp {
            MasterToClient::ChunkLocations { handle, locations } => Ok((handle, locations)),
            MasterToClient::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response",
            )),
        }
    }

    pub async fn read(&self, filename: &str, offset: u64, length: u64) -> io::Result<Vec<u8>> {
        if length == 0 {
            return Ok(Vec::new());
        }
        let metadata = self.get_chunk_metadata(filename).await?;

        let start_chunk = (offset / self.chunk_size) as usize;
        let end_chunk = ((offset + length - 1) / self.chunk_size) as usize;

        let mut result = Vec::with_capacity(length as usize);

        for chunk_idx in start_chunk..=end_chunk {
            if chunk_idx >= metadata.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "offset past end of file",
                ));
            }

            let (handle, ref locations) = metadata[chunk_idx];

            //calculate local offset and length for this chunk
            let chunk_offset = if chunk_idx == start_chunk {
                offset % self.chunk_size
            } else {
                0
            };
            let chunk_end = if chunk_idx == end_chunk {
                (offset + length - 1) % self.chunk_size + 1
            } else {
                self.chunk_size
            };
            let chunk_len = chunk_end - chunk_offset;

            // try each replica until one works
            let mut data = None;
            for addr in locations {
                match self
                    .read_from_chunkserver(addr, handle, chunk_offset, chunk_len)
                    .await
                {
                    Ok(bytes) => {
                        data = Some(bytes);
                        break;
                    }
                    Err(_) => continue,
                }
            }

            match data {
                Some(bytes) => result.extend_from_slice(&bytes),
                None => return Err(io::Error::new(io::ErrorKind::Other, "all replicas failed")),
            }
        }

        Ok(result)
    }

    pub async fn read_stream(
        &self,
        filename: &str,
    ) -> io::Result<tokio::sync::mpsc::Receiver<io::Result<Vec<u8>>>> {
        let metadata = self.get_chunk_metadata(filename).await?;
        let (tx, rx) = tokio::sync::mpsc::channel(4); // buffer 4 chunks ahead

        let chunk_size = self.chunk_size;

        tokio::spawn(async move {
            for (i, (handle, locations)) in metadata.into_iter().enumerate() {
                // try each replica
                let mut success = false;
                for addr in &locations {
                    let mut conn = match TcpStream::connect(addr).await {
                        Ok(c) => c,
                        Err(_) => continue,
                    };
                    let msg = ClientToChunkServer::Read {
                        handle,
                        offset: 0,
                        length: chunk_size,
                    };
                    if send_frame(&mut conn, MessageType::ClientToChunkServer, &msg)
                        .await
                        .is_err()
                    {
                        continue;
                    }
                    match read_frame::<ChunkServerToClient>(&mut conn).await {
                        Ok((_, ChunkServerToClient::Data(bytes))) => {
                            if tx.send(Ok(bytes)).await.is_err() {
                                return;
                            }
                            success = true;
                            break;
                        }
                        _ => continue,
                    }
                }
                if !success {
                    let _ = tx
                        .send(Err(io::Error::new(
                            io::ErrorKind::Other,
                            format!("all replicas failed for chunk {}", i),
                        )))
                        .await;
                    return;
                }
            }
        });

        Ok(rx)
    }

    /// write data to a file at a given offset
    /// uses the GFS two-phase write protocol:
    ///   1. push data through chunkserver chain (all replicas buffer it)
    ///   2. tell primary to commit (primary orders + forwards to secondaries)
    pub async fn write(&self, filename: &str, offset: u64, data: &[u8]) -> io::Result<()> {
        if data.is_empty() {
            return Ok(());
        }
        let metadata = self.get_chunk_metadata(filename).await?;

        let start_chunk = (offset / self.chunk_size) as usize;
        let end_chunk = ((offset + data.len() as u64 - 1) / self.chunk_size) as usize;

        let mut data_offset = 0usize;

        for chunk_idx in start_chunk..=end_chunk {
            if chunk_idx >= metadata.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "write past end of file",
                ));
            }

            let (handle, _) = metadata[chunk_idx];

            // calculate how much data goes into this chunk
            let chunk_start = if chunk_idx == start_chunk {
                (offset % self.chunk_size) as usize
            } else {
                0
            };
            let chunk_capacity = self.chunk_size as usize - chunk_start;
            let remaining_data = data.len() - data_offset;
            let write_len = remaining_data.min(chunk_capacity);
            let chunk_data = &data[data_offset..data_offset + write_len];
            data_offset += write_len;

            // ask master who the primary is (grants lease if needed)
            let (primary, secondaries) = self.get_primary(handle).await?;

            // build chain: primary first, then secondaries
            let mut chain = vec![primary.clone()];
            chain.extend(secondaries.clone());

            // phase 1: push data to all replicas via chain
            self.push_data_chain(&chain, handle, chunk_data).await?;

            // phase 2: tell primary to commit (primary coordinates secondaries)
            self.commit_write(&primary, handle, secondaries).await?;
        }

        Ok(())
    }

    async fn get_chunk_metadata(
        &self,
        filename: &str,
    ) -> io::Result<Vec<(ChunkHandle, Vec<String>)>> {
        //check cache
        let cache = self.metadata_cache.read().await;
        if let Some(infos) = cache.get(filename) {
            let stale = infos.iter().any(|i| i.fetched_at.elapsed().as_secs() > 30);
            //return if not stale
            if !stale {
                return Ok(infos
                    .iter()
                    .map(|i| (i.handle, i.locations.clone()))
                    .collect());
            }
        }
        drop(cache);
        //if stale or cache miss, fetch from master
        let chunks = self.fetch_metadata_from_master(filename).await?;
        let mut cache = self.metadata_cache.write().await;
        let now = Instant::now();
        cache.insert(
            filename.to_string(),
            chunks
                .iter()
                .map(|(h, locs)| CachedChunkInfo {
                    handle: *h,
                    locations: locs.clone(),
                    fetched_at: now,
                })
                .collect(),
        );

        Ok(chunks)
    }

    //fetch chunk handles and locations from master, return list of (chunk handle, chunkserver addresses)
    async fn fetch_metadata_from_master(
        &self,
        filename: &str,
    ) -> io::Result<Vec<(ChunkHandle, Vec<String>)>> {
        let mut conn = TcpStream::connect(&self.master_addr).await?;
        //get all chunk handles from master
        send_frame(
            &mut conn,
            MessageType::ClientToMaster,
            &ClientToMaster::GetFileChunks {
                filename: filename.to_string(),
            },
        )
        .await?;

        let (_, resp): (u8, MasterToClient) = read_frame(&mut conn).await?;
        let handles = match resp {
            MasterToClient::FileChunks(chunks) => chunks,
            MasterToClient::Error(e) => return Err(io::Error::new(io::ErrorKind::NotFound, e)),
            _ => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "unexpected response",
                ));
            }
        };

        // get locations for each chunk handle
        let mut result = Vec::with_capacity(handles.len());
        for handle in handles {
            send_frame(
                &mut conn,
                MessageType::ClientToMaster,
                &ClientToMaster::GetChunkLocations { handle },
            )
            .await?;

            let (_, resp): (u8, MasterToClient) = read_frame(&mut conn).await?;
            let locations = match resp {
                MasterToClient::ChunkLocations { locations, .. } => locations,
                MasterToClient::Error(e) => return Err(io::Error::new(io::ErrorKind::Other, e)),
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected response",
                    ));
                }
            };

            result.push((handle, locations));
        }

        Ok(result)
    }

    async fn read_from_chunkserver(
        &self,
        addr: &str,
        handle: ChunkHandle,
        offset: u64,
        length: u64,
    ) -> io::Result<Vec<u8>> {
        // Implementation for reading from a chunkserver
        let mut conn = TcpStream::connect(addr).await?;
        send_frame(
            &mut conn,
            MessageType::ClientToChunkServer,
            &ClientToChunkServer::Read {
                handle,
                offset,
                length,
            },
        )
        .await?;

        let (_, resp): (u8, ChunkServerToClient) = read_frame(&mut conn).await?;
        match resp {
            ChunkServerToClient::Data(bytes) => Ok(bytes),
            ChunkServerToClient::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response",
            )),
        }
    }

    async fn get_primary(&self, handle: ChunkHandle) -> io::Result<(String, Vec<String>)> {
        let mut conn = TcpStream::connect(&self.master_addr).await?;
        send_frame(
            &mut conn,
            MessageType::ClientToMaster,
            &ClientToMaster::GetPrimary { handle },
        )
        .await?;
        let (_, resp): (u8, MasterToClient) = read_frame(&mut conn).await?;
        match resp {
            MasterToClient::PrimaryInfo {
                primary,
                secondaries,
            } => Ok((primary, secondaries)),
            MasterToClient::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response",
            )),
        }
    }

    /// phase 1: push data through the chunkserver chain
    /// client sends to the first CS, which buffers and forwards to the next, and so on
    /// //todo : add retries
    async fn push_data_chain(
        &self,
        chain: &[String],
        handle: ChunkHandle,
        data: &[u8],
    ) -> io::Result<()> {
        if chain.is_empty() {
            return Err(io::Error::new(io::ErrorKind::Other, "empty chain"));
        }

        // connect to first chunkserver in the chain
        let mut conn = TcpStream::connect(&chain[0]).await?;

        // send ForwardData with remaining chain for it to continue the pipeline
        send_frame(
            &mut conn,
            MessageType::ChunkServerToChunkServer,
            // client initiates the chain using the same ForwardData message
            // that chunkservers use to forward to each other
            &ChunkServerToChunkServer::ForwardData {
                handle,
                data: data.to_vec(),
                remaining: chain[1..].iter().cloned().collect(),
            },
        )
        .await?;

        // wait for ack from first CS (it acks only after the whole chain succeeds)
        let (_, ack): (u8, ChunkServerAck) = read_frame(&mut conn).await?;
        match ack {
            ChunkServerAck::Ok => Ok(()),
            ChunkServerAck::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
        }
    }

    /// phase 2: tell the primary to commit buffered data
    /// primary assigns serial number, flushes its own buffer,
    /// then sends CommitWrite to each secondary in serial order
    async fn commit_write(
        &self,
        primary: &str,
        handle: ChunkHandle,
        secondaries: Vec<String>,
    ) -> io::Result<()> {
        let mut conn = TcpStream::connect(primary).await?;
        send_frame(
            &mut conn,
            MessageType::ClientToChunkServer,
            &ClientToChunkServer::Write {
                handle,
                secondaries,
            },
        )
        .await?;

        let (_, resp): (u8, ChunkServerToClient) = read_frame(&mut conn).await?;
        match resp {
            ChunkServerToClient::Ok => Ok(()),
            ChunkServerToClient::Error(e) => Err(io::Error::new(io::ErrorKind::Other, e)),
            _ => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "unexpected response",
            )),
        }
    }

    /// record append: client specifies only data, primary picks the offset
    /// if current last chunk is too full, allocates a new chunk and retries
    /// returns the offset where data was written
    pub async fn append(&self, filename: &str, data: &[u8]) -> io::Result<u64> {
        loop {
            let metadata = self.get_chunk_metadata(filename).await?;
            if metadata.is_empty() {
                return Err(io::Error::new(
                    io::ErrorKind::NotFound,
                    "file has no chunks",
                ));
            }

            // always append to the last chunk
            let (handle, _) = metadata[metadata.len() - 1];
            let (primary, secondaries) = self.get_primary(handle).await?;

            let mut conn = TcpStream::connect(&primary).await?;
            send_frame(
                &mut conn,
                MessageType::ClientToChunkServer,
                &ClientToChunkServer::Append {
                    handle,
                    data: data.to_vec(),
                    secondaries,
                },
            )
            .await?;

            let (_, resp): (u8, ChunkServerToClient) = read_frame(&mut conn).await?;
            match resp {
                ChunkServerToClient::AppendOk { offset } => return Ok(offset),
                ChunkServerToClient::RetryNewChunk => {
                    // current chunk full, allocate new one and retry
                    self.invalidate_cache(filename).await;
                    self.allocate_chunk(filename).await?;
                }
                ChunkServerToClient::Error(e) => {
                    return Err(io::Error::new(io::ErrorKind::Other, e))
                }
                _ => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unexpected response",
                    ))
                }
            }
        }
    }

    /// clear cached metadata for a file (used after allocating new chunk)
    async fn invalidate_cache(&self, filename: &str) {
        let mut cache = self.metadata_cache.write().await;
        cache.remove(filename);
    }
}
