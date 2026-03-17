use std::{collections::HashMap, io, time::Instant};

use tokio::{net::TcpStream, sync::RwLock};

use crate::{
    master::ChunkHandle,
    messages::{ChunkServerToClient, ClientToChunkServer, ClientToMaster, MasterToClient},
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

    pub async fn read(&self, filename: &str, offset: u64, length: u64) -> io::Result<Vec<u8>> {
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
}
