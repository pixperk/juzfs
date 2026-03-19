use serde::{Deserialize, Serialize};

use crate::master::ChunkHandle;

/// messages chunkservers send to the master
/// this is how the master learns what exists on disk
#[derive(Serialize, Deserialize, Debug)]
pub enum ChunkServerToMaster {
    /// first contact: chunkserver announces itself and how much disk it has
    Register { addr: String, available_space: u64 },
    /// periodic heartbeat: reports liveness and which chunks it holds with their versions
    /// master uses this to update its in-memory chunk location map
    /// and detect stale replicas via version mismatch
    Heartbeat {
        addr: String,
        chunks: Vec<(ChunkHandle, u64)>,
        available_space: u64,
    },
}

/// instructions master piggybacks on heartbeat responses
/// this avoids extra round-trips for background maintenance
#[derive(Serialize, Deserialize, Debug)]
pub enum MasterToChunkServer {
    /// nothing to do, carry on
    Ok,
    /// lease granted: update your local version for this chunk
    UpdateVersion { handle: ChunkHandle, version: u64 },
    /// garbage collection: these chunks are orphaned, delete them
    DeleteChunks(Vec<ChunkHandle>),
    /// replication: copy this chunk to another chunkserver
    ReplicateChunk { handle: ChunkHandle, target: String },
    /// COW snapshot: locally copy chunk data from src to dst handle
    CopyChunk { src: ChunkHandle, dst: ChunkHandle },
}
