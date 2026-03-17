use serde::{Deserialize, Serialize};

use crate::master::ChunkHandle;

/// messages chunkservers send to the master
/// this is how the master learns what exists on disk
#[derive(Serialize, Deserialize, Debug)]
pub enum ChunkServerToMaster {
    /// first contact: chunkserver announces itself and how much disk it has
    Register { addr: String, available_space: u64 },
    /// periodic heartbeat: reports liveness and which chunks it holds
    /// master uses this to update its in-memory chunk location map
    Heartbeat {
        addr: String,
        chunks: Vec<ChunkHandle>,
        available_space: u64,
    },
}

/// instructions master piggybacks on heartbeat responses
/// this avoids extra round-trips for background maintenance
#[derive(Serialize, Deserialize, Debug)]
pub enum MasterToChunkServer {
    /// nothing to do, carry on
    Ok,
    /// garbage collection: these chunks are orphaned, delete them
    DeleteChunks(Vec<ChunkHandle>),
    /// replication: copy this chunk to another chunkserver
    ReplicateChunk { handle: ChunkHandle, target: String },
}
