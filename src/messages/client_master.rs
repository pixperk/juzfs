use serde::{Deserialize, Serialize};

use crate::master::ChunkHandle;

/// requests the client sends to the master
/// master never touches actual data, only metadata
#[derive(Serialize, Deserialize, Debug)]
pub enum ClientToMaster {
    /// create a new file entry in the namespace
    CreateFile { filename: String },
    /// get all chunk handles for a file (client uses index to pick which chunk)
    GetFileChunks { filename: String },
    /// given a chunk handle, get which chunkservers hold replicas
    GetChunkLocations { handle: ChunkHandle },
    /// ask master to allocate a new chunk for this file and pick replica locations
    AllocateChunk { filename: String },
    /// ask master who the primary is for a chunk (master grants lease if none active)
    GetPrimary { handle: ChunkHandle, filename: String },
    /// lazy delete a file (renames to hidden name, GC cleans up later)
    DeleteFile { filename: String },
    /// COW snapshot: create an instant copy of a file
    Snapshot { src: String, dst: String },
}

/// responses the master sends back to the client
#[derive(Serialize, Deserialize, Debug)]
pub enum MasterToClient {
    /// generic success, no payload needed
    Ok,
    /// list of chunk handles for a file
    FileChunks(Vec<ChunkHandle>),
    /// chunk handle + which chunkservers hold it
    ChunkLocations {
        handle: ChunkHandle,
        locations: Vec<String>,
    },
    /// primary + secondaries for a chunk (used by client to coordinate writes)
    /// handle may differ from requested handle if COW triggered
    PrimaryInfo {
        handle: ChunkHandle,
        primary: String,
        secondaries: Vec<String>,
    },
    /// something went wrong
    Error(String),
}
