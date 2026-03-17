use serde::{Deserialize, Serialize};

use crate::master::ChunkHandle;

/// requests the client sends directly to a chunkserver
/// this is the data path, master is not involved here
#[derive(Serialize, Deserialize, Debug)]
pub enum ClientToChunkServer {
    /// read a range of bytes from a chunk
    Read {
        handle: ChunkHandle,
        offset: u64,
        length: u64,
    },
    /// phase 1 of write: push data to chunkserver's buffer (not yet committed)
    /// in GFS, client pipelines this to all replicas before issuing Write
    PushData {
        handle: ChunkHandle,
        data: Vec<u8>,
    },
    /// phase 2 of write: tell the primary to commit the buffered data
    /// primary assigns a serial number and forwards to secondaries
    Write { handle: ChunkHandle },
}

/// responses chunkserver sends back to the client
#[derive(Serialize, Deserialize, Debug)]
pub enum ChunkServerToClient {
    /// read response with the actual bytes
    Data(Vec<u8>),
    /// generic success (write ack, push ack)
    Ok,
    /// something went wrong
    Error(String),
}
