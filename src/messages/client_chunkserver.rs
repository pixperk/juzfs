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
    /// primary assigns a serial number, flushes its own buffer,
    /// then forwards CommitWrite to each secondary in serial order
    /// client passes secondaries so primary knows who to coordinate with
    Write {
        handle: ChunkHandle,
        secondaries: Vec<String>,
    },
    /// record append: client specifies only data, primary picks the offset
    /// if data fits in current chunk (< 64MB), append at EOF on all replicas
    /// if not, primary pads the chunk and responds with RetryNewChunk
    /// so client allocates a new chunk and retries
    Append {
        handle: ChunkHandle,
        data: Vec<u8>,
        secondaries: Vec<String>,
    },
}

/// responses chunkserver sends back to the client
#[derive(Serialize, Deserialize, Debug)]
pub enum ChunkServerToClient {
    /// read response with the actual bytes
    Data(Vec<u8>),
    /// generic success (write ack, push ack)
    Ok,
    /// append succeeded, returns the offset where data was written
    AppendOk { offset: u64 },
    /// chunk is too full for this append, client should allocate a new chunk and retry
    RetryNewChunk,
    /// something went wrong
    Error(String),
}
