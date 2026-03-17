use serde::{Deserialize, Serialize};

use crate::master::ChunkHandle;

/// messages chunkservers send to each other
/// used for the data push pipeline during writes
#[derive(Serialize, Deserialize, Debug)]
pub enum ChunkServerToChunkServer {
    /// forward pushed data along the chain
    /// each chunkserver buffers it and forwards to the next in `remaining`
    ForwardData {
        handle: ChunkHandle,
        data: Vec<u8>,
        remaining: Vec<String>, // addrs still to forward to
    },
    /// primary tells secondaries to commit the buffered data
    CommitWrite {
        handle: ChunkHandle,
        serial: u64, // serial number assigned by primary for ordering
    },
    /// primary tells secondaries to append data at exact offset
    /// all replicas must write at the same offset for consistency
    CommitAppend {
        handle: ChunkHandle,
        data: Vec<u8>,
        offset: u64,
        serial: u64,
    },
}

/// ack from one chunkserver to another
#[derive(Serialize, Deserialize, Debug)]
pub enum ChunkServerAck {
    Ok,
    Error(String),
}
