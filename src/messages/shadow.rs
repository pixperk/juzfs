use serde::{Deserialize, Serialize};

/// shadow master requests to the primary
#[derive(Serialize, Deserialize, Debug)]
pub enum ShadowToMaster {
    /// request oplog entries starting from this offset
    Subscribe { from_offset: u64 },
}

/// primary sends oplog entries to shadows
#[derive(Serialize, Deserialize, Debug)]
pub enum MasterToShadow {
    /// a batch of serialized oplog entries (length-prefixed bytes)
    OpLogEntries { entries: Vec<Vec<u8>> },
    /// caught up, switching to live stream
    LiveStream,
}
