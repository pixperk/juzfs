pub mod chunkserver;
pub mod client;
pub mod master;
pub mod messages;
pub mod namespace;
pub mod oplog;
pub mod protocol;
pub mod shadow;

/// default chunk size: 64MB (same as GFS)
/// use a smaller value in tests for faster runs
pub const CHUNK_SIZE: u64 = 64 * 1024 * 1024;
