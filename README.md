# juzfs

A distributed file system in Rust, based on the [Google File System paper](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf). Built from scratch with raw TCP and bincode serialization.

For a detailed breakdown of the GFS architecture, see [The Google File System: A Detailed Breakdown](https://www.pixperk.tech/blog/the-google-file-system-a-detailed-breakdown).

## Why GFS

GFS was built around assumptions that most filesystems ignore: files are huge, reads are sequential, writes are almost always appends, and disks fail constantly. Rather than fighting these realities with a general-purpose POSIX layer, Google designed around them. One master, big chunks, relaxed consistency for appends. juzfs takes this same approach and implements it in Rust with async I/O.

## Architecture

![GFS Architecture](https://www.pixperk.tech/assets/blog/gfs-1.jpg)

Three components, all talking raw TCP:

### Master

The single metadata server. Holds the file namespace (filenames to chunk handle lists), chunk metadata (version, primary lease, replica locations), and the state of every registered chunkserver. Everything lives in memory behind `RwLock`s. The master never sees file data -- clients ask it "where does this chunk live?", get an answer, and go talk to chunkservers directly. Keeping the master off the data path is the core scalability trick in GFS.

Chunk locations are deliberately not persisted. On restart, the master knows nothing about which chunkserver holds what. As chunkservers boot and heartbeat in, they report their full chunk inventory with version numbers. The master rebuilds its location map from these reports. This is more robust than persisting locations because the chunkservers are always the ground truth about what's physically on their disks.

All metadata mutations (file creation, chunk allocation, lease grants) are logged to an append-only operation log before being applied to memory. The master recovers from this log on startup, and periodically snapshots its state to a checkpoint file for faster recovery. More on this in the [Operation Log](#operation-log) and [Checkpointing](#checkpointing) sections.

### Chunkservers

The data layer. Each chunkserver manages a local directory with three types of files per chunk:

```
chunks/00000001.chunk    -- raw data, up to 64MB
checksums/00000001.csum  -- CRC32 per 64KB block, packed big-endian
versions/00000001.ver    -- chunk version number
```

Chunks default to 64MB (configurable via CLI for testing). Every 64KB block gets a CRC32 checksum computed on write and verified on every read. A mismatch means corruption, and the read fails so the client can try another replica.

For writes, chunkservers maintain an LRU push buffer (32 entries). Data arrives in phase 1 of the write protocol and sits in memory until the primary triggers a commit in phase 2. A monotonic `AtomicU64` serial counter on the primary ensures all replicas apply mutations in identical order.

Every 5 seconds, each chunkserver heartbeats to the master with its chunk list (handles + versions) and real available disk space (calculated from actual file sizes, not hardcoded).

### Client

A Rust library (`src/client.rs`) that handles all the protocol complexity. It caches chunk metadata locally with a 30-second staleness window to avoid redundant master lookups. The client manages the full read, write, and append flows, including transparent retry when a chunk fills up during record append.

## Protocol

Custom TCP framing with magic bytes and length-prefixed payloads:

```
[magic: 2B "JF"][version: 1B][msg_type: 1B][payload_len: 4B][payload]
```

The framing code is compact. Here's the core of `send_frame`:

```rust
const MAGIC: [u8; 2] = [0x4A, 0x46]; // 'J' 'F'
const VERSION: u8 = 1;

pub async fn send_frame<T: Serialize>(
    stream: &mut TcpStream, msg_type: MessageType, payload: &T,
) -> io::Result<()> {
    let body = bincode::serialize(payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    let mut header = [0u8; 8];
    header[0..2].copy_from_slice(&MAGIC);
    header[2] = VERSION;
    header[3] = msg_type as u8;
    header[4..8].copy_from_slice(&(body.len() as u32).to_le_bytes());
    stream.write_all(&header).await?;
    stream.write_all(&body).await?;
    Ok(())
}
```

Magic bytes `0x4A 0x46` catch misframed connections immediately. The `msg_type` byte routes messages:

| msg_type | Direction |
|----------|-----------|
| 1 | Client to Master |
| 2 | Master to Client |
| 3 | ChunkServer to Master |
| 4 | Master to ChunkServer |
| 5 | Client to ChunkServer |
| 6 | ChunkServer to Client |
| 7 | ChunkServer to ChunkServer |
| 8 | ChunkServer Ack |

Payloads are bincode v1. Each direction has its own serde enum, so extending the protocol is just adding a variant.

## Reads

The master is never on the read path. The client asks it for metadata once, then reads directly from chunkservers.

1. `GetFileChunks` returns the ordered list of chunk handles for a file
2. `GetChunkLocations` returns which chunkservers hold each chunk
3. Client sends `Read { handle, offset, length }` directly to a chunkserver
4. If that replica fails (network error, checksum mismatch), try the next one

For multi-chunk reads, the client calculates which chunks the byte range spans and issues per-chunk reads with the correct local offsets. If the requested length exceeds what the chunk actually has, the chunkserver clamps it and returns what's available.

**Streaming reads** pipe chunks through a tokio mpsc channel with a 4-chunk lookahead buffer. A background task fetches chunks in order and feeds them into the channel. The consumer processes data as it arrives without loading the entire file into memory.

## Writes

Writes separate data flow from control flow using a two-phase protocol. This is the most interesting part of the design.

![Write and Mutation Flow](https://www.pixperk.tech/assets/blog/gfs-4.jpg)

```mermaid
sequenceDiagram
    participant C as Client
    participant P as Primary
    participant S1 as Secondary 1
    participant S2 as Secondary 2

    Note over C,S2: Phase 1 -- data pipeline
    C->>P: ForwardData(handle, data, [S1, S2])
    P->>S1: ForwardData(handle, data, [S2])
    S1->>S2: ForwardData(handle, data, [])

    Note over C,S2: Phase 2 -- primary coordinates commit
    C->>P: Write(handle, [S1, S2])
    P->>P: serial=next(), flush to disk
    P->>S1: CommitWrite(handle, serial)
    S1-->>P: ack
    P->>S2: CommitWrite(handle, serial)
    S2-->>P: ack
    P-->>C: Ok
```

**Phase 1** pushes data through a chunkserver chain. The client sends to the first node, which buffers and forwards to the next, and so on. Data flows in a single network pass rather than the client uploading to each replica separately. All nodes buffer in memory (the LRU push buffer), nothing touches disk yet. If any node in the chain fails to forward, the error propagates back through the entire chain to the client via `ChunkServerAck::Error`:

```rust
// chunkserver buffers data, then forwards to next in chain
cs.buffer_push(handle, data.clone()).await;

let mut chain_err: Option<String> = None;
if let Some((next, rest)) = remaining.split_first() {
    let fwd = ChunkServerToChunkServer::ForwardData {
        handle, data, remaining: rest.to_vec(),
    };
    match TcpStream::connect(next).await {
        Ok(mut next_conn) => {
            // send and read downstream ack...
        }
        Err(e) => {
            chain_err = Some(format!("forward to {} unreachable: {}", next, e));
        }
    }
}
match chain_err {
    Some(e) => ChunkServerAck::Error(e),
    None => ChunkServerAck::Ok,
}
```

**Phase 2** is where ordering happens. The client tells the primary to commit. The primary assigns a monotonic serial number, flushes its own buffer to disk, then sends `CommitWrite` to each secondary with that serial. Secondaries flush and ack. Only after all replicas confirm does the primary ack the client.

The serial number is the consistency mechanism. Two concurrent writes to the same chunk get serialized by the primary. Every replica applies them in serial order, so they converge to identical state.

### Leases

Before any write, the client asks the master for a primary via `GetPrimary`. The master grants a 60-second lease to one chunkserver. The same lease is reused for all writes during that window. No round-trip to the master per write, just per lease period.

When a lease expires and a new one is needed, the master bumps the chunk version, notifies all replicas via `UpdateVersion`, and returns the new primary. This version bump is how stale replicas get caught (more on that below).

## Record Append

The dominant write pattern in GFS. The client provides only data, no offset. The primary decides where to place it. Multiple clients can append concurrently to the same file without any coordination between them.

![Record Append](https://www.pixperk.tech/assets/blog/gfs-5.jpg)

The flow:

1. Client finds the last chunk of the file and sends `Append { handle, data, secondaries }` to its primary
2. Primary checks: does `current_size + data_len` fit within the chunk size?
3. **Fits**: primary sets `offset = current_size`, appends locally, sends `CommitAppend` with the same offset and data to all secondaries. Returns `AppendOk { offset }` to the client.
4. **Doesn't fit**: primary pads the chunk to exactly 64MB on all replicas and returns `RetryNewChunk`. The client allocates a new chunk via the master, invalidates its metadata cache, and retries. This loop is transparent to the caller.

The padding matters. Without it, a half-full chunk on one replica could accept a conflicting append from a different client, breaking cross-replica consistency. Padding seals the chunk on all replicas simultaneously.

## Chunk Versioning

Every chunk starts at version 0. The version increments each time the master grants a new lease for that chunk. This is how the system detects replicas that missed mutations while they were down.

The version lives in three places:
- **Master memory**: `ChunkInfo.version`, the authoritative value
- **Chunkserver disk**: `.ver` file per chunk, survives restarts
- **Chunkserver memory**: `HashMap<ChunkHandle, u64>`, loaded from disk on init

When the master grants a lease and bumps the version, it connects to every replica and sends `UpdateVersion { handle, version }`. Each chunkserver persists the new version to its `.ver` file and inserts into its in-memory map. The insert-not-update matters -- `UpdateVersion` can arrive before the chunk data does (the version bump happens at lease grant time, data arrives later during the write). If the chunkserver only updated existing entries, the version would be lost and the subsequent `store_chunk` would default to version 0:

```rust
// update_version must always insert, not just update existing entries.
// UpdateVersion arrives at lease grant time, before any data is written.
// If we only did get_mut here, the version would be lost when store_chunk
// later inserts the handle with version 0.
pub async fn update_version(&self, handle: ChunkHandle, version: u64) -> io::Result<()> {
    self.write_version_to_disk(handle, version)?;
    let mut stored = self.stored_chunks.write().await;
    stored.insert(handle, version);  // insert, not get_mut
    Ok(())
}
```

On heartbeat, each chunkserver reports `(handle, version)` pairs. The master compares:
- **Version matches or ahead**: healthy replica, stays in the location list
- **Version behind**: stale replica, removed from locations

Once removed, the stale chunkserver is invisible to clients. No reads or writes will be directed to it. This happens automatically, no manual intervention.

## Operation Log

![Checkpointing](https://www.pixperk.tech/assets/blog/gfs-2.jpg)

The operation log (oplog) is how the master survives crashes. Every metadata mutation is written to an append-only log on disk before it is applied to memory. If the master crashes, it replays the log on startup and reconstructs its state.

The oplog records three types of mutations:

```rust
pub enum OpLogEntry {
    CreateFile { filename: String },
    AddChunk { filename: String, handle: ChunkHandle },
    GrantLease { handle: ChunkHandle, primary: String, version: u64 },
}
```

Each entry is length-prefixed and bincode-serialized. The critical invariant: **the log is flushed to disk before the mutation becomes visible to clients**. If a client got an OK response, that operation survived the crash. If the master crashed before flushing, the client got no response and will retry.

```rust
// in create_file: log BEFORE applying to memory
pub async fn create_file(&self, filename: String) -> io::Result<()> {
    let mut oplog = self.oplog.lock().await;
    oplog.log(&OpLogEntry::CreateFile { filename: filename.clone() })?;
    drop(oplog);

    let mut files = self.files.write().await;
    files.insert(filename, Vec::new());
    // ...
}
```

What the oplog does NOT store:
- **Chunk locations**: rebuilt from chunkserver heartbeats on startup
- **Leases**: expired after a crash anyway, chunkservers will request new ones
- **Chunkserver registrations**: transient state, rebuilt as chunkservers reconnect

On recovery, the master loads the latest checkpoint (if any), then replays the oplog entries on top. Chunkserver heartbeats fill in the locations, and the system is back online.

## Checkpointing

Replaying the entire oplog from the beginning of time gets slow as the system grows. Checkpointing solves this by periodically snapshotting the master's in-memory state to disk, following the GFS paper's approach.

The checkpoint captures the minimum state needed to restore the master:

```rust
pub struct Checkpoint {
    pub files: HashMap<String, Vec<ChunkHandle>>,
    pub chunks: HashMap<ChunkHandle, u64>,  // handle -> version
    pub next_chunk_handle: ChunkHandle,
}
```

The checkpointing flow follows the GFS design -- log rotation first, then background write:

1. **Rotate** the oplog: rename `oplog.bin` to `oplog.old.bin`, open a fresh `oplog.bin`. This is fast (just a rename + open) and happens under the oplog lock. New mutations immediately flow to the fresh log with zero blocking.
2. **Snapshot** the current state: read `files`, `chunks`, and `next_chunk_handle`. These are read locks, so mutations can proceed concurrently.
3. **Write checkpoint in the background**: serialize to `checkpoint.bin.tmp`, then atomically rename to `checkpoint.bin`. The tmp+rename ensures that an incomplete write is never visible. Once the checkpoint is written, delete `oplog.old.bin`.

```rust
pub async fn maybe_checkpoint(&self) -> io::Result<()> {
    let mut oplog = self.oplog.lock().await;
    if oplog.count() < CHECKPOINT_THRESHOLD { return Ok(()); }

    // step 1: rotate (fast, under lock)
    oplog.rotate(Path::new(&self.oplog_path))?;
    drop(oplog); // new mutations proceed immediately

    // step 2: snapshot state (read locks only)
    let files = self.files.read().await.clone();
    let chunks_map = /* ... */;
    let cp = Checkpoint { files, chunks: chunks_map, next_chunk_handle };

    // step 3: write in background
    tokio::task::spawn_blocking(move || {
        write_checkpoint(Path::new(&checkpoint_path), &cp)?;
        std::fs::remove_file(old_oplog_path).ok();
    });
    Ok(())
}
```

If the master crashes between rotate and checkpoint completion:
- `checkpoint.bin` is either missing or incomplete (detected and skipped)
- `oplog.old.bin` still exists with all pre-rotation entries
- `oplog.bin` has post-rotation entries
- Recovery replays `oplog.old.bin` first, then `oplog.bin`, and reconstructs everything

This is the same resilience property described in the GFS paper: "if checkpointing fails midway, the new log still contains all mutations, and incomplete checkpoints are detected and skipped."

The checkpoint triggers after every 100 oplog entries (configurable via `CHECKPOINT_THRESHOLD`). Recovery on startup:

1. Load `checkpoint.bin` if it exists
2. Replay `oplog.old.bin` if it exists (crash between rotate and checkpoint)
3. Replay `oplog.bin` (entries written after the last checkpoint)
4. Wait for chunkserver heartbeats to repopulate locations

## Data Integrity

![Consistency Model](https://www.pixperk.tech/assets/blog/gfs-3.png)

Every read goes through checksum verification:

1. Chunkserver reads the full chunk data from disk
2. Reads the stored CRC32 checksums (one per 64KB block)
3. Recomputes checksums from the data
4. Compares block by block

Any mismatch fails the read. The client then tries another replica. This catches silent disk corruption, partial writes, and bit rot without any external scrubbing process.

## Heartbeats

The heartbeat is the master's lifeline to the cluster. It serves three purposes:

1. **Liveness**: the master knows which chunkservers are up based on heartbeat timestamps
2. **Location rebuild**: chunk locations are reconstructed entirely from heartbeat reports, not persisted
3. **Stale detection**: version numbers in heartbeats let the master identify and exclude stale replicas

The cycle:
- On boot, a chunkserver sends `Register { addr, available_space }`
- Every 5 seconds after, it sends `Heartbeat { addr, chunks: Vec<(handle, version)>, available_space }`
- Available space is real: `capacity - sum(chunk file sizes on disk)`
- The master uses available space for placement decisions, preferring chunkservers with the most room

## Usage

Start the cluster in separate terminals:

```bash
# master
cargo run --bin master-server

# three chunkservers
cargo run --bin chunkserver-node -- 127.0.0.1:6001 /tmp/cs1 127.0.0.1:5000
cargo run --bin chunkserver-node -- 127.0.0.1:6002 /tmp/cs2 127.0.0.1:5000
cargo run --bin chunkserver-node -- 127.0.0.1:6003 /tmp/cs3 127.0.0.1:5000
```

Then use the CLI:

```bash
cargo run --bin juzfs -- create /hello.txt
cargo run --bin juzfs -- write /hello.txt 0 "the quick brown fox"
cargo run --bin juzfs -- read /hello.txt
cargo run --bin juzfs -- read /hello.txt 4 5
cargo run --bin juzfs -- append /hello.txt "another line"
cargo run --bin juzfs -- stream /hello.txt
```

### CLI

```
juzfs [options] <command> [args]

options:
  --master <addr>        master address (default: 127.0.0.1:5000)
  --chunk-size <bytes>   chunk size in bytes (default: 64MB)

commands:
  create <path>                create file and allocate first chunk
  write  <path> <offset> <data>
  read   <path> [offset] [length]
  append <path> <data>         record append
  stream <path>                streaming read
```

### Chunkserver

```
chunkserver-node <addr> <data_dir> <master_addr> [capacity_bytes] [chunk_size_bytes]
```

`capacity_bytes` defaults to 1GB. `chunk_size_bytes` defaults to 64MB. Available space in heartbeats is `capacity - actual disk usage`.

### Logging

Structured logging via `tracing`. Control with `RUST_LOG`:

```bash
RUST_LOG=info cargo run --bin master-server    # default: file ops, leases, registrations
RUST_LOG=debug cargo run --bin master-server   # adds: heartbeats, cache behavior
```

## Implemented

- Single master with in-memory metadata (namespace, chunks, leases)
- Operation log for crash recovery (append-only, length-prefixed bincode)
- GFS-style checkpointing (log rotation, background snapshot, atomic write)
- Recovery from checkpoint + oplog replay on startup
- Chunkservers with disk persistence, CRC32 checksums per 64KB block, version tracking
- Custom TCP protocol with magic bytes and bincode serialization
- Two-phase writes: pipelined data push through chunkserver chain, then primary-coordinated commit
- Chain error propagation (ForwardData failures bubble back to client)
- Mutation ordering via monotonic serial numbers on the primary
- Record append with chunk overflow detection, padding, and transparent retry
- 60-second write leases with version bump on renewal
- Stale replica detection via version comparison on heartbeat
- Master notifies replicas of version updates on lease grant
- Location rebuild from heartbeats (no persisted chunk locations)
- Available space tracking from real disk usage
- Client metadata caching (30s staleness window)
- Streaming reads with 4-chunk async buffer
- Chunkserver recovery on restart (scans chunks, checksums, versions from disk)
- CLI client with create, write, read, append, stream commands

## Remaining

![Garbage Collection Flow](https://www.pixperk.tech/assets/blog/gfs-7.jpg)

- Re-replication for under-replicated chunks
- Chunk rebalancing across chunkservers
- Garbage collection (lazy delete, heartbeat-driven cleanup -- see diagram above)
- Copy-on-write snapshots
- Namespace locking for concurrent operations

## References

- [The Google File System (2003)](https://static.googleusercontent.com/media/research.google.com/en//archive/gfs-sosp2003.pdf) -- Ghemawat, Gobioff, and Leung
- [The Google File System: A Detailed Breakdown](https://www.pixperk.tech/blog/the-google-file-system-a-detailed-breakdown) -- blog this implementation follows
