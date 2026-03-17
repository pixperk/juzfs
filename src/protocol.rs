use serde::{Serialize, de::DeserializeOwned};
use std::io;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;

//frame format
// magic [2 bytes] | ver [1 byte] | type [1 byte] | length [4 bytes] | payload [length bytes]

const MAGIC: [u8; 2] = [0x4A, 0x46]; // 'J' 'F' for "Just File"
const VERSION: u8 = 1;
const HEADER_SIZE: usize = 8; // 2 magic + 1 ver + 1 type + 4 len

#[repr(u8)]
#[derive(Clone, Copy)]
pub enum MessageType {
    ClientToMaster = 1,
    MasterToClient = 2,
    ChunkServerToMaster = 3,
    MasterToChunkServer = 4,
    ClientToChunkServer = 5,
    ChunkServerToClient = 6,
    ChunkServerToChunkServer = 7,
    ChunkServerAck = 8,
}

pub async fn send_frame<T: Serialize>(
    stream: &mut TcpStream,
    msg_type: MessageType,
    payload: &T,
) -> io::Result<()> {
    let payload_bytes =
        bincode::serialize(payload).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    let len = (payload_bytes.len() as u32).to_be_bytes();

    let mut header = [0u8; HEADER_SIZE];
    header[0..2].copy_from_slice(&MAGIC);
    header[2] = VERSION;
    header[3] = msg_type as u8;
    header[4..8].copy_from_slice(&len);

    stream.write_all(&header).await?;
    stream.write_all(&payload_bytes).await?;
    stream.flush().await?;
    Ok(())
}

pub async fn read_frame<T: DeserializeOwned>(stream: &mut TcpStream) -> io::Result<(u8, T)> {
    let mut header = [0u8; HEADER_SIZE];
    stream.read_exact(&mut header).await?;

    if header[0..2] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let _version = header[2];
    let msg_type = header[3];
    let len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

    // guard against absurd payloads
    if len > 64 * 1024 * 1024 + 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;

    let msg = bincode::deserialize(&payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;

    Ok((msg_type, msg))
}

/// reads just the header + raw payload bytes, returns (msg_type, raw_bytes)
/// use this when the server needs to check msg_type before deciding which enum to deserialize
pub async fn read_raw_frame(stream: &mut TcpStream) -> io::Result<(u8, Vec<u8>)> {
    let mut header = [0u8; HEADER_SIZE];
    stream.read_exact(&mut header).await?;

    if header[0..2] != MAGIC {
        return Err(io::Error::new(io::ErrorKind::InvalidData, "bad magic"));
    }

    let _version = header[2];
    let msg_type = header[3];
    let len = u32::from_be_bytes([header[4], header[5], header[6], header[7]]) as usize;

    if len > 64 * 1024 * 1024 + 1024 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            "frame too large",
        ));
    }

    let mut payload = vec![0u8; len];
    stream.read_exact(&mut payload).await?;

    Ok((msg_type, payload))
}

/// deserialize raw bytes into a specific type
pub fn decode_payload<T: DeserializeOwned>(payload: &[u8]) -> io::Result<T> {
    bincode::deserialize(payload)
        .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
}

// message enums live in src/messages/
