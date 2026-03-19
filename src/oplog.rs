use std::{
    collections::HashMap,
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    path::Path,
};

use serde::{Deserialize, Serialize};

use crate::master::{ChunkHandle, OpLogEntry};

pub struct OpLog {
    writer: BufWriter<File>,
    count: u64, //number of entries after which we should checkpoint
}

//checkpoint master state to disk
#[derive(Serialize, Deserialize)]
pub struct Checkpoint {
    pub files: HashMap<String, Vec<ChunkHandle>>,
    pub chunks: HashMap<ChunkHandle, (u64, u64)>, // handle -> (version, ref_count)
    pub next_chunk_handle: ChunkHandle,
    /// deleted files pending GC: hidden_name -> (original_name, deletion_timestamp)
    #[serde(default)]
    pub deleted_files: HashMap<String, (String, u64)>,
}

impl OpLog {
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
            count: 0,
        })
    }

    pub fn log(&mut self, entry: &OpLogEntry) -> io::Result<()> {
        let bytes = bincode::serialize(entry).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Serialization error: {}", e))
        })?;
        let len = (bytes.len() as u32).to_le_bytes();
        self.writer.write_all(&len)?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()?;
        self.count += 1;
        Ok(())
    }

    pub fn count(&self) -> u64 {
        self.count
    }

    ///rot
    pub fn rotate(&mut self, path: &Path) -> io::Result<()> {
        //rename current oplog
        let rotated_path = path.with_extension("old.bin");
        std::fs::rename(&path, &rotated_path)?;
        //create new oplog
        let file = OpenOptions::new().create(true).write(true).open(path)?;
        self.writer = BufWriter::new(file);
        self.count = 0;
        Ok(())
    }

    pub fn replay(path: &Path) -> io::Result<Vec<OpLogEntry>> {
        let mut file = File::open(path)?;
        let mut entries = Vec::new();
        let mut len_buf = [0u8; 4];
        loop {
            match file.read_exact(&mut len_buf) {
                Ok(()) => {}
                Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => break,
                Err(e) => return Err(e),
            }
            let len = u32::from_le_bytes(len_buf) as usize;
            let mut buf = vec![0u8; len];
            file.read_exact(&mut buf)?;
            let entry: OpLogEntry = bincode::deserialize(&buf)
                .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
            entries.push(entry);
        }

        Ok(entries)
    }
}

pub fn write_checkpoint(path: &Path, checkpoint: &Checkpoint) -> io::Result<()> {
    let tmp_path = path.with_extension("bin.tmp");
    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(&tmp_path)?;
    let mut writer = BufWriter::new(file);
    let bytes = bincode::serialize(checkpoint)
        .map_err(|e| io::Error::new(io::ErrorKind::Other, format!("Serialization error: {}", e)))?;
    writer.write_all(&bytes)?;
    writer.flush()?;
    std::fs::rename(&tmp_path, path)?;
    Ok(())
}

pub fn load_checkpoint(path: &Path) -> io::Result<Checkpoint> {
    let mut file = File::open(path)?;
    let mut buf = Vec::new();
    file.read_to_end(&mut buf)?;
    let checkpoint: Checkpoint =
        bincode::deserialize(&buf).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
    Ok(checkpoint)
}
