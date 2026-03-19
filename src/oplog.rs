use std::{
    fs::{File, OpenOptions},
    io::{self, BufWriter, Read, Write},
    path::Path,
};

use crate::master::OpLogEntry;

pub struct OpLog {
    writer: BufWriter<File>,
}

impl OpLog {
    pub fn new(path: &Path) -> io::Result<Self> {
        let file = OpenOptions::new().create(true).append(true).open(path)?;
        Ok(Self {
            writer: BufWriter::new(file),
        })
    }

    pub fn log(&mut self, entry: &OpLogEntry) -> io::Result<()> {
        let bytes = bincode::serialize(entry).map_err(|e| {
            io::Error::new(io::ErrorKind::Other, format!("Serialization error: {}", e))
        })?;
        let len = (bytes.len() as u32).to_le_bytes();
        self.writer.write_all(&len)?;
        self.writer.write_all(&bytes)?;
        self.writer.flush()
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
