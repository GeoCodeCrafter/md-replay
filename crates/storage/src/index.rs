use crate::StorageError;
use md_core::Event;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Write};
use std::path::Path;

const IDX_MAGIC: &[u8; 8] = b"MDEIDX01";
const IDX_VERSION: u16 = 1;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub timestamp_ns: u64,
    pub sequence: u64,
    pub byte_offset: u64,
}

pub struct IndexWriter {
    w: BufWriter<File>,
    stride: u32,
    seen: u64,
}

impl IndexWriter {
    pub fn create(path: &Path, stride: u32) -> Result<Self, StorageError> {
        if stride == 0 {
            return Err(StorageError::InvalidFormat(String::from(
                "index stride must be > 0",
            )));
        }
        let mut w = BufWriter::new(File::create(path)?);
        w.write_all(IDX_MAGIC)?;
        w.write_all(&IDX_VERSION.to_le_bytes())?;
        w.write_all(&stride.to_le_bytes())?;
        Ok(Self { w, stride, seen: 0 })
    }

    pub fn maybe_add(&mut self, event: &Event, offset: u64) -> Result<(), StorageError> {
        if self.seen.is_multiple_of(self.stride as u64) {
            self.w.write_all(&event.timestamp_ns.to_le_bytes())?;
            self.w.write_all(&event.sequence.to_le_bytes())?;
            self.w.write_all(&offset.to_le_bytes())?;
        }
        self.seen += 1;
        Ok(())
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.w.flush()?;
        Ok(())
    }
}

#[derive(Debug, Clone)]
pub struct IndexReader {
    stride: u32,
    entries: Vec<IndexEntry>,
}

impl IndexReader {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut r = BufReader::new(File::open(path)?);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != IDX_MAGIC {
            return Err(StorageError::InvalidFormat(String::from("bad index magic")));
        }

        let version = read_u16_le(&mut r)?;
        if version != IDX_VERSION {
            return Err(StorageError::InvalidFormat(format!(
                "unsupported index version {version}"
            )));
        }

        let stride = read_u32_le(&mut r)?;
        let mut entries = Vec::new();
        loop {
            let mut ts_buf = [0u8; 8];
            match r.read_exact(&mut ts_buf) {
                Ok(()) => {}
                Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => break,
                Err(err) => return Err(StorageError::Io(err)),
            }
            let timestamp_ns = u64::from_le_bytes(ts_buf);
            let sequence = read_u64_le(&mut r)?;
            let byte_offset = read_u64_le(&mut r)?;
            entries.push(IndexEntry {
                timestamp_ns,
                sequence,
                byte_offset,
            });
        }

        Ok(Self { stride, entries })
    }

    pub fn stride(&self) -> u32 {
        self.stride
    }

    pub fn entries(&self) -> &[IndexEntry] {
        &self.entries
    }

    pub fn seek_offset(&self, from_ns: u64) -> Option<u64> {
        if self.entries.is_empty() {
            return None;
        }
        let idx = self.entries.partition_point(|e| e.timestamp_ns <= from_ns);
        if idx == 0 {
            Some(self.entries[0].byte_offset)
        } else {
            Some(self.entries[idx - 1].byte_offset)
        }
    }
}

fn read_u16_le<R: Read>(r: &mut R) -> Result<u16, StorageError> {
    let mut buf = [0u8; 2];
    r.read_exact(&mut buf)?;
    Ok(u16::from_le_bytes(buf))
}

fn read_u32_le<R: Read>(r: &mut R) -> Result<u32, StorageError> {
    let mut buf = [0u8; 4];
    r.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn read_u64_le<R: Read>(r: &mut R) -> Result<u64, StorageError> {
    let mut buf = [0u8; 8];
    r.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn seek_finds_prior_offset() {
        let mut path = std::env::temp_dir();
        path.push(format!("md_replay_idx_{}.idx", std::process::id()));

        let mut w = IndexWriter::create(&path, 2).expect("writer");
        let events = vec![
            Event::trade(100, 1, "X", "AAPL", 1, 1),
            Event::trade(200, 2, "X", "AAPL", 1, 1),
            Event::trade(300, 3, "X", "AAPL", 1, 1),
            Event::trade(400, 4, "X", "AAPL", 1, 1),
        ];
        for (i, ev) in events.iter().enumerate() {
            w.maybe_add(ev, (i as u64) * 100).expect("index write");
        }
        w.flush().expect("flush");

        let idx = IndexReader::open(&path).expect("index open");
        assert_eq!(idx.seek_offset(50), Some(0));
        assert_eq!(idx.seek_offset(250), Some(0));
        assert_eq!(idx.seek_offset(350), Some(200));
    }
}
