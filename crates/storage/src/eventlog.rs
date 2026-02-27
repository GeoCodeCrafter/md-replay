use crate::StorageError;
use crc32fast::Hasher;
use md_core::Event;
use std::fs::File;
use std::io::{BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::path::Path;

const FILE_MAGIC: &[u8; 8] = b"MDELOG01";
const FILE_VERSION: u16 = 1;
const SCHEMA_DESC: &str = "event_v1";

#[derive(Debug, Clone)]
pub struct EventLogHeader {
    pub version: u16,
    pub schema_hash: u64,
    pub symbols: Vec<String>,
    pub data_offset: u64,
}

#[derive(Debug, Clone)]
pub struct ReadRecord {
    pub offset: u64,
    pub event: Event,
}

pub struct EventLogWriter {
    w: BufWriter<File>,
    offset: u64,
}

impl EventLogWriter {
    pub fn create(path: &Path, symbols: &[String], schema_hash: u64) -> Result<Self, StorageError> {
        let mut w = BufWriter::new(File::create(path)?);
        let mut offset = 0u64;

        w.write_all(FILE_MAGIC)?;
        offset += FILE_MAGIC.len() as u64;

        w.write_all(&FILE_VERSION.to_le_bytes())?;
        offset += 2;

        w.write_all(&schema_hash.to_le_bytes())?;
        offset += 8;

        w.write_all(&(symbols.len() as u32).to_le_bytes())?;
        offset += 4;

        for symbol in symbols {
            let bytes = symbol.as_bytes();
            if bytes.len() > u8::MAX as usize {
                return Err(StorageError::InvalidFormat(format!(
                    "symbol too long: {symbol}"
                )));
            }
            w.write_all(&[bytes.len() as u8])?;
            w.write_all(bytes)?;
            offset += 1 + bytes.len() as u64;
        }

        Ok(Self { w, offset })
    }

    pub fn append(&mut self, event: &Event) -> Result<u64, StorageError> {
        let payload = bincode::serialize(event)?;
        let len = payload.len() as u32;
        let crc = crc32fast::hash(&payload);
        let record_offset = self.offset;

        self.w.write_all(&len.to_le_bytes())?;
        self.w.write_all(&crc.to_le_bytes())?;
        self.w.write_all(&payload)?;

        self.offset += 8 + payload.len() as u64;
        Ok(record_offset)
    }

    pub fn flush(&mut self) -> Result<(), StorageError> {
        self.w.flush()?;
        Ok(())
    }
}

pub struct EventLogReader {
    r: BufReader<File>,
    header: EventLogHeader,
}

impl EventLogReader {
    pub fn open(path: &Path) -> Result<Self, StorageError> {
        let mut r = BufReader::new(File::open(path)?);
        let mut magic = [0u8; 8];
        r.read_exact(&mut magic)?;
        if &magic != FILE_MAGIC {
            return Err(StorageError::InvalidFormat(String::from("bad magic")));
        }

        let version = read_u16_le(&mut r)?;
        if version != FILE_VERSION {
            return Err(StorageError::InvalidFormat(format!(
                "unsupported version {version}"
            )));
        }

        let schema_hash = read_u64_le(&mut r)?;
        let symbol_count = read_u32_le(&mut r)? as usize;
        let mut symbols = Vec::with_capacity(symbol_count);
        for _ in 0..symbol_count {
            let mut len = [0u8; 1];
            r.read_exact(&mut len)?;
            let mut sym = vec![0u8; len[0] as usize];
            r.read_exact(&mut sym)?;
            symbols.push(
                String::from_utf8(sym)
                    .map_err(|_| StorageError::InvalidFormat(String::from("symbol utf8")))?,
            );
        }

        let data_offset = r.stream_position()?;
        let header = EventLogHeader {
            version,
            schema_hash,
            symbols,
            data_offset,
        };

        Ok(Self { r, header })
    }

    pub fn header(&self) -> &EventLogHeader {
        &self.header
    }

    pub fn seek(&mut self, offset: u64) -> Result<(), StorageError> {
        self.r.seek(SeekFrom::Start(offset))?;
        Ok(())
    }

    pub fn rewind_to_data(&mut self) -> Result<(), StorageError> {
        self.seek(self.header.data_offset)
    }

    pub fn next_record(&mut self) -> Result<Option<ReadRecord>, StorageError> {
        let offset = self.r.stream_position()?;

        let mut len_buf = [0u8; 4];
        match self.r.read_exact(&mut len_buf) {
            Ok(()) => {}
            Err(err) if err.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
            Err(err) => return Err(StorageError::Io(err)),
        }

        let len = u32::from_le_bytes(len_buf) as usize;
        let crc = read_u32_le(&mut self.r)?;
        let mut payload = vec![0u8; len];
        self.r.read_exact(&mut payload)?;

        let mut hasher = Hasher::new();
        hasher.update(&payload);
        if hasher.finalize() != crc {
            return Err(StorageError::CrcMismatch { offset });
        }

        let event = bincode::deserialize::<Event>(&payload)?;
        Ok(Some(ReadRecord { offset, event }))
    }
}

pub fn default_schema_hash() -> u64 {
    crc32fast::hash(SCHEMA_DESC.as_bytes()) as u64
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
    fn writes_and_reads_records() {
        let mut path = std::env::temp_dir();
        path.push(format!("md_replay_storage_{}.eventlog", std::process::id()));

        let symbols = vec![String::from("AAPL")];
        let mut writer =
            EventLogWriter::create(&path, &symbols, default_schema_hash()).expect("writer");
        let offset = writer
            .append(&Event::trade(1, 1, "X", "AAPL", 100, 2))
            .expect("append");
        writer.flush().expect("flush");

        let mut reader = EventLogReader::open(&path).expect("reader");
        let first = reader.next_record().expect("next").expect("record");
        assert_eq!(first.offset, offset);
        assert_eq!(first.event.sequence, 1);
    }

    #[test]
    fn crc_mismatch_is_detected() {
        let mut path = std::env::temp_dir();
        path.push(format!(
            "md_replay_storage_crc_{}.eventlog",
            std::process::id()
        ));

        let mut writer =
            EventLogWriter::create(&path, &[String::from("AAPL")], default_schema_hash())
                .expect("writer");
        writer
            .append(&Event::trade(1, 1, "X", "AAPL", 100, 2))
            .expect("append");
        writer.flush().expect("flush");

        let mut bytes = std::fs::read(&path).expect("read file");
        let last = bytes.len() - 1;
        bytes[last] ^= 0x55;
        std::fs::write(&path, bytes).expect("rewrite file");

        let mut reader = EventLogReader::open(&path).expect("open");
        let err = reader.next_record().expect_err("crc mismatch");
        match err {
            StorageError::CrcMismatch { .. } => {}
            _ => panic!("unexpected error"),
        }
    }
}
