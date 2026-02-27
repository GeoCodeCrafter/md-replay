pub mod eventlog;
pub mod index;

pub use eventlog::{
    default_schema_hash, EventLogHeader, EventLogReader, EventLogWriter, ReadRecord,
};
pub use index::{IndexEntry, IndexReader, IndexWriter};

use thiserror::Error;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("serialize error: {0}")]
    Serialize(#[from] bincode::Error),
    #[error("crc mismatch at offset {offset}")]
    CrcMismatch { offset: u64 },
    #[error("invalid file format: {0}")]
    InvalidFormat(String),
}
