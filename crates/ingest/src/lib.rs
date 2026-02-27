mod csv;
pub mod gen_pcap;
pub mod itch;
#[cfg(feature = "pcap")]
mod pcap_ingest;
#[cfg(not(feature = "pcap"))]
mod pcap_stub;
pub mod yahoo;

use md_core::{assign_sequences, Event, TickError, TickTable};
use std::path::Path;
use thiserror::Error;

pub use csv::{parse_csv_a, parse_csv_b, parse_csv_c};
#[cfg(feature = "pcap")]
pub use pcap_ingest::{ingest_pcap, ParseIssue, PcapIngestOutput};
#[cfg(not(feature = "pcap"))]
pub use pcap_stub::{ingest_pcap, ParseIssue, PcapIngestOutput};
pub use yahoo::ingest_yahoo;

#[derive(Debug, Error)]
pub enum IngestError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("csv error: {0}")]
    Csv(#[from] ::csv::Error),
    #[error("time parse error: {0}")]
    Time(#[from] chrono::ParseError),
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("json error: {0}")]
    Json(#[from] serde_json::Error),
    #[error("tick error: {0}")]
    Tick(#[from] TickError),
    #[cfg(feature = "pcap")]
    #[error("pcap error: {0}")]
    Pcap(#[from] pcap::Error),
    #[error("pcap support not enabled")]
    PcapUnavailable,
    #[error("parse error: {0}")]
    Parse(String),
}

pub fn ingest_csv_a(
    path: &Path,
    venue: &str,
    ticks: &TickTable,
) -> Result<Vec<Event>, IngestError> {
    let pending = parse_csv_a(path, venue, ticks)?;
    Ok(assign_sequences(pending))
}

pub fn ingest_csv_b(
    path: &Path,
    venue: &str,
    ticks: &TickTable,
) -> Result<Vec<Event>, IngestError> {
    let pending = parse_csv_b(path, venue, ticks)?;
    Ok(assign_sequences(pending))
}

pub fn ingest_csv_c(
    path: &Path,
    venue: &str,
    ticks: &TickTable,
) -> Result<Vec<Event>, IngestError> {
    let pending = parse_csv_c(path, venue, ticks)?;
    Ok(assign_sequences(pending))
}
