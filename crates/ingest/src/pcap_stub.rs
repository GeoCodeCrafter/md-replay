use crate::IngestError;
use md_core::Event;
use std::path::Path;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ParseIssue {
    pub packet_index: u64,
    pub offset: usize,
    pub detail: String,
}

#[derive(Debug, Clone)]
pub struct PcapIngestOutput {
    pub events: Vec<Event>,
    pub issues: Vec<ParseIssue>,
}

pub fn ingest_pcap(_path: &Path, _venue: &str) -> Result<PcapIngestOutput, IngestError> {
    Err(IngestError::PcapUnavailable)
}
