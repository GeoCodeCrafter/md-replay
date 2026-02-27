use crate::itch::{parse_message, MockItchMessage, Side};
use crate::IngestError;
use md_core::{assign_sequences, Event, Payload, PendingEvent};
use pcap::Capture;
use std::collections::HashMap;
use std::path::Path;
use tracing::warn;

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

#[derive(Debug, Default, Clone, Copy)]
struct TopBook {
    bid_px: i64,
    bid_sz: i64,
    ask_px: i64,
    ask_sz: i64,
}

pub fn ingest_pcap(path: &Path, venue: &str) -> Result<PcapIngestOutput, IngestError> {
    let mut cap = Capture::from_file(path)?;
    let mut pending = Vec::new();
    let mut issues = Vec::new();
    let mut books = HashMap::<String, TopBook>::new();
    let mut packet_index: u64 = 0;
    let mut ingest_order: u64 = 0;

    loop {
        let packet = match cap.next_packet() {
            Ok(packet) => packet,
            Err(pcap::Error::NoMorePackets) => break,
            Err(err) => return Err(IngestError::Pcap(err)),
        };
        packet_index += 1;

        let udp_payload = match extract_udp_payload(packet.data) {
            Ok(v) => v,
            Err((offset, detail)) => {
                issues.push(ParseIssue {
                    packet_index,
                    offset,
                    detail,
                });
                continue;
            }
        };

        match parse_message(udp_payload) {
            Ok(msg) => {
                ingest_order += 1;
                let evt = match msg {
                    MockItchMessage::Trade {
                        timestamp_ns,
                        symbol,
                        price_i64,
                        size_i64,
                    } => PendingEvent {
                        timestamp_ns,
                        venue: venue.to_string(),
                        symbol,
                        payload: Payload::Trade {
                            price_ticks: price_i64,
                            size: size_i64,
                        },
                        ingest_order,
                    },
                    MockItchMessage::AddOrder {
                        timestamp_ns,
                        symbol,
                        side,
                        price_i64,
                        size_i64,
                    } => {
                        let book = books.entry(symbol.clone()).or_default();
                        match side {
                            Side::Bid => {
                                book.bid_px = price_i64;
                                book.bid_sz = size_i64;
                            }
                            Side::Ask => {
                                book.ask_px = price_i64;
                                book.ask_sz = size_i64;
                            }
                        }
                        PendingEvent {
                            timestamp_ns,
                            venue: venue.to_string(),
                            symbol,
                            payload: Payload::Quote {
                                bid_px: book.bid_px,
                                bid_sz: book.bid_sz,
                                ask_px: book.ask_px,
                                ask_sz: book.ask_sz,
                            },
                            ingest_order,
                        }
                    }
                };
                pending.push(evt);
            }
            Err(err) => {
                issues.push(ParseIssue {
                    packet_index,
                    offset: err.offset,
                    detail: err.detail,
                });
            }
        }
    }

    for issue in &issues {
        warn!(
            packet = issue.packet_index,
            offset = issue.offset,
            detail = %issue.detail,
            "pcap parse error"
        );
    }

    Ok(PcapIngestOutput {
        events: assign_sequences(pending),
        issues,
    })
}

fn extract_udp_payload(data: &[u8]) -> Result<&[u8], (usize, String)> {
    if data.len() < 14 {
        return Err((0, String::from("short ethernet header")));
    }
    let ethertype = u16::from_be_bytes([data[12], data[13]]);
    if ethertype != 0x0800 {
        return Err((12, format!("unsupported ethertype 0x{ethertype:04x}")));
    }

    let ip_offset = 14;
    if data.len() < ip_offset + 20 {
        return Err((ip_offset, String::from("short ipv4 header")));
    }

    let version_ihl = data[ip_offset];
    let version = version_ihl >> 4;
    let ihl = (version_ihl & 0x0f) as usize * 4;
    if version != 4 {
        return Err((ip_offset, format!("unsupported ip version {version}")));
    }
    if ihl < 20 {
        return Err((ip_offset, String::from("invalid ipv4 ihl")));
    }
    if data.len() < ip_offset + ihl {
        return Err((ip_offset, String::from("truncated ipv4 header")));
    }

    let proto = data[ip_offset + 9];
    if proto != 17 {
        return Err((ip_offset + 9, format!("non-udp protocol {proto}")));
    }

    let udp_offset = ip_offset + ihl;
    if data.len() < udp_offset + 8 {
        return Err((udp_offset, String::from("short udp header")));
    }

    let udp_len = u16::from_be_bytes([data[udp_offset + 4], data[udp_offset + 5]]) as usize;
    if udp_len < 8 {
        return Err((udp_offset + 4, String::from("invalid udp length")));
    }
    if data.len() < udp_offset + udp_len {
        return Err((udp_offset + 4, String::from("truncated udp payload")));
    }

    Ok(&data[udp_offset + 8..udp_offset + udp_len])
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn rejects_short_ethernet() {
        let err = extract_udp_payload(&[1, 2, 3]).expect_err("must fail");
        assert_eq!(err.0, 0);
    }
}
