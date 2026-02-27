use crate::IngestError;
use chrono::DateTime;
use md_core::{Payload, PendingEvent, TickTable};
use serde::Deserialize;
use std::path::Path;

#[derive(Debug, Deserialize)]
struct RowA {
    timestamp: String,
    symbol: String,
    bid_px: String,
    bid_sz: i64,
    ask_px: String,
    ask_sz: i64,
}

#[derive(Debug, Deserialize)]
struct RowB {
    timestamp_ms: u64,
    symbol: String,
    price: String,
    size: i64,
}

#[derive(Debug, Deserialize)]
struct RowC {
    timestamp: String,
    symbol: String,
    r#type: String,
    #[serde(default)]
    price: String,
    #[serde(default)]
    size: String,
    #[serde(default)]
    bid_px: String,
    #[serde(default)]
    bid_sz: String,
    #[serde(default)]
    ask_px: String,
    #[serde(default)]
    ask_sz: String,
}

pub fn parse_csv_a(
    path: &Path,
    venue: &str,
    ticks: &TickTable,
) -> Result<Vec<PendingEvent>, IngestError> {
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;
    let mut out = Vec::new();
    for (idx, row) in rdr.deserialize::<RowA>().enumerate() {
        let row = row?;
        let ts = parse_rfc3339_ns(&row.timestamp)?;
        let bid_px = ticks.price_str_to_ticks(&row.symbol, &row.bid_px)?;
        let ask_px = ticks.price_str_to_ticks(&row.symbol, &row.ask_px)?;
        out.push(PendingEvent {
            timestamp_ns: ts,
            venue: venue.to_string(),
            symbol: row.symbol,
            payload: Payload::Quote {
                bid_px,
                bid_sz: row.bid_sz,
                ask_px,
                ask_sz: row.ask_sz,
            },
            ingest_order: idx as u64,
        });
    }
    Ok(out)
}

pub fn parse_csv_b(
    path: &Path,
    venue: &str,
    ticks: &TickTable,
) -> Result<Vec<PendingEvent>, IngestError> {
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;
    let mut out = Vec::new();
    for (idx, row) in rdr.deserialize::<RowB>().enumerate() {
        let row = row?;
        let ts = row
            .timestamp_ms
            .checked_mul(1_000_000)
            .ok_or_else(|| IngestError::Parse(format!("timestamp overflow at row {}", idx + 1)))?;
        let price_ticks = ticks.price_str_to_ticks(&row.symbol, &row.price)?;
        out.push(PendingEvent {
            timestamp_ns: ts,
            venue: venue.to_string(),
            symbol: row.symbol,
            payload: Payload::Trade {
                price_ticks,
                size: row.size,
            },
            ingest_order: idx as u64,
        });
    }
    Ok(out)
}

pub fn parse_csv_c(
    path: &Path,
    venue: &str,
    ticks: &TickTable,
) -> Result<Vec<PendingEvent>, IngestError> {
    let mut rdr = csv::ReaderBuilder::new()
        .trim(csv::Trim::All)
        .from_path(path)?;
    let mut out = Vec::new();
    for (idx, row) in rdr.deserialize::<RowC>().enumerate() {
        let row = row?;
        let ts = parse_mixed_ts_ns(&row.timestamp)?;
        let payload = match row.r#type.as_str() {
            "trade" | "Trade" | "TRADE" => {
                let price_ticks = ticks.price_str_to_ticks(&row.symbol, &row.price)?;
                let size = parse_i64_or_zero(&row.size)?;
                Payload::Trade { price_ticks, size }
            }
            "quote" | "Quote" | "QUOTE" => {
                let bid_px = ticks.price_str_to_ticks(&row.symbol, &row.bid_px)?;
                let ask_px = ticks.price_str_to_ticks(&row.symbol, &row.ask_px)?;
                let bid_sz = parse_i64_or_zero(&row.bid_sz)?;
                let ask_sz = parse_i64_or_zero(&row.ask_sz)?;
                Payload::Quote {
                    bid_px,
                    bid_sz,
                    ask_px,
                    ask_sz,
                }
            }
            other => {
                return Err(IngestError::Parse(format!(
                    "unknown row type '{other}' at row {}",
                    idx + 1
                )))
            }
        };
        out.push(PendingEvent {
            timestamp_ns: ts,
            venue: venue.to_string(),
            symbol: row.symbol,
            payload,
            ingest_order: idx as u64,
        });
    }
    Ok(out)
}

fn parse_rfc3339_ns(raw: &str) -> Result<u64, IngestError> {
    let dt = DateTime::parse_from_rfc3339(raw)?;
    let ns = dt
        .timestamp_nanos_opt()
        .ok_or_else(|| IngestError::Parse(format!("timestamp out of range: {raw}")))?;
    u64::try_from(ns).map_err(|_| IngestError::Parse(format!("negative timestamp: {raw}")))
}

fn parse_mixed_ts_ns(raw: &str) -> Result<u64, IngestError> {
    if raw.contains('T') {
        return parse_rfc3339_ns(raw);
    }
    let value = raw
        .parse::<u64>()
        .map_err(|_| IngestError::Parse(format!("invalid timestamp: {raw}")))?;
    value
        .checked_mul(1_000_000)
        .ok_or_else(|| IngestError::Parse(format!("timestamp overflow: {raw}")))
}

fn parse_i64_or_zero(raw: &str) -> Result<i64, IngestError> {
    let v = raw.trim();
    if v.is_empty() {
        return Ok(0);
    }
    v.parse::<i64>()
        .map_err(|_| IngestError::Parse(format!("invalid integer: {raw}")))
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::TickTable;
    use rust_decimal::Decimal;
    use std::fs::File;
    use std::io::Write;

    fn write_temp(content: &str, suffix: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let ts = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock")
            .as_nanos();
        path.push(format!(
            "md_replay_ingest_{}_{}_{}.csv",
            suffix,
            std::process::id(),
            ts
        ));
        let mut file = File::create(&path).expect("create temp csv");
        file.write_all(content.as_bytes()).expect("write temp csv");
        path
    }

    #[test]
    fn csv_a_parses_quote() {
        let path = write_temp(
            "timestamp,symbol,bid_px,bid_sz,ask_px,ask_sz\n2024-01-02T10:00:00Z,AAPL,100.00,10,100.01,11\n",
            "a",
        );
        let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
        let events = parse_csv_a(&path, "X", &ticks).expect("parse csv a");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            Payload::Quote { bid_px, ask_px, .. } => {
                assert_eq!((*bid_px, *ask_px), (10000, 10001));
            }
            _ => panic!("expected quote"),
        }
    }

    #[test]
    fn csv_b_parses_trade() {
        let path = write_temp(
            "timestamp_ms,symbol,price,size\n1700000000000,MSFT,200.10,5\n",
            "b",
        );
        let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
        let events = parse_csv_b(&path, "X", &ticks).expect("parse csv b");
        assert_eq!(events.len(), 1);
        match &events[0].payload {
            Payload::Trade { price_ticks, size } => {
                assert_eq!((*price_ticks, *size), (20010, 5));
            }
            _ => panic!("expected trade"),
        }
    }

    #[test]
    fn csv_c_handles_mixed() {
        let path = write_temp(
            "timestamp,symbol,type,price,size,bid_px,bid_sz,ask_px,ask_sz\n1700000000000,AAPL,trade,100.00,4,,, ,\n2024-01-02T10:00:00Z,AAPL,quote,,,99.99,8,100.01,9\n",
            "c",
        );
        let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
        let events = parse_csv_c(&path, "X", &ticks).expect("parse csv c");
        assert_eq!(events.len(), 2);
    }
}
