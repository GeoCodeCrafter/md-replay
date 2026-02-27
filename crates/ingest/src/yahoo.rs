use crate::IngestError;
use md_core::{assign_sequences, Event, Payload, PendingEvent, TickTable};
use reqwest::{Client, Url};
use serde::Deserialize;

const BASE_URL: &str = "https://query1.finance.yahoo.com/v8/finance/chart";

pub async fn ingest_yahoo(
    symbols: &[String],
    venue: &str,
    ticks: &TickTable,
    interval: &str,
    range: &str,
) -> Result<Vec<Event>, IngestError> {
    if symbols.is_empty() {
        return Err(IngestError::Parse(String::from("empty symbols list")));
    }
    let client = Client::builder().user_agent("md-replay/0.1").build()?;
    let mut pending = Vec::new();
    let mut ingest_order = 0u64;

    for symbol in symbols {
        let raw = fetch_symbol_chart(&client, symbol, interval, range).await?;
        let mut items = parse_symbol_payload(&raw, symbol, venue, ticks, ingest_order)?;
        ingest_order += items.len() as u64;
        pending.append(&mut items);
    }

    if pending.is_empty() {
        return Err(IngestError::Parse(String::from("no events returned")));
    }
    Ok(assign_sequences(pending))
}

async fn fetch_symbol_chart(
    client: &Client,
    symbol: &str,
    interval: &str,
    range: &str,
) -> Result<String, IngestError> {
    let mut url = Url::parse(BASE_URL).map_err(|e| IngestError::Parse(e.to_string()))?;
    url.path_segments_mut()
        .map_err(|_| IngestError::Parse(String::from("invalid yahoo url")))?
        .push(symbol);
    url.query_pairs_mut()
        .append_pair("interval", interval)
        .append_pair("range", range)
        .append_pair("includePrePost", "false")
        .append_pair("events", "history");

    let response = client.get(url).send().await?.error_for_status()?;
    response.text().await.map_err(IngestError::from)
}

fn parse_symbol_payload(
    raw: &str,
    symbol: &str,
    venue: &str,
    ticks: &TickTable,
    ingest_order_start: u64,
) -> Result<Vec<PendingEvent>, IngestError> {
    let payload: ChartEnvelope = serde_json::from_str(raw)?;
    if let Some(err) = payload.chart.error {
        let msg = err
            .description
            .unwrap_or_else(|| String::from("upstream error"));
        return Err(IngestError::Parse(format!("{symbol}: {msg}")));
    }

    let result = payload
        .chart
        .result
        .and_then(|list| list.into_iter().next())
        .ok_or_else(|| IngestError::Parse(format!("{symbol}: missing chart result")))?;
    let timestamps = result.timestamp.unwrap_or_default();
    let quote = result
        .indicators
        .quote
        .and_then(|list| list.into_iter().next())
        .ok_or_else(|| IngestError::Parse(format!("{symbol}: missing quote payload")))?;

    let mut out = Vec::new();
    for (idx, ts) in timestamps.into_iter().enumerate() {
        let ts = match u64::try_from(ts) {
            Ok(v) => v,
            Err(_) => continue,
        };
        let timestamp_ns = match ts.checked_mul(1_000_000_000) {
            Some(v) => v,
            None => continue,
        };

        let volume = value_i64_at(&quote.volume, idx).unwrap_or(1).max(1);

        if let Some(close) = value_f64_at(&quote.close, idx) {
            let price_ticks = f64_to_ticks(ticks, symbol, close)?;
            out.push(PendingEvent {
                timestamp_ns,
                venue: venue.to_string(),
                symbol: symbol.to_string(),
                payload: Payload::Trade {
                    price_ticks,
                    size: volume,
                },
                ingest_order: ingest_order_start + out.len() as u64,
            });
        }

        if let (Some(low), Some(high)) = (
            value_f64_at(&quote.low, idx),
            value_f64_at(&quote.high, idx),
        ) {
            let bid_px = f64_to_ticks(ticks, symbol, low.min(high))?;
            let ask_px = f64_to_ticks(ticks, symbol, high.max(low))?;
            out.push(PendingEvent {
                timestamp_ns,
                venue: venue.to_string(),
                symbol: symbol.to_string(),
                payload: Payload::Quote {
                    bid_px,
                    bid_sz: volume,
                    ask_px,
                    ask_sz: volume,
                },
                ingest_order: ingest_order_start + out.len() as u64,
            });
        }
    }

    Ok(out)
}

fn value_f64_at(series: &Option<Vec<Option<f64>>>, index: usize) -> Option<f64> {
    let v = series.as_ref()?.get(index).copied().flatten()?;
    if v.is_finite() {
        Some(v)
    } else {
        None
    }
}

fn value_i64_at(series: &Option<Vec<Option<i64>>>, index: usize) -> Option<i64> {
    series.as_ref()?.get(index).copied().flatten()
}

fn f64_to_ticks(ticks: &TickTable, symbol: &str, value: f64) -> Result<i64, IngestError> {
    if !value.is_finite() {
        return Err(IngestError::Parse(format!(
            "{symbol}: non-finite price {value}"
        )));
    }
    ticks
        .price_str_to_ticks(symbol, &format!("{value:.10}"))
        .map_err(IngestError::from)
}

#[derive(Debug, Deserialize)]
struct ChartEnvelope {
    chart: ChartPayload,
}

#[derive(Debug, Deserialize)]
struct ChartPayload {
    result: Option<Vec<ChartResult>>,
    error: Option<ChartError>,
}

#[derive(Debug, Deserialize)]
struct ChartError {
    description: Option<String>,
}

#[derive(Debug, Deserialize)]
struct ChartResult {
    timestamp: Option<Vec<i64>>,
    indicators: ChartIndicators,
}

#[derive(Debug, Deserialize)]
struct ChartIndicators {
    quote: Option<Vec<QuoteSet>>,
}

#[derive(Debug, Deserialize)]
struct QuoteSet {
    close: Option<Vec<Option<f64>>>,
    high: Option<Vec<Option<f64>>>,
    low: Option<Vec<Option<f64>>>,
    volume: Option<Vec<Option<i64>>>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::TickTable;
    use rust_decimal::Decimal;

    #[test]
    fn parses_trade_and_quote() {
        let raw = r#"{
          "chart": {
            "result": [{
              "timestamp": [1700000000],
              "indicators": {
                "quote": [{
                  "close": [101.25],
                  "high": [101.40],
                  "low": [101.10],
                  "volume": [12]
                }]
              }
            }],
            "error": null
          }
        }"#;
        let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
        let events = parse_symbol_payload(raw, "AAPL", "X", &ticks, 0).expect("parse");
        assert_eq!(events.len(), 2);
        match events[0].payload {
            Payload::Trade { price_ticks, size } => {
                assert_eq!(price_ticks, 10125);
                assert_eq!(size, 12);
            }
            _ => panic!("expected trade"),
        }
        match events[1].payload {
            Payload::Quote {
                bid_px,
                ask_px,
                bid_sz,
                ask_sz,
            } => {
                assert_eq!(bid_px, 10110);
                assert_eq!(ask_px, 10140);
                assert_eq!(bid_sz, 12);
                assert_eq!(ask_sz, 12);
            }
            _ => panic!("expected quote"),
        }
    }

    #[test]
    fn skips_missing_points() {
        let raw = r#"{
          "chart": {
            "result": [{
              "timestamp": [1700000000, 1700000060],
              "indicators": {
                "quote": [{
                  "close": [null, 99.99],
                  "high": [null, 100.01],
                  "low": [null, 99.90],
                  "volume": [null, 5]
                }]
              }
            }],
            "error": null
          }
        }"#;
        let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
        let events = parse_symbol_payload(raw, "MSFT", "X", &ticks, 0).expect("parse");
        assert_eq!(events.len(), 2);
    }
}
