use anyhow::{anyhow, Result};
use axum::extract::{Query, State};
use axum::response::Html;
use axum::routing::get;
use axum::{Json, Router};
use md_clients::{format_event, run_feature, FeatureConfig};
use md_core::{Event, Payload};
use md_replay_engine::read_events;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, VecDeque};
use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;

const INDEX_HTML: &str = include_str!("ui/index.html");

#[derive(Clone)]
struct UiState {
    events: Arc<Vec<Event>>,
    compare_events: Option<Arc<Vec<Event>>>,
    meta: Meta,
}

#[derive(Debug, Clone, Serialize)]
struct Meta {
    events: usize,
    trades: usize,
    quotes: usize,
    first_timestamp_ns: u64,
    last_timestamp_ns: u64,
    first_sequence: u64,
    last_sequence: u64,
    symbols: Vec<String>,
}

#[derive(Debug, Clone, Deserialize, Default)]
struct DataQuery {
    symbol: Option<String>,
    from_seq: Option<u64>,
    to_seq: Option<u64>,
    limit: Option<usize>,
}

#[derive(Debug, Serialize)]
struct EventRow {
    timestamp_ns: u64,
    sequence: u64,
    venue: String,
    symbol: String,
    kind: &'static str,
    price_ticks: Option<i64>,
    size: Option<i64>,
    bid_px: Option<i64>,
    bid_sz: Option<i64>,
    ask_px: Option<i64>,
    ask_sz: Option<i64>,
}

#[derive(Debug, Serialize)]
struct EventBatch {
    rows: Vec<EventRow>,
}

#[derive(Debug, Serialize)]
struct SeriesPoint {
    sequence: u64,
    timestamp_ns: u64,
    symbol: String,
    mid: f64,
    spread: i64,
    imbalance: f64,
    vol: f64,
    signal: Option<String>,
}

#[derive(Debug, Serialize)]
struct DiffReport {
    determinism: DeterminismReport,
    parser: Option<ParserDiffReport>,
}

#[derive(Debug, Serialize)]
struct DeterminismReport {
    ok: bool,
    lines: usize,
    first_mismatch_line: Option<usize>,
}

#[derive(Debug, Serialize)]
struct ParserDiffReport {
    ok: bool,
    left_events: usize,
    right_events: usize,
    matched_prefix: usize,
    first_mismatch: Option<ParserMismatch>,
}

#[derive(Debug, Serialize)]
struct ParserMismatch {
    index: usize,
    left_sequence: Option<u64>,
    right_sequence: Option<u64>,
    reason: String,
    left_line: Option<String>,
    right_line: Option<String>,
}

#[derive(Debug, Clone)]
struct BookState {
    bid_px: i64,
    bid_sz: i64,
    ask_px: i64,
    ask_sz: i64,
    mids: VecDeque<f64>,
    last_mid: Option<f64>,
    ewma_var: f64,
}

impl Default for BookState {
    fn default() -> Self {
        Self {
            bid_px: 0,
            bid_sz: 0,
            ask_px: 0,
            ask_sz: 0,
            mids: VecDeque::new(),
            last_mid: None,
            ewma_var: 0.0,
        }
    }
}

pub async fn serve_ui(
    log: PathBuf,
    index: Option<PathBuf>,
    compare_log: Option<PathBuf>,
    compare_index: Option<PathBuf>,
    from_ns: Option<u64>,
    to_ns: Option<u64>,
    addr: SocketAddr,
) -> Result<()> {
    let events = read_events(&log, index.as_deref(), from_ns, to_ns)?;
    if events.is_empty() {
        return Err(anyhow!("no events loaded from {}", log.display()));
    }

    let compare_events = match compare_log {
        Some(path) => Some(Arc::new(read_events(
            &path,
            compare_index.as_deref(),
            from_ns,
            to_ns,
        )?)),
        None => None,
    };

    let state = UiState {
        meta: build_meta(&events),
        events: Arc::new(events),
        compare_events,
    };

    let app = Router::new()
        .route("/", get(index_page))
        .route("/api/meta", get(meta_page))
        .route("/api/events", get(events_page))
        .route("/api/series", get(series_page))
        .route("/api/diff", get(diff_page))
        .with_state(state);

    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, app).await?;
    Ok(())
}

async fn index_page() -> Html<&'static str> {
    Html(INDEX_HTML)
}

async fn meta_page(State(state): State<UiState>) -> Json<Meta> {
    Json(state.meta)
}

async fn events_page(
    State(state): State<UiState>,
    Query(query): Query<DataQuery>,
) -> Json<EventBatch> {
    let rows = select_events(&state.events, &query, 500)
        .into_iter()
        .map(to_row)
        .collect::<Vec<_>>();
    Json(EventBatch { rows })
}

async fn series_page(
    State(state): State<UiState>,
    Query(query): Query<DataQuery>,
) -> Json<Vec<SeriesPoint>> {
    let events = select_events(&state.events, &query, 3000);
    Json(compute_series(&events, &FeatureConfig::default()))
}

async fn diff_page(
    State(state): State<UiState>,
    Query(query): Query<DataQuery>,
) -> Json<DiffReport> {
    let base = select_events(&state.events, &query, 10_000);
    let determinism = deterministic_report(&base);
    let parser = state
        .compare_events
        .as_ref()
        .map(|other| parser_diff(&base, &select_events(other, &query, 10_000)));
    Json(DiffReport {
        determinism,
        parser,
    })
}

fn select_events(events: &[Event], query: &DataQuery, fallback_limit: usize) -> Vec<Event> {
    let mut out = Vec::new();
    let limit = query.limit.unwrap_or(fallback_limit).clamp(1, 100_000);

    for event in events {
        if query.from_seq.is_some_and(|from| event.sequence < from) {
            continue;
        }
        if query.to_seq.is_some_and(|to| event.sequence > to) {
            break;
        }
        if query
            .symbol
            .as_deref()
            .is_some_and(|sym| !event.symbol.eq_ignore_ascii_case(sym))
        {
            continue;
        }
        out.push(event.clone());
        if out.len() == limit {
            break;
        }
    }

    out
}

fn build_meta(events: &[Event]) -> Meta {
    let mut symbols = BTreeSet::new();
    let mut trades = 0usize;
    let mut quotes = 0usize;

    for event in events {
        symbols.insert(event.symbol.clone());
        match &event.payload {
            Payload::Trade { .. } => trades += 1,
            Payload::Quote { .. } => quotes += 1,
        }
    }

    let first = events.first();
    let last = events.last();
    Meta {
        events: events.len(),
        trades,
        quotes,
        first_timestamp_ns: first.map_or(0, |e| e.timestamp_ns),
        last_timestamp_ns: last.map_or(0, |e| e.timestamp_ns),
        first_sequence: first.map_or(0, |e| e.sequence),
        last_sequence: last.map_or(0, |e| e.sequence),
        symbols: symbols.into_iter().collect(),
    }
}

fn to_row(event: Event) -> EventRow {
    match event.payload {
        Payload::Trade { price_ticks, size } => EventRow {
            timestamp_ns: event.timestamp_ns,
            sequence: event.sequence,
            venue: event.venue,
            symbol: event.symbol,
            kind: "trade",
            price_ticks: Some(price_ticks),
            size: Some(size),
            bid_px: None,
            bid_sz: None,
            ask_px: None,
            ask_sz: None,
        },
        Payload::Quote {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        } => EventRow {
            timestamp_ns: event.timestamp_ns,
            sequence: event.sequence,
            venue: event.venue,
            symbol: event.symbol,
            kind: "quote",
            price_ticks: None,
            size: None,
            bid_px: Some(bid_px),
            bid_sz: Some(bid_sz),
            ask_px: Some(ask_px),
            ask_sz: Some(ask_sz),
        },
    }
}

fn compute_series(events: &[Event], cfg: &FeatureConfig) -> Vec<SeriesPoint> {
    let mut st = std::collections::BTreeMap::<String, BookState>::new();
    let mut out = Vec::with_capacity(events.len());

    for event in events {
        let book = st.entry(event.symbol.clone()).or_default();
        match &event.payload {
            Payload::Quote {
                bid_px,
                bid_sz,
                ask_px,
                ask_sz,
            } => {
                book.bid_px = *bid_px;
                book.bid_sz = *bid_sz;
                book.ask_px = *ask_px;
                book.ask_sz = *ask_sz;
            }
            Payload::Trade { .. } => {}
        }

        let mid = if book.bid_px > 0 && book.ask_px > 0 {
            (book.bid_px as f64 + book.ask_px as f64) * 0.5
        } else {
            match &event.payload {
                Payload::Trade { price_ticks, .. } => *price_ticks as f64,
                Payload::Quote { .. } => 0.0,
            }
        };

        if mid > 0.0 {
            book.mids.push_back(mid);
            if book.mids.len() > cfg.mid_window.max(1) {
                book.mids.pop_front();
            }
        }

        let rolling_mid = if book.mids.is_empty() {
            mid
        } else {
            book.mids.iter().sum::<f64>() / book.mids.len() as f64
        };

        let spread = if book.bid_px > 0 && book.ask_px > 0 {
            book.ask_px - book.bid_px
        } else {
            0
        };
        let total = book.bid_sz + book.ask_sz;
        let imbalance = if total == 0 {
            0.0
        } else {
            (book.bid_sz - book.ask_sz) as f64 / total as f64
        };

        if mid > 0.0 {
            if let Some(last) = book.last_mid.replace(mid) {
                if last > 0.0 {
                    let ret = (mid / last).ln();
                    book.ewma_var =
                        cfg.ewma_alpha * ret * ret + (1.0 - cfg.ewma_alpha) * book.ewma_var;
                }
            }
        }
        let vol = book.ewma_var.sqrt();

        let mut tags = Vec::new();
        if spread > cfg.spread_threshold {
            tags.push("spread");
        }
        if imbalance.abs() > cfg.imbalance_threshold {
            tags.push("imb");
        }
        if vol > cfg.vol_threshold {
            tags.push("vol");
        }

        out.push(SeriesPoint {
            sequence: event.sequence,
            timestamp_ns: event.timestamp_ns,
            symbol: event.symbol.clone(),
            mid: rolling_mid,
            spread,
            imbalance,
            vol,
            signal: if tags.is_empty() {
                None
            } else {
                Some(tags.join("|"))
            },
        });
    }

    out
}

fn deterministic_report(events: &[Event]) -> DeterminismReport {
    let cfg = FeatureConfig::default();
    let run1 = run_feature(events, &cfg);
    let run2 = run_feature(events, &cfg);
    let first_mismatch = run1
        .iter()
        .zip(run2.iter())
        .position(|(a, b)| a != b)
        .map(|i| i + 1)
        .or_else(|| {
            if run1.len() == run2.len() {
                None
            } else {
                Some(run1.len().min(run2.len()) + 1)
            }
        });

    DeterminismReport {
        ok: first_mismatch.is_none(),
        lines: run1.len(),
        first_mismatch_line: first_mismatch,
    }
}

fn parser_diff(left: &[Event], right: &[Event]) -> ParserDiffReport {
    let max = left.len().max(right.len());
    let mut matched_prefix = 0usize;
    let mut first_mismatch = None;

    for i in 0..max {
        let l = left.get(i);
        let r = right.get(i);
        let same = match (l, r) {
            (Some(a), Some(b)) => a == b,
            (None, None) => true,
            _ => false,
        };
        if same {
            matched_prefix += 1;
            continue;
        }

        let reason = match (l, r) {
            (None, Some(_)) => String::from("left missing event"),
            (Some(_), None) => String::from("right missing event"),
            (Some(a), Some(b)) => mismatch_reason(a, b),
            (None, None) => String::from("unknown mismatch"),
        };

        first_mismatch = Some(ParserMismatch {
            index: i + 1,
            left_sequence: l.map(|e| e.sequence),
            right_sequence: r.map(|e| e.sequence),
            reason,
            left_line: l.map(format_event),
            right_line: r.map(format_event),
        });
        break;
    }

    ParserDiffReport {
        ok: first_mismatch.is_none() && left.len() == right.len(),
        left_events: left.len(),
        right_events: right.len(),
        matched_prefix,
        first_mismatch,
    }
}

fn mismatch_reason(left: &Event, right: &Event) -> String {
    if left.sequence != right.sequence {
        return String::from("sequence mismatch");
    }
    if left.timestamp_ns != right.timestamp_ns {
        return String::from("timestamp mismatch");
    }
    if left.symbol != right.symbol {
        return String::from("symbol mismatch");
    }
    if left.venue != right.venue {
        return String::from("venue mismatch");
    }
    if !left.payload.eq(&right.payload) {
        return String::from("payload mismatch");
    }
    String::from("event mismatch")
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::QuoteTicks;

    #[test]
    fn series_marks_signals() {
        let events = vec![
            Event::quote(
                1,
                1,
                "X",
                "AAPL",
                QuoteTicks {
                    bid_px: 100,
                    bid_sz: 90,
                    ask_px: 140,
                    ask_sz: 10,
                },
            ),
            Event::trade(2, 2, "X", "AAPL", 150, 4),
        ];
        let out = compute_series(&events, &FeatureConfig::default());
        assert_eq!(out.len(), 2);
        assert!(out[0].signal.is_some());
    }

    #[test]
    fn parser_diff_detects_change() {
        let left = vec![Event::trade(1, 1, "X", "AAPL", 100, 1)];
        let right = vec![Event::trade(1, 1, "X", "AAPL", 101, 1)];
        let diff = parser_diff(&left, &right);
        assert!(!diff.ok);
        assert!(diff.first_mismatch.is_some());
    }
}
