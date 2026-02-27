use crate::pb;
use md_core::{Event, Payload};
use md_storage::{EventLogReader, IndexReader, StorageError};
use std::path::Path;
use std::time::Duration;
use thiserror::Error;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tonic::Status;

#[derive(Debug, Clone)]
pub struct ReplayConfig {
    pub from_ns: Option<u64>,
    pub to_ns: Option<u64>,
    pub speed: f64,
    pub max_speed: bool,
    pub step_mode: bool,
}

impl Default for ReplayConfig {
    fn default() -> Self {
        Self {
            from_ns: None,
            to_ns: None,
            speed: 1.0,
            max_speed: false,
            step_mode: false,
        }
    }
}

#[derive(Debug, Error)]
pub enum ReplayError {
    #[error("storage error: {0}")]
    Storage(#[from] StorageError),
    #[error("transport error: {0}")]
    Transport(#[from] tonic::transport::Error),
}

pub fn read_events(
    log_path: &Path,
    index_path: Option<&Path>,
    from_ns: Option<u64>,
    to_ns: Option<u64>,
) -> Result<Vec<Event>, ReplayError> {
    let mut reader = EventLogReader::open(log_path)?;
    match (from_ns, index_path) {
        (Some(from), Some(idx_path)) if idx_path.exists() => {
            let idx = IndexReader::open(idx_path)?;
            if let Some(offset) = idx.seek_offset(from) {
                reader.seek(offset)?;
            } else {
                reader.rewind_to_data()?;
            }
        }
        _ => {
            reader.rewind_to_data()?;
        }
    }

    let mut out = Vec::new();
    loop {
        let next = reader.next_record()?;
        let Some(record) = next else {
            break;
        };

        if let Some(to) = to_ns {
            if record.event.timestamp_ns > to {
                break;
            }
        }

        if let Some(from) = from_ns {
            if record.event.timestamp_ns < from {
                continue;
            }
        }

        out.push(record.event);
    }

    out.sort_by_key(|e| e.sequence);
    Ok(out)
}

pub async fn stream_with_pacing(
    events: Vec<Event>,
    config: ReplayConfig,
    tx: mpsc::Sender<Result<pb::EventMessage, Status>>,
) {
    let mut first_ts = None;
    let start = Instant::now();

    for event in events {
        if !config.max_speed {
            if config.step_mode {
                tokio::task::yield_now().await;
            } else {
                let baseline = first_ts.get_or_insert(event.timestamp_ns);
                let dt = event.timestamp_ns.saturating_sub(*baseline);
                let speed = if config.speed <= 0.0 {
                    1.0
                } else {
                    config.speed
                };
                let target = Duration::from_nanos((dt as f64 / speed) as u64);
                let deadline = start + target;
                tokio::time::sleep_until(deadline).await;
            }
        }

        if tx.send(Ok(to_proto(&event))).await.is_err() {
            break;
        }
    }
}

pub fn to_proto(event: &Event) -> pb::EventMessage {
    let payload = match &event.payload {
        Payload::Trade { price_ticks, size } => {
            Some(pb::event_message::Payload::Trade(pb::Trade {
                price_ticks: *price_ticks,
                size: *size,
            }))
        }
        Payload::Quote {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        } => Some(pb::event_message::Payload::Quote(pb::Quote {
            bid_px: *bid_px,
            bid_sz: *bid_sz,
            ask_px: *ask_px,
            ask_sz: *ask_sz,
        })),
    };

    pb::EventMessage {
        timestamp_ns: event.timestamp_ns,
        sequence: event.sequence,
        venue: event.venue.clone(),
        symbol: event.symbol.clone(),
        payload,
    }
}

pub fn from_proto(msg: &pb::EventMessage) -> Option<Event> {
    let payload = match &msg.payload {
        Some(pb::event_message::Payload::Trade(t)) => Payload::Trade {
            price_ticks: t.price_ticks,
            size: t.size,
        },
        Some(pb::event_message::Payload::Quote(q)) => Payload::Quote {
            bid_px: q.bid_px,
            bid_sz: q.bid_sz,
            ask_px: q.ask_px,
            ask_sz: q.ask_sz,
        },
        None => return None,
    };

    let event_type = match &payload {
        Payload::Trade { .. } => md_core::EventType::Trade,
        Payload::Quote { .. } => md_core::EventType::Quote,
    };

    Some(Event {
        timestamp_ns: msg.timestamp_ns,
        sequence: msg.sequence,
        venue: msg.venue.clone(),
        symbol: msg.symbol.clone(),
        event_type,
        payload,
    })
}
