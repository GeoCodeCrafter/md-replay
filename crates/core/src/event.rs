use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EventType {
    Trade,
    Quote,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum Payload {
    Trade {
        price_ticks: i64,
        size: i64,
    },
    Quote {
        bid_px: i64,
        bid_sz: i64,
        ask_px: i64,
        ask_sz: i64,
    },
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub struct QuoteTicks {
    pub bid_px: i64,
    pub bid_sz: i64,
    pub ask_px: i64,
    pub ask_sz: i64,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct Event {
    pub timestamp_ns: u64,
    pub sequence: u64,
    pub venue: String,
    pub symbol: String,
    pub event_type: EventType,
    pub payload: Payload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PendingEvent {
    pub timestamp_ns: u64,
    pub venue: String,
    pub symbol: String,
    pub payload: Payload,
    pub ingest_order: u64,
}

impl PendingEvent {
    pub fn into_event(self, sequence: u64) -> Event {
        let event_type = match self.payload {
            Payload::Trade { .. } => EventType::Trade,
            Payload::Quote { .. } => EventType::Quote,
        };
        Event {
            timestamp_ns: self.timestamp_ns,
            sequence,
            venue: self.venue,
            symbol: self.symbol,
            event_type,
            payload: self.payload,
        }
    }
}

pub fn assign_sequences(mut pending: Vec<PendingEvent>) -> Vec<Event> {
    pending.sort_by(|a, b| {
        a.timestamp_ns
            .cmp(&b.timestamp_ns)
            .then_with(|| a.ingest_order.cmp(&b.ingest_order))
            .then_with(|| a.symbol.cmp(&b.symbol))
            .then_with(|| a.venue.cmp(&b.venue))
    });
    pending
        .into_iter()
        .enumerate()
        .map(|(i, item)| item.into_event((i + 1) as u64))
        .collect()
}

impl Event {
    pub fn trade(
        timestamp_ns: u64,
        sequence: u64,
        venue: impl Into<String>,
        symbol: impl Into<String>,
        price_ticks: i64,
        size: i64,
    ) -> Self {
        Self {
            timestamp_ns,
            sequence,
            venue: venue.into(),
            symbol: symbol.into(),
            event_type: EventType::Trade,
            payload: Payload::Trade { price_ticks, size },
        }
    }

    pub fn quote(
        timestamp_ns: u64,
        sequence: u64,
        venue: impl Into<String>,
        symbol: impl Into<String>,
        quote: QuoteTicks,
    ) -> Self {
        Self {
            timestamp_ns,
            sequence,
            venue: venue.into(),
            symbol: symbol.into(),
            event_type: EventType::Quote,
            payload: Payload::Quote {
                bid_px: quote.bid_px,
                bid_sz: quote.bid_sz,
                ask_px: quote.ask_px,
                ask_sz: quote.ask_sz,
            },
        }
    }
}
