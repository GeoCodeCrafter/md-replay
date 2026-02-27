use md_core::{Event, Payload};

pub fn format_event(event: &Event) -> String {
    match &event.payload {
        Payload::Trade { price_ticks, size } => format!(
            "{} {} {} {} trade px={} sz={}",
            event.sequence, event.timestamp_ns, event.venue, event.symbol, price_ticks, size
        ),
        Payload::Quote {
            bid_px,
            bid_sz,
            ask_px,
            ask_sz,
        } => format!(
            "{} {} {} {} quote bid={}x{} ask={}x{}",
            event.sequence,
            event.timestamp_ns,
            event.venue,
            event.symbol,
            bid_px,
            bid_sz,
            ask_px,
            ask_sz
        ),
    }
}
