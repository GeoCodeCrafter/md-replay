use md_core::{Event, Payload};
use std::collections::{BTreeMap, VecDeque};

#[derive(Debug, Clone)]
pub struct FeatureConfig {
    pub mid_window: usize,
    pub ewma_alpha: f64,
    pub spread_threshold: i64,
    pub imbalance_threshold: f64,
    pub vol_threshold: f64,
}

impl Default for FeatureConfig {
    fn default() -> Self {
        Self {
            mid_window: 8,
            ewma_alpha: 0.2,
            spread_threshold: 25,
            imbalance_threshold: 0.7,
            vol_threshold: 0.03,
        }
    }
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

pub fn run_feature(events: &[Event], cfg: &FeatureConfig) -> Vec<String> {
    let mut state = BTreeMap::<String, BookState>::new();
    let mut out = Vec::new();

    for event in events {
        let st = state.entry(event.symbol.clone()).or_default();

        match &event.payload {
            Payload::Quote {
                bid_px,
                bid_sz,
                ask_px,
                ask_sz,
            } => {
                st.bid_px = *bid_px;
                st.bid_sz = *bid_sz;
                st.ask_px = *ask_px;
                st.ask_sz = *ask_sz;
            }
            Payload::Trade { .. } => {}
        }

        let mid = compute_mid(st, event, cfg.mid_window);
        let spread = if st.bid_px > 0 && st.ask_px > 0 {
            st.ask_px - st.bid_px
        } else {
            0
        };
        let imbalance = compute_imbalance(st);

        update_ewma(st, cfg, mid);
        let vol = st.ewma_var.sqrt();

        let rolling_mid = if st.mids.is_empty() {
            mid
        } else {
            st.mids.iter().sum::<f64>() / st.mids.len() as f64
        };

        let mut signals = Vec::new();
        if spread > cfg.spread_threshold {
            signals.push("spread");
        }
        if imbalance.abs() > cfg.imbalance_threshold {
            signals.push("imb");
        }
        if vol > cfg.vol_threshold {
            signals.push("vol");
        }

        if !signals.is_empty() {
            out.push(format!(
                "{} {} {} mid={:.6} spread={} imb={:.6} vol={:.6} signal={}",
                event.sequence,
                event.timestamp_ns,
                event.symbol,
                rolling_mid,
                spread,
                imbalance,
                vol,
                signals.join("|")
            ));
        }
    }

    out
}

fn compute_mid(st: &mut BookState, event: &Event, window: usize) -> f64 {
    let mid = if st.bid_px > 0 && st.ask_px > 0 {
        (st.bid_px as f64 + st.ask_px as f64) * 0.5
    } else {
        match &event.payload {
            Payload::Trade { price_ticks, .. } => *price_ticks as f64,
            _ => 0.0,
        }
    };

    if mid > 0.0 {
        st.mids.push_back(mid);
        if st.mids.len() > window.max(1) {
            st.mids.pop_front();
        }
    }
    mid
}

fn compute_imbalance(st: &BookState) -> f64 {
    let total = st.bid_sz + st.ask_sz;
    if total == 0 {
        0.0
    } else {
        (st.bid_sz - st.ask_sz) as f64 / total as f64
    }
}

fn update_ewma(st: &mut BookState, cfg: &FeatureConfig, mid: f64) {
    if mid <= 0.0 {
        return;
    }

    let prev = st.last_mid.replace(mid);
    let Some(prev_mid) = prev else {
        return;
    };
    if prev_mid <= 0.0 {
        return;
    }

    let ret = (mid / prev_mid).ln();
    st.ewma_var = cfg.ewma_alpha * ret * ret + (1.0 - cfg.ewma_alpha) * st.ewma_var;
}

#[cfg(test)]
mod tests {
    use super::*;
    use md_core::QuoteTicks;

    #[test]
    fn emits_signals() {
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
            Event::quote(
                2,
                2,
                "X",
                "AAPL",
                QuoteTicks {
                    bid_px: 100,
                    bid_sz: 90,
                    ask_px: 150,
                    ask_sz: 5,
                },
            ),
            Event::trade(3, 3, "X", "AAPL", 170, 10),
        ];
        let lines = run_feature(&events, &FeatureConfig::default());
        assert!(!lines.is_empty());
    }
}
