use rust_decimal::prelude::ToPrimitive;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Deserialize;
use std::collections::HashMap;
use std::str::FromStr;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum TickError {
    #[error("invalid decimal: {0}")]
    InvalidDecimal(String),
    #[error("tick size must be positive")]
    NonPositiveTick,
    #[error("tick conversion overflow")]
    Overflow,
    #[error("tick config parse failed: {0}")]
    ConfigParse(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct TickConfigFile {
    pub default_tick: String,
    #[serde(default)]
    pub symbols: HashMap<String, String>,
}

#[derive(Debug, Clone)]
pub struct TickTable {
    default_tick: Decimal,
    symbols: HashMap<String, Decimal>,
}

impl TickTable {
    pub fn from_config(config: TickConfigFile) -> Result<Self, TickError> {
        let default_tick = parse_positive_decimal(&config.default_tick)?;
        let mut symbols = HashMap::with_capacity(config.symbols.len());
        for (sym, raw_tick) in config.symbols {
            symbols.insert(sym, parse_positive_decimal(&raw_tick)?);
        }
        Ok(Self {
            default_tick,
            symbols,
        })
    }

    pub fn from_toml_str(raw: &str) -> Result<Self, TickError> {
        let parsed: TickConfigFile =
            toml::from_str(raw).map_err(|e| TickError::ConfigParse(e.to_string()))?;
        Self::from_config(parsed)
    }

    pub fn uniform(tick_size: Decimal) -> Result<Self, TickError> {
        if tick_size <= Decimal::ZERO {
            return Err(TickError::NonPositiveTick);
        }
        Ok(Self {
            default_tick: tick_size,
            symbols: HashMap::new(),
        })
    }

    pub fn tick_for(&self, symbol: &str) -> Decimal {
        self.symbols
            .get(symbol)
            .copied()
            .unwrap_or(self.default_tick)
    }

    pub fn price_str_to_ticks(&self, symbol: &str, price: &str) -> Result<i64, TickError> {
        let px = Decimal::from_str(price).map_err(|_| TickError::InvalidDecimal(price.into()))?;
        self.price_to_ticks(symbol, px)
    }

    pub fn price_to_ticks(&self, symbol: &str, price: Decimal) -> Result<i64, TickError> {
        let tick = self.tick_for(symbol);
        if tick <= Decimal::ZERO {
            return Err(TickError::NonPositiveTick);
        }
        let ratio = price / tick;
        let rounded = ratio.round_dp_with_strategy(0, RoundingStrategy::MidpointAwayFromZero);
        rounded.to_i64().ok_or(TickError::Overflow)
    }

    pub fn ticks_to_price(&self, symbol: &str, ticks: i64) -> Decimal {
        Decimal::from(ticks) * self.tick_for(symbol)
    }
}

fn parse_positive_decimal(input: &str) -> Result<Decimal, TickError> {
    let v = Decimal::from_str(input).map_err(|_| TickError::InvalidDecimal(input.into()))?;
    if v <= Decimal::ZERO {
        return Err(TickError::NonPositiveTick);
    }
    Ok(v)
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal::Decimal;
    use std::collections::HashMap;

    #[test]
    fn rounds_half_away_from_zero() {
        let table = TickTable::uniform(Decimal::new(5, 2)).expect("tick table");
        assert_eq!(
            table.price_str_to_ticks("AAPL", "1.025").expect("ticks"),
            21
        );
        assert_eq!(
            table.price_str_to_ticks("AAPL", "1.024").expect("ticks"),
            20
        );
        assert_eq!(
            table.price_str_to_ticks("AAPL", "-1.025").expect("ticks"),
            -21
        );
    }

    #[test]
    fn symbol_override_works() {
        let cfg = TickConfigFile {
            default_tick: "0.01".into(),
            symbols: HashMap::from([(String::from("MSFT"), String::from("0.05"))]),
        };
        let table = TickTable::from_config(cfg).expect("tick table");
        assert_eq!(
            table.price_str_to_ticks("AAPL", "100.01").expect("ticks"),
            10001
        );
        assert_eq!(
            table.price_str_to_ticks("MSFT", "100.01").expect("ticks"),
            2000
        );
    }
}
