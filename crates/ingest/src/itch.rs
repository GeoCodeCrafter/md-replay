use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Side {
    Bid,
    Ask,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum MockItchMessage {
    AddOrder {
        timestamp_ns: u64,
        symbol: String,
        side: Side,
        price_i64: i64,
        size_i64: i64,
    },
    Trade {
        timestamp_ns: u64,
        symbol: String,
        price_i64: i64,
        size_i64: i64,
    },
}

#[derive(Debug, Clone, Error, PartialEq, Eq)]
#[error("{detail} at byte offset {offset}")]
pub struct ItchParseError {
    pub offset: usize,
    pub detail: String,
}

pub fn parse_message(payload: &[u8]) -> Result<MockItchMessage, ItchParseError> {
    let mut r = Reader::new(payload);
    let timestamp_ns = r.read_u64_be(0)?;
    let msg_type = r.read_u32_be(8)?;
    match msg_type {
        1 => {
            let symbol = r.read_symbol(12)?;
            let side = match r.read_u8(20)? {
                0 => Side::Bid,
                1 => Side::Ask,
                other => {
                    return Err(ItchParseError {
                        offset: 20,
                        detail: format!("invalid side {other}"),
                    })
                }
            };
            let price_i64 = r.read_i64_be(21)?;
            let size_i64 = r.read_i64_be(29)?;
            if r.remaining() != 0 {
                return Err(ItchParseError {
                    offset: r.offset,
                    detail: String::from("trailing bytes"),
                });
            }
            Ok(MockItchMessage::AddOrder {
                timestamp_ns,
                symbol,
                side,
                price_i64,
                size_i64,
            })
        }
        2 => {
            let symbol = r.read_symbol(12)?;
            let price_i64 = r.read_i64_be(20)?;
            let size_i64 = r.read_i64_be(28)?;
            if r.remaining() != 0 {
                return Err(ItchParseError {
                    offset: r.offset,
                    detail: String::from("trailing bytes"),
                });
            }
            Ok(MockItchMessage::Trade {
                timestamp_ns,
                symbol,
                price_i64,
                size_i64,
            })
        }
        other => Err(ItchParseError {
            offset: 8,
            detail: format!("unknown message type {other}"),
        }),
    }
}

struct Reader<'a> {
    data: &'a [u8],
    offset: usize,
}

impl<'a> Reader<'a> {
    fn new(data: &'a [u8]) -> Self {
        Self { data, offset: 0 }
    }

    fn remaining(&self) -> usize {
        self.data.len().saturating_sub(self.offset)
    }

    fn take(&mut self, len: usize, field_offset: usize) -> Result<&'a [u8], ItchParseError> {
        let end = self.offset.checked_add(len).ok_or_else(|| ItchParseError {
            offset: field_offset,
            detail: String::from("offset overflow"),
        })?;
        if end > self.data.len() {
            return Err(ItchParseError {
                offset: field_offset,
                detail: format!("short packet need {} bytes", len),
            });
        }
        let out = &self.data[self.offset..end];
        self.offset = end;
        Ok(out)
    }

    fn read_u8(&mut self, field_offset: usize) -> Result<u8, ItchParseError> {
        Ok(self.take(1, field_offset)?[0])
    }

    fn read_u32_be(&mut self, field_offset: usize) -> Result<u32, ItchParseError> {
        let bytes = self.take(4, field_offset)?;
        Ok(u32::from_be_bytes(
            bytes.try_into().expect("len checked for u32"),
        ))
    }

    fn read_u64_be(&mut self, field_offset: usize) -> Result<u64, ItchParseError> {
        let bytes = self.take(8, field_offset)?;
        Ok(u64::from_be_bytes(
            bytes.try_into().expect("len checked for u64"),
        ))
    }

    fn read_i64_be(&mut self, field_offset: usize) -> Result<i64, ItchParseError> {
        let bytes = self.take(8, field_offset)?;
        Ok(i64::from_be_bytes(
            bytes.try_into().expect("len checked for i64"),
        ))
    }

    fn read_symbol(&mut self, field_offset: usize) -> Result<String, ItchParseError> {
        let bytes = self.take(8, field_offset)?;
        if !bytes.is_ascii() {
            return Err(ItchParseError {
                offset: field_offset,
                detail: String::from("symbol is not valid ASCII"),
            });
        }
        let symbol = std::str::from_utf8(bytes).map_err(|_| ItchParseError {
            offset: field_offset,
            detail: String::from("symbol is not valid ASCII"),
        })?;
        Ok(symbol.trim_end_matches([' ', '\0']).to_string())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use proptest::prelude::*;

    #[test]
    fn parse_trade() {
        let mut msg = Vec::new();
        msg.extend_from_slice(&123u64.to_be_bytes());
        msg.extend_from_slice(&2u32.to_be_bytes());
        msg.extend_from_slice(b"AAPL    ");
        msg.extend_from_slice(&100i64.to_be_bytes());
        msg.extend_from_slice(&7i64.to_be_bytes());
        let parsed = parse_message(&msg).expect("parse trade");
        assert_eq!(
            parsed,
            MockItchMessage::Trade {
                timestamp_ns: 123,
                symbol: String::from("AAPL"),
                price_i64: 100,
                size_i64: 7,
            }
        );
    }

    #[test]
    fn parse_add_order() {
        let mut msg = Vec::new();
        msg.extend_from_slice(&123u64.to_be_bytes());
        msg.extend_from_slice(&1u32.to_be_bytes());
        msg.extend_from_slice(b"MSFT    ");
        msg.push(0);
        msg.extend_from_slice(&200i64.to_be_bytes());
        msg.extend_from_slice(&9i64.to_be_bytes());
        let parsed = parse_message(&msg).expect("parse add order");
        assert_eq!(
            parsed,
            MockItchMessage::AddOrder {
                timestamp_ns: 123,
                symbol: String::from("MSFT"),
                side: Side::Bid,
                price_i64: 200,
                size_i64: 9,
            }
        );
    }

    proptest! {
        #[test]
        fn fuzz_payload_no_panic(data: Vec<u8>) {
            let _ = parse_message(&data);
        }
    }
}
