use rand::prelude::*;
use rand_chacha::ChaCha8Rng;
use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use thiserror::Error;

#[derive(Debug, Error)]
pub enum GenPcapError {
    #[error("io error: {0}")]
    Io(#[from] std::io::Error),
    #[error("symbols list is empty")]
    EmptySymbols,
}

pub fn generate_pcap(
    out: &Path,
    symbols: &[String],
    events: usize,
    seed: u64,
) -> Result<(), GenPcapError> {
    if symbols.is_empty() {
        return Err(GenPcapError::EmptySymbols);
    }

    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    let file = File::create(out)?;
    let mut w = BufWriter::new(file);

    write_global_header(&mut w)?;

    let mut ts_ns = 1_700_000_000_000_000_000u64;
    for i in 0..events {
        ts_ns = ts_ns.saturating_add(rng.gen_range(200u64..5_000u64));
        if i % 97 == 0 {
            ts_ns = ts_ns.saturating_sub(rng.gen_range(1_000u64..40_000u64));
        }

        let symbol = &symbols[rng.gen_range(0..symbols.len())];
        let malformed = i % 137 == 0;
        let payload = if malformed {
            malformed_payload(&mut rng)
        } else if rng.gen_bool(0.55) {
            add_order_payload(
                ts_ns,
                symbol,
                if rng.gen_bool(0.5) { 0 } else { 1 },
                rng.gen_range(10_000i64..50_000i64),
                rng.gen_range(1i64..500i64),
            )
        } else {
            trade_payload(
                ts_ns,
                symbol,
                rng.gen_range(10_000i64..50_000i64),
                rng.gen_range(1i64..500i64),
            )
        };

        let frame = build_udp_frame(i as u16, &payload);
        write_packet(&mut w, ts_ns, &frame)?;
    }

    w.flush()?;
    Ok(())
}

fn write_global_header<W: Write>(w: &mut W) -> Result<(), std::io::Error> {
    w.write_all(&0xa1b2c3d4u32.to_le_bytes())?;
    w.write_all(&2u16.to_le_bytes())?;
    w.write_all(&4u16.to_le_bytes())?;
    w.write_all(&0i32.to_le_bytes())?;
    w.write_all(&0u32.to_le_bytes())?;
    w.write_all(&65_535u32.to_le_bytes())?;
    w.write_all(&1u32.to_le_bytes())?;
    Ok(())
}

fn write_packet<W: Write>(w: &mut W, ts_ns: u64, data: &[u8]) -> Result<(), std::io::Error> {
    let ts_sec = (ts_ns / 1_000_000_000) as u32;
    let ts_usec = ((ts_ns % 1_000_000_000) / 1_000) as u32;
    let len = data.len() as u32;
    w.write_all(&ts_sec.to_le_bytes())?;
    w.write_all(&ts_usec.to_le_bytes())?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(&len.to_le_bytes())?;
    w.write_all(data)?;
    Ok(())
}

fn add_order_payload(ts_ns: u64, symbol: &str, side: u8, price: i64, size: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(37);
    v.extend_from_slice(&ts_ns.to_be_bytes());
    v.extend_from_slice(&1u32.to_be_bytes());
    v.extend_from_slice(&pack_symbol(symbol));
    v.push(side);
    v.extend_from_slice(&price.to_be_bytes());
    v.extend_from_slice(&size.to_be_bytes());
    v
}

fn trade_payload(ts_ns: u64, symbol: &str, price: i64, size: i64) -> Vec<u8> {
    let mut v = Vec::with_capacity(36);
    v.extend_from_slice(&ts_ns.to_be_bytes());
    v.extend_from_slice(&2u32.to_be_bytes());
    v.extend_from_slice(&pack_symbol(symbol));
    v.extend_from_slice(&price.to_be_bytes());
    v.extend_from_slice(&size.to_be_bytes());
    v
}

fn malformed_payload(rng: &mut ChaCha8Rng) -> Vec<u8> {
    let len = rng.gen_range(1usize..16usize);
    let mut data = vec![0u8; len];
    rng.fill_bytes(&mut data);
    data
}

fn pack_symbol(symbol: &str) -> [u8; 8] {
    let mut out = [b' '; 8];
    let src = symbol.as_bytes();
    let n = src.len().min(8);
    out[..n].copy_from_slice(&src[..n]);
    out
}

fn build_udp_frame(ident: u16, payload: &[u8]) -> Vec<u8> {
    let eth_len = 14usize;
    let ip_len = 20usize;
    let udp_len = 8usize;
    let total_ip = (ip_len + udp_len + payload.len()) as u16;
    let total = eth_len + ip_len + udp_len + payload.len();

    let mut frame = vec![0u8; total];
    frame[..6].copy_from_slice(&[0x01, 0x00, 0x5e, 0x01, 0x02, 0x03]);
    frame[6..12].copy_from_slice(&[0x02, 0x00, 0x00, 0x00, 0x00, 0x01]);
    frame[12..14].copy_from_slice(&0x0800u16.to_be_bytes());

    let ip = 14;
    frame[ip] = 0x45;
    frame[ip + 1] = 0;
    frame[ip + 2..ip + 4].copy_from_slice(&total_ip.to_be_bytes());
    frame[ip + 4..ip + 6].copy_from_slice(&ident.to_be_bytes());
    frame[ip + 6..ip + 8].copy_from_slice(&0x4000u16.to_be_bytes());
    frame[ip + 8] = 64;
    frame[ip + 9] = 17;
    frame[ip + 10..ip + 12].copy_from_slice(&0u16.to_be_bytes());
    frame[ip + 12..ip + 16].copy_from_slice(&[10, 1, 1, 1]);
    frame[ip + 16..ip + 20].copy_from_slice(&[239, 1, 2, 3]);
    let csum = ipv4_checksum(&frame[ip..ip + ip_len]);
    frame[ip + 10..ip + 12].copy_from_slice(&csum.to_be_bytes());

    let udp = ip + ip_len;
    frame[udp..udp + 2].copy_from_slice(&40_000u16.to_be_bytes());
    frame[udp + 2..udp + 4].copy_from_slice(&50_000u16.to_be_bytes());
    frame[udp + 4..udp + 6].copy_from_slice(&((udp_len + payload.len()) as u16).to_be_bytes());
    frame[udp + 6..udp + 8].copy_from_slice(&0u16.to_be_bytes());

    frame[udp + udp_len..].copy_from_slice(payload);
    frame
}

fn ipv4_checksum(header: &[u8]) -> u16 {
    let mut sum: u32 = 0;
    let mut i = 0;
    while i + 1 < header.len() {
        sum += u16::from_be_bytes([header[i], header[i + 1]]) as u32;
        i += 2;
    }
    while (sum >> 16) != 0 {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    !(sum as u16)
}
