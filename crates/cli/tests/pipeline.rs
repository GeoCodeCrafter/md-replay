use md_clients::{format_event, verify_feature_determinism};
#[cfg(feature = "pcap")]
use md_clients::{run_feature, FeatureConfig};
use md_core::TickTable;
#[cfg(feature = "pcap")]
use md_ingest::gen_pcap::generate_pcap;
use md_ingest::ingest_csv_a;
#[cfg(feature = "pcap")]
use md_ingest::ingest_pcap;
use md_replay_engine::read_events;
use md_storage::{default_schema_hash, EventLogWriter, IndexWriter};
use rust_decimal::Decimal;
use std::collections::BTreeSet;
use std::path::{Path, PathBuf};
use tempfile::tempdir;

#[test]
fn csv_to_replay_matches_golden() {
    let dir = tempdir().expect("tempdir");
    let csv_path = dir.path().join("a.csv");
    std::fs::write(
        &csv_path,
        "timestamp,symbol,bid_px,bid_sz,ask_px,ask_sz\n2024-01-02T10:00:00Z,AAPL,100.00,10,100.02,11\n",
    )
    .expect("write csv");

    let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
    let events = ingest_csv_a(&csv_path, "X", &ticks).expect("ingest csv");
    let log_path = dir.path().join("norm.eventlog");
    let idx_path = write_log(&events, &log_path);

    let replayed = read_events(&log_path, Some(&idx_path), None, None).expect("read events");
    let lines = replayed
        .iter()
        .map(format_event)
        .collect::<Vec<_>>()
        .join("\n");
    let golden = "1 1704189600000000000 X AAPL quote bid=10000x10 ask=10002x11";
    assert_eq!(lines, golden);
}

#[test]
#[cfg(feature = "pcap")]
fn pcap_ingest_replay_is_deterministic() {
    let dir = tempdir().expect("tempdir");
    let pcap_path = dir.path().join("sample.pcap");
    generate_pcap(
        &pcap_path,
        &[String::from("AAPL"), String::from("MSFT")],
        200,
        7,
    )
    .expect("generate pcap");

    let out = ingest_pcap(&pcap_path, "X").expect("ingest pcap");
    assert!(!out.events.is_empty());
    assert!(!out.issues.is_empty());

    let log_path = dir.path().join("norm.eventlog");
    let idx_path = write_log(&out.events, &log_path);

    let run1 = read_events(&log_path, Some(&idx_path), None, None).expect("read events");
    let run2 = read_events(&log_path, Some(&idx_path), None, None).expect("read events");
    let f1 = run_feature(&run1, &FeatureConfig::default());
    let f2 = run_feature(&run2, &FeatureConfig::default());
    assert_eq!(f1, f2);
}

#[test]
fn verify_twice_same_bytes() {
    let dir = tempdir().expect("tempdir");
    let csv_path = dir.path().join("a.csv");
    std::fs::write(
        &csv_path,
        "timestamp,symbol,bid_px,bid_sz,ask_px,ask_sz\n2024-01-02T10:00:00Z,AAPL,100.00,10,100.04,11\n",
    )
    .expect("write csv");

    let ticks = TickTable::uniform(Decimal::new(1, 2)).expect("tick table");
    let events = ingest_csv_a(&csv_path, "X", &ticks).expect("ingest csv");
    let log_path = dir.path().join("norm.eventlog");
    let idx_path = write_log(&events, &log_path);

    let out1 = dir.path().join("v1.txt");
    let out2 = dir.path().join("v2.txt");
    verify_feature_determinism(&log_path, Some(&idx_path), 42, &out1).expect("verify 1");
    verify_feature_determinism(&log_path, Some(&idx_path), 42, &out2).expect("verify 2");
    let b1 = std::fs::read(&out1).expect("read out1");
    let b2 = std::fs::read(&out2).expect("read out2");
    assert_eq!(b1, b2);
}

fn write_log(events: &[md_core::Event], log_path: &Path) -> PathBuf {
    let mut symbols = BTreeSet::new();
    for e in events {
        symbols.insert(e.symbol.clone());
    }
    let symbols = symbols.into_iter().collect::<Vec<_>>();

    let mut writer =
        EventLogWriter::create(log_path, &symbols, default_schema_hash()).expect("eventlog writer");
    let idx_path = PathBuf::from(format!("{}.idx", log_path.display()));
    let mut idx = IndexWriter::create(&idx_path, 16).expect("index writer");

    for e in events {
        let offset = writer.append(e).expect("append");
        idx.maybe_add(e, offset).expect("index add");
    }
    writer.flush().expect("flush writer");
    idx.flush().expect("flush index");
    idx_path
}
