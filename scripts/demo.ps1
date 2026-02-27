param(
    [string]$Symbols = "AAPL,MSFT",
    [int]$Events = 10000,
    [int]$Seed = 42,
    [string]$Venue = "X",
    [string]$Pcap = "data/sample.pcap",
    [string]$Log = "data/norm.eventlog",
    [switch]$CsvFallback
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

if ($CsvFallback) {
    cargo run -p md-replay -- ingest-csv-c --input data/sample_csv_c.csv --venue $Venue --out $Log --tick-config configs/ticks.toml
} else {
    cargo run -p md-replay --features pcap -- gen-pcap --out $Pcap --symbols $Symbols --events $Events --seed $Seed
    cargo run -p md-replay --features pcap -- ingest-pcap --pcap $Pcap --schema mock_itch --venue $Venue --out $Log
}

cargo run -p md-replay -- bench --log $Log
cargo run -p md-replay -- verify --log $Log --client feature --seed $Seed --out data/verify.out
cargo run -p md-replay -- print --log $Log
