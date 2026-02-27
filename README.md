# md-replay

`md-replay` normalizes market data from multiple input formats into a canonical append-only event log, then replays those events deterministically through a gRPC stream.

It includes:
- canonical trade/quote event model
- CSV adapters (`csv_a`, `csv_b`, `csv_c`)
- real-data adapter (`yahoo` chart API)
- PCAP adapter for mocked multicast ITCH-style binary payloads
- append-only log + stride index
- deterministic replay server
- local browser UI
- printer client and feature client
- determinism verification harness
- replay benchmark command

## Repository layout

```text
.
├── crates
│   ├── core
│   ├── ingest
│   ├── storage
│   ├── replay
│   ├── clients
│   └── cli
├── configs
├── data
├── tests
└── .github/workflows/ci.yml
```

## Architecture

```text
       +-------------------+
       |  CSV_A / CSV_B /  |
       |   CSV_C adapters  |
       +---------+---------+
                 |
       +---------v---------+       +-------------------------+
       | PCAP adapter      |<------+ gen-pcap synthetic feed |
       | (UDP + mock ITCH) |       +-------------------------+
       +---------+---------+
                 |
                 v
       +-------------------+
       | Canonical Event   |
       | timestamp, seq,   |
       | venue, symbol,    |
       | trade/quote       |
       +---------+---------+
                 |
                 v
       +-------------------+      +-------------------+
       | eventlog writer   +----->| index writer      |
       | len + crc + bytes |      | ts, seq, offset   |
       +---------+---------+      +---------+---------+
                 |                          |
                 +------------+-------------+
                              v
                    +------------------+
                    | replay engine    |
                    | indexed seek +   |
                    | deterministic    |
                    | pacing           |
                    +--------+---------+
                             |
                             v
                    +------------------+
                    | gRPC stream      |
                    +--------+---------+
                             |
                    +--------+--------+
                    | printer / feature|
                    +------------------+
```

## Build

```bash
cargo build --workspace
```

## Data samples

- `data/sample_csv_a.csv`
- `data/sample_csv_b.csv`
- `data/sample_csv_c.csv`
- `configs/ticks.toml`

Generate a synthetic PCAP:

```bash
md-replay gen-pcap --out data/sample.pcap --symbols AAPL,MSFT --events 10000 --seed 42
```

## Ingestion

CSV A (ISO8601 quotes):

```bash
md-replay ingest-csv-a \
  --input data/sample_csv_a.csv \
  --venue X \
  --out data/norm.eventlog \
  --tick-config configs/ticks.toml
```

CSV B (epoch ms trades):

```bash
md-replay ingest-csv-b \
  --input data/sample_csv_b.csv \
  --venue X \
  --out data/norm.eventlog \
  --tick-config configs/ticks.toml
```

CSV C (mixed `type` column):

```bash
md-replay ingest-csv-c \
  --input data/sample_csv_c.csv \
  --venue X \
  --out data/norm.eventlog \
  --tick-config configs/ticks.toml
```

PCAP (mock ITCH):

```bash
md-replay ingest-pcap \
  --pcap data/sample.pcap \
  --schema mock_itch \
  --venue X \
  --out data/norm.eventlog
```

On Windows, this command needs Npcap runtime + Npcap SDK (`wpcap.lib`):

```powershell
# optional if SDK is in a non-default path
$env:NPCAP_SDK_DIR="C:\\Program Files\\Npcap SDK"
```

If `wpcap.lib` is still missing, install Npcap SDK and reopen the shell.

Build/run with PCAP enabled:

```bash
cargo run -p md-replay --features pcap -- ingest-pcap --pcap data/sample.pcap --schema mock_itch --venue X --out data/norm.eventlog
```

Real market data (Yahoo chart API):

```bash
md-replay ingest-real \
  --provider yahoo \
  --symbols AAPL,MSFT \
  --range 1d \
  --interval 1m \
  --venue X \
  --out data/real.eventlog \
  --tick-config configs/ticks.toml
```

Ingestion writes:
- `data/norm.eventlog`
- `data/norm.eventlog.idx`

## Replay server

```bash
md-replay serve \
  --log data/norm.eventlog \
  --speed 10x \
  --from 1704199200000000000 \
  --to 1704199201000000000 \
  --addr 127.0.0.1:50051
```

`--max-speed` disables sleeping and streams as fast as possible.

## GUI

Start the local dashboard:

```bash
md-replay ui --log data/real.eventlog --addr 127.0.0.1:8080
```

Optional parser-regression comparison against another normalized log:

```bash
md-replay ui \
  --log data/real.eventlog \
  --compare-log data/baseline.eventlog \
  --addr 127.0.0.1:8080
```

Then open:

```text
http://127.0.0.1:8080
```

Dashboard features:
- playback controls (`play`, `pause`, `step`, speed multiplier)
- live table updates during playback
- rolling charts for midprice, spread, imbalance, EWMA volatility
- signal markers (`spread`, `imb`, `vol`)
- determinism and parser-diff status panel

Backend endpoints:
- `GET /api/meta`
- `GET /api/events?symbol=&from_seq=&to_seq=&limit=`
- `GET /api/series?symbol=&from_seq=&to_seq=&limit=`
- `GET /api/diff?symbol=&from_seq=&to_seq=&limit=`

## Clients

Printer:

```bash
md-replay print --log data/norm.eventlog
```

Feature client:

```bash
md-replay feature --log data/norm.eventlog --seed 42 --out data/signals.log
```

Verification harness:

```bash
md-replay verify --log data/norm.eventlog --client feature --seed 42 --out data/verify.out
```

The verify command runs the feature pipeline twice and compares output bytes.

## Benchmark

```bash
md-replay bench --log data/norm.eventlog
```

Output:
- events/sec
- p99 replay latency (ns)
- parse throughput (events/sec)

## Design tradeoffs

- Event payload serialization uses `bincode` for compactness and speed; schema version + schema hash are kept in file header for compatibility checks.
- Log/index are separate files. Index can be rebuilt or tuned with a different stride without rewriting event payloads.
- PCAP adapter parses Ethernet/IPv4/UDP and then mock ITCH payload; malformed packets are skipped and reported with packet index + byte offset.
- Replay uses timestamp pacing scaled by `speed`. At `--max-speed`, pacing is disabled and order is still sequence-driven.

## Deterministic guarantees

- Normalization assigns global sequence numbers after stable ordering by `(timestamp_ns, ingest_order, symbol, venue)`.
- Replay emits events in sequence order only.
- Determinism checks compare exact byte output from two independent runs.
- Feature pipeline uses deterministic per-symbol state in `BTreeMap` and seed-controlled config.

## Performance notes

- IO paths use buffered readers/writers.
- Storage record framing is append-only with CRC32 per record.
- Index seek reduces replay startup cost for bounded windows.
- Hot path keeps allocations low by reusing simple in-memory state and integer tick prices.

## Test

```bash
cargo test --workspace
```

CI runs:
- `cargo fmt --all --check`
- `cargo clippy --workspace --all-targets -- -D warnings`
- `cargo test --workspace`

## Shortcuts (PowerShell)

```powershell
.\scripts\demo.ps1
```

```powershell
.\scripts\serve.ps1
```

```powershell
.\scripts\stream.ps1
```

```powershell
.\scripts\gui.ps1
```
