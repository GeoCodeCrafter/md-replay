mod gui;

use anyhow::{anyhow, Context, Result};
use clap::{Args, Parser, Subcommand};
use md_clients::{format_event, run_feature, verify_feature_determinism, FeatureConfig};
use md_core::TickTable;
use md_ingest::gen_pcap::generate_pcap;
use md_ingest::{ingest_csv_a, ingest_csv_b, ingest_csv_c, ingest_pcap, ingest_yahoo};
use md_replay_engine::{read_events, serve_grpc, ReplayConfig};
use md_storage::{default_schema_hash, EventLogReader, EventLogWriter, IndexWriter};
use rand::Rng;
use rand::SeedableRng;
use rand_chacha::ChaCha8Rng;
use std::collections::BTreeSet;
use std::hint::black_box;
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::time::Instant;
use tracing::info;
use tracing_subscriber::EnvFilter;

#[derive(Parser)]
#[command(name = "md-replay")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    IngestCsvA(IngestCsvArgs),
    IngestCsvB(IngestCsvArgs),
    IngestCsvC(IngestCsvArgs),
    IngestReal(IngestRealArgs),
    IngestPcap(IngestPcapArgs),
    GenPcap(GenPcapArgs),
    Serve(ServeArgs),
    Ui(UiArgs),
    Print(ReadArgs),
    Feature(FeatureArgs),
    Verify(VerifyArgs),
    Bench(BenchArgs),
}

#[derive(Args)]
struct IngestCsvArgs {
    #[arg(long)]
    input: PathBuf,
    #[arg(long)]
    venue: String,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = 1024)]
    index_stride: u32,
    #[arg(long)]
    tick_config: Option<PathBuf>,
}

#[derive(Args)]
struct IngestPcapArgs {
    #[arg(long)]
    pcap: PathBuf,
    #[arg(long)]
    schema: String,
    #[arg(long)]
    venue: String,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = 1024)]
    index_stride: u32,
}

#[derive(Args)]
struct IngestRealArgs {
    #[arg(long, default_value = "yahoo")]
    provider: String,
    #[arg(long)]
    symbols: String,
    #[arg(long, default_value = "1d")]
    range: String,
    #[arg(long, default_value = "1m")]
    interval: String,
    #[arg(long)]
    venue: String,
    #[arg(long)]
    out: PathBuf,
    #[arg(long, default_value_t = 1024)]
    index_stride: u32,
    #[arg(long)]
    tick_config: Option<PathBuf>,
}

#[derive(Args)]
struct GenPcapArgs {
    #[arg(long)]
    out: PathBuf,
    #[arg(long)]
    symbols: String,
    #[arg(long)]
    events: usize,
    #[arg(long, default_value_t = 42)]
    seed: u64,
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long)]
    log: PathBuf,
    #[arg(long)]
    index: Option<PathBuf>,
    #[arg(long, default_value = "1x")]
    speed: String,
    #[arg(long)]
    from: Option<u64>,
    #[arg(long)]
    to: Option<u64>,
    #[arg(long, default_value_t = false)]
    max_speed: bool,
    #[arg(long, default_value_t = false)]
    step_mode: bool,
    #[arg(long, default_value = "127.0.0.1:50051")]
    addr: String,
}

#[derive(Args)]
struct UiArgs {
    #[arg(long)]
    log: PathBuf,
    #[arg(long)]
    index: Option<PathBuf>,
    #[arg(long)]
    compare_log: Option<PathBuf>,
    #[arg(long)]
    compare_index: Option<PathBuf>,
    #[arg(long)]
    from: Option<u64>,
    #[arg(long)]
    to: Option<u64>,
    #[arg(long, default_value = "127.0.0.1:8080")]
    addr: String,
}

#[derive(Args)]
struct ReadArgs {
    #[arg(long)]
    log: PathBuf,
    #[arg(long)]
    index: Option<PathBuf>,
    #[arg(long)]
    from: Option<u64>,
    #[arg(long)]
    to: Option<u64>,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Args)]
struct FeatureArgs {
    #[arg(long)]
    log: PathBuf,
    #[arg(long)]
    index: Option<PathBuf>,
    #[arg(long)]
    from: Option<u64>,
    #[arg(long)]
    to: Option<u64>,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long)]
    out: Option<PathBuf>,
}

#[derive(Args)]
struct VerifyArgs {
    #[arg(long)]
    log: PathBuf,
    #[arg(long)]
    index: Option<PathBuf>,
    #[arg(long)]
    client: String,
    #[arg(long, default_value_t = 42)]
    seed: u64,
    #[arg(long, default_value = "verify.out")]
    out: PathBuf,
}

#[derive(Args)]
struct BenchArgs {
    #[arg(long)]
    log: PathBuf,
    #[arg(long)]
    index: Option<PathBuf>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_env_filter(
            EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new("info")),
        )
        .init();

    let cli = Cli::parse();

    match cli.command {
        Command::IngestCsvA(args) => {
            let ticks = load_tick_table(args.tick_config.as_deref())?;
            let events = ingest_csv_a(&args.input, &args.venue, &ticks)?;
            write_log_and_index(&events, &args.out, args.index_stride)?;
            info!(events = events.len(), out = %args.out.display(), "ingested csv_a");
        }
        Command::IngestCsvB(args) => {
            let ticks = load_tick_table(args.tick_config.as_deref())?;
            let events = ingest_csv_b(&args.input, &args.venue, &ticks)?;
            write_log_and_index(&events, &args.out, args.index_stride)?;
            info!(events = events.len(), out = %args.out.display(), "ingested csv_b");
        }
        Command::IngestCsvC(args) => {
            let ticks = load_tick_table(args.tick_config.as_deref())?;
            let events = ingest_csv_c(&args.input, &args.venue, &ticks)?;
            write_log_and_index(&events, &args.out, args.index_stride)?;
            info!(events = events.len(), out = %args.out.display(), "ingested csv_c");
        }
        Command::IngestReal(args) => {
            if args.provider != "yahoo" {
                return Err(anyhow!("unsupported real-data provider {}", args.provider));
            }
            let ticks = load_tick_table(args.tick_config.as_deref())?;
            let symbols = parse_symbols(&args.symbols)?;
            let events =
                ingest_yahoo(&symbols, &args.venue, &ticks, &args.interval, &args.range).await?;
            write_log_and_index(&events, &args.out, args.index_stride)?;
            info!(
                events = events.len(),
                out = %args.out.display(),
                provider = %args.provider,
                symbols = %args.symbols,
                "ingested real data"
            );
        }
        Command::IngestPcap(args) => {
            if args.schema != "mock_itch" {
                return Err(anyhow!("unsupported schema {}", args.schema));
            }
            let output = ingest_pcap(&args.pcap, &args.venue)?;
            write_log_and_index(&output.events, &args.out, args.index_stride)?;
            info!(
                events = output.events.len(),
                issues = output.issues.len(),
                out = %args.out.display(),
                "ingested pcap"
            );
        }
        Command::GenPcap(args) => {
            let symbols = parse_symbols(&args.symbols)?;
            generate_pcap(&args.out, &symbols, args.events, args.seed)?;
            info!(out = %args.out.display(), events = args.events, "generated pcap");
        }
        Command::Serve(args) => {
            let addr: SocketAddr = args
                .addr
                .parse()
                .with_context(|| format!("invalid addr {}", args.addr))?;
            let speed = parse_speed(&args.speed)?;
            let cfg = ReplayConfig {
                from_ns: args.from,
                to_ns: args.to,
                speed,
                max_speed: args.max_speed,
                step_mode: args.step_mode,
            };
            let index = args.index.or_else(|| maybe_index_path(&args.log));
            serve_grpc(args.log, index, addr, cfg).await?;
        }
        Command::Ui(args) => {
            let addr: SocketAddr = args
                .addr
                .parse()
                .with_context(|| format!("invalid addr {}", args.addr))?;
            let index = args.index.or_else(|| maybe_index_path(&args.log));
            if args.compare_log.is_none() && args.compare_index.is_some() {
                return Err(anyhow!("--compare-index requires --compare-log"));
            }
            let compare_index = match &args.compare_log {
                Some(path) => args
                    .compare_index
                    .or_else(|| maybe_index_path(path.as_path())),
                None => None,
            };
            info!(addr = %addr, log = %args.log.display(), "starting ui");
            gui::serve_ui(
                args.log,
                index,
                args.compare_log,
                compare_index,
                args.from,
                args.to,
                addr,
            )
            .await?;
        }
        Command::Print(args) => {
            let idx_path = args.index.or_else(|| maybe_index_path(&args.log));
            let events = read_events(&args.log, idx_path.as_deref(), args.from, args.to)?;
            let lines = events
                .iter()
                .map(format_event)
                .collect::<Vec<_>>()
                .join("\n");
            if let Some(out) = args.out {
                std::fs::write(out, format!("{}\n", lines))?;
            } else {
                println!("{}", lines);
            }
        }
        Command::Feature(args) => {
            let idx_path = args.index.or_else(|| maybe_index_path(&args.log));
            let events = read_events(&args.log, idx_path.as_deref(), args.from, args.to)?;
            let cfg = seeded_feature_config(args.seed);
            let lines = run_feature(&events, &cfg).join("\n");
            if let Some(out) = args.out {
                std::fs::write(out, format!("{}\n", lines))?;
            } else {
                println!("{}", lines);
            }
        }
        Command::Verify(args) => {
            if args.client != "feature" {
                return Err(anyhow!("unsupported verify client {}", args.client));
            }
            verify_feature_determinism(&args.log, args.index.as_deref(), args.seed, &args.out)?;
            info!(out = %args.out.display(), "verify passed");
        }
        Command::Bench(args) => {
            run_bench(&args.log, args.index.as_deref())?;
        }
    }

    Ok(())
}

fn load_tick_table(path: Option<&Path>) -> Result<TickTable> {
    match path {
        Some(p) => {
            let raw = std::fs::read_to_string(p)
                .with_context(|| format!("failed reading {}", p.display()))?;
            TickTable::from_toml_str(&raw).context("invalid tick config")
        }
        None => {
            TickTable::from_toml_str("default_tick = \"0.01\"\n").context("default tick config")
        }
    }
}

fn parse_symbols(raw: &str) -> Result<Vec<String>> {
    let syms = raw
        .split(',')
        .map(str::trim)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
        .collect::<Vec<_>>();
    if syms.is_empty() {
        return Err(anyhow!("empty symbols list"));
    }
    Ok(syms)
}

fn parse_speed(raw: &str) -> Result<f64> {
    let trimmed = raw.trim();
    let stripped = trimmed.strip_suffix('x').unwrap_or(trimmed);
    let speed = stripped
        .parse::<f64>()
        .with_context(|| format!("invalid speed {}", raw))?;
    if speed <= 0.0 {
        return Err(anyhow!("speed must be > 0"));
    }
    Ok(speed)
}

fn write_log_and_index(events: &[md_core::Event], out: &Path, stride: u32) -> Result<()> {
    let mut symbols = BTreeSet::new();
    for event in events {
        symbols.insert(event.symbol.clone());
    }
    let symbols = symbols.into_iter().collect::<Vec<_>>();

    let mut writer = EventLogWriter::create(out, &symbols, default_schema_hash())?;
    let idx_path = index_path_for_log(out);
    let mut idx = IndexWriter::create(&idx_path, stride)?;

    for event in events {
        let offset = writer.append(event)?;
        idx.maybe_add(event, offset)?;
    }

    writer.flush()?;
    idx.flush()?;
    Ok(())
}

fn maybe_index_path(log: &Path) -> Option<PathBuf> {
    let path = index_path_for_log(log);
    if path.exists() {
        Some(path)
    } else {
        None
    }
}

fn index_path_for_log(log: &Path) -> PathBuf {
    PathBuf::from(format!("{}.idx", log.display()))
}

fn seeded_feature_config(seed: u64) -> FeatureConfig {
    let mut rng = ChaCha8Rng::seed_from_u64(seed);
    FeatureConfig {
        mid_window: 8,
        ewma_alpha: 0.1 + rng.gen_range(0.0..0.25),
        spread_threshold: 20 + rng.gen_range(0..10),
        imbalance_threshold: 0.6 + rng.gen_range(0.0..0.2),
        vol_threshold: 0.02 + rng.gen_range(0.0..0.02),
    }
}

fn run_bench(log: &Path, index: Option<&Path>) -> Result<()> {
    let idx_path = index.map(PathBuf::from).or_else(|| maybe_index_path(log));
    let t0 = Instant::now();
    let events = read_events(log, idx_path.as_deref(), None, None)?;
    let replay_elapsed = t0.elapsed();

    let mut latencies = Vec::with_capacity(events.len());
    for event in &events {
        let s = Instant::now();
        black_box(format_event(event));
        latencies.push(s.elapsed().as_nanos() as u64);
    }
    latencies.sort_unstable();
    let p99 = if latencies.is_empty() {
        0
    } else {
        let idx = ((latencies.len() as f64) * 0.99).floor() as usize;
        latencies[idx.min(latencies.len() - 1)]
    };

    let parse_start = Instant::now();
    let mut reader = EventLogReader::open(log)?;
    reader.rewind_to_data()?;
    let mut parse_count = 0usize;
    while reader.next_record()?.is_some() {
        parse_count += 1;
    }
    let parse_elapsed = parse_start.elapsed();

    let replay_eps = if replay_elapsed.as_secs_f64() > 0.0 {
        events.len() as f64 / replay_elapsed.as_secs_f64()
    } else {
        0.0
    };
    let parse_eps = if parse_elapsed.as_secs_f64() > 0.0 {
        parse_count as f64 / parse_elapsed.as_secs_f64()
    } else {
        0.0
    };

    println!("events/sec: {:.2}", replay_eps);
    println!("p99 replay latency (ns): {}", p99);
    println!("parse throughput (events/sec): {:.2}", parse_eps);

    Ok(())
}
