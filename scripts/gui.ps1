param(
    [string]$Symbols = "AAPL,MSFT",
    [string]$Venue = "X",
    [string]$Log = "data/real.eventlog",
    [string]$CompareLog = "",
    [string]$CompareIndex = "",
    [string]$Addr = "127.0.0.1:8080",
    [string]$Range = "1d",
    [string]$Interval = "1m"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

cargo run -p md-replay -- ingest-real --provider yahoo --symbols $Symbols --range $Range --interval $Interval --venue $Venue --out $Log --tick-config configs/ticks.toml
$uiArgs = @("run", "-p", "md-replay", "--", "ui", "--log", $Log, "--addr", $Addr)
if ($CompareLog -ne "") {
    $uiArgs += @("--compare-log", $CompareLog)
}
if ($CompareIndex -ne "") {
    $uiArgs += @("--compare-index", $CompareIndex)
}
cargo @uiArgs
