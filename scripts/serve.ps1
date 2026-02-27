param(
    [string]$Log = "data/norm.eventlog",
    [string]$Addr = "127.0.0.1:50051",
    [string]$Speed = "10x"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

cargo run -p md-replay -- serve --log $Log --speed $Speed --addr $Addr
