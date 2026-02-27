param(
    [string]$Addr = "127.0.0.1:50051"
)

Set-StrictMode -Version Latest
$ErrorActionPreference = "Stop"

grpcurl -plaintext `
  -import-path crates/replay/proto `
  -proto replay.proto `
  -d "{}" `
  $Addr `
  replay.ReplayService/StreamEvents
