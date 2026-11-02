<###
Run the local pipeline end to end on Windows.

Usage:
  .\scripts\run_pipeline.ps1 -Start 2026-10-29 -End 2026-10-30
  .\scripts\run_pipeline.ps1 -Start 2026-10-29 -End 2026-10-30 -BuildInfrastructure

The date range is the UTC range used by the batch jobs. Streaming jobs stay running after the
batch jobs finish.
###>
param(
    [Parameter(Mandatory = $true)][string]$Start,
    [Parameter(Mandatory = $true)][string]$End,
    [switch]$BuildInfrastructure,
    [ValidateRange(10, 1800)][int]$RawTimeoutSeconds = 120
)

$ErrorActionPreference = "Stop"

$repo = Resolve-Path (Join-Path $PSScriptRoot "..")
$infra = Resolve-Path (Join-Path $PSScriptRoot "..\..\infrastructure")
$composeFile = Join-Path $infra "docker-compose.yml"
$sparkScript = Join-Path $repo "scripts\spark.ps1"
$migrationScript = Join-Path $infra "scripts\migrate_clickhouse.ps1"

function Invoke-Checked {
    param([string]$File, [string[]]$Arguments)
    & $File @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "$File failed with exit code $LASTEXITCODE"
    }
}

function Invoke-Compose {
    param([string[]]$Arguments)
    & docker compose -f $composeFile @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose failed with exit code $LASTEXITCODE"
    }
}

function Test-RawData {
    $output = & docker compose -f $composeFile run --rm --no-deps -T mc-init ls --recursive local/raw-data 2>$null
    return ($LASTEXITCODE -eq 0 -and @($output | Where-Object { $_ -match "\S" }).Count -gt 0)
}

function Wait-ClickHouse {
    $deadline = (Get-Date).AddSeconds(60)
    while ((Get-Date) -lt $deadline) {
        try {
            Invoke-WebRequest "http://localhost:8123/ping" -UseBasicParsing -TimeoutSec 3 | Out-Null
            return
        }
        catch {
            Start-Sleep -Seconds 3
        }
    }
    throw "ClickHouse did not become ready on http://localhost:8123/ping"
}

Write-Host "Starting infrastructure and Collector..." -ForegroundColor Cyan
$upArguments = @("up", "-d")
if ($BuildInfrastructure) {
    $upArguments += "--build"
}
Invoke-Compose $upArguments

Write-Host "Waiting for ClickHouse and applying idempotent migrations..." -ForegroundColor Cyan
Wait-ClickHouse
Invoke-Checked $migrationScript @()

Write-Host "Starting streaming jobs..." -ForegroundColor Cyan
Invoke-Checked $sparkScript @("start", "raw_to_minio")
Invoke-Checked $sparkScript @("start", "market_to_clickhouse")
Invoke-Checked $sparkScript @("start", "news_to_clickhouse")
Invoke-Checked $sparkScript @("start", "volume_anomaly")

Write-Host "Waiting for raw data to appear in MinIO (up to ${RawTimeoutSeconds}s)..." -ForegroundColor Cyan
$deadline = (Get-Date).AddSeconds($RawTimeoutSeconds)
$rawReady = $false
while ((Get-Date) -lt $deadline) {
    if (Test-RawData) {
        $rawReady = $true
        break
    }
    Start-Sleep -Seconds 5
}
if (-not $rawReady) {
    throw "No object found in MinIO raw-data before timeout. Check: .\scripts\spark.ps1 logs raw_to_minio"
}

Write-Host "Running 5-minute candle batch for $Start .. $End ..." -ForegroundColor Cyan
Invoke-Checked $sparkScript @("run", "batch_ohlcv_5m", "--start", $Start, "--end", $End)

Write-Host "Running MA20 batch for $Start .. $End ..." -ForegroundColor Cyan
Invoke-Checked $sparkScript @("run", "batch_indicators", "--start", $Start, "--end", $End)

Write-Host "Pipeline verification:" -ForegroundColor Green
$query = @"
SELECT 'market_data' AS table_name, count() AS rows FROM pipeline.market_data FINAL
UNION ALL SELECT 'news_data', count() FROM pipeline.news_data FINAL
UNION ALL SELECT 'candles_5m', count() FROM pipeline.candles_5m FINAL
UNION ALL SELECT 'indicators', count() FROM pipeline.indicators FINAL
UNION ALL SELECT 'signals', count() FROM pipeline.signals FINAL
"@
Invoke-Compose @("exec", "-T", "clickhouse", "clickhouse-client", "--query", $query)

Write-Host "Done. Streaming jobs remain running." -ForegroundColor Green
