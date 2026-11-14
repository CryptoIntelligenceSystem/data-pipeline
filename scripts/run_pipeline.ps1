<###
Run the local pipeline end to end on Windows.

Usage:
  .\scripts\run_pipeline.ps1 -Start 2026-10-29 -End 2026-10-30
  .\scripts\run_pipeline.ps1 -Start 2026-10-29 -End 2026-10-30 -BuildInfrastructure
  .\scripts\run_pipeline.ps1 -Start 2026-10-29 -End 2026-10-30 -SkipBackfill

The date range is inclusive and UTC. By default the script backfills that range through the
collector before running the batch jobs. Streaming jobs stay running after the batch jobs finish.
###>
param(
    [Parameter(Mandatory = $true)][string]$Start,
    [Parameter(Mandatory = $true)][string]$End,
    [switch]$BuildInfrastructure,
    [switch]$SkipBackfill,
    [ValidateRange(30, 3600)][int]$RawTimeoutSeconds = 300
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

function Test-RawDataDateRange {
    $current = [DateTime]::ParseExact($Start, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
    $last = [DateTime]::ParseExact($End, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
    while ($current -le $last) {
        $path = "local/raw-data/market/year=$($current.Year)/month=$($current.Month)/day=$($current.Day)"
        $output = & docker compose -f $composeFile run --rm --no-deps -T mc-init ls --recursive $path 2>$null
        if ($LASTEXITCODE -ne 0 -or @($output | Where-Object { $_ -match "\S" }).Count -eq 0) {
            return $false
        }
        $current = $current.AddDays(1)
    }
    return $true
}

function Get-UtcIso([DateTime]$value) {
    return $value.ToString("yyyy-MM-dd'T'HH:mm:ss'Z'", [Globalization.CultureInfo]::InvariantCulture)
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

if (-not $SkipBackfill) {
    $startDate = [DateTime]::ParseExact($Start, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture)
    $endDateExclusive = [DateTime]::ParseExact($End, "yyyy-MM-dd", [Globalization.CultureInfo]::InvariantCulture).AddDays(1)
    $startIso = Get-UtcIso $startDate
    $endIso = Get-UtcIso $endDateExclusive
    Write-Host "Backfilling $startIso .. $endIso into Kafka..." -ForegroundColor Cyan
    Invoke-Compose @(
        "run", "--rm", "--no-deps", "-T", "collector", "python", "-m", "collector",
        "--source", "all", "--start", $startIso, "--end", $endIso, "--send-kafka"
    )
}

Write-Host "Waiting for all requested raw-data partitions in MinIO (up to ${RawTimeoutSeconds}s)..." -ForegroundColor Cyan
$deadline = (Get-Date).AddSeconds($RawTimeoutSeconds)
$rawReady = $false
while ((Get-Date) -lt $deadline) {
    if (Test-RawDataDateRange) {
        $rawReady = $true
        break
    }
    Start-Sleep -Seconds 5
}
if (-not $rawReady) {
    throw "MinIO raw-data is missing one or more requested UTC date partitions before timeout. Check: .\scripts\spark.ps1 logs raw_to_minio"
}

Write-Host "Running 5-minute candle batch for $Start .. $End ..." -ForegroundColor Cyan
Invoke-Checked $sparkScript @("run", "batch_ohlcv_5m", "--start", $Start, "--end", $End)

Write-Host "Running MA20 batch for $Start .. $End ..." -ForegroundColor Cyan
Invoke-Checked $sparkScript @("run", "batch_indicators", "--start", $Start, "--end", $End)

Write-Host "Measuring News Impact for $Start .. $End ..." -ForegroundColor Cyan
Invoke-Checked $sparkScript @("run", "batch_news_impact", "--start", $Start, "--end", $End)

Write-Host "Pipeline verification:" -ForegroundColor Green
$query = @"
SELECT 'market_data' AS table_name, count() AS rows FROM pipeline.market_data FINAL
UNION ALL SELECT 'news_data', count() FROM pipeline.news_data FINAL
UNION ALL SELECT 'candles_5m', count() FROM pipeline.candles_5m FINAL
UNION ALL SELECT 'indicators', count() FROM pipeline.indicators FINAL
UNION ALL SELECT 'signals', count() FROM pipeline.signals FINAL
UNION ALL SELECT 'news_impact', count() FROM pipeline.news_impact FINAL
"@
Invoke-Compose @("exec", "-T", "clickhouse", "clickhouse-client", "--query", $query)

Write-Host "Done. Streaming jobs remain running." -ForegroundColor Green
