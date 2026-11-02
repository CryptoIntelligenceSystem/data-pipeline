<#
Run Spark jobs on the cluster from ..\infrastructure (docker compose).

  .\scripts\spark.ps1 start <job>             streaming job, detached; log in /tmp/<job>.log in spark-master
  .\scripts\spark.ps1 run   <job> [args...]   batch job, foreground (e.g. --start 2026-10-27 --end 2026-10-28)
  .\scripts\spark.ps1 stop  <job>
  .\scripts\spark.ps1 logs  <job>
  .\scripts\spark.ps1 test                    unit tests inside the Spark image (JDK 17, its own pyspark 3.5.9)

Job tuning variables set in $env: are forwarded: SPARK_STARTING_OFFSETS, SPARK_TRIGGER_INTERVAL,
SPARK_MAX_OFFSETS_PER_TRIGGER, SPARK_FAIL_ON_DATA_LOSS, ANOMALY_*, and per-job overrides such as
MARKET_TO_CLICKHOUSE_STARTING_OFFSETS.
#>
param(
    [Parameter(Mandatory = $true, Position = 0)][ValidateSet("start", "run", "stop", "logs", "test")][string]$Action,
    [Parameter(Position = 1)][string]$Job,
    [Parameter(ValueFromRemainingArguments = $true)][string[]]$JobArgs
)
$ErrorActionPreference = "Stop"

$infra = if ($env:INFRA_DIR) { $env:INFRA_DIR } else { Join-Path $PSScriptRoot "..\..\infrastructure" }
$composeFile = Join-Path (Resolve-Path $infra) "docker-compose.yml"
function Invoke-Compose { docker compose -f $composeFile @args; if ($LASTEXITCODE -ne 0) { exit $LASTEXITCODE } }

if ($Action -eq "test") {
    Invoke-Compose run --rm --no-deps -T -w /opt/pipeline --entrypoint sh spark-master -c `
        "export PYTHONDONTWRITEBYTECODE=1 PYTHONPATH=/opt/pipeline:/opt/spark/python:`$(ls /opt/spark/python/lib/py4j-*.zip) && pip install --quiet --user jsonschema pytest -r requirements.txt >/dev/null && python3 -m pytest -q -p no:cacheprovider tests"
    exit 0
}
if (-not $Job) { throw "job name required, e.g. market_to_clickhouse" }

$pattern = '^(SPARK_(STARTING_OFFSETS|TRIGGER_INTERVAL|MAX_OFFSETS_PER_TRIGGER|FAIL_ON_DATA_LOSS)|ANOMALY_|(RAW_TO_MINIO|MARKET_TO_CLICKHOUSE|NEWS_TO_CLICKHOUSE|VOLUME_ANOMALY)_)'
$envArgs = @()
Get-ChildItem env: | Where-Object { $_.Name -match $pattern } | ForEach-Object { $envArgs += @("-e", "$($_.Name)=$($_.Value)") }

$submit = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --name $Job /opt/pipeline/spark_jobs/jobs/$Job.py $($JobArgs -join ' ')"

switch ($Action) {
    "start" {
        Invoke-Compose exec -d @envArgs spark-master sh -c "$submit > /tmp/$Job.log 2>&1"
        Write-Host "$Job started. Logs: .\scripts\spark.ps1 logs $Job   UI: http://localhost:8080"
    }
    "run"  { Invoke-Compose exec -T @envArgs spark-master sh -c $submit }
    "stop" { Invoke-Compose exec -T spark-master pkill -f "spark_jobs/jobs/$Job.py"; Write-Host "$Job stopped" }
    "logs" { Invoke-Compose exec spark-master tail -n 100 -f "/tmp/$Job.log" }
}
