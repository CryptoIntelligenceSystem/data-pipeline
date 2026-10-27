<#
Run the test suite (including tests/spark) on Windows with JDK 17 and the venv's pyspark.

Works around two Windows problems:
- Spark's .cmd launchers break on non-ASCII paths (e.g. "Lưu trữ"): SPARK_HOME, JAVA_HOME and the
  working directory are passed as 8.3 short paths.
- JDK 17 creates a Unix domain socket for NIO pipes in %TEMP%; some profiles reject that
  ("Unable to establish loopback connection"), so the socket directory is moved to C:\Windows\Temp.

  .\scripts\test_spark.ps1                 # all tests
  .\scripts\test_spark.ps1 tests/spark -x  # extra pytest arguments
#>
param([Parameter(ValueFromRemainingArguments = $true)][string[]]$PytestArgs)
$ErrorActionPreference = "Stop"

$fso = New-Object -ComObject Scripting.FileSystemObject
$repo = $fso.GetFolder((Join-Path $PSScriptRoot "..")).ShortPath
$python = Join-Path $repo ".venv\Scripts\python.exe"
if (-not (Test-Path $python)) { throw "Create the venv first: python -m venv .venv; pip install -r requirements.txt -r requirements-spark.txt" }

$jdk = if ($env:JAVA17_HOME) { $env:JAVA17_HOME } else {
    Get-ChildItem "C:\Program Files\Microsoft", "C:\Program Files\Eclipse Adoptium", "C:\Program Files\Java" -Directory -Filter "jdk-17*" -ErrorAction SilentlyContinue |
        Select-Object -First 1 -ExpandProperty FullName
}
if (-not $jdk) { throw "JDK 17 not found (winget install Microsoft.OpenJDK.17) or set JAVA17_HOME" }

$env:JAVA_HOME = $fso.GetFolder($jdk).ShortPath
$env:SPARK_HOME = $fso.GetFolder((& $python -c "import os, pyspark; print(os.path.dirname(pyspark.__file__))")).ShortPath
$env:PYSPARK_PYTHON = $python
$env:JDK_JAVA_OPTIONS = "-Djdk.net.unixdomain.tmpdir=C:\Windows\Temp"
$env:PYTHONDONTWRITEBYTECODE = "1"

Push-Location $repo
try {
    & $python -m pytest -q -p no:cacheprovider @PytestArgs
    exit $LASTEXITCODE
} finally {
    Pop-Location
}
