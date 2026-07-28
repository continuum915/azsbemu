[CmdletBinding()]
param(
    [ValidateRange(1, 20)] [int]$Runs = 3,
    [ValidateRange(1, 5000)] [int]$Entities = 500,
    [ValidateRange(1, 10000000)] [int]$Messages = 50000,
    [ValidateRange(1, 256)] [int]$Producers = 16,
    [ValidateRange(1, 256)] [int]$Consumers = 16,
    [ValidateRange(1, 1048576)] [int]$MessageSize = 1024,
    [ValidateRange(1, 300)] [int]$ReceiveTimeoutSeconds = 30
)

$ErrorActionPreference = 'Stop'
$benchmarkDirectory = $PSScriptRoot
$project = Join-Path $benchmarkDirectory 'AzsbEmu.Benchmarks.csproj'
$resultsDirectory = Join-Path $benchmarkDirectory 'results'
$timestamp = Get-Date -Format 'yyyyMMdd-HHmmss'
$runDirectory = Join-Path $resultsDirectory "backlog-$timestamp"
$runResults = [System.Collections.Generic.List[object]]::new()

New-Item -ItemType Directory -Path $runDirectory -Force | Out-Null

[pscustomobject]@{
    Timestamp = (Get-Date).ToString('O')
    Machine = $env:COMPUTERNAME
    PowerShell = $PSVersionTable.PSVersion.ToString()
    Processor = (Get-CimInstance Win32_Processor | Select-Object -First 1 -ExpandProperty Name).Trim()
    LogicalProcessors = [Environment]::ProcessorCount
    Runs = $Runs
    Entities = $Entities
    Messages = $Messages
    Producers = $Producers
    Consumers = $Consumers
    MessageSizeBytes = $MessageSize
} | ConvertTo-Json | Set-Content -Path (Join-Path $runDirectory 'environment.json')

function Convert-BenchmarkNumber([string]$Value) {
    [double]::Parse($Value, [System.Globalization.CultureInfo]::InvariantCulture)
}

function Get-Median([double[]]$Values) {
    $sorted = @($Values | Sort-Object)
    $middle = [int]($sorted.Count / 2)
    if ($sorted.Count % 2 -eq 1) { return $sorted[$middle] }
    ($sorted[$middle - 1] + $sorted[$middle]) / 2
}

Write-Host 'Building Release benchmark project...' -ForegroundColor Cyan
& dotnet build $project -c Release
if ($LASTEXITCODE -ne 0) { throw "Release build failed with exit code $LASTEXITCODE." }

for ($run = 1; $run -le $Runs; $run++) {
    $logPath = Join-Path $runDirectory ('run-{0:D2}.log' -f $run)
    Write-Host "`nStarting baseline run $run of $Runs..." -ForegroundColor Cyan

    & dotnet run --project $project -c Release --no-build -- `
        --benchmark concurrent `
        --mode backlog `
        --entities $Entities `
        --messages $Messages `
        --producers $Producers `
        --consumers $Consumers `
        --message-size $MessageSize `
        --receive-timeout-seconds $ReceiveTimeoutSeconds 2>&1 |
        Tee-Object -FilePath $logPath

    if ($LASTEXITCODE -ne 0) { throw "Baseline run $run failed with exit code $LASTEXITCODE. See $logPath." }

    $log = Get-Content -LiteralPath $logPath -Raw
    $pattern = '(?s)Backlog Drain Receive & Complete Results:\s*- Throughput: ([\d.]+) ops/sec.*?Latency \(Avg\): ([\d.]+) ms.*?Latency \(P50 / P95 / P99\): ([\d.]+) ms / ([\d.]+) ms / ([\d.]+) ms'
    if ($log -notmatch $pattern) {
        throw "Baseline run $run did not produce a backlog-drain result. See $logPath."
    }

    $runResults.Add([pscustomobject]@{
        Run = $run
        ThroughputOpsPerSecond = Convert-BenchmarkNumber $matches[1]
        AverageLatencyMs = Convert-BenchmarkNumber $matches[2]
        P50LatencyMs = Convert-BenchmarkNumber $matches[3]
        P95LatencyMs = Convert-BenchmarkNumber $matches[4]
        P99LatencyMs = Convert-BenchmarkNumber $matches[5]
        LogPath = $logPath
    })
}

$runResults | Export-Csv -Path (Join-Path $runDirectory 'runs.csv') -NoTypeInformation

$summary = [pscustomobject]@{
    Timestamp = (Get-Date).ToString('O')
    Runs = $Runs
    DrainThroughputOpsPerSecondMedian = Get-Median @($runResults.ThroughputOpsPerSecond)
    DrainAverageLatencyMsMedian = Get-Median @($runResults.AverageLatencyMs)
    DrainP50LatencyMsMedian = Get-Median @($runResults.P50LatencyMs)
    DrainP95LatencyMsMedian = Get-Median @($runResults.P95LatencyMs)
    DrainP99LatencyMsMedian = Get-Median @($runResults.P99LatencyMs)
}
$summary | ConvertTo-Json | Set-Content -Path (Join-Path $runDirectory 'summary.json')

Write-Host "`nBacklog-drain median baseline:" -ForegroundColor Green
$summary | Format-List
Write-Host "Logs, per-run data, and summary saved to: $runDirectory" -ForegroundColor Green