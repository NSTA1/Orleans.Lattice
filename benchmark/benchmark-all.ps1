<#
.SYNOPSIS
    Iterates over every scenario file in the scenarios folder and invokes
    benchmark.ps1 once per scenario, trapping errors so a single failure does
    not abort the rest of the run.

.DESCRIPTION
    For each *.env file in benchmark/scenarios/, this script derives the
    scenario id from the file name (minus extension) and runs:

        ./benchmark.ps1 -Scenario <id>

    Each scenario is executed in its own try/catch so that a failure in one
    scenario is reported immediately but does not prevent subsequent scenarios
    from running. After all scenarios have been attempted, a summary table is
    printed showing the outcome of each scenario (success or failure, with the
    failure detail when applicable). The script exits with a non-zero code if
    any scenario failed.

.PARAMETER ScenariosPath
    Optional path to the scenarios folder. Defaults to ./scenarios relative to
    this script.

.PARAMETER KeepRunning
    Forwarded to benchmark.ps1 for every scenario invocation. Note: leaving
    stacks running across multiple scenarios will likely cause port conflicts;
    use with care.

.EXAMPLE
    ./benchmark-all.ps1

.EXAMPLE
    ./benchmark-all.ps1 -ScenariosPath ./scenarios
#>
[CmdletBinding()]
param(
    [string] $ScenariosPath,
    [switch] $KeepRunning
)

$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$benchmarkScript = Join-Path $scriptRoot 'benchmark.ps1'

if (-not (Test-Path -LiteralPath $benchmarkScript)) {
    throw "benchmark.ps1 not found at '$benchmarkScript'."
}

if ([string]::IsNullOrWhiteSpace($ScenariosPath)) {
    $ScenariosPath = Join-Path $scriptRoot 'scenarios'
}

if (-not (Test-Path -LiteralPath $ScenariosPath)) {
    throw "Scenarios folder not found at '$ScenariosPath'."
}

$scenarioFiles = Get-ChildItem -LiteralPath $ScenariosPath -File |
    Sort-Object -Property Name

if ($scenarioFiles.Count -eq 0) {
    Write-Warning "No scenario files found in '$ScenariosPath'."
    return
}

Write-Host ''
Write-Host "Discovered $($scenarioFiles.Count) scenario file(s) in '$ScenariosPath':" -ForegroundColor Cyan
foreach ($file in $scenarioFiles) {
    Write-Host "  - $([System.IO.Path]::GetFileNameWithoutExtension($file.Name))"
}
Write-Host ''

$results = New-Object System.Collections.Generic.List[object]

# benchmark.ps1 invokes `docker compose` with paths relative to the current
# working directory (e.g. docker-compose.yml). Push into the benchmark folder
# so every scenario inherits that working directory, regardless of where the
# caller launched benchmark-all.ps1 from.
Push-Location -LiteralPath $scriptRoot
try {
    $isFirstScenario = $true
    foreach ($file in $scenarioFiles) {
        $scenario = [System.IO.Path]::GetFileNameWithoutExtension($file.Name)

        Write-Host ''
        Write-Host ('=' * 80) -ForegroundColor DarkGray
        Write-Host "Running scenario: $scenario" -ForegroundColor Cyan
        Write-Host ('=' * 80) -ForegroundColor DarkGray

        $started = Get-Date
        $entry = [pscustomobject]@{
            Scenario = $scenario
            Status   = 'Pending'
            Duration = [TimeSpan]::Zero
            Error    = $null
        }

        try {
            $params = @{ Scenario = $scenario }
            if ($KeepRunning.IsPresent) {
                $params['KeepRunning'] = $true
            }

            # The first scenario builds the images; every subsequent scenario
            # reuses them (-NoBuild) to avoid rebuilding on every iteration.
            if (-not $isFirstScenario) {
                $params['NoBuild'] = $true
            }

            & $benchmarkScript @params

            if ($LASTEXITCODE -ne 0 -and $null -ne $LASTEXITCODE) {
                throw "benchmark.ps1 exited with non-zero code $LASTEXITCODE."
            }

            $entry.Status = 'Success'
            Write-Host ''
            Write-Host "[PASS] $scenario" -ForegroundColor Green
        }
        catch {
            $entry.Status = 'Failure'
            $entry.Error  = $_.Exception.Message

            Write-Host ''
            Write-Host "[FAIL] $scenario" -ForegroundColor Red
            Write-Host $_.Exception.Message -ForegroundColor Red
            if ($_.ScriptStackTrace) {
                Write-Host $_.ScriptStackTrace -ForegroundColor DarkGray
            }
        }
        finally {
            $entry.Duration = (Get-Date) - $started
            $results.Add($entry) | Out-Null
            $isFirstScenario = $false
        }
    }
}
finally {
    Pop-Location
}

Write-Host ''
Write-Host ('=' * 80) -ForegroundColor DarkGray
Write-Host 'Benchmark run summary' -ForegroundColor Cyan
Write-Host ('=' * 80) -ForegroundColor DarkGray

$successCount = ($results | Where-Object { $_.Status -eq 'Success' }).Count
$failureCount = ($results | Where-Object { $_.Status -eq 'Failure' }).Count

$nameWidth = ($results | ForEach-Object { $_.Scenario.Length } | Measure-Object -Maximum).Maximum
if ($nameWidth -lt 8) { $nameWidth = 8 }

$header = '{0,-' + $nameWidth + '}  {1,-7}  {2,10}  {3}'
Write-Host ($header -f 'Scenario', 'Status', 'Duration', 'Detail') -ForegroundColor White
Write-Host ($header -f ('-' * $nameWidth), '-------', '----------', '------') -ForegroundColor DarkGray

foreach ($r in $results) {
    $duration = '{0:hh\:mm\:ss}' -f $r.Duration
    $detail = if ($r.Status -eq 'Failure') { $r.Error } else { '' }
    $line = $header -f $r.Scenario, $r.Status, $duration, $detail
    if ($r.Status -eq 'Success') {
        Write-Host $line -ForegroundColor Green
    }
    else {
        Write-Host $line -ForegroundColor Red
    }
}

Write-Host ''
Write-Host "Total: $($results.Count)  Success: $successCount  Failure: $failureCount" -ForegroundColor Cyan
Write-Host ''

if ($failureCount -gt 0) {
    exit 1
}
