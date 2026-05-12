#requires -Version 7.0
<#
.SYNOPSIS
    Stand up the benchmark history stack (VictoriaMetrics + Grafana) and open
    the Grafana UI in the default browser.

.DESCRIPTION
    Stand-alone counterpart to `./benchmark.ps1 -OpenHistory`. Runs
    `docker compose up -d` against `benchmark/history/docker-compose.history.yml`,
    waits for Grafana's health endpoint to respond, then launches the default
    browser at the Grafana URL.

    URLs honour the same overrides used by `benchmark.ps1`:
      $env:BENCH_HISTORY_GRAFANA_URL   (default http://localhost:3001)
      $env:BENCH_HISTORY_VM_URL        (default http://localhost:8428)

.PARAMETER NoBrowser
    Bring the stack up but do not launch the browser. Useful in CI / headless
    sessions.

.PARAMETER TimeoutSeconds
    How long to poll Grafana's /api/health endpoint before giving up and
    opening the browser anyway. Default: 60.

.EXAMPLE
    ./benchmark/start-history.ps1
    Bring the stack up and open Grafana.

.EXAMPLE
    ./benchmark/start-history.ps1 -NoBrowser
    Bring the stack up only.
#>
[CmdletBinding()]
param(
    [switch] $NoBrowser,
    [int]    $TimeoutSeconds = 60
)

$ErrorActionPreference = 'Stop'

$benchmarkRoot     = $PSScriptRoot
$historyRoot       = Join-Path $benchmarkRoot 'history'
$historyCompose    = Join-Path $historyRoot 'docker-compose.history.yml'
$historyGrafanaUrl = if ($env:BENCH_HISTORY_GRAFANA_URL) { $env:BENCH_HISTORY_GRAFANA_URL } else { 'http://localhost:3001' }
$historyVmUrl      = if ($env:BENCH_HISTORY_VM_URL)      { $env:BENCH_HISTORY_VM_URL }      else { 'http://localhost:8428' }

if (-not (Test-Path $historyCompose)) {
    throw "History compose file not found at $historyCompose."
}

# Sanity-check docker is on PATH so the failure mode is one clear line.
if (-not (Get-Command docker -ErrorAction SilentlyContinue)) {
    throw "docker not found on PATH. Install Docker Desktop (or the engine + compose plugin) before running this script."
}

Write-Host "[history] up -d ($historyCompose)" -ForegroundColor Cyan
Push-Location $historyRoot
try {
    & docker compose -f $historyCompose up -d
    if ($LASTEXITCODE -ne 0) {
        throw "docker compose up exited with code $LASTEXITCODE."
    }
}
finally {
    Pop-Location
}

# Poll Grafana's /api/health endpoint - it returns HTTP 200 once provisioning is far enough along to serve.
$grafanaHealth = "$historyGrafanaUrl/api/health"
$deadline = (Get-Date).AddSeconds($TimeoutSeconds)
$ready    = $false
Write-Host "[history] waiting for Grafana at $grafanaHealth (timeout ${TimeoutSeconds}s) ..." -ForegroundColor Cyan
while ((Get-Date) -lt $deadline) {
    try {
        $r = Invoke-WebRequest -Uri $grafanaHealth -UseBasicParsing -TimeoutSec 2 -ErrorAction Stop
        if ($r.StatusCode -eq 200) { $ready = $true; break }
    } catch {
        # Service still booting; swallow and retry.
    }
    Start-Sleep -Milliseconds 750
}

if ($ready) {
    Write-Host "[history] Grafana is ready." -ForegroundColor Green
} else {
    Write-Warning "[history] Grafana did not respond within ${TimeoutSeconds}s. Check 'docker logs vfs-history-grafana'."
}

Write-Host ""
Write-Host "History stack:" -ForegroundColor Green
Write-Host "  VictoriaMetrics: $historyVmUrl" -ForegroundColor Green
Write-Host "  Grafana:         $historyGrafanaUrl  (anonymous viewer; persona dashboards under 'Lattice History - *')" -ForegroundColor Green
Write-Host ""
Write-Host "Stop with: ./benchmark.ps1 -CloseHistory   (named volumes preserved)" -ForegroundColor DarkGray

if ($NoBrowser) {
    Write-Host "[history] -NoBrowser specified; not launching browser." -ForegroundColor DarkGray
    return
}

Write-Host "[history] opening $historyGrafanaUrl in default browser ..." -ForegroundColor Cyan
try {
    Start-Process $historyGrafanaUrl | Out-Null
} catch {
    Write-Warning "Could not launch browser automatically: $($_.Exception.Message)"
    Write-Host "Open this URL manually: $historyGrafanaUrl" -ForegroundColor Yellow
}
