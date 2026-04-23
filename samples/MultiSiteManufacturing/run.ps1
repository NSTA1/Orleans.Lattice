<#
.SYNOPSIS
  Launches the Multi-Site Manufacturing sample (M14: Docker Compose).

.DESCRIPTION
  The sample runs as two independent Orleans clusters ("forge" +
  "heattreat"), each with two silos and its own Azurite, all inside
  Docker Compose. Only HTTP ports 5001..5004 are exposed to the host:

    http://localhost:5001  silo-forge-a      (forge cluster, UI + gRPC)
    http://localhost:5002  silo-forge-b      (forge cluster, UI + gRPC)
    http://localhost:5003  silo-heattreat-a  (heattreat cluster)
    http://localhost:5004  silo-heattreat-b  (heattreat cluster)

  Orleans silo + gateway traffic and Azurite endpoints live on internal
  Compose networks only (forge-net, heattreat-net, wan). See
  docker-compose.yml for the topology, and plan.md §14 for rationale.

  For the legacy host-process launcher (no Docker), use run-legacy.ps1.

.PARAMETER Down
  Stop and remove all containers, networks, and named volumes.

.PARAMETER Logs
  Tail logs from all silos (follow mode). Ctrl+C detaches without
  stopping the containers.

.PARAMETER NoBuild
  Skip "docker compose build" — reuse the cached msmfg-host:dev image.

.PARAMETER Service
  Restrict -Logs to a single compose service, e.g. silo-forge-a.

.EXAMPLE
  ./run.ps1
    Build the image (if needed), start all services, wait until healthy,
    then print the four host URLs and exit. Containers keep running.

.EXAMPLE
  ./run.ps1 -Logs
    Tail logs from every silo until Ctrl+C.

.EXAMPLE
  ./run.ps1 -Down
    Tear everything down including the Azurite volumes (seeded state
    is deleted — next ./run.ps1 will re-seed).
#>
param(
  [switch]$Down,
  [switch]$Logs,
  [switch]$NoBuild,
  [string]$Service
)

$ErrorActionPreference = "Stop"

# Always run compose from this script's directory so docker-compose.yml,
# Dockerfile, and the relative build context (../../) resolve correctly.
Push-Location $PSScriptRoot
try {
    # Sanity-check: docker must be on PATH and the daemon reachable.
    $dockerCmd = Get-Command docker -ErrorAction SilentlyContinue
    if (-not $dockerCmd) {
        throw "docker is not on PATH. Install Docker Desktop (Windows) or the Docker engine."
    }
    & docker info --format "{{.ServerVersion}}" 2>$null | Out-Null
    if ($LASTEXITCODE -ne 0) {
        throw "docker daemon is not reachable. Start Docker Desktop and retry."
    }

    if ($Down) {
        Write-Host "Stopping msmfg stack and removing volumes..." -ForegroundColor Yellow
        & docker compose down --volumes --remove-orphans
        return
    }

    if ($Logs) {
        if ($Service) {
            & docker compose logs -f $Service
        } else {
            & docker compose logs -f
        }
        return
    }

    if (-not $NoBuild) {
        Write-Host "Building msmfg-host:dev image (docker compose build)..." -ForegroundColor Cyan
        & docker compose build
        if ($LASTEXITCODE -ne 0) { throw "docker compose build failed (exit $LASTEXITCODE)." }
    }

    Write-Host "Starting msmfg stack (docker compose up -d)..." -ForegroundColor Cyan
    & docker compose up -d
    if ($LASTEXITCODE -ne 0) { throw "docker compose up failed (exit $LASTEXITCODE)." }

    # Poll the published ports on the host side until they accept TCP.
    # Each silo opens its HTTP listener well before the Orleans cluster
    # membership settles, so this is a minimal "process is alive" probe,
    # not a readiness gate. Full cluster bootstrap (replication reminders,
    # seed run) can take another 10-30 seconds and is visible in logs.
    function Wait-ForTcpPort {
        param([string]$HostName, [int]$Port, [int]$TimeoutSeconds = 60)
        $sw = [System.Diagnostics.Stopwatch]::StartNew()
        while ($sw.Elapsed.TotalSeconds -lt $TimeoutSeconds) {
            try {
                $client = [System.Net.Sockets.TcpClient]::new()
                $iar = $client.BeginConnect($HostName, $Port, $null, $null)
                if ($iar.AsyncWaitHandle.WaitOne(1000)) {
                    $client.EndConnect($iar)
                    $client.Close()
                    return $true
                }
                $client.Close()
            } catch { Start-Sleep -Milliseconds 500 }
        }
        return $false
    }

    $urls = @(
        @{ Name = "silo-forge-a";      Port = 5001 }
        @{ Name = "silo-forge-b";      Port = 5002 }
        @{ Name = "silo-heattreat-a";  Port = 5003 }
        @{ Name = "silo-heattreat-b";  Port = 5004 }
    )

    foreach ($u in $urls) {
        Write-Host -NoNewline "Waiting for $($u.Name) on :$($u.Port)... "
        if (Wait-ForTcpPort -HostName "localhost" -Port $u.Port -TimeoutSeconds 90) {
            Write-Host "ready" -ForegroundColor Green
        } else {
            Write-Host "TIMEOUT" -ForegroundColor Red
            Write-Host "  Check: docker compose logs $($u.Name)"
        }
    }

    Write-Host ""
    Write-Host "Dashboard URLs:" -ForegroundColor Cyan
    foreach ($u in $urls) {
        Write-Host ("  http://localhost:{0}  ({1})" -f $u.Port, $u.Name)
    }
    Write-Host ""
    Write-Host "Useful commands:" -ForegroundColor Cyan
    Write-Host "  ./run.ps1 -Logs                       tail all silo logs"
    Write-Host "  ./run.ps1 -Logs -Service silo-forge-a tail one silo"
    Write-Host "  docker compose ps                     show container state"
    Write-Host "  docker network disconnect msmfg_wan msmfg-silo-forge-a"
    Write-Host "                                        simulate a cross-cluster partition"
    Write-Host "  ./run.ps1 -Down                       stop + wipe volumes"
}
finally {
    Pop-Location
}
