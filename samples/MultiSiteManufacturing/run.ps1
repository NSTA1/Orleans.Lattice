<#
.SYNOPSIS
  Launches the Multi-Site Manufacturing sample (M14: Docker Compose + Traefik).

.DESCRIPTION
  The sample runs as two independent Orleans clusters ("us" +
  "eu"), each with two silos and its own Azurite, all inside
  Docker Compose. A per-cluster Traefik reverse proxy provides sticky-
  session load balancing across the cluster's two silos. Only TWO host
  ports are published:

    http://localhost:5001  traefik-us  -> silo-us-a | silo-us-b
    http://localhost:5002  traefik-eu  -> silo-eu-a | silo-eu-b

  Individual silo HTTP ports (:8080), Orleans silo (:11111) and gateway
  (:30000) ports, and Azurite endpoints live on internal Compose
  networks only (us-net, eu-net, wan). See docker-compose.yml
  for the topology, and plan.md §14 for rationale.

  For the legacy host-process launcher (no Docker), use run-legacy.ps1.

.PARAMETER Down
  Stop and remove all containers, networks, and named volumes.

.PARAMETER Clean
  Wipe any pre-existing containers, networks, and named volumes before
  starting the stack. Use this when you want a fresh run with no seeded
  state carried over from a previous ./run.ps1 invocation. Ignored when
  combined with -Down or -Logs.

.PARAMETER Logs
  Tail logs from all silos (follow mode). Ctrl+C detaches without
  stopping the containers.

.PARAMETER NoBuild
  Skip "docker compose build" — reuse the cached msmfg-host:dev image.

.PARAMETER Service
  Restrict -Logs to a single compose service, e.g. silo-us-a.

.EXAMPLE
  ./run.ps1
    Build the image (if needed), start all services, wait until the two
    Traefik entrypoints accept TCP, then print the two cluster URLs.

.EXAMPLE
  ./run.ps1 -Logs
    Tail logs from every silo until Ctrl+C.

.EXAMPLE
  ./run.ps1 -Down
    Tear everything down including the Azurite volumes (seeded state
    is deleted — next ./run.ps1 will re-seed).

.EXAMPLE
  ./run.ps1 -Clean
    Wipe any previous state, then build and start the stack fresh.
#>
param(
  [switch]$Down,
  [switch]$Logs,
  [switch]$NoBuild,
  [switch]$Clean,
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

    if ($Clean) {
        Write-Host "Clearing pre-existing msmfg state (containers, networks, volumes)..." -ForegroundColor Yellow
        & docker compose down --volumes --remove-orphans
        if ($LASTEXITCODE -ne 0) { throw "docker compose down failed (exit $LASTEXITCODE)." }
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
        @{ Name = "traefik-us (US cluster)"; Port = 5001 }
        @{ Name = "traefik-eu (EU cluster)"; Port = 5002 }
    )

    foreach ($u in $urls) {
        Write-Host -NoNewline "Waiting for $($u.Name) on :$($u.Port)... "
        if (Wait-ForTcpPort -HostName "localhost" -Port $u.Port -TimeoutSeconds 90) {
            Write-Host "ready" -ForegroundColor Green
        } else {
            Write-Host "TIMEOUT" -ForegroundColor Red
            # Traefik itself comes up in ~1s; a timeout here usually means
            # no healthy backend silo, so point the operator at silo logs.
            Write-Host "  Check: docker compose logs traefik-us traefik-eu"
            Write-Host "  Check: docker compose logs silo-us-a silo-us-b silo-eu-a silo-eu-b"
        }
    }

    Write-Host ""
    Write-Host "Cluster URLs (sticky-LB via Traefik):" -ForegroundColor Cyan
    Write-Host "  http://localhost:5001  US cluster (routes to silo-us-a | silo-us-b)"
    Write-Host "  http://localhost:5002  EU cluster (routes to silo-eu-a | silo-eu-b)"
    Write-Host ""
    Write-Host "Useful commands:" -ForegroundColor Cyan
    Write-Host "  ./run.ps1 -Logs                    tail all silo logs"
    Write-Host "  ./run.ps1 -Logs -Service silo-us-a tail one silo"
    Write-Host "  docker compose ps                  show container state"
    Write-Host "  docker network disconnect msmfg_wan msmfg-silo-us-a"
    Write-Host "                                     simulate a cross-cluster partition"
    Write-Host "  ./run.ps1 -Clean                   wipe state, then start fresh"
    Write-Host "  ./run.ps1 -Down                    stop + wipe volumes"
}
finally {
    Pop-Location
}

