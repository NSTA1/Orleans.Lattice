<#
.SYNOPSIS
    Single-parameter runner for the Orleans.Lattice benchmark suite.

.DESCRIPTION
    Stands up the docker-compose stack defined in benchmark/, drives load through
    the Vehicle Fleet Simulator's HTTP API, applies any scenario-specific chaos,
    and tears the stack back down on completion (unless -KeepRunning is passed).

    Each scenario id (B-01..B-12) maps to a scenarios/<id>.env file whose contents
    parameterise the silo (Telemetry:Sink, LatticeSink:*, Replication:*) and the
    runner itself (BENCH_FLEET_SIZE, BENCH_DURATION_SECONDS, BENCH_CHAOS_*).

.PARAMETER Scenario
    The scenario id to run, e.g. "B-03". Case-insensitive.

.PARAMETER KeepRunning
    Leave the stack up after the measurement window so Grafana stays accessible
    at http://localhost:3000.

.EXAMPLE
    ./benchmark.ps1 B-03

.EXAMPLE
    ./benchmark.ps1 -Scenario B-06 -KeepRunning
#>
[CmdletBinding()]
param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string] $Scenario,

    [switch] $KeepRunning
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# ── Paths ───────────────────────────────────────────────────────────────────────
$benchmarkRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot      = Split-Path -Parent $benchmarkRoot
$dashboardSrc  = Join-Path $repoRoot 'src/lattice.dashboards/Grafana'
$dashboardDst  = Join-Path $benchmarkRoot 'grafana/dashboards'
$runDir        = Join-Path $benchmarkRoot '.run'
$scenarioFile  = Join-Path $benchmarkRoot ("scenarios/{0}.env" -f $Scenario.ToUpperInvariant())

if (-not (Test-Path $scenarioFile)) {
    throw "Unknown scenario '$Scenario'. Expected $scenarioFile."
}

# ── Helpers ─────────────────────────────────────────────────────────────────────
function Read-EnvFile {
    param([string] $Path)
    $map = [ordered]@{}
    foreach ($line in Get-Content -Path $Path) {
        $trimmed = $line.Trim()
        if ([string]::IsNullOrWhiteSpace($trimmed)) { continue }
        if ($trimmed.StartsWith('#')) { continue }
        $eq = $trimmed.IndexOf('=')
        if ($eq -lt 1) { continue }
        $key = $trimmed.Substring(0, $eq).Trim()
        $val = $trimmed.Substring($eq + 1).Trim()
        if ($val.StartsWith('"') -and $val.EndsWith('"') -and $val.Length -ge 2) {
            $val = $val.Substring(1, $val.Length - 2)
        }
        $map[$key] = $val
    }
    return $map
}

function Set-ProcessEnv {
    param([System.Collections.IDictionary] $Map)
    foreach ($k in $Map.Keys) {
        Set-Item -Path "Env:$k" -Value $Map[$k]
    }
}

function Wait-ApiReady {
    param([int] $TimeoutSeconds = 120)
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    do {
        try {
            $r = Invoke-WebRequest -Uri 'http://localhost:8080/api/ping/health?message=ready' `
                                   -UseBasicParsing -TimeoutSec 5 -ErrorAction Stop
            if ($r.StatusCode -eq 200) { return }
        } catch {
            Start-Sleep -Seconds 2
        }
    } while ((Get-Date) -lt $deadline)
    throw "API did not become ready within $TimeoutSeconds seconds."
}

function Sync-Dashboards {
    # Copy the embedded Orleans.Lattice.Dashboards JSON into the Grafana mount path.
    # Substitute the ${DS_PROMETHEUS} input placeholder with the provisioned
    # datasource UID (`prometheus`) so dashboards bind without manual selection,
    # and strip the __inputs block which Grafana ignores during provisioning but
    # whose presence triggers a "datasource not found" toast on first load.
    if (-not (Test-Path $dashboardSrc)) {
        Write-Warning "Dashboard source $dashboardSrc not found — skipping copy."
        return
    }
    if (Test-Path $dashboardDst) {
        Get-ChildItem -Path $dashboardDst -Filter *.json -Force | Remove-Item -Force
    } else {
        New-Item -ItemType Directory -Path $dashboardDst -Force | Out-Null
    }

    foreach ($src in Get-ChildItem -Path $dashboardSrc -Filter *.json) {
        $json = Get-Content -Path $src.FullName -Raw
        # 1. Strip the top-level __inputs array (Grafana only honours it on UI import).
        $json = [regex]::Replace($json, '"__inputs"\s*:\s*\[[^\]]*\],?\s*', '', 'Singleline')
        # 2. Substitute ${DS_PROMETHEUS} with the literal UID.
        $json = $json -replace '\$\{DS_PROMETHEUS\}', 'prometheus'
        $dst = Join-Path $dashboardDst $src.Name
        Set-Content -Path $dst -Value $json -Encoding utf8 -NoNewline
    }

    Write-Host "Synced $((Get-ChildItem $dashboardDst -Filter *.json).Count) dashboard(s) into $dashboardDst" -ForegroundColor Green
}

function Invoke-Compose {
    param(
        [string[]] $ComposeFiles,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]] $Args
    )
    $fileArgs = @()
    foreach ($f in $ComposeFiles) { $fileArgs += @('-f', $f) }
    Push-Location $benchmarkRoot
    try {
        & docker compose @fileArgs @Args
        if ($LASTEXITCODE -ne 0) { throw "docker compose $($Args -join ' ') failed (exit $LASTEXITCODE)." }
    } finally {
        Pop-Location
    }
}

function Post-VehicleBatch {
    param([int] $Count)
    # The simulator exposes /api/vehicles/batch which accepts an array of VehicleSpec.
    # An empty spec ({}) generates a vehicle with a server-assigned id, default config,
    # and a default route — the simulator picks a pseudo-random route from the city graph.
    $batchSize = 250
    $remaining = $Count
    $totalCreated = 0
    while ($remaining -gt 0) {
        $n = [Math]::Min($batchSize, $remaining)
        $body = (1..$n | ForEach-Object { @{} }) | ConvertTo-Json -Depth 4 -AsArray
        $r = Invoke-RestMethod -Uri 'http://localhost:8080/api/vehicles/batch' `
                               -Method Post -Body $body -ContentType 'application/json' `
                               -TimeoutSec 60
        $totalCreated += [int]$r.count
        $remaining -= $n
    }
    return $totalCreated
}

function Invoke-Chaos {
    param(
        [string]    $Action,
        [string]    $Target,
        [int]       $AfterSeconds,
        [int]       $DurationSeconds,
        [string[]]  $ComposeFiles
    )
    if ([string]::IsNullOrWhiteSpace($Action) -or $Action -eq 'none') { return }
    Write-Host ("[chaos] sleeping {0}s before {1} on {2}" -f $AfterSeconds, $Action, $Target) -ForegroundColor Yellow
    Start-Sleep -Seconds $AfterSeconds
    switch ($Action) {
        'pause' {
            Invoke-Compose -ComposeFiles $ComposeFiles -Args @('pause', $Target)
            Start-Sleep -Seconds $DurationSeconds
            Invoke-Compose -ComposeFiles $ComposeFiles -Args @('unpause', $Target)
        }
        'kill' {
            Invoke-Compose -ComposeFiles $ComposeFiles -Args @('kill', $Target)
            Start-Sleep -Seconds $DurationSeconds
            Invoke-Compose -ComposeFiles $ComposeFiles -Args @('up', '-d', $Target)
        }
        default {
            throw "Unknown BENCH_CHAOS action '$Action'. Expected pause or kill."
        }
    }
    Write-Host "[chaos] complete" -ForegroundColor Yellow
}

# ── Main ────────────────────────────────────────────────────────────────────────
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Orleans.Lattice benchmark — $Scenario" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

$envMap = Read-EnvFile -Path $scenarioFile
foreach ($k in $envMap.Keys) {
    Write-Host (" {0,-30}= {1}" -f $k, $envMap[$k]) -ForegroundColor DarkGray
}

# B-02 is a pointer to the harness-level micro-benchmark — there is no docker-compose
# topology to stand up.
if ($envMap['BENCH_KIND'] -eq 'microbench') {
    Write-Host ""
    Write-Host ("Scenario {0} runs at the harness layer, not in docker-compose." -f $Scenario) -ForegroundColor Yellow
    Write-Host ("  hint: {0}" -f $envMap['BENCH_HARNESS_HINT']) -ForegroundColor Yellow
    return
}

# Set the env vars in the current process so docker compose (which inherits them)
# substitutes them into docker-compose.yml.
Set-ProcessEnv -Map $envMap

# Pick compose files. Replication scenarios add the overlay.
$composeFiles = @('docker-compose.yml')
if ($envMap['BENCH_REPLICATION_OVERLAY'] -eq 'true') {
    $composeFiles += 'docker-compose.replication.yml'
}

# Ensure scratch dirs exist.
New-Item -ItemType Directory -Path $runDir -Force | Out-Null
Sync-Dashboards

# Bring up the stack.
Write-Host ""
Write-Host "[compose] up --build -d ($($composeFiles -join ', '))" -ForegroundColor Cyan
Invoke-Compose -ComposeFiles $composeFiles -Args @('up', '--build', '-d')

try {
    Wait-ApiReady -TimeoutSeconds 180
    Write-Host "[api] ready" -ForegroundColor Green

    # Seed the fleet and start it.
    $fleetSize  = [int]($envMap['BENCH_FLEET_SIZE'] ?? '2000')
    $warmup     = [int]($envMap['BENCH_WARMUP_SECONDS'] ?? '30')
    $duration   = [int]($envMap['BENCH_DURATION_SECONDS'] ?? '300')
    $chaos      = $envMap['BENCH_CHAOS']
    $chaosTgt   = $envMap['BENCH_CHAOS_TARGET']
    $chaosAfter = [int]($envMap['BENCH_CHAOS_AFTER_SECONDS'] ?? '0')
    $chaosDur   = [int]($envMap['BENCH_CHAOS_DURATION_SECONDS'] ?? '0')

    Write-Host "[load] creating $fleetSize vehicles ..." -ForegroundColor Cyan
    $created = Post-VehicleBatch -Count $fleetSize
    Write-Host "[load] created $created vehicles" -ForegroundColor Green

    Write-Host "[load] start-all" -ForegroundColor Cyan
    Invoke-RestMethod -Uri 'http://localhost:8080/api/vehicles/start-all' -Method Post `
                      -ContentType 'application/json' -TimeoutSec 60 | Out-Null

    Write-Host ("[run] warmup ({0}s) + measurement ({1}s) — Grafana at http://localhost:3000" -f $warmup, $duration) -ForegroundColor Cyan
    Start-Sleep -Seconds $warmup

    $start = Get-Date
    $end   = $start.AddSeconds($duration)

    if ($chaos -and $chaos -ne 'none') {
        # Run chaos in parallel with the measurement window so pause/kill happen mid-run.
        $chaosJob = Start-Job -ScriptBlock {
            param($a, $t, $after, $dur, $files, $bench)
            Set-Location $bench
            $fileArgs = @()
            foreach ($f in $files) { $fileArgs += @('-f', $f) }
            Start-Sleep -Seconds $after
            switch ($a) {
                'pause' {
                    & docker compose @fileArgs pause $t
                    Start-Sleep -Seconds $dur
                    & docker compose @fileArgs unpause $t
                }
                'kill' {
                    & docker compose @fileArgs kill $t
                    Start-Sleep -Seconds $dur
                    & docker compose @fileArgs up -d $t
                }
            }
        } -ArgumentList $chaos, $chaosTgt, $chaosAfter, $chaosDur, $composeFiles, $benchmarkRoot
    }

    while ((Get-Date) -lt $end) {
        Start-Sleep -Seconds 10
        $remaining = [int]($end - (Get-Date)).TotalSeconds
        if ($remaining -lt 0) { break }
        Write-Host ("[run] {0}s remaining" -f $remaining) -ForegroundColor DarkGray
    }

    if ($chaosJob) {
        Wait-Job -Job $chaosJob -Timeout 60 | Out-Null
        Receive-Job -Job $chaosJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "[chaos] $_" -ForegroundColor Yellow }
        Remove-Job -Job $chaosJob -Force
    }

    Write-Host "[load] stop-all" -ForegroundColor Cyan
    try {
        Invoke-RestMethod -Uri 'http://localhost:8080/api/vehicles/stop-all' -Method Post `
                          -ContentType 'application/json' -TimeoutSec 60 | Out-Null
    } catch {
        Write-Warning "stop-all failed: $_"
    }

    # Print fleet stats summary.
    try {
        $stats = Invoke-RestMethod -Uri 'http://localhost:8080/api/fleet/stats' -TimeoutSec 30
        Write-Host ""
        Write-Host "Fleet stats:" -ForegroundColor Cyan
        ($stats | ConvertTo-Json -Depth 4 -Compress) | Write-Host
    } catch {
        Write-Warning "fleet stats unavailable: $_"
    }

    Write-Host ""
    Write-Host "Run complete." -ForegroundColor Green
    Write-Host "Grafana dashboards: http://localhost:3000 (anonymous viewer)." -ForegroundColor Green
    Write-Host "Prometheus:         http://localhost:9090" -ForegroundColor Green
} finally {
    if (-not $KeepRunning) {
        Write-Host ""
        Write-Host "[compose] down -v" -ForegroundColor Cyan
        try { Invoke-Compose -ComposeFiles $composeFiles -Args @('down', '-v') } catch {
            Write-Warning "compose down failed: $_"
        }
    } else {
        Write-Host ""
        Write-Host "Stack left running (`-KeepRunning`). Tear down with:" -ForegroundColor Yellow
        Write-Host ("  docker compose {0} down -v" -f (($composeFiles | ForEach-Object { "-f $_" }) -join ' ')) -ForegroundColor Yellow
    }
}
