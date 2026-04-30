<#
.SYNOPSIS
    Single-parameter runner for the Orleans.Lattice benchmark suite, plus
    cross-run comparison and history-archive pushing modes.

.DESCRIPTION
    Default mode (with -Scenario): stands up the docker-compose stack defined
    in benchmark/, drives load through the Vehicle Fleet Simulator's HTTP API,
    captures a fixed panel of summary scalars from Prometheus before teardown,
    writes them to .run/<scenario>/<runId>/results.json, and (if reachable)
    opportunistically pushes them into the history VictoriaMetrics so the
    cross-run trend dashboard fills in over time.

    -Compare:         aggregate every .run/B-*/*/results.json into a Markdown +
                      CSV summary at .run/comparison.{md,csv}.
    -CompareAgainst:  add a "Δ vs. <baseline>" column to the comparison output
                      (the simulator-baseline delta the plan calls for).
    -ImportHistory:   bulk-import every .run/**/results.json into the history
                      VictoriaMetrics (idempotent; dedupes by run_id label).
    -OpenHistory:     stand up the history docker-compose stack and print URLs.
    -CloseHistory:    tear the history stack down (volumes preserved).

    Each scenario id (B-01..B-12) maps to a scenarios/<id>.env file whose
    contents parameterise the silo (Telemetry:Sink, LatticeSink:*, Replication:*)
    and the runner itself (BENCH_FLEET_SIZE, BENCH_DURATION_SECONDS, BENCH_CHAOS_*).

.PARAMETER Scenario
    The scenario id to run, e.g. "B-03". Case-insensitive.

.PARAMETER KeepRunning
    Leave the per-run stack up after the measurement window so Grafana stays
    accessible at http://localhost:3000.

.PARAMETER Compare
    Aggregate previously-recorded results (no scenario run).

.PARAMETER CompareAgainst
    When -Compare is set, baseline scenario id (e.g. B-01) for delta columns.

.PARAMETER ImportHistory
    Backfill every .run/**/results.json into the history VictoriaMetrics.

.PARAMETER OpenHistory
    Stand up the history docker-compose stack (VictoriaMetrics + Grafana :3001).

.PARAMETER CloseHistory
    Stop the history stack (named volumes preserved across stops).

.EXAMPLE
    ./benchmark.ps1 B-03

.EXAMPLE
    ./benchmark.ps1 -Scenario B-06 -KeepRunning

.EXAMPLE
    ./benchmark.ps1 -Compare -CompareAgainst B-01

.EXAMPLE
    ./benchmark.ps1 -OpenHistory; ./benchmark.ps1 B-03; ./benchmark.ps1 B-04
#>
[CmdletBinding(DefaultParameterSetName = 'Run')]
param(
    [Parameter(ParameterSetName = 'Run', Mandatory = $true, Position = 0)]
    [string] $Scenario,

    [Parameter(ParameterSetName = 'Run')]
    [switch] $KeepRunning,

    [Parameter(ParameterSetName = 'Compare', Mandatory = $true)]
    [switch] $Compare,

    [Parameter(ParameterSetName = 'Compare')]
    [string] $CompareAgainst,

    [Parameter(ParameterSetName = 'Import', Mandatory = $true)]
    [switch] $ImportHistory,

    [Parameter(ParameterSetName = 'Open', Mandatory = $true)]
    [switch] $OpenHistory,

    [Parameter(ParameterSetName = 'Close', Mandatory = $true)]
    [switch] $CloseHistory
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# ── Paths ───────────────────────────────────────────────────────────────────────
$benchmarkRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot      = Split-Path -Parent $benchmarkRoot
$dashboardSrc  = Join-Path $repoRoot 'src/lattice.dashboards/Grafana'
$dashboardDst  = Join-Path $benchmarkRoot 'grafana/dashboards'
$runDir        = Join-Path $benchmarkRoot '.run'
$historyRoot   = Join-Path $benchmarkRoot 'history'
$historyCompose = Join-Path $historyRoot 'docker-compose.history.yml'

# ── Endpoints (defaults; overridable via env if needed) ─────────────────────────
$prometheusUrl = $env:BENCH_PROMETHEUS_URL ?? 'http://localhost:9090'
$apiUrl        = $env:BENCH_API_URL        ?? 'http://localhost:8080'
$historyVmUrl  = $env:BENCH_HISTORY_VM_URL ?? 'http://localhost:8428'
$historyGrafanaUrl = $env:BENCH_HISTORY_GRAFANA_URL ?? 'http://localhost:3001'

# ── Fixed scalar metric panel (the contract for results.json) ───────────────────
#
# Each entry is (key, promql). The script substitutes {Ws} with the duration in
# seconds. Missing series resolve to $null in the JSON so panels that don't apply
# to a given scenario (e.g. replication metrics under a non-replication run) are
# absent rather than zero — zero would lie about "0 ms apply lag".
$ScalarPanel = [ordered]@{
    # ── Lattice commit path ─────────────────────────────────────────────────────
    'lattice_commit_p50_ms' =
        'histogram_quantile(0.50, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket[{Ws}s])))'
    'lattice_commit_p95_ms' =
        'histogram_quantile(0.95, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket[{Ws}s])))'
    'lattice_commit_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket[{Ws}s])))'
    'lattice_commits_per_second' =
        'sum(rate(orleans_lattice_leaf_commit_duration_milliseconds_count[{Ws}s]))'
    'lattice_shard_writes_per_second' =
        'sum(rate(orleans_lattice_shard_writes_total[{Ws}s]))'
    'lattice_shard_reads_per_second' =
        'sum(rate(orleans_lattice_shard_reads_total[{Ws}s]))'

    # ── Lattice events / cache ──────────────────────────────────────────────────
    'lattice_events_published_per_second' =
        'sum(rate(orleans_lattice_events_published_total[{Ws}s]))'
    'lattice_events_dropped_total' =
        'sum(increase(orleans_lattice_events_dropped_total[{Ws}s]))'
    'lattice_cache_hit_ratio' =
        'sum(rate(orleans_lattice_cache_hits_total[{Ws}s])) / clamp_min(sum(rate(orleans_lattice_cache_hits_total[{Ws}s]) + rate(orleans_lattice_cache_misses_total[{Ws}s])), 1)'
    'lattice_cache_hits_total' =
        'sum(increase(orleans_lattice_cache_hits_total[{Ws}s]))'
    'lattice_cache_misses_total' =
        'sum(increase(orleans_lattice_cache_misses_total[{Ws}s]))'

    # ── LatticeSink (Bench.Sink) ────────────────────────────────────────────────
    'sink_published_per_second' =
        'sum(rate(vehicle_fleet_simulator_sink_published_total[{Ws}s]))'
    'sink_dropped_total' =
        'sum(increase(vehicle_fleet_simulator_sink_dropped_total[{Ws}s])) + sum(increase(vehicle_fleet_simulator_sink_dropped_on_shutdown_total[{Ws}s]))'
    'sink_queue_depth_max' =
        'max_over_time(vehicle_fleet_simulator_sink_queue_depth[{Ws}s])'
    'sink_flush_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(vehicle_fleet_simulator_sink_flush_duration_ms_milliseconds_bucket[{Ws}s])))'

    # ── Replication ─────────────────────────────────────────────────────────────
    'replication_wal_appends_per_second' =
        'sum(rate(orleans_lattice_replication_wal_entries_appended_total[{Ws}s]))'
    'replication_apply_lag_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_replication_apply_lag_milliseconds_bucket[{Ws}s])))'
    'replication_entries_behind_max' =
        'max(max_over_time(orleans_lattice_replication_peer_entries_behind[{Ws}s]))'
    'replication_peer_fell_off_log_total' =
        'sum(increase(orleans_lattice_replication_peer_fell_off_log_total[{Ws}s]))'

    # ── Process / runtime ───────────────────────────────────────────────────────
    'process_cpu_seconds_total' =
        'sum(increase(dotnet_process_cpu_time_seconds_total[{Ws}s]))'
    'process_working_set_bytes_p95' =
        'quantile_over_time(0.95, sum(dotnet_process_memory_working_set_bytes)[{Ws}s:5s])'
    'dotnet_gc_gen2_collections' =
        'sum(increase(dotnet_gc_collections_total{gc_heap_generation="gen2"}[{Ws}s]))'
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
            $r = Invoke-WebRequest -Uri "$apiUrl/api/ping/health?message=ready" `
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
        [string]   $Cwd,
        [Parameter(ValueFromRemainingArguments = $true)]
        [string[]] $Args
    )
    $fileArgs = @()
    foreach ($f in $ComposeFiles) { $fileArgs += @('-f', $f) }
    Push-Location ($Cwd ?? $benchmarkRoot)
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
        $r = Invoke-RestMethod -Uri "$apiUrl/api/vehicles/batch" `
                               -Method Post -Body $body -ContentType 'application/json' `
                               -TimeoutSec 60
        $totalCreated += [int]$r.count
        $remaining -= $n
    }
    return $totalCreated
}

# ── Prometheus query / results.json capture ─────────────────────────────────────

function Invoke-PromInstantQuery {
    # Returns the first scalar value as [double], or $null if no series matched
    # or the query errored. We never throw — a missing metric is a normal outcome
    # for scenarios that don't exercise that subsystem.
    param([string] $Query)
    try {
        $uri  = "$prometheusUrl/api/v1/query?query=$([uri]::EscapeDataString($Query))"
        $resp = Invoke-RestMethod -Uri $uri -TimeoutSec 15 -ErrorAction Stop
        if ($resp.status -ne 'success') { return $null }
        $r = $resp.data.result
        if (-not $r -or $r.Count -eq 0) { return $null }
        $v = $r[0].value[1]
        if ([string]::IsNullOrEmpty($v)) { return $null }
        # Prometheus returns "NaN" as a string for invalid percentile-over-empty,
        # division-by-zero, etc. Normalise to $null.
        if ($v -eq 'NaN' -or $v -eq '+Inf' -or $v -eq '-Inf') { return $null }
        $d = 0.0
        if ([double]::TryParse($v, [Globalization.NumberStyles]::Float, [Globalization.CultureInfo]::InvariantCulture, [ref] $d)) {
            return $d
        }
        return $null
    } catch {
        return $null
    }
}

function Get-ScalarMetrics {
    param(
        [int] $WindowSeconds,
        [System.Collections.IDictionary] $Panel
    )
    $out = [ordered]@{}
    foreach ($key in $Panel.Keys) {
        $promQl = $Panel[$key].Replace('{Ws}', $WindowSeconds.ToString([Globalization.CultureInfo]::InvariantCulture))
        $val = Invoke-PromInstantQuery -Query $promQl
        $out[$key] = $val
    }
    return $out
}

function Get-GitSha {
    try {
        Push-Location $repoRoot
        $sha = (& git rev-parse --short HEAD 2>$null)
        if ($LASTEXITCODE -eq 0 -and -not [string]::IsNullOrWhiteSpace($sha)) {
            return $sha.Trim()
        }
    } catch {} finally { Pop-Location -ErrorAction SilentlyContinue }
    return 'unknown'
}

function Save-RunResults {
    param(
        [string]                          $ScenarioId,
        [string]                          $RunId,
        [System.Collections.IDictionary]  $Config,
        [datetime]                        $Started,
        [datetime]                        $Ended,
        [System.Collections.IDictionary]  $Metrics,
        $FleetStats
    )
    $scenarioDir = Join-Path $runDir $ScenarioId
    $runResultDir = Join-Path $scenarioDir $RunId
    New-Item -ItemType Directory -Path $runResultDir -Force | Out-Null
    $payload = [ordered]@{
        scenario   = $ScenarioId
        run_id     = $RunId
        git_sha    = Get-GitSha
        started    = $Started.ToUniversalTime().ToString('o')
        ended      = $Ended.ToUniversalTime().ToString('o')
        duration_s = [int]($Ended - $Started).TotalSeconds
        config     = $Config
        metrics    = $Metrics
        fleetStats = $FleetStats
    }
    $resultsPath = Join-Path $runResultDir 'results.json'
    $json = ConvertTo-Json -InputObject $payload -Depth 6
    Set-Content -Path $resultsPath -Value $json -Encoding utf8
    return $resultsPath
}

# ── History VM push ─────────────────────────────────────────────────────────────

function Test-HistoryVmReachable {
    try {
        $r = Invoke-WebRequest -Uri "$historyVmUrl/health" -UseBasicParsing -TimeoutSec 3 -ErrorAction Stop
        return $r.StatusCode -eq 200
    } catch {
        return $false
    }
}

function ConvertTo-PromExposition {
    # Convert one results.json payload into Prometheus text-exposition format.
    # Each scalar metric becomes one gauge sample tagged with scenario, run_id,
    # git_sha. NaN / null values are dropped (Prometheus exposition can't carry
    # null; the absence is the signal).
    param([Parameter(Mandatory = $true)] $Payload)
    $sb = [System.Text.StringBuilder]::new()
    $scenario = $Payload.scenario
    $runId    = $Payload.run_id
    $gitSha   = $Payload.git_sha
    # ConvertFrom-Json auto-coerces ISO8601 strings to [DateTime]; passing that
    # to [datetimeoffset]::Parse would round-trip through current-culture
    # ToString() and fail on locales whose short-date pattern doesn't match
    # what Parse expects. Handle both shapes deterministically.
    $endedDto = if ($Payload.ended -is [datetime]) {
        [datetimeoffset]::new([datetime]::SpecifyKind($Payload.ended, [System.DateTimeKind]::Utc))
    } else {
        [datetimeoffset]::Parse(
            [string]$Payload.ended,
            [System.Globalization.CultureInfo]::InvariantCulture,
            [System.Globalization.DateTimeStyles]::RoundtripKind)
    }
    $tsMs = [int64]$endedDto.ToUnixTimeMilliseconds()
    foreach ($prop in $Payload.metrics.PSObject.Properties) {
        $val = $prop.Value
        if ($null -eq $val) { continue }
        # Sanitise label values — VM accepts the same escaping rules as Prometheus.
        $name = "bench_$($prop.Name)"
        $line = '{0}{{scenario="{1}",run_id="{2}",git_sha="{3}"}} {4} {5}' -f `
            $name, $scenario, $runId, $gitSha, $val, $tsMs
        [void]$sb.AppendLine($line)
    }
    return $sb.ToString()
}

function Push-HistoryResults {
    param([Parameter(Mandatory = $true)] [string] $ResultsPath)
    if (-not (Test-HistoryVmReachable)) {
        Write-Host "[history] VM at $historyVmUrl unreachable — skipping push (results.json archived locally)" -ForegroundColor DarkYellow
        return $false
    }
    try {
        $payload = Get-Content -Path $ResultsPath -Raw | ConvertFrom-Json
        $body = ConvertTo-PromExposition -Payload $payload
        if ([string]::IsNullOrWhiteSpace($body)) {
            Write-Host "[history] no non-null metrics to push for $($payload.scenario)/$($payload.run_id)" -ForegroundColor DarkYellow
            return $false
        }
        Invoke-RestMethod -Uri "$historyVmUrl/api/v1/import/prometheus" `
                          -Method Post -Body $body -ContentType 'text/plain' -TimeoutSec 30 | Out-Null
        Write-Host ("[history] pushed {0}/{1} ({2} samples)" -f `
            $payload.scenario, $payload.run_id, ($body -split "`n").Count) -ForegroundColor Green
        return $true
    } catch {
        Write-Warning "[history] push failed: $_"
        return $false
    }
}

# ── Comparison ──────────────────────────────────────────────────────────────────

function Get-AllResults {
    if (-not (Test-Path $runDir)) { return @() }
    Get-ChildItem -Path $runDir -Recurse -Filter 'results.json' -ErrorAction SilentlyContinue | ForEach-Object {
        try { Get-Content $_.FullName -Raw | ConvertFrom-Json } catch { Write-Warning "Skipping unparseable $($_.FullName): $_" }
    } | Where-Object { $_ -and $_.scenario -and $_.run_id }
}

function Get-LatestPerScenario {
    Get-AllResults |
        Group-Object -Property scenario |
        ForEach-Object {
            $_.Group | Sort-Object -Property ended -Descending | Select-Object -First 1
        } |
        Sort-Object -Property scenario
}

function Format-MetricCell {
    param($Value)
    if ($null -eq $Value) { return '–' }
    if ($Value -is [double]) {
        if ($Value -ge 1000) { return ('{0:N0}' -f $Value) }
        if ($Value -ge 10)   { return ('{0:N1}' -f $Value) }
        return ('{0:N3}' -f $Value)
    }
    return [string] $Value
}

function Format-DeltaCell {
    param($Current, $Baseline)
    if ($null -eq $Current -or $null -eq $Baseline) { return '–' }
    $delta = $Current - $Baseline
    if ($Baseline -eq 0) { return ('{0}' -f (Format-MetricCell $delta)) }
    $pct = ($delta / $Baseline) * 100.0
    $sign = if ($delta -ge 0) { '+' } else { '' }
    return ('{0}{1} ({2}{3:N1}%)' -f $sign, (Format-MetricCell $delta), $sign, $pct)
}

function Invoke-Compare {
    param([string] $Baseline)
    $results = Get-LatestPerScenario
    if (-not $results -or $results.Count -eq 0) {
        Write-Warning "No results found under $runDir. Run a scenario first."
        return
    }

    $baselineRun = $null
    if ($Baseline) {
        $baselineRun = $results | Where-Object { $_.scenario -ieq $Baseline } | Select-Object -First 1
        if (-not $baselineRun) {
            Write-Warning "Baseline scenario '$Baseline' has no recorded run; delta column will be omitted."
        }
    }

    $metricKeys = $ScalarPanel.Keys
    $hasDelta   = $null -ne $baselineRun

    # Markdown ─────────────────────────────────────────────────────────────────
    $md = [System.Text.StringBuilder]::new()
    [void]$md.AppendLine("# Benchmark comparison")
    [void]$md.AppendLine()
    [void]$md.AppendLine("Generated $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss zzz') from $($results.Count) recorded runs.")
    if ($hasDelta) { [void]$md.AppendLine("Delta column compares against **$Baseline** ($($baselineRun.run_id)).") }
    [void]$md.AppendLine()
    foreach ($metric in $metricKeys) {
        [void]$md.AppendLine("## ``$metric``")
        [void]$md.AppendLine()
        $hdr = if ($hasDelta) { "| Scenario | Run id | Value | Δ vs. $Baseline |" } else { "| Scenario | Run id | Value |" }
        $sep = if ($hasDelta) { "|---|---|---:|---:|" }                      else { "|---|---|---:|" }
        [void]$md.AppendLine($hdr)
        [void]$md.AppendLine($sep)
        foreach ($r in $results) {
            $val = $r.metrics.$metric
            $row = "| {0} | {1} | {2} |" -f $r.scenario, $r.run_id, (Format-MetricCell $val)
            if ($hasDelta) {
                $base = $baselineRun.metrics.$metric
                $row += " {0} |" -f (Format-DeltaCell -Current $val -Baseline $base)
            }
            [void]$md.AppendLine($row)
        }
        [void]$md.AppendLine()
    }
    $mdPath = Join-Path $runDir 'comparison.md'
    Set-Content -Path $mdPath -Value $md.ToString() -Encoding utf8
    Write-Host "Wrote $mdPath" -ForegroundColor Green

    # CSV ──────────────────────────────────────────────────────────────────────
    $csvRows = @()
    foreach ($r in $results) {
        $row = [ordered]@{
            scenario = $r.scenario
            run_id   = $r.run_id
            git_sha  = $r.git_sha
            ended    = $r.ended
        }
        foreach ($metric in $metricKeys) {
            $row[$metric] = $r.metrics.$metric
        }
        $csvRows += [pscustomobject]$row
    }
    $csvPath = Join-Path $runDir 'comparison.csv'
    $csvRows | Export-Csv -Path $csvPath -NoTypeInformation -Encoding utf8
    Write-Host "Wrote $csvPath" -ForegroundColor Green

    # Console preview ──────────────────────────────────────────────────────────
    Write-Host ""
    Write-Host "Latest results (one row per scenario):" -ForegroundColor Cyan
    $csvRows | Format-Table -Property scenario, run_id, lattice_commit_p99_ms, sink_published_per_second, sink_dropped_total, replication_apply_lag_p99_ms -AutoSize
}

function Invoke-ImportHistory {
    if (-not (Test-HistoryVmReachable)) {
        Write-Warning "History VM at $historyVmUrl unreachable. Bring it up with: ./benchmark.ps1 -OpenHistory"
        return
    }
    $files = if (Test-Path $runDir) { Get-ChildItem -Path $runDir -Recurse -Filter 'results.json' -ErrorAction SilentlyContinue } else { @() }
    if (-not $files -or $files.Count -eq 0) {
        Write-Warning "No results files found under $runDir."
        return
    }
    Write-Host "Importing $($files.Count) result file(s) into history VM ..." -ForegroundColor Cyan
    $ok = 0; $skip = 0
    foreach ($f in $files) {
        if (Push-HistoryResults -ResultsPath $f.FullName) { $ok++ } else { $skip++ }
    }
    Write-Host "Done: $ok pushed, $skip skipped." -ForegroundColor Green
}

function Invoke-OpenHistory {
    if (-not (Test-Path $historyCompose)) {
        throw "History compose file not found at $historyCompose."
    }
    Write-Host "[history] up -d ($historyCompose)" -ForegroundColor Cyan
    Invoke-Compose -ComposeFiles @($historyCompose) -Cwd $historyRoot -Args @('up', '-d')
    Write-Host ""
    Write-Host "History stack:" -ForegroundColor Green
    Write-Host "  VictoriaMetrics: $historyVmUrl" -ForegroundColor Green
    Write-Host "  Grafana:         $historyGrafanaUrl  (anonymous viewer)" -ForegroundColor Green
    Write-Host "Use ./benchmark.ps1 -ImportHistory to backfill existing .run/**/results.json." -ForegroundColor DarkGray
    Write-Host "Use ./benchmark.ps1 -CloseHistory to stop (named volumes are preserved)." -ForegroundColor DarkGray
}

function Invoke-CloseHistory {
    if (-not (Test-Path $historyCompose)) {
        throw "History compose file not found at $historyCompose."
    }
    Write-Host "[history] down ($historyCompose) — volumes preserved" -ForegroundColor Cyan
    Invoke-Compose -ComposeFiles @($historyCompose) -Cwd $historyRoot -Args @('down')
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
switch ($PSCmdlet.ParameterSetName) {
    'Compare' { Invoke-Compare -Baseline $CompareAgainst; return }
    'Import'  { Invoke-ImportHistory; return }
    'Open'    { Invoke-OpenHistory; return }
    'Close'   { Invoke-CloseHistory; return }
}

# Default: run a scenario.
$scenarioFile = Join-Path $benchmarkRoot ("scenarios/{0}.env" -f $Scenario.ToUpperInvariant())
if (-not (Test-Path $scenarioFile)) {
    throw "Unknown scenario '$Scenario'. Expected $scenarioFile."
}

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

# Pre-compute the run id used for results.json placement and the history `run_id` label.
# Use UTC ISO8601 with `:` → `-` so it survives Windows path constraints.
$runId = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH-mm-ssZ')

# Bring up the stack.
Write-Host ""
Write-Host "[compose] up --build -d ($($composeFiles -join ', '))" -ForegroundColor Cyan
Invoke-Compose -ComposeFiles $composeFiles -Args @('up', '--build', '-d')

$runStart = Get-Date
$runEnd   = $runStart   # placeholder; updated after measurement window
$capturedMetrics = $null
$capturedFleet   = $null

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
    Invoke-RestMethod -Uri "$apiUrl/api/vehicles/start-all" -Method Post `
                      -ContentType 'application/json' -TimeoutSec 60 | Out-Null

    Write-Host ("[run] warmup ({0}s) + measurement ({1}s) - Grafana at http://localhost:3000" -f $warmup, $duration) -ForegroundColor Cyan
    Start-Sleep -Seconds $warmup

    $measureStart = Get-Date
    $measureEnd   = $measureStart.AddSeconds($duration)

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

    while ((Get-Date) -lt $measureEnd) {
        Start-Sleep -Seconds 10
        $remaining = [int]($measureEnd - (Get-Date)).TotalSeconds
        if ($remaining -lt 0) { break }
        Write-Host ("[run] {0}s remaining" -f $remaining) -ForegroundColor DarkGray
    }

    if ($chaosJob) {
        Wait-Job -Job $chaosJob -Timeout 60 | Out-Null
        Receive-Job -Job $chaosJob -ErrorAction SilentlyContinue | ForEach-Object { Write-Host "[chaos] $_" -ForegroundColor Yellow }
        Remove-Job -Job $chaosJob -Force
    }

    $runEnd = Get-Date

    # ── Capture summary scalars from Prometheus while the stack is still up. ──
    Write-Host "[capture] querying Prometheus over the ${duration}s measurement window ..." -ForegroundColor Cyan
    $capturedMetrics = Get-ScalarMetrics -WindowSeconds $duration -Panel $ScalarPanel
    $nonNull = ($capturedMetrics.Values | Where-Object { $null -ne $_ }).Count
    Write-Host ("[capture] {0}/{1} metrics populated" -f $nonNull, $capturedMetrics.Count) -ForegroundColor Green

    Write-Host "[load] stop-all" -ForegroundColor Cyan
    try {
        Invoke-RestMethod -Uri "$apiUrl/api/vehicles/stop-all" -Method Post `
                          -ContentType 'application/json' -TimeoutSec 60 | Out-Null
    } catch {
        Write-Warning "stop-all failed: $_"
    }

    # Print fleet stats summary and capture for results.json.
    try {
        $capturedFleet = Invoke-RestMethod -Uri "$apiUrl/api/fleet/stats" -TimeoutSec 30
        Write-Host ""
        Write-Host "Fleet stats:" -ForegroundColor Cyan
        ($capturedFleet | ConvertTo-Json -Depth 4 -Compress) | Write-Host
    } catch {
        Write-Warning "fleet stats unavailable: $_"
    }

    Write-Host ""
    Write-Host "Run complete." -ForegroundColor Green
    Write-Host "Grafana dashboards: http://localhost:3000 (anonymous viewer)." -ForegroundColor Green
    Write-Host "Prometheus:         http://localhost:9090" -ForegroundColor Green
} finally {
    # ── Persist results.json (always, even if the run failed mid-window). ──
    if ($capturedMetrics) {
        $resultsPath = Save-RunResults `
            -ScenarioId $Scenario.ToUpperInvariant() `
            -RunId      $runId `
            -Config     $envMap `
            -Started    $runStart `
            -Ended      $runEnd `
            -Metrics    $capturedMetrics `
            -FleetStats $capturedFleet
        Write-Host ""
        Write-Host "Results: $resultsPath" -ForegroundColor Green

        # Opportunistic push to history VM (non-fatal if VM is down).
        Push-HistoryResults -ResultsPath $resultsPath | Out-Null
    } else {
        Write-Warning "No metrics captured (run did not reach measurement window). Skipping results.json."
    }

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
