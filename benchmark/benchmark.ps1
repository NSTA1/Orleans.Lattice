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

    Each scenario id maps to a scenarios/<slug>.env file whose contents
    parameterise the silo (Telemetry:Sink, LatticeSink:*, Replication:*) and the
    runner itself (BENCH_FLEET_SIZE, BENCH_DURATION_SECONDS, BENCH_CHAOS_*).

.PARAMETER Scenario
    The scenario id to run, e.g. "current-state-no-replication". Slug must match
    a scenarios/<slug>.env filename (case-sensitive, kebab-case).

.PARAMETER KeepRunning
    Leave the per-run stack up after the measurement window so Grafana stays
    accessible at http://localhost:3000.

.PARAMETER NoBuild
    Skip the `--build` step when standing up the docker-compose stack. Use this
    when the images have already been built by an earlier run in the same
    session (e.g. when benchmark-all.ps1 sweeps every scenario back-to-back) to
    avoid rebuilding on every iteration.

.PARAMETER FleetSizeOverride
    Override BENCH_FLEET_SIZE (and the .fleet-size config calibrated value) for
    a single run. Used by initialise.ps1 to sweep the saturation ladder, but
    also handy for one-off probes. Zero (default) means "no override".

.PARAMETER SkipFleetSizeCheck
    Skip the precondition that .fleet-size.config exists. Used by
    initialise.ps1 (which is the script that creates the config in the first
    place) and for ad-hoc runs that intentionally bypass calibration. The
    microbench scenario always skips this check because it doesn't drive any
    fleet.

.PARAMETER NoHistoryPush
    Skip the opportunistic push of results.json into the history VictoriaMetrics.
    Used by initialise.ps1 so calibration sweep data does not pollute the
    long-lived regression-trend dashboards.

.PARAMETER Compare
    Aggregate every .run/B-*/*/results.json into a Markdown + CSV summary at
    .run/comparison.{md,csv}.

.PARAMETER CompareAgainst
    Add a "Δ vs. <baseline>" column to the comparison output (the
    simulator-baseline delta the plan calls for). Requires -Compare.

.PARAMETER ImportHistory
    Bulk-import every .run/**/results.json into the history VictoriaMetrics
    (idempotent; dedupes by run_id label).

.PARAMETER OpenHistory
    Stand up the history docker-compose stack and print URLs.

.PARAMETER CloseHistory
    Tear the history stack down (volumes preserved).

.EXAMPLE
    ./benchmark.ps1 current-state-no-replication

.EXAMPLE
    ./benchmark.ps1 -Scenario replication-backpressure -KeepRunning

.EXAMPLE
    ./benchmark.ps1 -Compare -CompareAgainst simulator-baseline

.EXAMPLE
    ./benchmark.ps1 -OpenHistory; ./benchmark.ps1 current-state-no-replication; ./benchmark.ps1 current-state-single-peer
#>
[CmdletBinding(DefaultParameterSetName = 'Run')]
param(
    [Parameter(ParameterSetName = 'Run', Mandatory = $true, Position = 0)]
    [string] $Scenario,

    [Parameter(ParameterSetName = 'Run')]
    [switch] $KeepRunning,

    [Parameter(ParameterSetName = 'Run')]
    [switch] $NoBuild,

    [Parameter(ParameterSetName = 'Run')]
    [int] $FleetSizeOverride = 0,

    [Parameter(ParameterSetName = 'Run')]
    [switch] $SkipFleetSizeCheck,

    [Parameter(ParameterSetName = 'Run')]
    [switch] $NoHistoryPush,

    # Microbench-only: override BENCH_MICROBENCH_WORKLOADS for this invocation.
    # Comma-separated BDN --filter glob list (e.g. '*.PointWrite,*.PointRead').
    # Ignored for non-microbench scenarios.
    [Parameter(ParameterSetName = 'Run')]
    [string] $Workloads = '',

    # Microbench-only: override BENCH_MICROBENCH_FIDELITY for this invocation.
    # One of 'dry' | 'quick' | 'full'. Ignored for non-microbench scenarios.
    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('', 'dry', 'quick', 'full')]
    [string] $Fidelity = '',

    # Microbench-only: enable per-method EventPipe profiling. One of
    # 'off' | 'alloc' | 'cpu' | 'both'. Default 'off' is a no-op. The
    # profile.json sidecar is written next to results.json. NOTE: profiling
    # perturbs measurements - do not use profile-enabled runs as cohort
    # baselines. Refused when -Fidelity full (forking BDN toolchain breaks
    # the in-process EventPipe attach).
    [Parameter(ParameterSetName = 'Run')]
    [ValidateSet('', 'off', 'alloc', 'cpu', 'both')]
    [string] $Profile = '',

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

# ── Hybrid auto-discovery configuration (the contract for results.json) ────────
#
# Capture-time strategy: at the end of the measurement window we hit Prometheus
# /api/v1/metadata, enumerate every instrument whose name matches one of the
# allow-listed prefixes (or sits in the explicit dotnet allow-list), and
# synthesise PromQL deterministically by instrument type. That means a NEW
# meter-instrument added to Orleans.Lattice (or Bench.Sink, or replication)
# auto-populates the next benchmark run''s results.json + history dashboard with
# zero benchmark-side maintenance.
#
# Three escape hatches keep the auto-defaults sensible:
#
#  • $AutoDiscoverPrefixes   – instrument-name prefixes considered "ours".
#  • $AutoDiscoverDotnetAllow – explicit allow-list for runtime metrics
#                               (dotnet emits ~90 instruments; we only want
#                               the operationally interesting handful).
#  • $ScalarPanelExclude     – synthesised keys to drop (for noisy or
#                               redundant defaults).
#  • $ScalarPanelExtra       – hand-crafted PromQL the synthesiser cannot
#                               express (ratios, label-filtered counters,
#                               derived gauges, KPI aliases).
#
# Synthesis rules - driven by the metadata `type` field, NOT the name suffix,
# because dotnet counters like dotnet_process_cpu_time_seconds and
# dotnet_gc_collections do not end in `_total`:
#
#   counter   → <name>_per_second  = sum(rate(<name>_total[{Ws}s]))
#               <name>_increase    = sum(increase(<name>_total[{Ws}s]))
#   gauge     → <name>_max         = max(max_over_time(<name>[{Ws}s]))
#               <name>_avg         = avg(avg_over_time(<name>[{Ws}s]))
#   histogram → <name>_p50/p95/p99 = histogram_quantile(q, sum by (le) (rate(<name>_bucket[{Ws}s])))
#               <name>_per_second  = sum(rate(<name>_count[{Ws}s]))
#   summary   → <name>_p99         = max(<name>{quantile="0.99"})

$AutoDiscoverPrefixes = @(
    'orleans_lattice_',
    'vehicle_fleet_simulator_'
)

$AutoDiscoverDotnetAllow = @(
    'dotnet_gc_collections',
    'dotnet_gc_pause_time_seconds',
    'dotnet_gc_heap_total Allocated Bytes',
    'dotnet_gc_last_collection_heap_size_bytes',
    'dotnet_gc_last_collection_memory_committed_size_bytes'
)

# Synthesised keys to drop after auto-discovery. Add entries here when a
# default form is misleading or redundant.
$ScalarPanelExclude = @()

# Hand-crafted overrides + derived metrics. Win on key collision with
# auto-discovery, so an entry here can pretty-name an auto-key (e.g. give the
# ungainly orleans_lattice_leaf_commit_duration_milliseconds_p99 a short
# `lattice_commit_p99_ms` alias for the KPI tiles to bind to).
$ScalarPanelExtra = [ordered]@{
    # ── KPI aliases (short, stable names the cockpit dashboard's header binds to) ──
    'lattice_commit_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket[{Ws}s])))'
    'lattice_commits_per_second' =
        'sum(rate(orleans_lattice_leaf_commit_duration_milliseconds_count[{Ws}s]))'
    'sink_published_per_second' =
        'sum(rate(vehicle_fleet_simulator_sink_published_total[{Ws}s]))'
    'sink_dropped_combined_increase' =
        'sum(increase(vehicle_fleet_simulator_sink_dropped_total[{Ws}s])) + sum(increase(vehicle_fleet_simulator_sink_dropped_on_shutdown_total[{Ws}s]))'

    # ── Replication KPI aliases moved to $ScalarAliases (below) so they read
    #    auto-discovered values rather than re-issuing duplicate PromQL queries.
    #    The duplicate-query approach observed run-to-run divergence of up to
    #    10x on tail-heavy runs because each query snapshotted a slightly
    #    different rate window; the alias-copy approach is bit-identical to
    #    the underlying auto-discovered field by construction. ──

    # ── WAL-step aliases (label-filtered cuts of the step-tagged
    #    orleans.lattice.leaf.commit.duration histogram, exposed by the
    #    dual-durability commit path on BPlusLeafGrain). The WAL Performance
    #    dashboard's KPI tiles bind to these so the WAL-append step, the
    #    in-memory Apply step, and the legacy shadow-write step are visible
    #    independently - under dual-durability the four steps add up to the
    #    full commit tail, and after the shadow-write removal flip the
    #    shadow alias should drop to zero while WAL-append + Apply remain
    #    the load-bearing path. The WAL-appends-per-second alias is the
    #    throughput companion (count of WAL-step samples). ──
    'lattice_wal_append_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket{step="wal"}[{Ws}s])))'
    'lattice_apply_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket{step="apply"}[{Ws}s])))'
    'lattice_shadow_write_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket{step="shadow"}[{Ws}s])))'
    'lattice_wal_appends_per_second' =
        'sum(rate(orleans_lattice_leaf_commit_duration_milliseconds_count{step="wal"}[{Ws}s]))'

    # ── Digest-publish step aliases (label-filtered cuts of the same
    #    step-tagged orleans.lattice.leaf.commit.duration histogram for
    #    the parent-digest publish hop instrumented on the foreground
    #    leaf commit paths). Auto-discovery aggregates the histogram
    #    across all step values and would otherwise dilute the digest
    #    slice ~4x against the {wal, apply, observer, digest} fold,
    #    making per-step optimisations of the digest hop invisible at
    #    the aggregate percentile. Both p95 and p99 are exported because
    #    the digest hop is the smallest of the four steps and its tail
    #    behaviour at p95 carries as much signal as p99 (a smaller tail
    #    has a lower noise floor). The publishes-per-second alias is the
    #    throughput companion (count of digest-step samples). ──
    'lattice_digest_publish_p95_ms' =
        'histogram_quantile(0.95, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket{step="digest"}[{Ws}s])))'
    'lattice_digest_publish_p99_ms' =
        'histogram_quantile(0.99, sum by (le) (rate(orleans_lattice_leaf_commit_duration_milliseconds_bucket{step="digest"}[{Ws}s])))'
    'lattice_digest_publishes_per_second' =
        'sum(rate(orleans_lattice_leaf_commit_duration_milliseconds_count{step="digest"}[{Ws}s]))'

    # ── Derived metrics (auto-discovery cannot synthesise ratios) ──
    #
    # IMPORTANT: aggregate each rate to a scalar BEFORE adding. PromQL's `+`
    # is vector matching - `rate(A) + rate(B)` only retains series whose
    # labels line up exactly between A and B. If hits and misses are emitted
    # with even one differing label (e.g. different service.instance.id, or
    # one metric carries a label the other doesn't), the addition produces
    # an empty / heavily-thinned vector, the outer sum collapses, clamp_min
    # returns 1, and the ratio degenerates to `hits_rate / 1` - i.e. the
    # absolute hits-per-second instead of a 0..1 ratio. The downstream
    # `{0:P0}` formatter then multiplies by 100 and produces nonsense like
    # "9,548%". Aggregating each rate independently with sum(...) collapses
    # the label dimension first, so the addition is between two scalars.
    'lattice_cache_hit_ratio' =
        'sum(rate(orleans_lattice_cache_hits_total[{Ws}s])) / clamp_min(sum(rate(orleans_lattice_cache_hits_total[{Ws}s])) + sum(rate(orleans_lattice_cache_misses_total[{Ws}s])), 1)'

    # ── Label-filtered counter (auto-discovery treats the whole metric as one series) ──
    'dotnet_gc_gen2_collections_increase' =
        'sum(increase(dotnet_gc_collections_total{gc_heap_generation="gen2"}[{Ws}s]))'

    # ── Silo process CPU utilisation (Phase A attribution column) ──
    # The OpenTelemetry.Instrumentation.Runtime package publishes process CPU
    # consumption via the `process.cpu.time` instrument (Counter<double>,
    # CPU-seconds), which the Prometheus AspNetCore exporter renders as
    # `dotnet_process_cpu_time_seconds_total`. It is emitted with a
    # `cpu_mode` label split across {user, system}, so a sum() collapses the
    # two modes into wall-CPU-seconds consumed by the silo process. A
    # companion gauge `dotnet_process_cpu_count` carries the host core count
    # (16 on the current bench host) so the percent can be normalised to
    # "0-100% of total host CPU" rather than "cores * 100" - the latter
    # would saturate at 1600 on a 16-core box and be unreadable in the
    # attribution column. Auto-discovery is restricted to the
    # `orleans_lattice_*` / `vehicle_fleet_simulator_*` prefixes plus a
    # curated GC allow-list, so the dotnet runtime CPU counters are
    # otherwise dropped from results.json. The two entries below synthesise
    # the CPU-percent surface that the Phase A attribution report's
    # `CpuPct` column resolves against (`process_cpu_percent_avg`,
    # `process_cpu_percent_max`).
    #
    # avg = window-wide rate (rate over [Ws] already collapses to the
    #       window mean, so no subquery is needed).
    # max = peak of the per-30s rate sampled at 10s intervals across the
    #       window, which captures the worst CPU stretch rather than the
    #       smoothed average.
    # Both are scalar-divided by the host core count (taken at the most
    # recent scrape - the value is host-constant for any single run).
    'process_cpu_percent_avg' =
        '100 * sum(rate(dotnet_process_cpu_time_seconds_total[{Ws}s])) / scalar(max(dotnet_process_cpu_count))'
    'process_cpu_percent_max' =
        '100 * max_over_time((sum(rate(dotnet_process_cpu_time_seconds_total[30s])))[{Ws}s:10s]) / scalar(max(dotnet_process_cpu_count))'
}

# ── Curated KPI aliases (post-process; no PromQL queries issued) ────────────
#
# Maps a short stable KPI name to the canonical auto-discovered key produced
# by Get-AutoScalarPanel. Resolution happens in-memory after Get-ScalarMetrics
# returns: for each (short, canonical) pair, metrics[short] = metrics[canonical].
#
# This replaces the previous "duplicate PromQL alias" approach, which observed
# run-to-run divergence of up to 10x on tail-heavy runs because each query
# snapshotted a slightly different rate window. The alias-copy approach is
# bit-identical to the underlying auto-discovered field by construction and
# costs zero extra HTTP round-trips.
#
# Add a new entry here when a downstream consumer (dashboard, CI threshold,
# history-VM aggregation) needs a short, stable name for a metric the
# auto-discovery already produces.
$ScalarAliases = [ordered]@{
    # Replication KPI shorts (the auto-discovered names carry the OTel→Prom
    # _milliseconds suffix that gets appended for unit:"ms"; these aliases
    # hide that mangling behind short, stable names the persona dashboards
    # and any CI threshold checks bind to).
    'replication_ship_p95_ms'                     = 'orleans_lattice_replication_ship_duration_milliseconds_p95'
    'replication_ship_p99_ms'                     = 'orleans_lattice_replication_ship_duration_milliseconds_p99'
    'replication_apply_lag_p95_ms'                = 'orleans_lattice_replication_apply_lag_milliseconds_p95'
    'replication_apply_lag_p99_ms'                = 'orleans_lattice_replication_apply_lag_milliseconds_p99'
    'replication_wal_entries_appended_per_second' = 'orleans_lattice_replication_wal_entries_appended_per_second'
    'replication_wal_entries_shipped_per_second'  = 'orleans_lattice_replication_wal_entries_shipped_per_second'

    # Phase A / A2 cross-grain dispatch attribution. WalCommitLogWriter
    # clocks the awaited IWalShardGrain.AppendAsync / AppendBatchAsync RPC
    # so the Orleans turn-queue wait at the target WAL activation becomes
    # visible. Subtracting wal.append.turn_wait (the WAL grain's own
    # self-clock) from this dispatch histogram isolates the scheduling
    # tax on the single WAL activation per partition - the dominant cost
    # under the default WalPartitions = 1. Auto-discovery already emits
    # the long mangled name; these aliases give the diagnostic reports
    # short, stable names to bind to without re-issuing duplicate PromQL.
    'lattice_wal_shard_dispatch_p95_ms'           = 'orleans_lattice_wal_shard_dispatch_duration_milliseconds_p95'
    'lattice_wal_shard_dispatch_p99_ms'           = 'orleans_lattice_wal_shard_dispatch_duration_milliseconds_p99'
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

# ── Scenario env-var leak guard ────────────────────────────────────────────────
#
# `Set-ProcessEnv` only *adds* keys; it never unsets a pre-existing process env
# var that's absent from the new scenario's .env file. That breaks scenario
# isolation when the script is invoked back-to-back via benchmark-all.ps1: any
# key set by an earlier scenario (e.g. `BENCH_ORIGIN_PEER_ENDPOINT` from
# `current-state-single-peer.env`) leaks into the next scenario's `docker
# compose up`, where the `${VAR:-}` default substitution picks up the leaked
# value instead of the empty string the next scenario expects.
#
# The concrete failure mode this guards against: `observer-no-peer.env` does
# *not* set `BENCH_ORIGIN_PEER_ENDPOINT`, so the silo container should be
# brought up with an empty `Replication__GrpcPeers__vfs-bench-replica` value
# (which Bench.Silo's `.Where(c => !string.IsNullOrWhiteSpace(c.Value))` filter
# discards, leaving the no-op transport in place). With the leak, the silo gets
# `http://silo-replica:5001` from the previous scenario, registers
# `GrpcPushTransport`, activates a per-(tree, peer) shipper, and then every
# ship attempt fails at the gRPC connect timeout because the replica overlay
# isn't running for `observer-no-peer`. The failure-path duration (~250-500 ms
# per attempt) lands in the `[250, 500)` histogram bucket and `ship.duration`
# p95 reports ~475 ms - making the no-peer control look like the slowest
# replication scenario in the suite.
#
# `$ScenarioControlledEnvKeys` is computed once at script load by scanning
# every `scenarios/*.env` and collecting every assigned key name. Before each
# scenario applies its own .env via `Set-ProcessEnv`, we call
# `Reset-ScenarioEnv` to remove every key in the union set from process env.
# `Set-ProcessEnv` then re-adds only the keys present in the current scenario,
# so any "absent in this scenario but present in another" var ends up unset
# rather than carrying over.
#
# Vars set externally (e.g. `BENCH_API_URL`, `BENCH_PROMETHEUS_URL`,
# `BENCH_GIT_SHA`) and vars set by the script itself for results.json
# bookkeeping (`BENCH_SCENARIO`, `BENCH_RUN_ID`) are NOT in any .env file, so
# they are not in `$ScenarioControlledEnvKeys` and are left untouched.
$ScenarioControlledEnvKeys = @(
    Get-ChildItem -Path (Join-Path $benchmarkRoot 'scenarios') -Filter '*.env' -File | ForEach-Object {
        Get-Content $_.FullName | ForEach-Object {
            $trimmed = $_.Trim()
            if ($trimmed.StartsWith('#') -or [string]::IsNullOrWhiteSpace($trimmed)) { return }
            $eq = $trimmed.IndexOf('=')
            if ($eq -lt 1) { return }
            $trimmed.Substring(0, $eq).Trim()
        }
    }
) | Sort-Object -Unique

function Reset-ScenarioEnv {
    foreach ($key in $ScenarioControlledEnvKeys) {
        if (Test-Path "Env:$key") {
            Remove-Item -Path "Env:$key" -ErrorAction SilentlyContinue
        }
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
        Write-Warning "Dashboard source $dashboardSrc not found - skipping copy."
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
    # PowerShell's ?? operator only treats $null as null, not the empty string.
    # `[string] $Cwd` with no caller-supplied value binds to '', so `?? $benchmarkRoot`
    # would return '' and Push-Location '' is a no-op leaving the location at the
    # caller's cwd. When the harness is invoked from the repo root (./benchmark/benchmark.ps1)
    # rather than from inside benchmark/, this silently breaks every compose call because
    # docker compose then looks for docker-compose.yml at the repo root. Use an explicit
    # IsNullOrEmpty test so empty strings fall back to $benchmarkRoot too.
    $resolvedCwd = if ([string]::IsNullOrEmpty($Cwd)) { $benchmarkRoot } else { $Cwd }
    Push-Location $resolvedCwd
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
    # and a default route - the simulator picks a pseudo-random route from the city graph.
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
    # or the query errored. We never throw - a missing metric is a normal outcome
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

function Get-AutoScalarPanel {
    <#
    .SYNOPSIS
        Discovers Prometheus instruments and synthesises PromQL by type.

    .DESCRIPTION
        Hits /api/v1/metadata, filters to instruments matching $Prefixes (or
        sitting in $DotnetAllow), then synthesises an [ordered] dictionary of
        (key → PromQL template) entries deterministically by instrument type.
        Returns an empty ordered hashtable on any error so the caller can fall
        back to $ScalarPanelExtra alone.
    #>
    [CmdletBinding()]
    param(
        [string]   $PrometheusUrl,
        [string[]] $Prefixes,
        [string[]] $DotnetAllow,
        [string[]] $Exclude
    )
    $panel = [ordered]@{}
    try {
        $resp = Invoke-RestMethod -Uri "$PrometheusUrl/api/v1/metadata" -TimeoutSec 15 -ErrorAction Stop
    } catch {
        Write-Warning "[auto-discover] /api/v1/metadata unreachable: $_"
        return $panel
    }
    if ($resp.status -ne 'success' -or -not $resp.data) { return $panel }

    foreach ($prop in ($resp.data.PSObject.Properties | Sort-Object Name)) {
        $name = $prop.Name
        $info = $prop.Value | Select-Object -First 1
        if (-not $info) { continue }
        $type = "$($info.type)".ToLowerInvariant()

        $included = $false
        foreach ($p in $Prefixes) {
            if ($name.StartsWith($p, [StringComparison]::Ordinal)) { $included = $true; break }
        }
        if (-not $included -and ($DotnetAllow -contains $name)) { $included = $true }
        if (-not $included) { continue }

        switch ($type) {
            'counter' {
                # OTel→Prom appends `_total` to counter exposition names.
                $series = "${name}_total"
                $key1 = "${name}_per_second"
                $key2 = "${name}_increase"
                if ($Exclude -notcontains $key1) {
                    $panel[$key1] = "sum(rate(${series}[{Ws}s]))"
                }
                if ($Exclude -notcontains $key2) {
                    $panel[$key2] = "sum(increase(${series}[{Ws}s]))"
                }
            }
            'gauge' {
                $key1 = "${name}_max"
                $key2 = "${name}_avg"
                if ($Exclude -notcontains $key1) {
                    $panel[$key1] = "max(max_over_time(${name}[{Ws}s]))"
                }
                if ($Exclude -notcontains $key2) {
                    $panel[$key2] = "avg(avg_over_time(${name}[{Ws}s]))"
                }
            }
            'histogram' {
                # OTel→Prom emits <name>_bucket / <name>_count / <name>_sum.
                $bucket = "${name}_bucket"
                $count  = "${name}_count"
                foreach ($q in @(0.50, 0.95, 0.99)) {
                    $suffix = ('p{0:D2}' -f [int]([Math]::Round($q * 100)))
                    $key = "${name}_${suffix}"
                    if ($Exclude -contains $key) { continue }
                    $panel[$key] = "histogram_quantile($q, sum by (le) (rate(${bucket}[{Ws}s])))"
                }
                $rateKey = "${name}_per_second"
                if ($Exclude -notcontains $rateKey) {
                    $panel[$rateKey] = "sum(rate(${count}[{Ws}s]))"
                }
            }
            'summary' {
                $key = "${name}_p99"
                if ($Exclude -notcontains $key) {
                    $panel[$key] = "max(${name}{quantile=`"0.99`"})"
                }
            }
            default {
                # UpDownCounter / unknown - treat as gauge.
                $key1 = "${name}_max"
                $key2 = "${name}_avg"
                if ($Exclude -notcontains $key1) {
                    $panel[$key1] = "max(max_over_time(${name}[{Ws}s]))"
                }
                if ($Exclude -notcontains $key2) {
                    $panel[$key2] = "avg(avg_over_time(${name}[{Ws}s]))"
                }
            }
        }
    }
    return $panel
}

function Resolve-ScalarPanel {
    <#
    .SYNOPSIS
        Merges auto-discovered metrics with hand-crafted overrides.

    .DESCRIPTION
        Returns an [ordered] dictionary whose keys are the union of the
        auto-discovered panel and $Extra. On collision, $Extra wins - that''s
        the documented contract for pretty-naming auto-keys (e.g. an
        ungainly orleans_lattice_leaf_commit_duration_milliseconds_p99 can
        be aliased to the short lattice_commit_p99_ms).
    #>
    [CmdletBinding()]
    param(
        [string]                          $PrometheusUrl,
        [string[]]                        $Prefixes,
        [string[]]                        $DotnetAllow,
        [string[]]                        $Exclude,
        [System.Collections.IDictionary]  $Extra
    )
    $auto = Get-AutoScalarPanel `
        -PrometheusUrl $PrometheusUrl `
        -Prefixes      $Prefixes `
        -DotnetAllow   $DotnetAllow `
        -Exclude       $Exclude
    if ($Extra) {
        foreach ($k in $Extra.Keys) { $auto[$k] = $Extra[$k] }
    }
    return $auto
}

function Get-ScalarMetrics {
    param(
        [int] $WindowSeconds,
        [System.Collections.IDictionary] $Panel
    )
    $out = [ordered]@{
    }
    foreach ($key in $Panel.Keys) {
        $promQl = $Panel[$key].Replace('{Ws}', $WindowSeconds.ToString([Globalization.CultureInfo]::InvariantCulture))
        $val = Invoke-PromInstantQuery -Query $promQl
        $out[$key] = $val
    }
    return $out
}

function Resolve-ScalarAliases {
    <#
    .SYNOPSIS
        In-memory alias resolution: copies auto-discovered metric values
        into short, stable KPI keys.

    .DESCRIPTION
        Iterates $Aliases (short → canonical) and assigns
        $Metrics[short] = $Metrics[canonical] for every entry whose
        canonical key is present in $Metrics. If the canonical key is
        absent (e.g. its underlying instrument never emitted a sample
        in this scenario), the alias is set to $null so consumers can
        distinguish "alias never resolved" from "alias resolved to 0".

        This replaces the previous duplicate-PromQL approach which
        observed up to 10x divergence between the alias and the
        auto-discovered field on tail-heavy runs (each query
        snapshotted a slightly different rate window). The copy
        approach is bit-identical by construction.
    #>
    [CmdletBinding()]
    param(
        [System.Collections.IDictionary] $Metrics,
        [System.Collections.IDictionary] $Aliases
    )
    if (-not $Metrics -or -not $Aliases) { return $Metrics }
    foreach ($short in $Aliases.Keys) {
        $canonical = $Aliases[$short]
        if ($Metrics.Contains($canonical)) {
            $Metrics[$short] = $Metrics[$canonical]
        } else {
            $Metrics[$short] = $null
        }
    }
    return $Metrics
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
        # Sanitise label values - VM accepts the same escaping rules as Prometheus.
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
        Write-Host "[history] VM at $historyVmUrl unreachable - skipping push (results.json archived locally)" -ForegroundColor DarkYellow
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

# ── Microbench (microbench scenario) ───────────────────────────────────────────

function Invoke-Microbench {
    <#
    .SYNOPSIS
        Runs the BenchmarkDotNet-driven ILattice micro-benchmark in lieu of the
        docker-compose flow. Writes a harness-shaped results.json to
        .run/<scenario>/<run_id>/results.json and opportunistically pushes to the
        history VM, exactly like the docker-driven scenarios.
    .NOTES
        The microbench project lives at benchmark/host/Bench.Microbench/. It uses
        Orleans.TestingHost for an in-process single-silo cluster and BDN's
        InProcessEmitToolchain so the cluster comes up once and serves all
        [Benchmark] methods (vs. paying the ~5s cluster startup per child .exe
        with the default forking toolchain).
    #>
    param(
        [Parameter(Mandatory = $true)] [string] $ScenarioId,
        [Parameter(Mandatory = $true)] [System.Collections.IDictionary] $EnvMap,
        [Parameter(Mandatory = $true)] [string] $RunId
    )

    # Flow this exactly like the docker scenarios so the downstream artefacts (Compare,
    # history push) work without special-casing the microbench.
    $scenarioDir   = Join-Path $runDir $ScenarioId
    $thisRunDir    = Join-Path $scenarioDir $RunId
    $resultsPath   = Join-Path $thisRunDir 'results.json'
    New-Item -ItemType Directory -Path $thisRunDir -Force | Out-Null

    # Stamp the env vars the BDN exporter consumes when it builds the JSON payload.
    # BENCH_GIT_SHA mirrors what the docker pipeline already captures.
    $started   = (Get-Date).ToUniversalTime().ToString('o')
    $startEpoch = [int][double]::Parse((Get-Date -UFormat '%s'))
    # Use the shared Get-GitSha helper so docker and microbench produce identical sha shapes.
    # Mismatched lengths (7 vs 10 chars) silently break trend continuity in the history dashboard.
    $gitSha = Get-GitSha
    # Apply microbench CLI overrides to $EnvMap before Set-ProcessEnv so
    # HarnessConfig (which reads BENCH_MICROBENCH_FIDELITY from the process
    # env) and the log lines below all see the resolved values. -Workloads
    # and -Fidelity are no-ops on non-microbench scenarios.
    if (-not [string]::IsNullOrWhiteSpace($Fidelity)) {
        $EnvMap['BENCH_MICROBENCH_FIDELITY'] = $Fidelity
    }
    if (-not [string]::IsNullOrWhiteSpace($Workloads)) {
        $EnvMap['BENCH_MICROBENCH_WORKLOADS'] = $Workloads
    }
    if (-not [string]::IsNullOrWhiteSpace($Profile)) {
        $EnvMap['BENCH_MICROBENCH_PROFILE'] = $Profile
    }
    Set-ProcessEnv -Map $EnvMap

    $env:BENCH_SCENARIO  = $ScenarioId
    $env:BENCH_RUN_ID    = $RunId
    $env:BENCH_GIT_SHA   = $gitSha
    $env:BENCH_STARTED   = $started
    $env:BENCH_RESULTS_PATH = $resultsPath

    # 1. Build the microbench project once (`--no-build` on run).
    $projectPath = Join-Path $benchmarkRoot 'host/Bench.Microbench/Orleans.Lattice.Benchmark.Microbench.csproj'
    Write-Host ""
    Write-Host "[microbench] building $projectPath ..." -ForegroundColor Cyan
    & dotnet build $projectPath -c Release --nologo /clp:ErrorsOnly | Out-Null
    if ($LASTEXITCODE -ne 0) { throw "microbench build failed (exit $LASTEXITCODE)" }

    # 2. Run BDN. The exporter writes results.json to BENCH_RESULTS_PATH on completion.
    #
    # BDN's --filter accepts comma-separated globs within a SINGLE argument value
    # (the binder splits on comma internally) - e.g. '*.PointWrite,*.PointRead'
    # correctly matches both pattern families. Repeated --filter flags do NOT
    # accumulate; only the last one wins. Space-separated values after one
    # --filter flag are also not consumed past the first.
    # -Workloads (CLI) and BENCH_MICROBENCH_WORKLOADS (env) are already resolved
    # into $EnvMap above.
    $workloadsSpec = $EnvMap['BENCH_MICROBENCH_WORKLOADS']
    $filterArgs = @()
    if (-not [string]::IsNullOrWhiteSpace($workloadsSpec)) {
        # Normalise (trim whitespace around commas) and forward as a single arg.
        $patterns = $workloadsSpec.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }
        if ($patterns.Count -gt 0) {
            $filterArgs = @('--filter', ($patterns -join ','))
            Write-Host ("[microbench] workload filter: {0}" -f ($patterns -join ', ')) -ForegroundColor Cyan
        }
    }
    Write-Host "[microbench] running BenchmarkDotNet (fidelity=$($EnvMap['BENCH_MICROBENCH_FIDELITY']))" -ForegroundColor Cyan
    & dotnet run --project $projectPath -c Release --no-build -- --results $resultsPath @filterArgs

    if ($LASTEXITCODE -ne 0) { throw "microbench run failed (exit $LASTEXITCODE)" }

    # 3. Stamp the wall-clock duration onto the JSON the exporter wrote.
    $endEpoch = [int][double]::Parse((Get-Date -UFormat '%s'))
    $duration = $endEpoch - $startEpoch
    if (Test-Path $resultsPath) {
        $payload = Get-Content $resultsPath -Raw | ConvertFrom-Json -AsHashtable
        $payload.duration_s = $duration
        $payload | ConvertTo-Json -Depth 10 | Set-Content -Path $resultsPath -Encoding utf8
    } else {
        throw "microbench did not produce $resultsPath"
    }

    Write-Host ""
    Write-Host ("[microbench] results: {0}" -f $resultsPath) -ForegroundColor Green

    # 4. Opportunistic history push (same path as the docker scenarios).
    if (-not $NoHistoryPush.IsPresent) {
        [void] (Push-HistoryResults -ResultsPath $resultsPath)
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

    # Take the column union across every recorded run so old runs don't lose
    # newly-discovered metrics (results.json schemas evolve as new instruments
    # ship). Sort alphabetically for stable output. Pretty-name aliases from
    # $ScalarPanelExtra naturally bubble up too because they're recorded under
    # their alias key.
    $keySet = [System.Collections.Generic.HashSet[string]]::new([string[]] @())
    foreach ($r in $results) {
        if ($null -eq $r.metrics) { continue }
        foreach ($p in $r.metrics.PSObject.Properties) { [void]$keySet.Add($p.Name) }
    }
    $metricKeys = $keySet | Sort-Object
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
    # Console preview pulls from the curated KPI-alias keys (defined in
    # $ScalarPanelExtra and $ScalarAliases) so the summary stays stable
    # even as auto-discovery widens the underlying schema. The visible
    # column set switches based on scenario so replication-focused runs
    # surface ship/apply KPIs instead of irrelevant sink/commit ones.
    if ($Scenario -match 'replication') {
        $csvRows | Format-Table -Property scenario, run_id,
            replication_ship_p95_ms, replication_ship_p99_ms,
            replication_apply_lag_p95_ms, replication_apply_lag_p99_ms,
            replication_wal_entries_appended_per_second,
            replication_wal_entries_shipped_per_second,
            lattice_commits_per_second -AutoSize
    } else {
        $csvRows | Format-Table -Property scenario, run_id,
            lattice_commit_p99_ms, lattice_commits_per_second,
            sink_published_per_second, sink_dropped_combined_increase -AutoSize
    }
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
    Write-Host "[history] down ($historyCompose) - volumes preserved" -ForegroundColor Cyan
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
$scenarioFile = Join-Path $benchmarkRoot ("scenarios/{0}.env" -f $Scenario)
if (-not (Test-Path $scenarioFile)) {
    throw "Unknown scenario '$Scenario'. Expected $scenarioFile."
}

Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Orleans.Lattice benchmark - $Scenario" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan

$envMap = Read-EnvFile -Path $scenarioFile

# Track provenance for keys that can be overridden after the .env load, so the
# banner can show *where* each effective value came from instead of silently
# substituting (the .fleet-size.config override has, in the past, caused
# "scenario said 2000 but the run reported 3500" confusion - the banner now
# annotates the source so the operator can spot a mismatch at a glance).
$envSource = @{}
foreach ($k in $envMap.Keys) { $envSource[$k] = 'scenario .env' }

# ── Fleet-size calibration: .fleet-size.config wins over the .env default ──────
#
# benchmark/initialise.ps1 runs a saturation ladder against the host and writes
# the chosen operating fleet size to benchmark/.fleet-size.config (gitignored).
# That value overrides BENCH_FLEET_SIZE from the scenario .env so every
# scenario runs at the host-calibrated load. -FleetSizeOverride wins over both
# (used by initialise.ps1 to sweep the ladder one rung at a time).
#
# The microbench scenario doesn't drive a fleet, so the check is bypassed.
$fleetSizeConfigPath = Join-Path $benchmarkRoot '.fleet-size.config'
$isMicrobench = ($envMap['BENCH_KIND'] -eq 'microbench')
if (-not $isMicrobench -and -not $SkipFleetSizeCheck.IsPresent -and $FleetSizeOverride -le 0) {
    if (-not (Test-Path $fleetSizeConfigPath)) {
        Write-Host ""
        Write-Host "============================================================" -ForegroundColor Red
        Write-Host " Fleet size has not been calibrated for this host." -ForegroundColor Red
        Write-Host "============================================================" -ForegroundColor Red
        Write-Host ""
        Write-Host "The benchmark suite needs to know the fleet size that stresses" -ForegroundColor White
        Write-Host "this host without saturating it. That number depends on your" -ForegroundColor White
        Write-Host "CPU, memory, and storage, so it has to be measured per host." -ForegroundColor White
        Write-Host ""
        Write-Host "Run the initialisation script first:" -ForegroundColor Yellow
        Write-Host ""
        Write-Host "    ./initialise.ps1" -ForegroundColor White
        Write-Host ""
        Write-Host "It runs a load ladder, looks for the saturation knee, and writes" -ForegroundColor DarkGray
        Write-Host "the calibrated fleet size to benchmark/.fleet-size.config (which" -ForegroundColor DarkGray
        Write-Host "is gitignored, so each host has its own value)." -ForegroundColor DarkGray
        Write-Host ""
        Write-Host "To bypass the check (not recommended; benchmark numbers will not" -ForegroundColor DarkGray
        Write-Host "be comparable across hosts), pass -SkipFleetSizeCheck." -ForegroundColor DarkGray
        Write-Host ""
        exit 1
    }
    $configMap = Read-EnvFile -Path $fleetSizeConfigPath
    if ($configMap.Contains('BENCH_FLEET_SIZE')) {
        $envMap['BENCH_FLEET_SIZE'] = $configMap['BENCH_FLEET_SIZE']
        $envSource['BENCH_FLEET_SIZE'] = '.fleet-size.config (host-calibrated; overrides .env)'
    }
}

# -FleetSizeOverride beats both the .env default and the .fleet-size.config.
if ($FleetSizeOverride -gt 0) {
    $envMap['BENCH_FLEET_SIZE'] = "$FleetSizeOverride"
    $envSource['BENCH_FLEET_SIZE'] = '-FleetSizeOverride CLI arg (overrides .env and .fleet-size.config)'
}

# ── Phase A attribution-driver pass-through ────────────────────────────────────
#
# The Phase A diagnostic driver (benchmark-attribution.ps1) sweeps a matrix
# of WAL knobs by stamping BENCH_LATTICE_WAL_* / BENCH_WAL_PIPELINE_PHASE_TWO
# into the process env. docker-compose's `${VAR:-}` interpolation picks them
# up at compose-up time, but those vars are NOT in any scenario .env, so
# Read-EnvFile never sees them and they never landed in results.json's
# `config` block - which meant a later results.json could not be mapped back
# to the cell that produced it.
#
# Promote any matrix-driver process-env knobs into $envMap so they are
# persisted in results.json. This is a no-op for regular (non-attribution)
# runs because those env vars are absent.
$matrixDriverPassthrough = @(
    'BENCH_LATTICE_WAL_PARTITIONS',
    'BENCH_LATTICE_WAL_MAX_PENDING_BATCHES',
    'BENCH_WAL_PIPELINE_PHASE_TWO',
    'BENCH_WAL_RETRY_MAX_ATTEMPTS',
    'BENCH_WAL_RETRY_DELAY_MS',
    'BENCH_WAL_RETRY_MAX_DELAY_MS',
    'BENCH_WAL_RETRY_NETWORK_TIMEOUT_MS'
)
foreach ($k in $matrixDriverPassthrough) {
    $v = [Environment]::GetEnvironmentVariable($k)
    if (-not [string]::IsNullOrEmpty($v)) {
        $envMap[$k] = $v
        $envSource[$k] = 'process env (attribution driver)'
    }
}

foreach ($k in $envMap.Keys) {
    $src = $envSource[$k]
    if ([string]::IsNullOrEmpty($src)) {
        Write-Host (" {0,-30}= {1}" -f $k, $envMap[$k]) -ForegroundColor DarkGray
    } else {
        Write-Host (" {0,-30}= {1}   # from {2}" -f $k, $envMap[$k], $src) -ForegroundColor DarkGray
    }
}

# Ensure the .run scratch root exists for both flow branches (microbench writes
# results directly into a per-scenario subdir, the docker flow uses Sync-Dashboards
# and the `vfs-prometheus` scrape window).
New-Item -ItemType Directory -Path $runDir -Force | Out-Null

# Pre-compute the run id used for results.json placement and the history `run_id` label.
# Use UTC ISO8601 with `:` → `-` so it survives Windows path constraints.
$runId = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH-mm-ssZ')

# The microbench scenario (BENCH_KIND=microbench) bypasses the docker-compose flow
# and instead drives the BenchmarkDotNet harness in benchmark/host/Bench.Microbench/.
# The harness writes its own results.json, then we opportunistically push to the
# history VM - same as the docker scenarios.
if ($envMap['BENCH_KIND'] -eq 'microbench') {
    Invoke-Microbench -ScenarioId $Scenario -EnvMap $envMap -RunId $runId
    return
}

# Set the env vars in the current process so docker compose (which inherits them)
# substitutes them into docker-compose.yml. `Reset-ScenarioEnv` runs first so any
# scenario-controlled BENCH_* var set by a previous scenario (e.g. when
# benchmark-all.ps1 invokes us back-to-back) is cleared before this scenario's
# .env is applied. Without the reset, vars absent from the current scenario's
# .env would silently inherit the previous scenario's value through `docker
# compose`'s `${VAR:-}` default substitution. See the comment block above
# `$ScenarioControlledEnvKeys` for the concrete failure mode this guards.
Reset-ScenarioEnv
Set-ProcessEnv -Map $envMap

# Pick compose files. Replication scenarios add the overlay.
$composeFiles = @('docker-compose.yml')
if ($envMap['BENCH_REPLICATION_OVERLAY'] -eq 'true') {
    $composeFiles += 'docker-compose.replication.yml'
}

Sync-Dashboards

# Bring up the stack.
Write-Host ""
$composeUpArgs = @('up', '-d')
if (-not $NoBuild.IsPresent) {
    $composeUpArgs = @('up', '--build', '-d')
    Write-Host "[compose] up --build -d ($($composeFiles -join ', '))" -ForegroundColor Cyan
}
else {
    Write-Host "[compose] up -d ($($composeFiles -join ', ')) (build skipped: -NoBuild)" -ForegroundColor Cyan
}
Invoke-Compose -ComposeFiles $composeFiles -Args $composeUpArgs

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
    Write-Host "[capture] resolving auto-discovered + curated metric panel ..." -ForegroundColor Cyan
    $resolvedPanel = Resolve-ScalarPanel `
        -PrometheusUrl $prometheusUrl `
        -Prefixes      $AutoDiscoverPrefixes `
        -DotnetAllow   $AutoDiscoverDotnetAllow `
        -Exclude       $ScalarPanelExclude `
        -Extra         $ScalarPanelExtra
    Write-Host ("[capture] panel: {0} keys ({1} extra overrides)" -f $resolvedPanel.Count, $ScalarPanelExtra.Count) -ForegroundColor DarkGray
    Write-Host "[capture] querying Prometheus over the ${duration}s measurement window ..." -ForegroundColor Cyan
    $capturedMetrics = Get-ScalarMetrics -WindowSeconds $duration -Panel $resolvedPanel
    $capturedMetrics = Resolve-ScalarAliases -Metrics $capturedMetrics -Aliases $ScalarAliases
    $nonNull = ($capturedMetrics.Values | Where-Object { $null -ne $_ }).Count
    Write-Host ("[capture] {0}/{1} metrics populated ({2} alias keys)" -f $nonNull, $capturedMetrics.Count, $ScalarAliases.Count) -ForegroundColor Green

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
            -ScenarioId $Scenario `
            -RunId      $runId `
            -Config     $envMap `
            -Started    $runStart `
            -Ended      $runEnd `
            -Metrics    $capturedMetrics `
            -FleetStats $capturedFleet
        Write-Host ""
        Write-Host "Results: $resultsPath" -ForegroundColor Green

        # Opportunistic push to history VM (non-fatal if VM is down).
        if (-not $NoHistoryPush.IsPresent) {
            Push-HistoryResults -ResultsPath $resultsPath | Out-Null
        }
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
