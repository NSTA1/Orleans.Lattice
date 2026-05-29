<#
.SYNOPSIS
    Phase A diagnostic attribution matrix for the horizontal-scaling
    initiative described in scaling.md.

.DESCRIPTION
    Sweeps the local benchmark scenarios that exercise the three suspect
    surfaces - core WAL grain (current-state-no-replication), Azure Tables
    provider (current-state-no-replication-azuretable), and saga path
    (atomic-write, atomic-write-replication) - across a controlled grid
    of LatticeOptions knobs:

        WalPartitions             ∈ { 1, 4, 16 }
        WalMaxPendingBatches      ∈ { 1, 4, 16 }
        PipelinePhaseTwoCommits   ∈ { off, on }   (azuretable scenarios only)

    Microbench is also driven as the in-process CPU baseline floor so the
    report can attribute the gap between "pure code" and "code + Orleans
    grain scheduling".

    Each cell drives the matrix via benchmark.ps1, harvests the
    per-cell results.json that benchmark.ps1 already produces under
    .run/<scenario>/<runId>/results.json, and folds the row into a
    diagnostic report at
    benchmark/diagnostic-reports/diagnostic-report-<UTC-timestamp>.md.

    Reports are timestamped and tracked in git so each Phase A/B/C/D run
    has a committable evidence record. The report captures, per matrix
    cell:

      - scenario id
      - WalPartitions / WalMaxPendingBatches / PipelinePhaseTwoCommits
      - sample throughput (ops/s)
      - p50 / p99 commit latency
      - host CPU%
      - Azure Tables server-timing sum (when the provider is exercised)
      - Phase A histogram quantiles (wal.append, provider.commit, saga.*)

    No code defaults change. No production hot path runs differently
    because of this script.

.PARAMETER WalPartitions
    Override the WalPartitions sweep. Comma-separated integers. Default
    "1,4,16".

.PARAMETER WalMaxPendingBatches
    Override the WalMaxPendingBatches sweep. Comma-separated integers.
    Default "1,4,16".

.PARAMETER PipelinePhaseTwoCommits
    Override the PipelinePhaseTwoCommits sweep. Comma-separated booleans
    ("false","true"). Default "false,true". Only applied to azuretable
    scenarios.

.PARAMETER Scenarios
    Override the scenario list. Comma-separated scenario ids. Default
    "microbench,current-state-no-replication,current-state-no-replication-azuretable,atomic-write,atomic-write-replication".

.PARAMETER SkipAzureThroughput
    Skip the real-Azure benchmark/azure-throughput rung. Default is to
    skip - the real-Azure ladder is a separate, more expensive workflow
    invoked from benchmark/azure-throughput/scripts. The flag exists so
    the matrix can be extended manually after a Phase-A local pass.

.PARAMETER IncludeAzureThroughput
    Include the real-Azure benchmark/azure-throughput rung. Requires the
    Azure context to already exist (see benchmark/azure-throughput/scripts).

.PARAMETER ReportPath
    Override the diagnostic-report path. Default is a UTC-timestamped
    file under benchmark/diagnostic-reports/, e.g.
    diagnostic-report-2026-05-24T15-22-09Z.md.

.PARAMETER DryRun
    Print the matrix without executing it. Used to sanity-check the
    cells the driver will exercise before committing to a several-hour
    run.

.PARAMETER ResumeFrom
    Skip every matrix cell whose ordinal index is below this value.
    Used to resume after a transient docker failure without re-running
    the cells that already wrote a results.json. Default 0.

.PARAMETER ReportOnly
    Skip cell execution entirely and rebuild the report from the
    newest results.json already on disk under .run/<scenario>/. Useful
    when the matrix executed cleanly but the report extractor was
    updated and the report needs to be regenerated without burning
    another wall-clock pass.

.EXAMPLE
    ./benchmark/benchmark-attribution.ps1 -DryRun

    Prints the matrix the driver would run, without standing up any
    docker-compose stack.

.EXAMPLE
    ./benchmark/benchmark-attribution.ps1

    Runs the full local Phase A matrix and writes
    benchmark/diagnostic-reports/diagnostic-report-<UTC-timestamp>.md.

.EXAMPLE
    ./benchmark/benchmark-attribution.ps1 -Scenarios current-state-no-replication -WalPartitions 1,4 -WalMaxPendingBatches 1,8

    Targeted re-run for just the WAL grain scenario across a smaller grid.

.NOTES
    This script is intentionally a Phase A driver only. Phase B/C/D
    remediations (default flips, provider parallelism, saga fan-out) are
    out of scope and gated on the report this script produces.
#>
[CmdletBinding()]
param(
    [string] $WalPartitions = '1,4,16',
    [string] $WalMaxPendingBatches = '1,4,16',
    [string] $PipelinePhaseTwoCommits = 'false,true',
    [string] $Scenarios = 'microbench,current-state-no-replication,current-state-no-replication-azuretable,atomic-write,atomic-write-replication',
    [switch] $SkipAzureThroughput,
    [switch] $IncludeAzureThroughput,
    [string] $ReportPath = '',
    [switch] $DryRun,
    [int]    $ResumeFrom = 0,
    [switch] $ReportOnly
)

$ErrorActionPreference = 'Stop'
$PSNativeCommandUseErrorActionPreference = $true

# ── Paths ───────────────────────────────────────────────────────────────────────
$benchmarkRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot      = Split-Path -Parent $benchmarkRoot
$runDir        = Join-Path $benchmarkRoot '.run'
$benchScript   = Join-Path $benchmarkRoot 'benchmark.ps1'
# Report file is timestamped (UTC, sortable) and lives under
# benchmark/diagnostic-reports/. The directory and its contents are tracked
# in git so report snapshots are committable evidence for Phase A/B/C/D
# decisions. -ReportPath overrides the default if the caller wants a
# specific filename (e.g. a pilot-only output).
$reportDir = Join-Path $benchmarkRoot 'diagnostic-reports'
New-Item -ItemType Directory -Path $reportDir -Force | Out-Null
if ([string]::IsNullOrWhiteSpace($ReportPath)) {
    $reportTimestamp = (Get-Date).ToUniversalTime().ToString('yyyy-MM-ddTHH-mm-ssZ')
    $ReportPath = Join-Path $reportDir ("diagnostic-report-{0}.md" -f $reportTimestamp)
}

if (-not (Test-Path $benchScript)) {
    throw "Could not find benchmark.ps1 at $benchScript - run from a clean checkout."
}

# ── Preflight: docker port conflict gate ───────────────────────────────────────
#
# The docker-compose stack binds a fixed set of host ports (Azurite at
# 10000-10002, the API at 8080, Prometheus at 9090, Grafana at 3000). If
# any of those ports are already bound when the matrix starts, every
# `docker compose up` for the rest of the run will fail with
#
#   Bind for 0.0.0.0:10000 failed: port is already allocated
#
# - and the matrix will burn hours producing an empty report. The most
# common offender in this repo is an `azurite-test` container left over
# from the unit-test suite (which pins 10000-10002 for its lifetime).
# Detect every conflict before cell 1 and refuse to start, so the
# operator has actionable information instead of "5 h, no data".
#
# A second guard rail (Wait-PortsFree) is invoked after each cell's
# `docker compose down` so a racy teardown does not strand a port-bind
# until the next cell paints over it.
$ProtectedPorts = @(
    @{ Port = 10000; Service = 'Azurite blob endpoint' }
    @{ Port = 10001; Service = 'Azurite queue endpoint' }
    @{ Port = 10002; Service = 'Azurite table endpoint' }
    @{ Port = 8080;  Service = 'VFS API' }
    @{ Port = 9090;  Service = 'Prometheus' }
    @{ Port = 3000;  Service = 'Grafana' }
)

function Test-PortListeners {
    <#
    .SYNOPSIS
        Returns a list of (Port, OwningProcess, ProcessName, ContainerName)
        tuples for every port in $Ports that is currently in the Listen state.
        Empty list means every port in $Ports is free.
    #>
    param([int[]] $Ports)
    $out = New-Object System.Collections.Generic.List[hashtable]
    foreach ($p in $Ports) {
        $listeners = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
        if (-not $listeners) { continue }
        $owner = $listeners | Select-Object -First 1
        $procName = $null
        try {
            $procName = (Get-Process -Id $owner.OwningProcess -ErrorAction SilentlyContinue).ProcessName
        } catch { }
        # Docker Desktop proxies host ports through `com.docker.backend` /
        # vpnkit; the actual container name is the human-readable signal,
        # so we ask docker which container (if any) maps to this port.
        $container = $null
        try {
            $hit = docker ps --filter "publish=$p" --format '{{.Names}}' 2>$null | Select-Object -First 1
            if ($hit) { $container = $hit }
        } catch { }
        $out.Add(@{
            Port          = $p
            OwningProcess = $owner.OwningProcess
            ProcessName   = $procName
            ContainerName = $container
        })
    }
    return ,$out
}

function Wait-PortsFree {
    <#
    .SYNOPSIS
        Polls every $TimeoutSeconds*0.5s until every protected port has no
        listener, or the timeout elapses. Returns $true when all ports are
        free, $false on timeout.
    #>
    param(
        [int[]] $Ports,
        [int]   $TimeoutSeconds = 30
    )
    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        $busy = Test-PortListeners -Ports $Ports
        if ($busy.Count -eq 0) { return $true }
        Start-Sleep -Milliseconds 500
    }
    return $false
}

# Run the up-front preflight before doing anything else. ReportOnly mode
# never launches docker, so the preflight gate is not just unnecessary
# there - it also reports a confusing conflict when the operator already
# has a benchmark stack running (which is the common case when iterating
# on the report extractor).
if (-not $ReportOnly) {
    Write-Host ""
    Write-Host "[preflight] checking protected host ports are free ..." -ForegroundColor DarkGray
    $conflicts = Test-PortListeners -Ports ($ProtectedPorts.Port)
    if ($conflicts.Count -gt 0) {
        Write-Host ""
        Write-Host "============================================================" -ForegroundColor Red
        Write-Host " Port conflict detected - matrix will not start." -ForegroundColor Red
        Write-Host "============================================================" -ForegroundColor Red
        Write-Host ""
        Write-Host " The following host ports are already in use:" -ForegroundColor White
        foreach ($c in $conflicts) {
            $svc = ($ProtectedPorts | Where-Object { $_.Port -eq $c.Port } | Select-Object -First 1).Service
            $procDesc = if ($c.ContainerName) { "docker container '$($c.ContainerName)'" }
                        elseif ($c.ProcessName) { "process '$($c.ProcessName)' (PID $($c.OwningProcess))" }
                        else { "PID $($c.OwningProcess)" }
            Write-Host ("   {0,-6}  {1,-30}  held by {2}" -f $c.Port, $svc, $procDesc) -ForegroundColor Yellow
        }
        Write-Host ""
        Write-Host " To free Azurite (10000-10002), the usual fix is:" -ForegroundColor DarkGray
        Write-Host "     docker stop azurite-test; docker rm azurite-test" -ForegroundColor DarkGray
        Write-Host " For other ports, identify the owner (Get-NetTCPConnection -LocalPort N)" -ForegroundColor DarkGray
        Write-Host " and stop it before re-running this matrix." -ForegroundColor DarkGray
        Write-Host ""
        throw "Aborted: port conflict on $($conflicts.Count) protected port(s)."
    }
    Write-Host "[preflight] all protected ports are free." -ForegroundColor DarkGray
}

# ── Parse sweep params ─────────────────────────────────────────────────────────
function ConvertTo-IntList {
    param([string] $Raw)
    return ($Raw.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ } | ForEach-Object { [int]$_ })
}

function ConvertTo-BoolList {
    param([string] $Raw)
    return ($Raw.Split(',') | ForEach-Object { $_.Trim().ToLowerInvariant() } | Where-Object { $_ } | ForEach-Object {
        switch ($_) {
            'true'  { $true }
            'false' { $false }
            default { throw "Invalid boolean '$_' (expected 'true' or 'false')." }
        }
    })
}

$partitionsList = ConvertTo-IntList $WalPartitions
$pendingList    = ConvertTo-IntList $WalMaxPendingBatches
$phaseTwoList   = ConvertTo-BoolList $PipelinePhaseTwoCommits
$scenarioList   = $Scenarios.Split(',') | ForEach-Object { $_.Trim() } | Where-Object { $_ }

# Resolve azure-throughput inclusion: -IncludeAzureThroughput wins over -SkipAzureThroughput.
$runAzureRung = $false
if ($IncludeAzureThroughput.IsPresent) { $runAzureRung = $true }
elseif ($SkipAzureThroughput.IsPresent) { $runAzureRung = $false }

# ── Build the matrix ────────────────────────────────────────────────────────────
#
# Per scenario the matrix axes are:
#
#  - microbench: a single cell, in-process, ignores WAL knobs (no grain layer).
#  - current-state-no-replication: WalPartitions × WalMaxPendingBatches.
#  - current-state-no-replication-azuretable: WalPartitions × WalMaxPendingBatches × PipelinePhaseTwoCommits.
#  - atomic-write / atomic-write-replication: WalPartitions × WalMaxPendingBatches.
#
# Each matrix cell is materialised into a hashtable so the driver loop can
# print, dry-run, or execute uniformly.
$matrix = New-Object System.Collections.Generic.List[hashtable]
$index  = 0

function Add-Cell {
    param(
        [string] $Scenario,
        [int]    $Partitions,
        [int]    $MaxPending,
        [Nullable[bool]] $PipelinePhaseTwo
    )
    $script:index++
    $script:matrix.Add(@{
        Index           = $script:index
        Scenario        = $Scenario
        WalPartitions   = $Partitions
        MaxPending      = $MaxPending
        PipelinePhase2  = $PipelinePhaseTwo
    })
}

foreach ($s in $scenarioList) {
    switch ($s) {
        'microbench' {
            # Microbench has no grain layer, so the WAL knobs are not
            # observable. A single cell records the in-process CPU baseline.
            Add-Cell -Scenario $s -Partitions 0 -MaxPending 0 -PipelinePhaseTwo $null
        }
        'current-state-no-replication' {
            foreach ($p in $partitionsList) {
                foreach ($b in $pendingList) {
                    Add-Cell -Scenario $s -Partitions $p -MaxPending $b -PipelinePhaseTwo $null
                }
            }
        }
        'current-state-no-replication-azuretable' {
            foreach ($p in $partitionsList) {
                foreach ($b in $pendingList) {
                    foreach ($pp in $phaseTwoList) {
                        Add-Cell -Scenario $s -Partitions $p -MaxPending $b -PipelinePhaseTwo $pp
                    }
                }
            }
        }
        'atomic-write' {
            foreach ($p in $partitionsList) {
                foreach ($b in $pendingList) {
                    Add-Cell -Scenario $s -Partitions $p -MaxPending $b -PipelinePhaseTwo $null
                }
            }
        }
        'atomic-write-replication' {
            foreach ($p in $partitionsList) {
                foreach ($b in $pendingList) {
                    Add-Cell -Scenario $s -Partitions $p -MaxPending $b -PipelinePhaseTwo $null
                }
            }
        }
        default {
            # Unknown / custom scenario - run it once at the matrix midpoint
            # so the operator can plug their own scenario into the report
            # without modifying this script.
            $midP = if ($partitionsList.Count -gt 0) { $partitionsList[[int]([math]::Floor($partitionsList.Count / 2))] } else { 1 }
            $midB = if ($pendingList.Count    -gt 0) { $pendingList[[int]([math]::Floor($pendingList.Count    / 2))] } else { 1 }
            Add-Cell -Scenario $s -Partitions $midP -MaxPending $midB -PipelinePhaseTwo $null
        }
    }
}

# Real-Azure throughput rung is appended after the local matrix; it is opt-in
# because it costs money and requires the Azure context to be provisioned.
if ($runAzureRung) {
    $script:index++
    $matrix.Add(@{
        Index           = $script:index
        Scenario        = 'azure-throughput'
        WalPartitions   = 0
        MaxPending      = 0
        PipelinePhase2  = $null
        AzureRung       = $true
    })
}

# ── Banner ──────────────────────────────────────────────────────────────────────
Write-Host ""
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host " Phase A diagnostic attribution matrix" -ForegroundColor Cyan
Write-Host "============================================================" -ForegroundColor Cyan
Write-Host (" Scenarios     : {0}" -f ($scenarioList -join ', ')) -ForegroundColor DarkGray
Write-Host (" Partitions    : {0}" -f ($partitionsList -join ', ')) -ForegroundColor DarkGray
Write-Host (" MaxPending    : {0}" -f ($pendingList -join ', ')) -ForegroundColor DarkGray
Write-Host (" PhaseTwoPipe  : {0}" -f ($phaseTwoList -join ', ')) -ForegroundColor DarkGray
Write-Host (" Azure rung    : {0}" -f $runAzureRung) -ForegroundColor DarkGray
Write-Host (" Cells total   : {0}" -f $matrix.Count) -ForegroundColor DarkGray
Write-Host (" Report path   : {0}" -f $ReportPath) -ForegroundColor DarkGray
Write-Host (" Resume from   : {0}" -f $ResumeFrom) -ForegroundColor DarkGray
Write-Host ""

foreach ($c in $matrix) {
    $pp = if ($null -eq $c.PipelinePhase2) { '-' } else { $c.PipelinePhase2.ToString().ToLowerInvariant() }
    $skipMark = if ($c.Index -lt $ResumeFrom) { ' (SKIP)' } else { '' }
    Write-Host ("  [{0,2}] scenario={1,-44} partitions={2,3} pending={3,3} pipelinePhase2={4,-5}{5}" -f `
        $c.Index, $c.Scenario, $c.WalPartitions, $c.MaxPending, $pp, $skipMark) -ForegroundColor DarkGray
}

if ($DryRun.IsPresent) {
    Write-Host ""
    Write-Host "[dry-run] no cells executed. Re-run without -DryRun to drive the matrix." -ForegroundColor Yellow
    return
}

# ── Cell execution ──────────────────────────────────────────────────────────────
function Invoke-MatrixCell {
    [CmdletBinding()]
    param([hashtable] $Cell)

    $sc      = [string]$Cell.Scenario
    $isAzure = [bool]($Cell.AzureRung)

    Write-Host ""
    Write-Host "============================================================" -ForegroundColor Cyan
    Write-Host (" [{0,2}/{1}] {2}" -f $Cell.Index, $matrix.Count, $sc) -ForegroundColor Cyan
    if (-not $isAzure) {
        Write-Host (" WalPartitions={0}, WalMaxPendingBatches={1}, PipelinePhase2={2}" -f `
            $Cell.WalPartitions, $Cell.MaxPending, ($(if ($null -eq $Cell.PipelinePhase2) { 'n/a' } else { $Cell.PipelinePhase2 }))) -ForegroundColor Cyan
    }
    Write-Host "============================================================" -ForegroundColor Cyan

    if ($isAzure) {
        # The real-Azure rung is intentionally delegated to its own driver
        # so this script does not duplicate Azure-context handling.
        $azScript = Join-Path $benchmarkRoot 'azure-throughput/scripts/40-ladder.ps1'
        if (-not (Test-Path $azScript)) {
            Write-Warning "Azure rung requested but $azScript not found - skipping."
            return $null
        }
        Write-Host ("[azure-throughput] delegating to {0}" -f $azScript) -ForegroundColor Yellow
        & pwsh -NoProfile -File $azScript
        return $null
    }

    # Stamp the WAL knobs into the process env so docker-compose's
    # ${BENCH_LATTICE_WAL_*:-} expansion picks them up. These keys are NOT
    # in any scenarios/*.env, so benchmark.ps1's Reset-ScenarioEnv guard
    # leaves them in place across back-to-back invocations.
    if ($Cell.WalPartitions -gt 0) {
        $env:BENCH_LATTICE_WAL_PARTITIONS = [string]$Cell.WalPartitions
    } else {
        Remove-Item Env:BENCH_LATTICE_WAL_PARTITIONS -ErrorAction SilentlyContinue
    }
    if ($Cell.MaxPending -gt 0) {
        $env:BENCH_LATTICE_WAL_MAX_PENDING_BATCHES = [string]$Cell.MaxPending
    } else {
        Remove-Item Env:BENCH_LATTICE_WAL_MAX_PENDING_BATCHES -ErrorAction SilentlyContinue
    }
    if ($null -ne $Cell.PipelinePhase2) {
        # docker-compose env for the azuretable provider switch is already
        # wired (see benchmark/docker-compose.yml's Lattice__Wal__PipelinePhaseTwo).
        $env:BENCH_WAL_PIPELINE_PHASE_TWO = if ([bool]$Cell.PipelinePhase2) { 'true' } else { 'false' }
    } else {
        Remove-Item Env:BENCH_WAL_PIPELINE_PHASE_TWO -ErrorAction SilentlyContinue
    }

    # Note the run timestamp so we can locate the results.json that this
    # cell's benchmark.ps1 invocation writes. benchmark.ps1 names the run
    # directory with a timestamp prefix so the freshest entry under
    # .run/<scenario>/ after the run completes is this cell's output.
    $beforeRun = Get-Date

    $skipFleet = ($sc -eq 'microbench')
    $bargs = @('-Scenario', $sc, '-NoHistoryPush')
    if ($skipFleet) { $bargs += '-SkipFleetSizeCheck' }

    # Before invoking benchmark.ps1, confirm the protected ports are free.
    # benchmark.ps1's own `docker compose down --remove-orphans` from the
    # *previous* cell may have returned before the kernel actually released
    # the host port; if we kick off the next `compose up` while the bind is
    # still stale, it fails with "port is already allocated" and the cell
    # produces no results.json. The grace window is generous (30s) because
    # the failure mode it prevents is the same one that ate the entire
    # previous matrix run.
    if (-not $skipFleet) {
        $ready = Wait-PortsFree -Ports ($ProtectedPorts.Port) -TimeoutSeconds 30
        if (-not $ready) {
            $stuck = Test-PortListeners -Ports ($ProtectedPorts.Port)
            $names = ($stuck | ForEach-Object { "$($_.Port)/$($_.ContainerName ?? $_.ProcessName ?? 'pid='+$_.OwningProcess)" }) -join ', '
            Write-Warning ("[{0}] protected ports still busy after 30s: {1} - skipping cell" -f $sc, $names)
            return @{ Cell = $Cell; Success = $false; ResultsPath = $null }
        }
    }

    try {
        # Force the child's stdout straight to the host. Without Out-Host,
        # every Write-Host line emitted by benchmark.ps1 (which is its
        # primary progress surface) leaks into the parent pipeline as a
        # plain string and gets folded into this function's return value,
        # blowing up the downstream List<hashtable>.Add(...) call.
        & pwsh -NoProfile -File $benchScript @bargs 2>&1 | Out-Host
        $ok = ($LASTEXITCODE -eq 0)
    } catch {
        Write-Warning ("[{0}] benchmark.ps1 threw: {1}" -f $sc, $_.Exception.Message)
        $ok = $false
    }

    # Locate the per-cell results.json (newest under .run/<scenario>/).
    $resultsPath = $null
    $scenarioRunDir = Join-Path $runDir $sc
    if (Test-Path $scenarioRunDir) {
        $latest = Get-ChildItem -Path $scenarioRunDir -Directory `
            | Where-Object { $_.LastWriteTime -ge $beforeRun } `
            | Sort-Object LastWriteTime -Descending `
            | Select-Object -First 1
        if ($latest) {
            $candidate = Join-Path $latest.FullName 'results.json'
            if (Test-Path $candidate) { $resultsPath = $candidate }

            # Stamp a cell.json sidecar so the cell tuple (scenario +
            # WalPartitions + MaxPending + PipelinePhase2) can be matched
            # back to the run dir later. Without this, -ReportOnly cannot
            # disambiguate cells in a multi-cell sweep because the only
            # other timestamp signal is the run dir name, which is
            # monotonic but knob-agnostic.
            $cellPath = Join-Path $latest.FullName 'cell.json'
            $phase2For = if ($null -eq $cell.PipelinePhase2) { $null } else { [bool]$cell.PipelinePhase2 }
            $cellPayload = [ordered]@{
                cell_index               = $cell.Index
                scenario                 = $sc
                wal_partitions           = $cell.WalPartitions
                wal_max_pending_batches  = $cell.MaxPending
                pipeline_phase_two       = $phase2For
                stamped_utc              = (Get-Date).ToUniversalTime().ToString('o')
            }
            try {
                ($cellPayload | ConvertTo-Json -Depth 3) | Set-Content -Path $cellPath -Encoding utf8
            } catch {
                Write-Warning ("[{0}] failed to write cell.json: {1}" -f $sc, $_.Exception.Message)
            }
        }
    }

    return @{
        Cell        = $Cell
        Success     = $ok
        ResultsPath = $resultsPath
    }
}

$cellOutcomes = New-Object System.Collections.Generic.List[hashtable]
$matrixStarted = Get-Date

# ── Cell <-> run-dir matching ───────────────────────────────────────────────────
#
# Each cell of the matrix produces a single .run/<scenario>/<runId>/ dir.
# A multi-cell sweep produces several run dirs per scenario, so finding "the
# results.json for this cell" requires matching by knob tuple, not by
# timestamp ordering. Two signals are checked, in this order:
#
#   1. cell.json sidecar written by Invoke-MatrixCell. This is the
#      canonical, typed signal.
#   2. results.json's config.BENCH_LATTICE_WAL_* / config.BENCH_WAL_*
#      fields, populated by benchmark.ps1's matrix-driver pass-through.
#
# If both are absent, the matcher returns $null - which is intentional, so
# the report row renders as "(no run dir found for this cell)" rather than
# silently re-using an unrelated run's metrics.
function Find-CellRunDir {
    param(
        [string]   $ScenarioRunDir,
        [hashtable] $Cell
    )
    if (-not (Test-Path $ScenarioRunDir)) { return $null }

    $wantP   = $Cell.WalPartitions
    $wantM   = $Cell.MaxPending
    # PipelinePhase2 is $null for scenarios where the knob does not apply.
    $wantPp  = $null
    if ($null -ne $Cell.PipelinePhase2) { $wantPp = [bool]$Cell.PipelinePhase2 }

    # Walk newest-first so a re-run of the same cell tuple resolves to its
    # most recent execution.
    $candidates = Get-ChildItem -Path $ScenarioRunDir -Directory -ErrorAction SilentlyContinue `
        | Sort-Object LastWriteTime -Descending

    foreach ($dir in $candidates) {
        $cellPath = Join-Path $dir.FullName 'cell.json'
        if (Test-Path $cellPath) {
            try {
                $cj = Get-Content $cellPath -Raw | ConvertFrom-Json -AsHashtable
                $sameP  = ([int]$cj['wal_partitions']          -eq [int]$wantP)
                $sameM  = ([int]$cj['wal_max_pending_batches'] -eq [int]$wantM)
                $samePp = $true
                if ($null -ne $wantPp -or $null -ne $cj['pipeline_phase_two']) {
                    $samePp = ([bool]$cj['pipeline_phase_two'] -eq [bool]$wantPp)
                }
                if ($sameP -and $sameM -and $samePp) {
                    $rp = Join-Path $dir.FullName 'results.json'
                    if (Test-Path $rp) { return $rp }
                }
            } catch { }
            continue
        }
        # Fall back to config.BENCH_LATTICE_* on results.json
        $rp = Join-Path $dir.FullName 'results.json'
        if (-not (Test-Path $rp)) { continue }
        try {
            $j = Get-Content $rp -Raw | ConvertFrom-Json -AsHashtable
            $cfg = $j['config']
            if ($null -eq $cfg) { continue }
            $jp = $cfg['BENCH_LATTICE_WAL_PARTITIONS']
            $jm = $cfg['BENCH_LATTICE_WAL_MAX_PENDING_BATCHES']
            $jpp = $cfg['BENCH_WAL_PIPELINE_PHASE_TWO']
            if ([string]::IsNullOrEmpty($jp) -or [string]::IsNullOrEmpty($jm)) { continue }
            $sameP  = ([int]$jp -eq [int]$wantP)
            $sameM  = ([int]$jm -eq [int]$wantM)
            $samePp = $true
            if ($null -ne $wantPp -or -not [string]::IsNullOrEmpty($jpp)) {
                $samePp = (([string]$jpp -eq 'true') -eq [bool]$wantPp)
            }
            if ($sameP -and $sameM -and $samePp) { return $rp }
        } catch { }
    }
    return $null
}

foreach ($cell in $matrix) {
    if ($cell.Index -lt $ResumeFrom) {
        Write-Host ("[skip] cell {0} (below -ResumeFrom {1})" -f $cell.Index, $ResumeFrom) -ForegroundColor DarkGray
        continue
    }

    if ($ReportOnly) {
        # Report-only mode: do not invoke benchmark.ps1; instead, match
        # this cell to an existing run dir via cell.json / config tags
        # and treat the cell as a successful no-op if a match exists.
        $scenarioRunDir = Join-Path $runDir $cell.Scenario
        if ($cell.Scenario -eq 'microbench') {
            # Microbench has no WAL knobs - take the newest run dir.
            $resultsPath = $null
            if (Test-Path $scenarioRunDir) {
                $latest = Get-ChildItem -Path $scenarioRunDir -Directory -ErrorAction SilentlyContinue `
                    | Sort-Object LastWriteTime -Descending `
                    | Select-Object -First 1
                if ($latest) {
                    $candidate = Join-Path $latest.FullName 'results.json'
                    if (Test-Path $candidate) { $resultsPath = $candidate }
                }
            }
        } else {
            $resultsPath = Find-CellRunDir -ScenarioRunDir $scenarioRunDir -Cell $cell
        }
        if ($null -eq $resultsPath) {
            Write-Warning ("[{0}] no run dir matches cell {1} (P={2} M={3} Pp2={4}) - row will be empty" -f `
                $cell.Scenario, $cell.Index, $cell.WalPartitions, $cell.MaxPending, $cell.PipelinePhase2)
        }
        $cellOutcomes.Add(@{
            Cell        = $cell
            Success     = ($null -ne $resultsPath)
            ResultsPath = $resultsPath
        })
        continue
    }

    # Wrap in @() so any stray output that leaked into the function's
    # pipeline gets captured as an array; the hashtable we actually
    # want is always the last element (PowerShell appends function
    # return values in emission order).
    $emitted = @(Invoke-MatrixCell -Cell $cell)
    $outcome = $null
    for ($i = $emitted.Count - 1; $i -ge 0; $i--) {
        if ($emitted[$i] -is [hashtable]) { $outcome = $emitted[$i]; break }
    }
    if ($null -ne $outcome) {
        $cellOutcomes.Add($outcome)
    }
}

$matrixEnded = Get-Date

# ── Report assembly ─────────────────────────────────────────────────────────────
#
# The report shape matches what scaling.md's Phase A exit condition asks for:
# a single markdown table per scenario whose columns are the matrix axes plus
# the harvested scalars (throughput / p50 / p99 / cpu / azure-server-timing /
# Phase A histogram quantiles).
#
# We read results.json directly (the same artefact the docker pipeline
# already produces); we do not re-query Prometheus here because the
# per-cell scrape has already been folded into the JSON.
function Resolve-Scalar {
    param([hashtable] $Json, [string[]] $Candidates)
    if ($null -eq $Json) { return $null }
    # results.json is the artefact produced by benchmark.ps1 and has the
    # shape { scenario, run_id, git_sha, started, ended, duration_s,
    # config, metrics, fleetStats }. Every numeric scalar we care about
    # lives under .metrics; we keep a root-level lookup as a fallback so
    # this resolver also works against future flattened artefacts.
    $buckets = @()
    if ($Json.ContainsKey('metrics') -and $Json['metrics'] -is [hashtable]) {
        $buckets += ,$Json['metrics']
    }
    $buckets += ,$Json
    foreach ($bucket in $buckets) {
        foreach ($k in $Candidates) {
            if ($bucket.ContainsKey($k) -and $null -ne $bucket[$k]) {
                return $bucket[$k]
            }
        }
    }
    return $null
}

function Format-Scalar {
    param($Value, [int] $Decimals = 1)
    if ($null -eq $Value) { return '-' }
    try {
        $d = [double]$Value
        if ([double]::IsNaN($d) -or [double]::IsInfinity($d)) { return '-' }
        return $d.ToString("N$Decimals", [Globalization.CultureInfo]::InvariantCulture)
    } catch {
        return '-'
    }
}

function Get-CellRow {
    param([hashtable] $Outcome)
    $cell = $Outcome.Cell
    $json = $null
    if ($Outcome.ResultsPath -and (Test-Path $Outcome.ResultsPath)) {
        try {
            $json = Get-Content $Outcome.ResultsPath -Raw | ConvertFrom-Json -AsHashtable
        } catch {
            $json = $null
        }
    }

    # The benchmark JSON is auto-discovered, so metric names follow the
    # OTel -> Prometheus convention (counters end in _per_second, histograms
    # carry _pNN suffixes). The candidate lists below tolerate the canonical
    # spelling, the short-alias spelling, and the microbench-style keys
    # (microbench_*) so a single matrix row can render both load-driven and
    # BDN-only cells.
    #
    # `microbench_point_write_per_second` is BDN's headline call-rate for
    # the canonical "point write" benchmark and is the closest analogue to
    # `lattice_commits_per_second` from the loaded scenarios.
    $ops = Resolve-Scalar $json @(
        'lattice_commits_per_second',
        'orleans_lattice_leaf_commit_duration_milliseconds_per_second',
        'orleans_lattice_leaf_commit_per_second',
        'vehicle_fleet_simulator_sink_published_per_second',
        'vfs_sink_events_processed_per_second',
        'microbench_point_write_per_second',
        'microbench_atomic_write_per_second'
    )
    # Latency: ms for loaded scenarios, ns for microbench BDN. We harvest
    # whichever is present; nanosecond values are converted to milliseconds
    # downstream so a single column reads correctly across both.
    $p50ms = Resolve-Scalar $json @(
        'orleans_lattice_leaf_commit_duration_milliseconds_p50',
        'lattice_commit_p50_ms'
    )
    $p50ns = Resolve-Scalar $json @(
        'microbench_point_write_p50_ns',
        'microbench_atomic_write_p50_ns'
    )
    if ($null -eq $p50ms -and $null -ne $p50ns) { $p50 = [double]$p50ns / 1e6 } else { $p50 = $p50ms }

    $p99ms = Resolve-Scalar $json @(
        'orleans_lattice_leaf_commit_duration_milliseconds_p99',
        'lattice_commit_p99_ms'
    )
    $p99ns = Resolve-Scalar $json @(
        'microbench_point_write_p99_ns',
        'microbench_atomic_write_p99_ns'
    )
    if ($null -eq $p99ms -and $null -ne $p99ns) { $p99 = [double]$p99ns / 1e6 } else { $p99 = $p99ms }

    $cpu = Resolve-Scalar $json @(
        'process_cpu_percent_max',
        'process_cpu_percent_avg'
    )
    $azureSrv = Resolve-Scalar $json @(
        'orleans_lattice_provider_commit_duration_milliseconds_p99',
        'azure_tables_server_timing_sum'
    )

    # Phase A histograms emitted by LatticeMetrics under the
    # orleans_lattice_wal_*, orleans_lattice_provider_*, and
    # orleans_lattice_saga_* prefixes. Auto-discovery names them with the
    # _p99 / _per_second suffix convention.
    $walProv = Resolve-Scalar $json @(
        'orleans_lattice_wal_append_provider_duration_milliseconds_p99'
    )
    $walTurn = Resolve-Scalar $json @(
        'orleans_lattice_wal_append_turn_wait_milliseconds_p99'
    )
    $sagaFan = Resolve-Scalar $json @(
        'orleans_lattice_saga_fanout_size_p99'
    )

    $pp = if ($null -eq $cell.PipelinePhase2) { '-' } else { $cell.PipelinePhase2.ToString().ToLowerInvariant() }

    return [pscustomobject]@{
        Index        = $cell.Index
        Scenario     = $cell.Scenario
        WalPartitions = $cell.WalPartitions
        MaxPending   = $cell.MaxPending
        PipelinePhase2 = $pp
        OpsPerSec    = Format-Scalar $ops 0
        P50Ms        = Format-Scalar $p50 2
        P99Ms        = Format-Scalar $p99 2
        CpuPct       = Format-Scalar $cpu 1
        AzureSrvP99  = Format-Scalar $azureSrv 2
        WalProvP99   = Format-Scalar $walProv 2
        WalTurnP99   = Format-Scalar $walTurn 2
        SagaFanP99   = Format-Scalar $sagaFan 1
        Success      = $Outcome.Success
        ResultsPath  = $Outcome.ResultsPath
    }
}

$rows = $cellOutcomes | ForEach-Object { Get-CellRow $_ }

# Group by scenario so each section reads top-to-bottom as a single sweep.
$reportLines = New-Object System.Collections.Generic.List[string]
$reportLines.Add('# Phase A diagnostic attribution report')
$reportLines.Add('')
$reportLines.Add(('Generated: {0:o}' -f (Get-Date).ToUniversalTime()))
$reportLines.Add(('Matrix wall-clock: {0:N1} min' -f (($matrixEnded - $matrixStarted).TotalMinutes)))
$reportLines.Add(('Cells executed: {0} / {1} (resume-from {2})' -f $rows.Count, $matrix.Count, $ResumeFrom))
$reportLines.Add('')
$reportLines.Add('Source plan: `scaling.md` (Phase A). Reports under `benchmark/diagnostic-reports/` are tracked in git as the per-run evidence trail.')
$reportLines.Add('')
$reportLines.Add('## Legend')
$reportLines.Add('')
$reportLines.Add('| Column | Source metric | Meaning |')
$reportLines.Add('|---|---|---|')
$reportLines.Add('| `OpsPerSec` | `lattice_commits_per_second` (loaded) / `microbench_point_write_per_second` (BDN) | End-to-end throughput at the silo commit point. |')
$reportLines.Add('| `P50Ms` / `P99Ms` | `orleans_lattice_leaf_commit_duration_milliseconds_pNN` (loaded) / `microbench_point_write_pNN_ns` (BDN) | End-to-end commit latency quantiles. Microbench ns values are converted to ms. |')
$reportLines.Add('| `CpuPct` | `process_cpu_percent_(max|avg)` derived from `dotnet_process_cpu_time_seconds_total / dotnet_process_cpu_count` | Silo container CPU% (0-100% of host cores; `max` is the peak 30s slice in the window, `avg` is the window-mean). |')
$reportLines.Add('| `AzureSrvP99` | `orleans_lattice_provider_commit_duration_milliseconds_p99` | Azure Tables provider commit duration p99 (azuretable scenarios only). |')
$reportLines.Add('| `WalProvP99` | `orleans_lattice_wal_append_provider_duration_milliseconds_p99` | WAL grain provider call duration p99. |')
$reportLines.Add('| `WalTurnP99` | `orleans_lattice_wal_append_turn_wait_milliseconds_p99` | WAL grain turn-wait p99 (grain-scheduling backpressure signal). |')
$reportLines.Add('| `SagaFanP99` | `orleans_lattice_saga_fanout_size_p99` | Atomic-write saga fan-out p99 (saga scenarios only). |')
$reportLines.Add('')

$bySc = $rows | Group-Object -Property Scenario
foreach ($g in $bySc) {
    $reportLines.Add(('## {0}' -f $g.Name))
    $reportLines.Add('')
    $reportLines.Add('| # | WalPartitions | MaxPending | PipelinePhase2 | OpsPerSec | P50 (ms) | P99 (ms) | CPU% | AzureSrv P99 | WalProv P99 | WalTurn P99 | SagaFan P99 | OK |')
    $reportLines.Add('|---|---:|---:|:---:|---:|---:|---:|---:|---:|---:|---:|---:|:---:|')
    foreach ($r in ($g.Group | Sort-Object Index)) {
        $okMark = if ($r.Success) { 'yes' } else { 'NO' }
        $reportLines.Add(('| {0} | {1} | {2} | {3} | {4} | {5} | {6} | {7} | {8} | {9} | {10} | {11} | {12} |' -f `
            $r.Index, $r.WalPartitions, $r.MaxPending, $r.PipelinePhase2, `
            $r.OpsPerSec, $r.P50Ms, $r.P99Ms, $r.CpuPct, `
            $r.AzureSrvP99, $r.WalProvP99, $r.WalTurnP99, $r.SagaFanP99, $okMark))
    }
    $reportLines.Add('')
}

# Attribution helper section - lifted from scaling.md so the operator does
# not have to switch documents to interpret the report.
$reportLines.Add('## Attribution heuristics (from scaling.md Phase A)')
$reportLines.Add('')
$reportLines.Add('| Symptom | Primary suspect | Phase that fixes it |')
$reportLines.Add('|---|---|---|')
$reportLines.Add('| Microbench >> current-state-no-replication, low CPU | Orleans grain scheduling / single WalShardGrain activation | Phase B |')
$reportLines.Add('| current-state-no-replication flat as WalMaxPendingBatches rises | Per-partition serialisation | Phase B |')
$reportLines.Add('| current-state-no-replication-azuretable << current-state-no-replication, low AzureSrv P99 | Provider client-side cost (phase-2 sync, payload, retry/backoff) | Phase C |')
$reportLines.Add('| AzureSrv P99 ~= wall time, p99 spikes correlate with ServerBusy | Real partition-server saturation | Phase B + C |')
$reportLines.Add('| atomic-write << current-state at same key rate | Saga-internal serialisation | Phase D |')
$reportLines.Add('')

# Per-cell pointer back to the raw results.json so a follow-up deep-dive
# can re-load the exact scrape that produced the row.
$reportLines.Add('## Raw artefacts')
$reportLines.Add('')
$reportLines.Add('| # | Scenario | Results path |')
$reportLines.Add('|---|---|---|')
foreach ($r in ($rows | Sort-Object Index)) {
    $path = if ([string]::IsNullOrEmpty($r.ResultsPath)) { '(missing)' } else { $r.ResultsPath }
    $reportLines.Add(('| {0} | {1} | `{2}` |' -f $r.Index, $r.Scenario, $path))
}
$reportLines.Add('')

Set-Content -Path $ReportPath -Value ($reportLines -join "`n") -Encoding utf8 -NoNewline

Write-Host ""
Write-Host ("Phase A report written to {0}" -f $ReportPath) -ForegroundColor Green
Write-Host ("Cells executed: {0} / {1}" -f $rows.Count, $matrix.Count) -ForegroundColor Green
Write-Host ""
